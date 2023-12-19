"""
This module creates the Azure Batch tasks for parsing the x_stock timeseries
parquet files stored on the OEDI AWS S3 bucket.

A Batch task is created for each of the parquet files, the processing job
reads in the parquet file, extracts the relevant information and stores the
data in a BICEP database.

"""

from loguru import logger

import boto3
from botocore import UNSIGNED
from botocore.client import Config

from azure.batch import BatchServiceClient
from azure.batch.batch_auth import SharedKeyCredentials
import azure.batch.models as batchmodels

from utils.sensitive_config import (BATCH_ACCOUNT_URL, BATCH_ACCOUNT_NAME,
                                    BATCH_ACCOUNT_KEY,
                                    BICEP_ACR_PASSWORD, BICEP_ACR_USER)

s3_client = boto3.client('s3', config=Config(signature_version=UNSIGNED))
oedi_s3_bucket = 'oedi-data-lake'
xstock_folder_base = 'nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/'

BICEP_ACR = 'bicepcontainers.azurecr.io'
BICEP_IMAGE = f"{BICEP_ACR}/bicep:latest"

# Create a Batch service client. We'll now be interacting with the Batch
# service in addition to Storage
credentials = SharedKeyCredentials(BATCH_ACCOUNT_NAME,
                                   BATCH_ACCOUNT_KEY)

batch_client = BatchServiceClient(credentials,
                                  batch_url=BATCH_ACCOUNT_URL)


def configure_batch(batch_service_client, pool_id, job_id, num_nodes=10):
    """Creates a Batch Pool and an associated job"""
    logger.debug("Creating Pool")
    image_ref_to_use = batchmodels.ImageReference(
        publisher='microsoft-azure-batch',
        offer='ubuntu-server-container',
        sku='20-04-lts',
        version='latest')

    # Specify a container registry
    container_registry = batchmodels.ContainerRegistry(
        registry_server=BICEP_ACR,
        user_name=BICEP_ACR_USER,
        password=BICEP_ACR_PASSWORD)

    # Create container configuration, prefetching Docker images from the container registry
    container_conf = batchmodels.ContainerConfiguration(
        container_image_names=[BICEP_IMAGE],
        container_registries=[container_registry],
        type='dockerCompatible')

    new_pool = batchmodels.PoolAddParameter(
        id=pool_id,
        virtual_machine_configuration=batchmodels.VirtualMachineConfiguration(
            image_reference=image_ref_to_use,
            container_configuration=container_conf,
            node_agent_sku_id='batch.node.ubuntu 20.04'),
        vm_size='standard_d2s_v3',
        target_dedicated_nodes=num_nodes)
    batch_service_client.pool.add(new_pool)

    # create job
    logger.debug("Creating Job")
    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id))
    batch_service_client.job.add(job)


def add_task(presigned_url, task_id, job_id, batch_service_client=batch_client):
    logger.debug(f"Creating Task: {task_id}")
    task_container_settings = batchmodels.TaskContainerSettings(
        image_name=BICEP_IMAGE,
        container_run_options='--workdir /bicep'
    )

    # attempt the task again if there is a non-zero exit code
    task_constraints = batchmodels.TaskConstraints(max_task_retry_count=2)

    task = batchmodels.TaskAddParameter(
        id=task_id,
        command_line=f'/bin/sh -c \"python -m x_stock.x_stock_parsing --url \'{presigned_url}\'\"',
        container_settings=task_container_settings,
        constraints=task_constraints
    )
    batch_service_client.task.add(job_id=job_id, task=task)


def get_all_urls(xstock_release):
    target_folder_prefix = f'{xstock_folder_base}{xstock_release}'
    logger.info("Retrieving list of all objects")

    all_files = []

    # Use pagination to retrieve all objects
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=oedi_s3_bucket,
                                   Prefix=target_folder_prefix):
        for content in page.get('Contents', []):
            all_files.append(content['Key'])

    logger.info('Generating pre-signed URLs')

    presigned_files = []

    for file in all_files:
        presigned_url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": oedi_s3_bucket, "Key": file},
            ExpiresIn=60 * 60 * 24,  # Set expiry time in seconds (8 hour)
        )
        presigned_files.append(presigned_url)

    return presigned_files


if __name__ == '__main__':
    # comstock_0 = '2023/comstock_amy2018_release_2/timeseries_individual_buildings/by_state/upgrade=0/state=CA/'
    # resstock_0 = '2022/resstock_amy2018_release_1.1/timeseries_individual_buildings/by_state/upgrade=0/state=CA/'
    resstock_6 = '2022/resstock_amy2018_release_1.1/timeseries_individual_buildings/by_state/upgrade=6/state=CA/'

    target_files = get_all_urls(resstock_6)

    job = 'resStock23_upgrade_6'
    configure_batch(batch_client,
                    pool_id='resStock23',
                    job_id=job,
                    num_nodes=150)

    tasks = [f"task-{file.split('/')[-1].split('.')[0]}" for file in target_files]
    input_list = zip(target_files, tasks, [job] * len(target_files))

    # adds tasks in parallel
    import concurrent.futures
    with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
        executor.map(lambda arg: add_task(*arg), input_list)

    # for file in target_files:
    #     parquet_num = file.split('/')[-1].split('.')[0]
    #     task_id = f'task-{parquet_num}'
    #     add_task(file, task_id=task_id, job_id=job_id)
