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

aws_region = 'us-west-2'
s3_client = boto3.client('s3',
                         region_name=aws_region,
                         config=Config(signature_version=UNSIGNED))
oedi_s3_bucket = 'oedi-data-lake'
xstock_folder_base = 'nrel-pds-building-stock/end-use-load-profiles-for-us-building-stock/'

BICEP_ACR = 'bicepcontainers.azurecr.io'
BICEP_IMAGE = f"{BICEP_ACR}/bicep:all-states"

# Create a Batch service client. We'll now be interacting with the Batch
# service in addition to Storage
credentials = SharedKeyCredentials(BATCH_ACCOUNT_NAME,
                                   BATCH_ACCOUNT_KEY)

batch_client = BatchServiceClient(credentials,
                                  batch_url=BATCH_ACCOUNT_URL)


def configure_batch(batch_service_client, pool_id, job_id, num_nodes=10,
                    create_new_pool=True):
    """Creates a Batch Pool and an associated job"""
    if create_new_pool:
        logger.debug("Creating Pool")
        image_ref_to_use = batchmodels.ImageReference(
            publisher='microsoft-dsvm',
            offer='ubuntu-hpc',
            sku='2204',
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
                node_agent_sku_id='batch.node.ubuntu 22.04'),
            vm_size='STANDARD_D2S_V3',
            target_dedicated_nodes=num_nodes)
        batch_service_client.pool.add(new_pool)

    # create job
    logger.debug("Creating Job")
    job = batchmodels.JobAddParameter(
        id=job_id,
        pool_info=batchmodels.PoolInformation(pool_id=pool_id))
    batch_service_client.job.add(job)


def add_task(presigned_url, state, task_id, job_id, batch_service_client=batch_client):
    logger.debug(f"Creating Task: {task_id}")
    task_container_settings = batchmodels.TaskContainerSettings(
        image_name=BICEP_IMAGE,
    )

    # attempt the task again if there is a non-zero exit code
    task_constraints = batchmodels.TaskConstraints(max_task_retry_count=2)

    task = batchmodels.TaskAddParameter(
        id=task_id,
        command_line=f'python -m x_stock.x_stock_parsing --url "{presigned_url}" --state "{state}"',
        container_settings=task_container_settings,
        constraints=task_constraints
    )
    batch_service_client.task.add(job_id=job_id, task=task)
    logger.debug(f"Task added: {task_id}")


def get_state_directories(xstock_release_base):
    """
    Get a list of all state directories for a given xstock release base path.

    Args:
        xstock_release_base:
        Base path like '2023/comstock_amy2018_release_2/timeseries_individual_buildings/by_state/upgrade=3/'

    Returns:
        List of state codes (e.g., ['CA', 'TX', 'NY', ...])
    """
    target_folder_prefix = f'{xstock_folder_base}{xstock_release_base}'
    logger.info(f"Retrieving list of state directories from: {target_folder_prefix}")

    logger.info(f'bucket: {oedi_s3_bucket}')

    # Use list_objects_v2 with Delimiter to get "folders"
    response = s3_client.list_objects_v2(
        Bucket=oedi_s3_bucket,
        Prefix=target_folder_prefix,
        Delimiter='/'
    )

    state_codes = []
    # CommonPrefixes contains the "folder" paths
    for prefix_info in response.get('CommonPrefixes', []):
        prefix = prefix_info['Prefix']
        # Extract state code from path like "...upgrade=3/state=CA/"
        if 'state=' in prefix:
            state_code = prefix.split('state=')[-1].rstrip('/')
            state_codes.append(state_code)

    logger.info(f"Found {len(state_codes)} states: {state_codes}")
    return state_codes


def get_all_urls_for_state(xstock_release, state_code):
    """Get all URLs for a specific state"""
    target_folder_prefix = f'{xstock_folder_base}{xstock_release}state={state_code}/'
    logger.info(f"Retrieving files for state: {state_code}")

    all_files = []
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=oedi_s3_bucket,
                                   Prefix=target_folder_prefix):
        for content in page.get('Contents', []):
            all_files.append(content['Key'])

    logger.info(f'Generating pre-signed URLs for {len(all_files)} files in {state_code}')
    presigned_files = []
    for file in all_files:
        presigned_url = s3_client.generate_presigned_url(
            ClientMethod="get_object",
            Params={"Bucket": oedi_s3_bucket, "Key": file},
            ExpiresIn=60 * 60 * 24,  # 24 hours
        )
        presigned_files.append(presigned_url)

    return presigned_files


if __name__ == '__main__':

    job_name = 'all-states-ComStock-0'

    # uncomment to create new azure batch pool of vms
    # configure_batch(batch_client,
    #                 pool_id='all-states-parsing-pool',
    #                 job_id=job_name,
    #                 num_nodes=150,
    #                 create_new_pool=True)

    comstock_base = '2023/comstock_amy2018_release_2/timeseries_individual_buildings/by_state'
    restock_base = '2022/resstock_amy2018_release_1.1/timeseries_individual_buildings/by_state'

    comstock_upgrades = [0, 3]
    restock_upgrades = [0, 4, 6]

    for upgrade in restock_upgrades[1:]:
        stock_run = f'{restock_base}/upgrade={upgrade}/'

        # Get all available states
        available_states = get_state_directories(stock_run)

        for current_state in available_states:

            # skip california since already calculate
            if current_state == 'CA':
                continue

            target_files = get_all_urls_for_state(stock_run, current_state)

            state_input = [current_state] * len(target_files)
            tasks = [f"task-{file.split('/')[-1].split('.')[0]}-res" for file in target_files]

            input_list = zip(target_files, state_input, tasks, [job_name] * len(target_files))

            # adds tasks in parallel
            import concurrent.futures

            with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
                executor.map(lambda arg: add_task(*arg), input_list)

    # sequential execution - THIS IS MUCH SLOWER
    # for file in target_files:
    #     parquet_num = file.split('/')[-1].split('.')[0]
    #     task_id = f'task-{parquet_num}'
    #     add_task(file, task_id=task_id, job_id=job_id)
