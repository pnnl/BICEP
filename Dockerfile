FROM python:3.11 AS bicep-base
COPY requirements.txt /bicep/requirements.txt
RUN pip install -r /bicep/requirements.txt

FROM bicep-base
COPY . /bicep
WORKDIR /bicep

# Install BICEP as a package
RUN pip install .


### deployment commands
# docker build -t bicep .
### build for linux image if on apple silicon
# docker buildx build --platform linux/amd64 -t bicep .
# az login
# az acr login --name bicepContainers
# docker tag bicep bicepcontainers.azurecr.io/bicep:latest
# docker push bicepcontainers.azurecr.io/bicep

# docker run -it -p 80:8000 --name bicep bicep:latest
