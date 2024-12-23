# Databricks notebook source
import mlflow
import mlflow.deployments
from mlflow import MlflowClient
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import WorkspaceObjectAccessControlRequest, WorkspaceObjectPermissionLevel
from databricks.sdk.service.serving import ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel
from databricks.sdk.service.serving import DataframeSplitInput
import requests
import json
import re

mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

def get_latest_model_version(model_name_in:str = None):
    """
    Get latest version of registered model
    """
    client = MlflowClient()
    model_version_infos = client.search_model_versions("name = '%s'" % model_name_in)

    if model_version_infos:
      return max([int(model_version_info.version) for model_version_info in model_version_infos])
    else:
      return None

# COMMAND ----------

class Tag:
    def __init__(self, key, value):
        self.key = key
        self.value = value

    def as_dict(self):
        return {'key': self.key, 'value': self.value}
    
def deploy_model_serving_endpoint(endpoint_name, endpoint_config):
    
    # Initiate the workspace client
    w = WorkspaceClient()
    
    # Get endpoint if it exists
    existing_endpoint = next(
        (e for e in w.serving_endpoints.list() if e.name == endpoint_name), None
    )

    # If endpoint doesn't exist, create it
    if existing_endpoint is None:

        # Exemple on how to tag the endpoint
        tags = [Tag("team", "data science")]
        print(f"Creating the endpoint {endpoint_name}, this will take a few minutes to package and deploy the endpoint...")
        w.serving_endpoints.create_and_wait(name=endpoint_name, config=endpoint_config, tags = tags)

        # Exemple on how to set up permissions to the endpoint
        print(f"Setting up permissions to the endpoint {endpoint_name}...")
        serving_endpoint_id = w.serving_endpoints.get(endpoint_name).id
        access_control_list=[
            ServingEndpointAccessControlRequest(
                group_name="users",
                permission_level=ServingEndpointPermissionLevel.CAN_MANAGE
                )]
        w.serving_endpoints.set_permissions(serving_endpoint_id=serving_endpoint_id, access_control_list=access_control_list)

    # If endpoint does exist, update it to serve the new version
    else:
        print(f"Updating the endpoint {endpoint_name}, this will take a few minutes to package and deploy the endpoint...")
        w.serving_endpoints.update_config_and_wait(served_entities=endpoint_config.served_entities, name=endpoint_name)

# COMMAND ----------

def create_tf_serving_json(data):
    return {'inputs': {name: data[name].tolist() for name in data.keys()} if isinstance(data, dict) else data.tolist()}

def score_model(endpoint_name, dataset):

    w = WorkspaceClient()

    # Score model using the model serving endpoint
    dataframe_split=DataframeSplitInput.from_dict(dataset.to_dict(orient='split'))
    response = w.serving_endpoints.query(name = endpoint_name, dataframe_split = dataframe_split)

    return response.predictions
