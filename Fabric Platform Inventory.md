# ==============================================================================
# MICROSOFT FABRIC PLATFORM INVENTORY & CATALOG
# ==============================================================================
# Description:
#   This script scans a Microsoft Fabric tenant to inventory all items (Workspaces, 
#   Lakehouses, Warehouses, Reports, etc.), maps lineage, audits security (Users/Roles), 
#   and parses deep metadata (TMSL) for Semantic Models.
#
# Prerequisites:
#   - Microsoft Fabric Capacity
#   - 'sempy' library installed (pip install semantic-link)
#   - Spark Session active (Run in Fabric Notebook)
# ==============================================================================
```python
def extract_text_between_brackets(header):
    import re
    match = re.search(r'\[(.*?)\]', header)
    return match.group(1) if match else header
```

```python
import sempy.fabric as fabric
import pandas as pd
spark.conf.set("spark.sql.caseSensitive", "true")
import time
```

```python


import sempy.fabric as fabric
import inspect

# List only functions
functions = [f for f in dir(fabric) if inspect.isfunction(getattr(fabric, f))]
print("Functions:", functions)

# List only classes
classes = [c for c in dir(fabric) if inspect.isclass(getattr(fabric, c))]
print("Classes:", classes)


```

```python


import sempy.fabric as fabric

# List all attributes (modules, classes, functions, constants)
print(dir(fabric))



```
# Fabric All Items load a table
```python

# Welcome to your new notebook
# Type here in the cell editor to add code!
#!pip install --upgrade semantic-link --q #upgrade to semantic-link v0.5

df = pd.concat([fabric.list_items(workspace=ws) for ws in fabric.list_workspaces().query('`Is On Dedicated Capacity` == True').Id], ignore_index=True)
df
def clean_data(df):
    # Rename column 'Display Name' to 'DisplayName'
    df = df.rename(columns={'Display Name': 'DisplayName'})
    # Rename column 'Workspace Id' to 'WorkspaceId'
    df = df.rename(columns={'Workspace Id': 'WorkspaceId'})
    df = df.rename(columns={'Folder Id': 'FolderId'})
    return df

df_clean = clean_data(df.copy())
df2 = spark.createDataFrame(df_clean)
df2.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("FABRIC_ALLITEMS")

```
# Get all Semantic Models
```python

import sempy.fabric as fabric
import pandas as pd

# Get all workspaces
workspaces_df = fabric.list_workspaces()

# Filter workspaces on dedicated capacity
dedicated_ws_ids = workspaces_df.query("`Is On Dedicated Capacity` == True")["Id"]

# Collect datasets and include workspace ID
dataset_dfs = []
for ws_id in dedicated_ws_ids:
    df = fabric.list_datasets(workspace=ws_id, mode='rest')
    df["WorkspaceId"] = ws_id  # Add workspace ID to each dataset row
    dataset_dfs.append(df)

# Combine all datasets
df_dax = pd.concat(dataset_dfs, ignore_index=True)
df_dax = df_dax.astype(str)

# Clean and prepare the DataFrame
df_dax.columns = df_dax.columns.str.replace(" ", "", regex=False)
#df_dax = df_dax.drop(columns=['UpstreamDatasets', 'Users','QueryScaleOutSettings'])

# Convert to Spark DataFrame and save
df2 = spark.createDataFrame(df_dax)
df2.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("LIST_DATASETS")
```
# Get all PowerBI Reports
```python

import sempy.fabric as fabric
import pandas as pd
from concurrent.futures import ThreadPoolExecutor

workspaces_df = fabric.list_workspaces()
dedicated_ws_ids = workspaces_df.query("`Is On Dedicated Capacity` == True")["Id"].tolist()

def get_reports_safe(ws_id):
    try:
        df = fabric.list_reports(workspace=ws_id)
        if not df.empty:
            df["WorkspaceId"] = ws_id
            return df
    except:
        return None # Return None on error
    return None # Return None if empty

# Run in parallel
report_dfs = []
with ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(get_reports_safe, dedicated_ws_ids)
    # Filter out None values (errors or empty workspaces)
    report_dfs = [r for r in results if r is not None]

if report_dfs:
    df_reports = pd.concat(report_dfs, ignore_index=True)
    df_reports = df_reports.astype(str)
    df_reports.columns = df_reports.columns.str.replace(" ", "", regex=False)
    
    spark.createDataFrame(df_reports).write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("LIST_REPORTS")
    print("Table LIST_REPORTS updated.")

```
# Get all Dataflows
```python

import sempy.fabric as fabric
import pandas as pd

# Get all workspaces
workspaces_df = fabric.list_workspaces()

# Filter workspaces on dedicated capacity
dedicated_ws_ids = workspaces_df.query("`Is On Dedicated Capacity` == True")["Id"]

# Collect datasets and include workspace ID
dataset_dfs = []
for ws_id in dedicated_ws_ids:
    df = fabric.list_dataflows(workspace=ws_id)
    df["WorkspaceId"] = ws_id  # Add workspace ID to each dataset row
    dataset_dfs.append(df)

# Combine all datasets
df_dax = pd.concat(dataset_dfs, ignore_index=True)

# Clean and prepare the DataFrame
df_dax.columns = df_dax.columns.str.replace(" ", "", regex=False)
#df_dax = df_dax.drop(columns=['UpstreamDatasets', 'Users','QueryScaleOutSettings'])

# Convert to Spark DataFrame and save
df2 = spark.createDataFrame(df_dax)
df2.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("LIST_DATAFLOWS")

```
# Get folders in workspace
```python

import sempy.fabric as fabric
import pandas as pd

# Get all workspaces
workspaces_df = fabric.list_workspaces()

# Filter workspaces that are on dedicated capacity
dedicated_ws_ids = workspaces_df.query("`Is On Dedicated Capacity` == True")["Id"]

# Collect folders from each workspace
folder_dfs = []
for ws_id in dedicated_ws_ids:
    folders = fabric.list_folders(workspace=ws_id)
    folder_dfs.append(folders)

# Concatenate all folder dataframes
df = pd.concat(folder_dfs, ignore_index=True)

# Display the result
print(df)

def clean_data(df):
    # Rename column 'Display Name' to 'DisplayName'
    df = df.rename(columns={'Display Name': 'DisplayName'})
    # Rename column 'Workspace Id' to 'WorkspaceId'
    df = df.rename(columns={'Workspace Id': 'WorkspaceId'})
    df = df.rename(columns={'Parent Folder Id': 'ParentFolderId'})
    return df

df_clean = clean_data(df.copy())
df2 = spark.createDataFrame(df_clean)
df2.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("FABRIC_WSPFOLDERS")

```
# Get Workspace Details
```python
import sempy
import sempy.fabric as fabric
#workspaceName = '' #Enter the workspace name to be used as a filter
df_dax = fabric.list_workspaces()

df_dax.columns = [extract_text_between_brackets(col) for col in df_dax.columns]
from datetime import datetime

# Get the current date and time
current_datetime = datetime.now()

# Add a new column to df_dax with the current date and time
df_dax['Update_DateTime'] = current_datetime

# Display the updated dataframe with the new column
df_dax.columns =df_dax.columns.str.replace("(", "", regex=False)
df_dax.columns =df_dax.columns.str.replace(")", "", regex=False)
df_dax.columns =df_dax.columns.str.replace(" ", "", regex=False)
df_dax.columns =df_dax.columns.str.replace("\n", "", regex=False)
df_dax
print(df_dax)
df_dax = spark.createDataFrame(df_dax)
df_dax.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("WORKSPACE_DETAILS")

```
# Capacity Details
```python

df_dax=fabric.list_capacities()
# Display the updated dataframe with the new column
df_dax.columns =df_dax.columns.str.replace("(", "", regex=False)
df_dax.columns =df_dax.columns.str.replace(")", "", regex=False)
df_dax.columns =df_dax.columns.str.replace(" ", "", regex=False)
df_dax.columns =df_dax.columns.str.replace("\n", "", regex=False)
df_dax
#print(df_dax)
df_dax = spark.createDataFrame(df_dax)
df_dax.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("CAPACITY_DETAILS")

```
# List Apps
```python

import sempy
import sempy.fabric as fabric
df_dax = fabric.list_apps()
# Display the updated dataframe with the new column
df_dax.columns =df_dax.columns.str.replace("(", "", regex=False)
df_dax.columns =df_dax.columns.str.replace(")", "", regex=False)
df_dax.columns =df_dax.columns.str.replace(" ", "", regex=False)
df_dax.columns =df_dax.columns.str.replace("\n", "", regex=False)
df_dax
#print(df_dax)
df_dax = spark.createDataFrame(df_dax)
df_dax.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("APP_DETAILS")
```
# Gateway Details
```python

import sempy.fabric as fabric
df_dax = fabric.list_gateways()
df_dax.columns =df_dax.columns.str.replace("(", "", regex=False)
df_dax.columns =df_dax.columns.str.replace(")", "", regex=False)
df_dax.columns =df_dax.columns.str.replace(" ", "", regex=False)
df_dax.columns =df_dax.columns.str.replace("\n", "", regex=False)
df_dax
#print(df_dax)
df_dax = spark.createDataFrame(df_dax)
df_dax.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("GATEWAY_DETAILS")

```
# Lakehouse Details
```python
import sempy
import sempy.fabric as fabric
import pandas as pd
import json

def get_all_lakehouse_details():
    header = pd.DataFrame(columns=['LakehouseName', 'LakehouseID', 'WorkspaceName', 'WorkspaceID', 'OneLakeTablesPath', 'OneLakeFilesPath', 'SQLEndpointConnectionString', 'SQLEndpointID', 'SQLEndpointProvisioningStatus'])
    df = pd.DataFrame(header)
    
    client = fabric.FabricRestClient()
    
    # Get all workspaces
    workspaces_response = client.get("/v1/workspaces")
    workspaces = workspaces_response.json()['value']
    
    # Iterate through all workspaces
    for workspace in workspaces:
        workspaceID = workspace['id']
        workspaceName = workspace['displayName']
        
        try:
            # Get all items in the workspace
            items_response = client.get(f"/v1/workspaces/{workspaceID}/items")
            items = items_response.json()['value']
            
            # Filter for Lakehouses
            lakehouses = [item for item in items if item['type'] == 'Lakehouse']
            
            # Process each Lakehouse
            for lakehouse in lakehouses:
                lakehouseID = lakehouse['id']
                
                try:
                    # Get Lakehouse details
                    response = client.get(f"/v1/workspaces/{workspaceID}/lakehouses/{lakehouseID}")
                    responseJson = response.json()
                    lakehouseName = responseJson['displayName']
                    prop = responseJson['properties']
                    oneLakeTP = prop['oneLakeTablesPath']
                    oneLakeFP = prop['oneLakeFilesPath']
                    sqlEPCS = prop['sqlEndpointProperties']['connectionString']
                    sqlepid = prop['sqlEndpointProperties']['id']
                    sqlepstatus = prop['sqlEndpointProperties']['provisioningStatus']
                    
                    new_data = {
                        'LakehouseName': lakehouseName, 
                        'LakehouseID': lakehouseID, 
                        'WorkspaceName': workspaceName, 
                        'WorkspaceID': workspaceID, 
                        'OneLakeTablesPath': oneLakeTP, 
                        'OneLakeFilesPath': oneLakeFP, 
                        'SQLEndpointConnectionString': sqlEPCS, 
                        'SQLEndpointID': sqlepid, 
                        'SQLEndpointProvisioningStatus': sqlepstatus
                    }
                    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    
                except Exception as e:
                    print(f"Error processing Lakehouse {lakehouseID} in workspace {workspaceName}: {str(e)}")
                    
        except Exception as e:
            print(f"Error accessing workspace {workspaceName}: {str(e)}")
    
    # Convert to Spark DataFrame
    df_LH = spark.createDataFrame(df)
    return df_LH

# Call the function
df_all_lakehouses = get_all_lakehouse_details()
df_all_lakehouses.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("LAKEHOUSE_DETAILS")
#display(df_all_lakehouses)

```
# Warehouse details
```python

import sempy
import sempy.fabric as fabric
import pandas as pd
import json

def get_all_warehouse_details():
    header = pd.DataFrame(columns=['WarehouseName', 'WarehouseID', 'WorkspaceName', 'WorkspaceID', 'ConnectionString', 'CreatedDate', 'lastUpdatedTime'])
    df = pd.DataFrame(header)
    
    client = fabric.FabricRestClient()
    
    # Get all workspaces
    workspaces_response = client.get("/v1/workspaces")
    workspaces = workspaces_response.json()['value']
    
    # Iterate through all workspaces
    for workspace in workspaces:
        workspaceID = workspace['id']
        workspaceName = workspace['displayName']
        
        try:
            # Get all items in the workspace
            items_response = client.get(f"/v1/workspaces/{workspaceID}/items")
            items = items_response.json()['value']
            
            # Filter for Warehouses
            warehouses = [item for item in items if item['type'] == 'Warehouse']
            
            # Process each Warehouse
            for warehouse in warehouses:
                warehouseID = warehouse['id']
                warehouseName = warehouse['displayName']
                
                try:
                    # Get Warehouse details
                    response = client.get(f"/v1/workspaces/{workspaceID}/warehouses/{warehouseID}")
                    responseJson = response.json()
                    
                    warehouseName = responseJson['displayName']
                                        
                    # Get connection string from properties
                    prop = responseJson.get('properties', {})
                    connectionString = prop.get('connectionString', 'N/A')
                    createdDate = prop.get('createdDate', 'N/A')
                    lastUpdatedTime = prop.get('lastUpdatedTime', 'N/A')
                    
                    new_data = {
                        'WarehouseName': warehouseName, 
                        'WarehouseID': warehouseID, 
                        'WorkspaceName': workspaceName, 
                        'WorkspaceID': workspaceID, 
                        'ConnectionString': connectionString,
                        'CreatedDate': createdDate,
                        'lastUpdatedTime': lastUpdatedTime
                    }
                    df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    
                except Exception as e:
                    print(f"Error processing Warehouse {warehouseID} in workspace {workspaceName}: {str(e)}")
                    
        except Exception as e:
            print(f"Error accessing workspace {workspaceName}: {str(e)}")
    
    # Convert to Spark DataFrame
    df_WH = spark.createDataFrame(df)
    return df_WH

# Call the function
df_all_warehouses = get_all_warehouse_details()
df_all_warehouses.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("WAREHOUSE_DETAILS")
# display(df_all_warehouses)
```
# Dataset - Data source Details

```python

import sempy
import sempy.fabric as fabric
import pandas as pd
import json

def get_all_dataset_details():
    header = pd.DataFrame(columns=[
        'DatasetName', 'DatasetID', 'WorkspaceName', 'WorkspaceID', 
        'isRefreshable', 'ConfiguredBy', 'isOnPremGatewayRequired', 
        'targetStorageMode', 'DatasourceId', 'DatasourceType', 
        'GatewayId', 'ConnectionDetails'
    ])
    df = pd.DataFrame(header)
    
    client = fabric.FabricRestClient()
    pbiclient = fabric.PowerBIRestClient()
    
    # Get all workspaces
    workspaces_response = client.get("/v1/workspaces")
    workspaces = workspaces_response.json()['value']
    
    # Filter workspaces on dedicated capacity
    workspaces = [
        ws for ws in workspaces 
        if ws.get('capacityId') and ws['capacityId'] != '00000000-0000-0000-0000-000000000000'
    ]
    
    # Iterate through all workspaces
    for workspace in workspaces:
        workspaceID = workspace['id']
        workspaceName = workspace['displayName']
        
        try:
            # Get all items in the workspace
            items_response = client.get(f"/v1/workspaces/{workspaceID}/items")
            items = items_response.json()['value']
            
            # Filter for Datasets (SemanticModel)
            datasets = [item for item in items if item['type'] == 'SemanticModel']
            
            # Process each Dataset
            for dataset in datasets:
                datasetID = dataset['id']
                datasetName = dataset['displayName']
                
                try:
                    # Get Dataset details
                    response = pbiclient.get(f"/v1.0/myorg/groups/{workspaceID}/datasets/{datasetID}")
                    responseJson = response.json()
                    
                    datasetName = responseJson['name']
                    isRefreshable = responseJson.get('isRefreshable', 'N/A')
                    isOnPremGatewayRequired = responseJson.get('isOnPremGatewayRequired', 'N/A')
                    configuredBy = responseJson.get('configuredBy', 'N/A')
                    targetStorageMode = responseJson.get('targetStorageMode', 'N/A')
                    
                    # Get datasources/connections
                    try:
                        datasources_response = pbiclient.get(f"/v1.0/myorg/groups/{workspaceID}/datasets/{datasetID}/datasources")
                        datasources = datasources_response.json().get('value', [])
                        
                        # If datasources exist, create one row per datasource
                        if datasources:
                            for ds in datasources:
                                datasource_type = ds.get('datasourceType', 'N/A')
                                datasource_id = ds.get('datasourceId', 'N/A')
                                gateway_id = ds.get('gatewayId', 'N/A')
                                connection_details = json.dumps(ds.get('connectionDetails', {}))
                                
                                new_data = {
                                    'DatasetName': datasetName, 
                                    'DatasetID': datasetID, 
                                    'WorkspaceName': workspaceName, 
                                    'WorkspaceID': workspaceID,
                                    'ConfiguredBy': configuredBy,
                                    'targetStorageMode': targetStorageMode,
                                    'isRefreshable': isRefreshable,
                                    'isOnPremGatewayRequired': isOnPremGatewayRequired,
                                    'DatasourceId': datasource_id,
                                    'DatasourceType': datasource_type,
                                    'GatewayId': gateway_id,
                                    'ConnectionDetails': connection_details
                                }
                                df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                        else:
                            # No datasources found - create one row with N/A values
                            new_data = {
                                'DatasetName': datasetName, 
                                'DatasetID': datasetID, 
                                'WorkspaceName': workspaceName, 
                                'WorkspaceID': workspaceID,
                                'ConfiguredBy': configuredBy,
                                'targetStorageMode': targetStorageMode,
                                'isRefreshable': isRefreshable,
                                'isOnPremGatewayRequired': isOnPremGatewayRequired,
                                'DatasourceId': 'N/A',
                                'DatasourceType': 'N/A',
                                'GatewayId': 'N/A',
                                'ConnectionDetails': 'N/A'
                            }
                            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                            
                    except Exception as conn_error:
                        # Error getting datasources - create one row with error
                        new_data = {
                            'DatasetName': datasetName, 
                            'DatasetID': datasetID, 
                            'WorkspaceName': workspaceName, 
                            'WorkspaceID': workspaceID,
                            'ConfiguredBy': configuredBy,
                            'targetStorageMode': targetStorageMode,
                            'isRefreshable': isRefreshable,
                            'isOnPremGatewayRequired': isOnPremGatewayRequired,
                            'DatasourceId': 'Error',
                            'DatasourceType': 'Error',
                            'GatewayId': 'Error',
                            'ConnectionDetails': str(conn_error)
                        }
                        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    
                except Exception as e:
                    print(f"Error processing Dataset {datasetID} in workspace {workspaceName}: {str(e)}")
                    
        except Exception as e:
            print(f"Error accessing workspace {workspaceName}: {str(e)}")
    
    # Convert to Spark DataFrame
    df_DS = spark.createDataFrame(df)
    return df_DS

# Call the function
df_all_datasets = get_all_dataset_details()
df_all_datasets.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("DATASET_DATASOURCES")
#display(df_all_datasets)

```
# Dataflow - Data source Details

```python

import sempy
import sempy.fabric as fabric
import pandas as pd
import json

def get_all_dataset_details():
    header = pd.DataFrame(columns=[
        'DataflowName', 'DataflowID', 'WorkspaceName', 'WorkspaceID', 
        'ConfiguredBy', 'generation', 
         'DatasourceId', 'DatasourceType', 
        'GatewayId', 'ConnectionDetails'
    ])
    df = pd.DataFrame(header)
    
    client = fabric.FabricRestClient()
    pbiclient = fabric.PowerBIRestClient()
    
    # Get all workspaces
    workspaces_response = client.get("/v1/workspaces")
    # workspaces_response = pbiclient.get("/v1.0/myorg/groups")
    workspaces = workspaces_response.json()['value']
    
    # Filter workspaces on dedicated capacity
    workspaces = [
        ws for ws in workspaces 
        if ws.get('capacityId') and ws['capacityId'] != '00000000-0000-0000-0000-000000000000'
    ]
    
    # Iterate through all workspaces
    for workspace in workspaces:
        workspaceID = workspace['id']
        workspaceName = workspace['displayName']
        
        try:
            # Get all items in the workspace
            # items_response = client.get(f"/v1/workspaces/{workspaceID}/items")
            items_response = pbiclient.get(f"/v1.0/myorg/groups/{workspaceID}/dataflows")
            items = items_response.json()['value']
            
            # Filter for Datasets (SemanticModel)
            dataflows = items
            # dataflows = [item for item in items if item['type'] == 'Dataflow']
            
            # Process each Dataset
            for dataflow in dataflows:
                dataflowID = dataflow['objectId']
                dataflowName = dataflow['name']
                configuredBy = dataflow['configuredBy']
                generation = dataflow['generation']
                try:
                    # Get Dataset details
                    response = pbiclient.get(f"/v1.0/myorg/groups/{workspaceID}/dataflows/{dataflowID}")
                    responseJson = response.json()
                    
                    dataflowName = responseJson['name']
                    #isRefreshable = responseJson.get('isRefreshable', 'N/A')
                    #isOnPremGatewayRequired = responseJson.get('isOnPremGatewayRequired', 'N/A')
                    #configuredBy = responseJson.get('configuredBy', 'N/A')
                    #targetStorageMode = responseJson.get('targetStorageMode', 'N/A')
                    
                    # Get datasources/connections
                    try:
                        datasources_response = pbiclient.get(f"/v1.0/myorg/groups/{workspaceID}/dataflows/{dataflowID}/datasources")
                        datasources = datasources_response.json().get('value', [])
                        
                        # If datasources exist, create one row per datasource
                        if datasources:
                            for ds in datasources:
                                datasource_type = ds.get('datasourceType', 'N/A')
                                datasource_id = ds.get('datasourceId', 'N/A')
                                gateway_id = ds.get('gatewayId', 'N/A')
                                connection_details = json.dumps(ds.get('connectionDetails', {}))
                                
                                new_data = {
                                    'DataflowName': dataflowName, 
                                    'DataflowID': dataflowID, 
                                    'WorkspaceName': workspaceName, 
                                    'WorkspaceID': workspaceID,
                                    'ConfiguredBy': configuredBy,
                                    'generation': generation,
                                    'DatasourceId': datasource_id,
                                    'DatasourceType': datasource_type,
                                    'GatewayId': gateway_id,
                                    'ConnectionDetails': connection_details
                                }
                                df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                        else:
                            # No datasources found - create one row with N/A values
                            new_data = {
                                'DataflowName': dataflowName, 
                                'DataflowID': dataflowID, 
                                'WorkspaceName': workspaceName, 
                                'WorkspaceID': workspaceID,
                                'ConfiguredBy': configuredBy,
                                'generation': generation,
                                'DatasourceId': 'N/A',
                                'DatasourceType': 'N/A',
                                'GatewayId': 'N/A',
                                'ConnectionDetails': 'N/A'
                            }
                            df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                            
                    except Exception as conn_error:
                        # Error getting datasources - create one row with error
                        new_data = {
                            'DataflowName': dataflowName, 
                            'DataflowID': dataflowID, 
                            'WorkspaceName': workspaceName, 
                            'WorkspaceID': workspaceID,
                            'ConfiguredBy': configuredBy,
                            'generation': generation,
                            'DatasourceId': 'Error',
                            'DatasourceType': 'Error',
                            'GatewayId': 'Error',
                            'ConnectionDetails': str(conn_error)
                        }
                        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    
                except Exception as e:
                    print(f"Error processing Dataset {dataflowID} in workspace {workspaceName}: {str(e)}")
                    
        except Exception as e:
            print(f"Error accessing workspace {workspaceName}: {str(e)}")
    
    # Convert to Spark DataFrame
    df_DS = spark.createDataFrame(df)
    return df_DS

# Call the function
df_all_datasets = get_all_dataset_details()
df_all_datasets.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("DATAFLOW_DATASOURCES")
#display(df_all_datasets)

```
# Get All DataFlows Gen1, 2 CICD

```python

import sempy
import sempy.fabric as fabric
import pandas as pd
import json

def get_all_dataflow_details():
    header = pd.DataFrame(columns=[
        'DataflowName', 'DataflowID', 'WorkspaceName', 'WorkspaceID', 
        'ConfiguredBy', 'generation', 
         'Type', 'Call','isParametric'
    ])
    df = pd.DataFrame(header)
    
    client = fabric.FabricRestClient()
    pbiclient = fabric.PowerBIRestClient()
    
    # Get all workspaces
    workspaces_response = client.get("/v1/workspaces")
    # workspaces_response = pbiclient.get("/v1.0/myorg/groups")
    workspaces = workspaces_response.json()['value']
    
    # Filter workspaces on dedicated capacity
    workspaces = [
        ws for ws in workspaces 
        if ws.get('capacityId') and ws['capacityId'] != '00000000-0000-0000-0000-000000000000'
    ]
    
    # Iterate through all workspaces
    for workspace in workspaces:
        workspaceID = workspace['id']
        workspaceName = workspace['displayName']
        
        try:
            # Get all items in the workspace
            # items_response = client.get(f"/v1/workspaces/{workspaceID}/items")
            items_response = pbiclient.get(f"/v1.0/myorg/groups/{workspaceID}/dataflows")
            items = items_response.json()['value']
            
            # Filter for Datasets (SemanticModel)
            dataflows = items
            # dataflows = [item for item in items if item['type'] == 'Dataflow']
            
            # Process each Dataset
            for dataflow in dataflows:
                dataflowID = dataflow['objectId']
                dataflowName = dataflow['name']
                configuredBy = dataflow['configuredBy']
                generation = dataflow['generation']
                try:
                    # Error getting datasources - create one row with error
                        new_data = {
                            'DataflowName': dataflowName, 
                            'DataflowID': dataflowID, 
                            'WorkspaceName': workspaceName, 
                            'WorkspaceID': workspaceID,
                            'ConfiguredBy': configuredBy,
                            'generation': generation,
                            'Type': 'Dataflow',
                            'Call':'PBIRESTAPI'
                            }
                        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    
                except Exception as e:
                    print(f"Error processing Dataset {dataflowID} in workspace {workspaceName}: {str(e)}")
                    
        except Exception as e:
            print(f"Error accessing workspace {workspaceName}: {str(e)}")
    
    # Convert to Spark DataFrame
    df_DS = spark.createDataFrame(df)
    return df_DS

# Call the function
df_all_dataflows = get_all_dataflow_details()
df_all_dataflows.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("DATAFLOWS_ALL")
# display(df_all_dataflows)

```
# Get All DataFlows Gen2

```python

import sempy
import sempy.fabric as fabric
import pandas as pd
import json

def get_all_dataset_details():
    header = pd.DataFrame(columns=[
        'DataflowName', 'DataflowID', 'WorkspaceName', 'WorkspaceID', 
        'ConfiguredBy', 'generation', 
         'Type', 'Call','isParametric'
    ])
    df = pd.DataFrame(header)
    
    client = fabric.FabricRestClient()
    pbiclient = fabric.PowerBIRestClient()
    
    # Get all workspaces
    workspaces_response = client.get("/v1/workspaces")
    # workspaces_response = pbiclient.get("/v1.0/myorg/groups")
    workspaces = workspaces_response.json()['value']
    
    # Filter workspaces on dedicated capacity
    workspaces = [
        ws for ws in workspaces 
        if ws.get('capacityId') and ws['capacityId'] != '00000000-0000-0000-0000-000000000000'
    ]
    
    # Iterate through all workspaces
    for workspace in workspaces:
        workspaceID = workspace['id']
        workspaceName = workspace['displayName']
        
        try:
            # Get all items in the workspace
            items_response = client.get(f"/v1/workspaces/{workspaceID}/dataflows")
            #items_response = pbiclient.get(f"/v1.0/myorg/groups/{workspaceID}/dataflows")
            items = items_response.json()['value']
            
            # Filter for Datasets (SemanticModel)
            dataflows = items
            # dataflows = [item for item in items if item['type'] == 'Dataflow']
            
            # Process each Dataset
            for dataflow in dataflows:
                dataflowID = dataflow['id']
                dataflowName = dataflow['displayName']
                #configuredBy = dataflow['configuredBy']
                Type = dataflow['type']
                prop = dataflow['properties']
                isParametric = prop['isParametric']
                try:
                    # Error getting datasources - create one row with error
                        new_data = {
                            'DataflowName': dataflowName, 
                            'DataflowID': dataflowID, 
                            'WorkspaceName': workspaceName, 
                            'WorkspaceID': workspaceID,
                            'ConfiguredBy': '',
                            'generation': 2,
                            'Type': 'Dataflow',
                            'Call':'FABRESTAPI',
                            'isParametric':isParametric
                            }
                        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    
                except Exception as e:
                    print(f"Error processing Dataset {dataflowID} in workspace {workspaceName}: {str(e)}")
                    
        except Exception as e:
            print(f"Error accessing workspace {workspaceName}: {str(e)}")
    
    # Convert to Spark DataFrame
    df_DS = spark.createDataFrame(df)
    return df_DS

# Call the function
df_all_datasets = get_all_dataset_details()
df_all_datasets.write.mode("append").format("delta").option("mergeSchema", "true").saveAsTable("DATAFLOWS_ALL")
# display(df_all_datasets)

```
# Get Workspace users

```python
import sempy
import sempy.fabric as fabric
import pandas as pd
import json

def get_all_dataflow_details():
    header = pd.DataFrame(columns=[
        'userdisplayName', 'emailAddress', 'WorkspaceName', 'WorkspaceID', 
        'groupUserAccessRight', 'identifier', 
         'principalType'
    ])
    df = pd.DataFrame(header)
    
    client = fabric.FabricRestClient()
    pbiclient = fabric.PowerBIRestClient()
    
    # Get all workspaces
    workspaces_response = client.get("/v1/workspaces")
    # workspaces_response = pbiclient.get("/v1.0/myorg/groups")
    workspaces = workspaces_response.json()['value']
    
    # Filter workspaces on dedicated capacity
    workspaces = [
        ws for ws in workspaces 
        if ws.get('capacityId') and ws['capacityId'] != '00000000-0000-0000-0000-000000000000'
    ]
    
    # Iterate through all workspaces
    for workspace in workspaces:
        workspaceID = workspace['id']
        workspaceName = workspace['displayName']
        
        try:
            # Get all items in the workspace
            # items_response = client.get(f"/v1/workspaces/{workspaceID}/items")
            items_response = pbiclient.get(f"/v1.0/myorg/groups/{workspaceID}/users")
            items = items_response.json()['value']
            
            # Filter for Datasets (SemanticModel)
            dataflows = items
            # dataflows = [item for item in items if item['type'] == 'Dataflow']
            
            # Process each Dataset
            for dataflow in dataflows:
                userdisplayName = dataflow['displayName']
                emailAddress = dataflow.get('emailAddress',"N/A")
                #datasource_id = ds.get('datasourceId', 'N/A')
                groupUserAccessRight = dataflow['groupUserAccessRight']
                identifier = dataflow['identifier']
                principalType=dataflow['principalType']
                try:
                    # Error getting datasources - create one row with error
                        new_data = {
                            'userdisplayName': userdisplayName, 
                            'emailAddress': emailAddress, 
                            'WorkspaceName': workspaceName, 
                            'WorkspaceID': workspaceID,
                            'groupUserAccessRight': groupUserAccessRight,
                            'identifier': identifier,
                            'principalType': principalType
                            }
                        df = pd.concat([df, pd.DataFrame(new_data, index=[0])], ignore_index=True)
                    
                except Exception as e:
                    print(f"Error processing Dataset {dataflowID} in workspace {workspaceName}: {str(e)}")
                    
        except Exception as e:
            print(f"Error accessing workspace {workspaceName}: {str(e)}")
    
    # Convert to Spark DataFrame
    df_DS = spark.createDataFrame(df)
    return df_DS

# Call the function
df_all_dataflows = get_all_dataflow_details()
df_all_dataflows.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("WORKSPACE_USERS")

```
# Dataset Users with rights

```python
import sempy.fabric as fabric
import pandas as pd
import time
import random
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.types import StructType, StructField, StringType

# ==========================================
# STEP 0: Define Schema
# ==========================================
schema = StructType([
    StructField("DatasetName", StringType(), True),
    StructField("DatasetID", StringType(), True),
    StructField("WorkspaceName", StringType(), True),
    StructField("WorkspaceID", StringType(), True),
    StructField("configuredBy", StringType(), True),
    StructField("identifier", StringType(), True),
    StructField("principalType", StringType(), True),
    StructField("datasetUserAccessRight", StringType(), True)
])

# ==========================================
# STEP 1: Clear / Initialize Table
# ==========================================
table_name = "DATASET_USERS"
# NOTE: If you are restarting to finish the last 5, COMMENT THIS OUT so you don't lose the first 60!
# If starting from scratch, keep it.
print(f"🧹 Clearing table '{table_name}' to start fresh...")
spark.createDataFrame([], schema).write.mode("overwrite").format("delta").saveAsTable(table_name)
print(f"✅ Table '{table_name}' is cleared and ready.")

# ==========================================
# STEP 2: Fetch Logic (With Timeout)
# ==========================================
def fetch_users_with_retry(workspaceID, workspaceName, dataset, max_retries=5):
    datasetID = dataset['id']
    datasetName = dataset['name']
    configuredBy = dataset.get('configuredBy', 'N/A')
    url = f"/v1.0/myorg/groups/{workspaceID}/datasets/{datasetID}/users"
    results = []
    
    for attempt in range(max_retries):
        try:
            pbiclient = fabric.PowerBIRestClient()
            # 30 second timeout prevents hanging threads
            response = pbiclient.get(url, timeout=60)
            
            if response.status_code == 200:
                users = response.json().get('value', [])
                if users:
                    for user in users:
                        results.append({
                            'DatasetName': datasetName, 'DatasetID': datasetID,
                            'WorkspaceName': workspaceName, 'WorkspaceID': workspaceID,
                            'configuredBy': configuredBy,
                            'identifier': user.get('identifier', 'N/A'),
                            'principalType': user.get('principalType', 'N/A'),
                            'datasetUserAccessRight': user.get('datasetUserAccessRight', 'N/A')
                        })
                else:
                    results.append({
                        'DatasetName': datasetName, 'DatasetID': datasetID,
                        'WorkspaceName': workspaceName, 'WorkspaceID': workspaceID,
                        'configuredBy': configuredBy,
                        'identifier': 'N/A', 'principalType': 'N/A', 'datasetUserAccessRight': 'N/A'
                    })
                return results 

            elif response.status_code == 429:
                wait_time = (attempt + 1) * 3 + random.uniform(0, 1)
                print(f"⚠️ 429 Too Many Requests on '{datasetName}'. Waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue 

            elif response.status_code == 401:
                print(f"🔄 Token Expired on '{datasetName}'. Retrying...")
                time.sleep(2)
                continue 

            else:
                print(f"❌ Error {response.status_code} on {datasetName}")
                break 

        except Exception as e:
            print(f"❌ Exception on {datasetName} (Attempt {attempt+1}): {e}")
            time.sleep(2)
            continue
            
    return results

def get_datasets_safe(workspace_id):
    for attempt in range(3):
        try:
            client = fabric.PowerBIRestClient()
            resp = client.get(f"/v1.0/myorg/groups/{workspace_id}/datasets", timeout=30)
            if resp.status_code == 200:
                return [d for d in resp.json()['value'] if d['name'] != 'SemanticModel']
            elif resp.status_code == 401:
                time.sleep(2)
                continue
            elif resp.status_code == 429:
                time.sleep(5)
                continue
            else:
                return []
        except:
            return []
    return []

# ==========================================
# STEP 3: Main Loop (Aggressive Cleanup)
# ==========================================
def process_workspaces_incrementally():
    client = fabric.FabricRestClient()
    ws_resp = client.get("/v1/workspaces")
    
    # Filter
    workspaces = [
        ws for ws in ws_resp.json()['value'] 
        if ws.get('capacityId') and ws['capacityId'] != '00000000-0000-0000-0000-000000000000' 
        and not ws['displayName'].startswith('GISC')
    ]
    
    # Sort
    workspaces = sorted(workspaces, key=lambda x: x['displayName'])
    total_ws = len(workspaces)
    
    print(f"🚀 Starting processing for {total_ws} workspaces...")

    for i, ws in enumerate(workspaces):
        ws_id = ws['id']
        ws_name = ws['displayName']
        workspace_results = []
        
        # SKIP LOGIC: If you are restarting and want to skip the first 60, uncomment below:
        # if i < 60: continue 
        
        try:
            datasets = get_datasets_safe(ws_id)
            
            if datasets:
                # Reduced max_workers to 5 to be gentler on the driver near end of run
                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = {executor.submit(fetch_users_with_retry, ws_id, ws_name, ds): ds for ds in datasets}
                    for future in as_completed(futures):
                        data = future.result()
                        if data:
                            workspace_results.extend(data)
                
                if workspace_results:
                    df_ws = pd.DataFrame(workspace_results)
                    spark.createDataFrame(df_ws, schema=schema).write.mode("append").format("delta").saveAsTable(table_name)
                    print(f"   [{i+1}/{total_ws}] 💾 Saved {len(workspace_results)} rows from '{ws_name}'")
            else:
                print(f"   [{i+1}/{total_ws}] No datasets in '{ws_name}'")

        except Exception as e:
            print(f"   ❌ Critical error processing workspace {ws_name}: {e}")

        # --- AGGRESSIVE CLEANUP (Per Workspace) ---
        # 1. Force Python to release memory immediately
        gc.collect()
        # 2. Clear Spark internal cache (optional, but safe)
        # spark.catalog.clearCache() 
        
        # --- BATCH PAUSE (Keep this for API limits) ---
        if (i + 1) % 10 == 0 and (i + 1) < total_ws:
            print(f"\n⏸️  Processed 10 workspaces. Pausing 3 minutes for API cooldown...\n")
            time.sleep(60) 

# Run
process_workspaces_incrementally()
print("🎉 Full extraction complete.")

```
#Dataset Users and rights
```python
# In[2]:


import sempy.fabric as fabric
import pandas as pd
import time
import random
import gc
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.types import StructType, StructField, StringType

# ==========================================
# STEP 0: Define Schema
# ==========================================
schema = StructType([
    StructField("DatasetName", StringType(), True),
    StructField("DatasetID", StringType(), True),
    StructField("WorkspaceName", StringType(), True),
    StructField("WorkspaceID", StringType(), True),
    StructField("configuredBy", StringType(), True),
    StructField("identifier", StringType(), True),
    StructField("principalType", StringType(), True),
    StructField("datasetUserAccessRight", StringType(), True)
])

# ==========================================
# STEP 1: Clear / Initialize Table
# ==========================================
table_name = "DATASET_USERS"
# NOTE: If you are restarting to finish the last 5, COMMENT THIS OUT so you don't lose the first 60!
# If starting from scratch, keep it.
print(f"🧹 Clearing table '{table_name}' to start fresh...")
spark.createDataFrame([], schema).write.mode("overwrite").format("delta").saveAsTable(table_name)
print(f"✅ Table '{table_name}' is cleared and ready.")

# ==========================================
# STEP 2: Fetch Logic (With Timeout)
# ==========================================
def fetch_users_with_retry(workspaceID, workspaceName, dataset, max_retries=5):
    datasetID = dataset['id']
    datasetName = dataset['name']
    configuredBy = dataset.get('configuredBy', 'N/A')
    url = f"/v1.0/myorg/groups/{workspaceID}/datasets/{datasetID}/users"
    results = []
    
    for attempt in range(max_retries):
        try:
            pbiclient = fabric.PowerBIRestClient()
            # 30 second timeout prevents hanging threads
            response = pbiclient.get(url, timeout=60)
            
            if response.status_code == 200:
                users = response.json().get('value', [])
                if users:
                    for user in users:
                        results.append({
                            'DatasetName': datasetName, 'DatasetID': datasetID,
                            'WorkspaceName': workspaceName, 'WorkspaceID': workspaceID,
                            'configuredBy': configuredBy,
                            'identifier': user.get('identifier', 'N/A'),
                            'principalType': user.get('principalType', 'N/A'),
                            'datasetUserAccessRight': user.get('datasetUserAccessRight', 'N/A')
                        })
                else:
                    results.append({
                        'DatasetName': datasetName, 'DatasetID': datasetID,
                        'WorkspaceName': workspaceName, 'WorkspaceID': workspaceID,
                        'configuredBy': configuredBy,
                        'identifier': 'N/A', 'principalType': 'N/A', 'datasetUserAccessRight': 'N/A'
                    })
                return results 

            elif response.status_code == 429:
                wait_time = (attempt + 1) * 3 + random.uniform(0, 1)
                print(f"⚠️ 429 Too Many Requests on '{datasetName}'. Waiting {wait_time:.1f}s...")
                time.sleep(wait_time)
                continue 

            elif response.status_code == 401:
                print(f"🔄 Token Expired on '{datasetName}'. Retrying...")
                time.sleep(2)
                continue 

            else:
                print(f"❌ Error {response.status_code} on {datasetName}")
                break 

        except Exception as e:
            print(f"❌ Exception on {datasetName} (Attempt {attempt+1}): {e}")
            time.sleep(2)
            continue
            
    return results

def get_datasets_safe(workspace_id):
    for attempt in range(3):
        try:
            client = fabric.PowerBIRestClient()
            resp = client.get(f"/v1.0/myorg/groups/{workspace_id}/datasets", timeout=30)
            if resp.status_code == 200:
                return [d for d in resp.json()['value'] if d['name'] != 'SemanticModel']
            elif resp.status_code == 401:
                time.sleep(2)
                continue
            elif resp.status_code == 429:
                time.sleep(5)
                continue
            else:
                return []
        except:
            return []
    return []

# ==========================================
# STEP 3: Main Loop (Aggressive Cleanup)
# ==========================================
def process_workspaces_incrementally():
    client = fabric.FabricRestClient()
    ws_resp = client.get("/v1/workspaces")
    
    # Filter
    workspaces = [
        ws for ws in ws_resp.json()['value'] 
        if ws.get('capacityId') and ws['capacityId'] != '00000000-0000-0000-0000-000000000000' 
        and not ws['displayName'].startswith('GISC')
    ]
    
    # Sort
    workspaces = sorted(workspaces, key=lambda x: x['displayName'])
    total_ws = len(workspaces)
    
    print(f"🚀 Starting processing for {total_ws} workspaces...")

    for i, ws in enumerate(workspaces):
        ws_id = ws['id']
        ws_name = ws['displayName']
        workspace_results = []
        
        # SKIP LOGIC: If you are restarting and want to skip the first 60, uncomment below:
        # if i < 60: continue 
        
        try:
            datasets = get_datasets_safe(ws_id)
            
            if datasets:
                # Reduced max_workers to 5 to be gentler on the driver near end of run
                with ThreadPoolExecutor(max_workers=5) as executor:
                    futures = {executor.submit(fetch_users_with_retry, ws_id, ws_name, ds): ds for ds in datasets}
                    for future in as_completed(futures):
                        data = future.result()
                        if data:
                            workspace_results.extend(data)
                
                if workspace_results:
                    df_ws = pd.DataFrame(workspace_results)
                    spark.createDataFrame(df_ws, schema=schema).write.mode("append").format("delta").saveAsTable(table_name)
                    print(f"   [{i+1}/{total_ws}] 💾 Saved {len(workspace_results)} rows from '{ws_name}'")
            else:
                print(f"   [{i+1}/{total_ws}] No datasets in '{ws_name}'")

        except Exception as e:
            print(f"   ❌ Critical error processing workspace {ws_name}: {e}")

        # --- AGGRESSIVE CLEANUP (Per Workspace) ---
        # 1. Force Python to release memory immediately
        gc.collect()
        # 2. Clear Spark internal cache (optional, but safe)
        # spark.catalog.clearCache() 
        
        # --- BATCH PAUSE (Keep this for API limits) ---
        if (i + 1) % 10 == 0 and (i + 1) < total_ws:
            print(f"\n⏸️  Processed 10 workspaces. Pausing 3 minutes for API cooldown...\n")
            time.sleep(60) 

# Run
process_workspaces_incrementally()
print("🎉 Full extraction complete.")

```
# Get Dataset M Query and Columns and Measures

```python

import sempy.fabric as fabric
import pandas as pd
import json
import time
import os
import gc
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from pyspark.sql.types import StructType, StructField, StringType
from notebookutils import mssparkutils
# ==========================================
# 0. CONFIGURATION & UTILS
# ==========================================
TMSL_SAVE_PATH = "/lakehouse/default/Files/PowerBI_TMSLFILES"

# Ensure target directory exists
try:
    mssparkutils.fs.mkdirs(TMSL_SAVE_PATH)
    print(f"✅ Directory verified/created: {TMSL_SAVE_PATH}")
except Exception as e:
    print(f"⚠️ Warning: Could not verify directory {TMSL_SAVE_PATH}. Error: {e}")

def sanitize_filename(name):
    """Removes illegal characters for filenames."""
    return re.sub(r'[\\/*?:"<>|]', "", name).strip()


# ==========================================
# 1. SETUP & SCHEMA DEFINITIONS
# ==========================================

# Added "DOC_EXPRESSIONS" for items where Enable Load = False
TABLES = {
    "measures": "DOC_MEASURES",
    "m_queries": "DOC_M_QUERIES",      # ENABLE LOAD = TRUE
    "expressions": "DOC_EXPRESSIONS",  # ENABLE LOAD = FALSE
    "columns": "DOC_COLUMNS",
    "relationships": "DOC_RELATIONSHIPS",
    "roles": "DOC_ROLES",
    "hierarchies": "DOC_HIERARCHIES"
}

# Clear tables (Drop to reset schema)
print("🧹 Clearing output tables...")
# spark.sql(f"DROP TABLE IF EXISTS {t_name}")
for t_name in TABLES.values():
    spark.sql(f"TRUNCATE TABLE {t_name}")

# ==========================================
# 2. WORKER FUNCTION
# ==========================================
def parse_dataset_tmsl(row):
    ws_id = row['WorkspaceId']
    ds_id = row['DatasetId'] 
    ds_name = row['DatasetName'] 
    
    data = {k: [] for k in TABLES.keys()}
    
    try:
        tmsl_str = fabric.get_tmsl(dataset=ds_id, workspace=ws_id)

        # --- NEW: SAVE TO ONELAKE ---
        clean_name = sanitize_filename(ds_name)
        # Use ds_id to ensure uniqueness in case names duplicate
        file_path = f"{TMSL_SAVE_PATH}/{clean_name}_{ds_id}.json" 
        
        # Write file using standard Python I/O (Works for OneLake /lakehouse/default paths)
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(tmsl_str)
        # -----------------------------

        tmsl = json.loads(tmsl_str)
        model = tmsl.get('model', {})
        
        # --- A. LOADED TABLES (Enable Load = TRUE) ---
        if 'tables' in model:
            for table in model['tables']:
                t_name = table.get('name')
                
                # 1. M Queries (Partitions)
                if 'partitions' in table:
                    for p in table['partitions']:
                        src = p.get('source', {})
                        if src.get('type') == 'm':
                            raw_expr = src.get('expression')
                            m_code = "\n".join(raw_expr) if isinstance(raw_expr, list) else str(raw_expr)
                            data['m_queries'].append({
                                "WorkspaceId": ws_id, "DatasetName": ds_name, "DatasetId": ds_id,
                                "TableName": t_name, "ObjectName": p.get('name'), 
                                "Type": "Table (Loaded)",  # Explicitly stating this is loaded
                                "EnableLoad": "True",      # <--- KEY FLAG
                                "Expression": m_code
                            })

                # 2. Measures
                if 'measures' in table:
                    for m in table['measures']:
                        data['measures'].append({
                            "WorkspaceId": ws_id, "DatasetName": ds_name, "DatasetId": ds_id,
                            "TableName": t_name, "ObjectName": m.get('name'), "Type": "Measure",
                            "Expression": m.get('expression')
                        })

                # 3. Columns
                if 'columns' in table:
                    for c in table['columns']:
                        data['columns'].append({
                            "WorkspaceId": ws_id, "DatasetName": ds_name, "DatasetId": ds_id,
                            "TableName": t_name, "ColumnName": c.get('name'),
                            "DataType": c.get('dataType', 'String'), "Type": c.get('type', 'Data'),
                            "Expression": c.get('expression', ''), "Hidden": str(c.get('isHidden', False))
                        })

                # 4. Hierarchies
                if 'hierarchies' in table:
                    for h in table['hierarchies']:
                        levels = [l['name'] for l in h.get('levels', [])]
                        data['hierarchies'].append({
                            "WorkspaceId": ws_id, "DatasetName": ds_name, "DatasetId": ds_id,
                            "TableName": t_name, "HierarchyName": h.get('name'),
                            "Levels": " > ".join(levels)
                        })

        # --- B. UNLOADED EXPRESSIONS (Enable Load = FALSE) ---
        # These are Staging Queries, Parameters, or Functions
        if 'expressions' in model:
            for expr in model['expressions']:
                raw_expr = expr.get('expression')
                m_code = "\n".join(raw_expr) if isinstance(raw_expr, list) else str(raw_expr)
                
                data['expressions'].append({
                    "WorkspaceId": ws_id, "DatasetName": ds_name, "DatasetId": ds_id,
                    "ExpressionName": expr.get('name'),
                    "Kind": expr.get('kind', 'm'),   # 'm' means Query/Param
                    "EnableLoad": "False",           # <--- KEY FLAG
                    "Expression": m_code,
                    "Description": expr.get('description', '')
                })

        # --- C. MODEL LEVEL (Relationships, Roles) ---
        if 'relationships' in model:
            for r in model['relationships']:
                data['relationships'].append({
                    "WorkspaceId": ws_id, "DatasetName": ds_name, "DatasetId": ds_id,
                    "FromTable": r.get('fromTable'), "FromColumn": r.get('fromColumn'),
                    "ToTable": r.get('toTable'), "ToColumn": r.get('toColumn'),
                    "Cardinality": r.get('cardinality', 'One'), "IsActive": str(r.get('isActive', True))
                })

        if 'roles' in model:
            for r in model['roles']:
                role_name = r.get('name')
                perm = r.get('modelPermission', 'Read')
                if 'tablePermissions' in r:
                    for tp in r['tablePermissions']:
                        data['roles'].append({
                            "WorkspaceId": ws_id, "DatasetName": ds_name, "DatasetId": ds_id,
                            "RoleName": role_name, "Permission": perm,
                            "SecuredTable": tp.get('name'), "DAXFilter": tp.get('filterExpression', '')
                        })
                else:
                     data['roles'].append({
                        "WorkspaceId": ws_id, "DatasetName": ds_name, "DatasetId": ds_id,
                        "RoleName": role_name, "Permission": perm, "SecuredTable": "All", "DAXFilter": ""
                    })
                    
        return data

    except Exception as e:
        print(f"⚠️ Error parsing {ds_name}: {e}")
        return {k: [] for k in TABLES.keys()}

# ==========================================
# 3. MAIN ORCHESTRATOR
# ==========================================
def process_all_datasets_tmsl():
    df_input = spark.table("LIST_DATASETS").select("WorkspaceId", "DatasetId", "DatasetName").distinct().collect()
    total_ds = len(df_input)
    print(f"🚀 Starting TMSL extraction for {total_ds} datasets...")
    
    batch_size = 20
    
    for i in range(0, total_ds, batch_size):
        batch = df_input[i:i + batch_size]
        print(f"   Processing batch {i} to {i + len(batch)}...")
        
        batch_results = {k: [] for k in TABLES.keys()}
        
        with ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(parse_dataset_tmsl, row) for row in batch]
            
            for future in as_completed(futures):
                result = future.result()
                for key in TABLES.keys():
                    batch_results[key].extend(result[key])

        print(f"   💾 Saving batch data...")
        for key, table_name in TABLES.items():
            if batch_results[key]:
                df = pd.DataFrame(batch_results[key]).fillna("")
                spark.createDataFrame(df).write.mode("append").option("mergeSchema", "true").saveAsTable(table_name)
        
        gc.collect()
        spark.catalog.clearCache()
        time.sleep(5)

# Run
process_all_datasets_tmsl()
print("🎉 Extraction complete. Check DOC_M_QUERIES (Loaded) and DOC_EXPRESSIONS (Not Loaded).")

```
# # Storage Details (Lakehose, Warehouse)
# Contains both Used and Unused filed and storage in MB

```python

import sempy.fabric as fabric
from notebookutils import mssparkutils
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
from pyspark.sql.utils import AnalysisException
from datetime import datetime

# --- CONFIGURATION ---
TARGET_TABLE_NAME = "StorageInventory"

# Add the exact names of workspaces you want to skip here
EXCLUDED_WORKSPACES = [
    "Dev Sandbox",
    "HR Sensitive Data"
]

# 1. Helper: Physical Size & Count (Recursive Scan)
def get_folder_stats(path):
    total_bytes = 0
    total_files = 0
    stack = [path]
    loop_count = 0
    MAX_LOOPS = 100000 
    
    try:
        while stack:
            current_path = stack.pop()
            loop_count += 1
            if loop_count > MAX_LOOPS: return total_bytes, total_files, "Limit Reached"
            
            try:
                items = mssparkutils.fs.ls(current_path)
            except Exception:
                continue

            for item in items:
                if item.isDir:
                    stack.append(item.path)
                else:
                    total_bytes += item.size
                    total_files += 1
                    
        return total_bytes, total_files, None
    except Exception as e:
        return 0, 0, str(e)

# 2. Helper: Active Size & Count (Delta Log)
def get_active_stats(path):
    try:
        df = spark.sql(f"DESCRIBE DETAIL delta.`{path}`")
        row = df.select("sizeInBytes", "numFiles").collect()
        if row:
            return (float(row[0][0]) if row[0][0] else 0.0, 
                    int(row[0][1]) if row[0][1] else 0)
        return 0.0, 0
    except AnalysisException:
        return 0.0, 0 
    except Exception:
        return 0.0, 0

def scan_tenant_structure():
    run_id = datetime.now().strftime("%Y%m%d%H%M")
    all_results = []

    print("--- Step 1: Discovering Items via SemPy ---")
    df_workspaces = fabric.list_workspaces()
    print(f"Found {len(df_workspaces)} Workspaces.")

    for index, ws in df_workspaces.iterrows():
        ws_name = ws['Name']
        ws_id = ws['Id']
        
        # --- EXCLUSION FILTER ---
        if ws_name in EXCLUDED_WORKSPACES:
            print(f"Skipping excluded workspace: {ws_name}")
            continue
        
        try:
            df_items = fabric.list_items(workspace=ws_id)
            df_items = df_items[df_items['Type'].isin(['Lakehouse', 'Warehouse'])]
            
            if df_items.empty: continue
                
            print(f"\nScanning Workspace: {ws_name}")

            for idx, item in df_items.iterrows():
                item_name = item['Display Name']
                item_id = item['Id']
                item_type = item['Type']
                
                base_path = f"abfss://{ws_id}@onelake.dfs.fabric.microsoft.com/{item_id}/Tables"
                
                try:
                    try:
                        root_items = mssparkutils.fs.ls(base_path)
                    except:
                        continue 

                    tables_to_process = []
                    
                    for root_item in root_items:
                        if not root_item.isDir: continue
                        
                        try:
                            sub_items = mssparkutils.fs.ls(root_item.path)
                            if any(x.name == "_delta_log" for x in sub_items):
                                tables_to_process.append({
                                    "Schema": "dbo", "Table": root_item.name, "Path": root_item.path
                                })
                            else:
                                schema_name = root_item.name
                                for sub_item in sub_items:
                                    if sub_item.isDir:
                                        tables_to_process.append({
                                            "Schema": schema_name, "Table": sub_item.name, "Path": sub_item.path
                                        })
                        except:
                            continue

                    for t in tables_to_process:
                        full_name = f"{t['Schema']}.{t['Table']}"
                        print(f"  > {item_name} -> {full_name}...", end=" ")
                        
                        active_bytes, active_files = get_active_stats(t['Path'])
                        active_mb = active_bytes / (1024 * 1024)
                        
                        phys_bytes, phys_files, error = get_folder_stats(t['Path'])
                        phys_mb = phys_bytes / (1024 * 1024)
                        
                        status = "Success"
                        if error: 
                            status = f"Error: {error}"
                            print(f"[FAIL]")
                        else:
                            print(f"[Files: {phys_files} | Phys: {phys_mb:.1f} MB]")
                        
                        all_results.append({
                            "RunID": run_id,
                            "ScanTimestamp": datetime.now(),
                            "WorkspaceName": ws_name,
                            "ItemName": item_name,
                            "ItemType": item_type,
                            "SchemaName": t['Schema'],
                            "TableName": t['Table'],
                            "ActiveSizeMB": float(f"{active_mb:.2f}"),
                            "ActiveFileCount": active_files,
                            "PhysicalSizeMB": float(f"{phys_mb:.2f}"),
                            "PhysicalFileCount": phys_files,
                            "Status": status,
                            "FullPath": t['Path']
                        })

                except Exception as e:
                    print(f"  Error accessing {item_name}: {e}")
                    
        except Exception as e:
            pass

    if all_results:
        print(f"\nSaving {len(all_results)} records to {TARGET_TABLE_NAME}...")
        
        schema = StructType([
            StructField("RunID", StringType(), True),
            StructField("ScanTimestamp", TimestampType(), True),
            StructField("WorkspaceName", StringType(), True),
            StructField("ItemName", StringType(), True),
            StructField("ItemType", StringType(), True),
            StructField("SchemaName", StringType(), True),
            StructField("TableName", StringType(), True),
            StructField("ActiveSizeMB", DoubleType(), True),
            StructField("ActiveFileCount", LongType(), True),
            StructField("PhysicalSizeMB", DoubleType(), True),
            StructField("PhysicalFileCount", LongType(), True),
            StructField("Status", StringType(), True),
            StructField("FullPath", StringType(), True)
        ])
        
        df = spark.createDataFrame(all_results, schema)
        df.write.format("delta").mode("append").option("mergeSchema", "true").saveAsTable(TARGET_TABLE_NAME)
        print("Report saved successfully.")
    else:
        print("No accessible items found.")
scan_tenant_structure()
```


