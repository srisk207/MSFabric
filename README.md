# Microsoft Fabric Platform Inventory & Catalog

## Overview
This project contains a comprehensive **Microsoft Fabric Notebook** designed to automate the inventory and cataloging of an entire Microsoft Fabric tenant. 

Using the `sempy` (Semantic Link) library and Fabric REST APIs, this notebook scans your environment to fetch metadata, lineage, security settings, and storage statistics. It consolidates this information into **Delta Tables** stored in your Lakehouse, serving as a centralized "Metadata Lake" for governance, auditing, and capacity management.

## Key Features

* **Full Tenant Inventory:** Captures details for Workspaces, Capacities, Gateways, and Apps.
* **Item-Level Granularity:** Catalogs Lakehouses, Warehouses, Dataflows (Gen1/Gen2), Datasets (Semantic Models), and Reports.
* **Security & Access Auditing:** Detailed extraction of Workspace roles and Dataset-level user permissions.
* **Deep Metadata Extraction (TMSL):** Parses Semantic Models to extract M queries, DAX measures, calculated columns, and relationships.
* **Storage Forensics:** Analyzes physical vs. active storage usage for Delta tables to identify "vacuum" candidates and storage waste.
* **Data Lineage:** Maps data sources for Dataflows and Datasets.

## Prerequisites

1.  **Microsoft Fabric Capacity:** The workspace running this notebook must be on a Fabric capacity.
2.  **Semantic Link:** The notebook relies on `sempy`. (Included in Fabric Runtime 1.2+).
3.  **Permissions:** The user running the notebook needs:
    * **Member/Admin** access to the workspaces being scanned.
    * **Tenant Admin** read rights (optional, but recommended for full visibility).
4.  **Attached Lakehouse:** You **must** attach a default Lakehouse to the notebook. All Delta tables will be saved here.

## Installation & Usage

1.  **Import:** Download the `.ipynb` file from this repository and import it into your Microsoft Fabric workspace.
2.  **Attach Lakehouse:** Open the notebook and add a Lakehouse to the explorer on the left. Set it as the **Default Lakehouse**.
3.  **Install Dependencies:** If you are on an older runtime, uncomment the pip install line in the first cell:
    ```python
    # !pip install --upgrade semantic-link
    ```
4.  **Run All:** Execute all cells. The notebook is designed to run sequentially.

> **Note on API Throttling:** The notebook includes logic to handle `429 Too Many Requests` errors with retries and backoff periods. For very large tenants, execution may take some time.

## Output Schema (Delta Tables)

The notebook generates the following Delta Tables in your Lakehouse:

### 1. High-Level Inventory
| Table Name | Description |
| :--- | :--- |
| `FABRIC_ALLITEMS` | Master list of all items (Reports, Notebooks, etc.) in dedicated workspaces. |
| `WORKSPACE_DETAILS` | Metadata for all workspaces (ID, Name, Capacity ID). |
| `CAPACITY_DETAILS` | Details of Fabric capacities available to the user. |
| `APP_DETAILS` | Inventory of published Fabric/Power BI Apps. |
| `GATEWAY_DETAILS` | List of On-premises Data Gateways and Virtual Network Gateways. |

### 2. Item Specifics
| Table Name | Description |
| :--- | :--- |
| `LAKEHOUSE_DETAILS` | SQL Endpoint strings, OneLake paths, and provisioning status. |
| `WAREHOUSE_DETAILS` | Warehouse connection strings and creation dates. |
| `LIST_REPORTS` | Inventory of all Power BI Reports. |
| `FABRIC_WSPFOLDERS` | Folder structures within workspaces. |

### 3. Dataflows & Lineage
| Table Name | Description |
| :--- | :--- |
| `DATAFLOWS_ALL` | Consolidated list of Gen1 and Gen2 Dataflows. |
| `DATAFLOW_DATASOURCES`| Lineage information connecting Dataflows to their upstream sources. |
| `DATASET_DATASOURCES` | Lineage information connecting Semantic Models to their sources. |

### 4. Security & Access
| Table Name | Description |
| :--- | :--- |
| `WORKSPACE_USERS` | List of users and groups assigned to Workspaces (Admins, Members, Contributors). |
| `DATASET_USERS` | **(Heavy Operation)** Iterates through every dataset to log user access rights and principal types. |

### 5. Deep Metadata (TMSL Parsing)
These tables break down the internal structure of your Semantic Models:
| Table Name | Description |
| :--- | :--- |
| `DOC_MEASURES` | All DAX measures and their formulas. |
| `DOC_M_QUERIES` | Power Query (M) code for loaded tables. |
| `DOC_EXPRESSIONS` | Unloaded M queries, functions, and parameters. |
| `DOC_COLUMNS` | Column names, data types, and hidden status. |
| `DOC_RELATIONSHIPS` | Data model relationships (cardinality, cross-filter direction). |
| `DOC_ROLES` | RLS (Row Level Security) roles and DAX filters. |

### 6. Storage Analytics
| Table Name | Description |
| :--- | :--- |
| `StorageInventory` | Compares **Active Size** (current data) vs **Physical Size** (including historical versions) to help optimize OneLake storage costs. |

## Configuration

You can customize the exclusions in the "Storage Details" section to skip specific workspaces (e.g., Sandboxes or Sensitive data):

```python
# Add the exact names of workspaces you want to skip
EXCLUDED_WORKSPACES = [
    "Dev Sandbox",
    "HR Sensitive Data"
]
