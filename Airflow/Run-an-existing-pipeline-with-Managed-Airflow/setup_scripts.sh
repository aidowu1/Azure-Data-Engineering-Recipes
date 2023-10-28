# Define constants
SUBSCRIPTION = "16f263d1-840b-4414-8415-243b4662d595"
RESOURCE_GROUP = "rg-airflow"
LOCATION = "uksouth"
STORAGE_ACCOUNT_NAME = airflow-dags
EMAIL = "adeidowu@hotmail.com"
CONTAINER_NAME = "airflow-dags"
ROLE = "/subscriptions//resourceGroups/${RESOURCE_GROUP}/providers/Microsoft.Storage/storageAccounts/${STORAGE_ACCOUNT_NAME}/blobServices/default/containers/${CONTAINER_NAME}"


# Create Resource group
az group create --name ${RESOURCE_GROUP} --location ${LOCATION}

# Create Storage Account
az storage account create \
    --name ${STORAGE_ACCOUNT_NAME} \
    --resource-group ${RESOURCE_GROUP} \
    --location ${LOCATION} \
    --dns-endpoint-type AzureDnsZone

az storage container create \
    --account-name ${STORAGE_ACCOUNT_NAME} \
    --name ${CONTAINER_NAME} \
    --auth-mode login

# Create the write role
az role assignment create --role "Storage Blob Data Contributor" \
   --assignee ${EMAIL} \ 
   --scope ${ROLE}