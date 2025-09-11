# LambdaReplicaCLI

This repository contains artifacts of the EuroSys '26 paper "Serverless Replication of Dynamic Objects across Clouds and Regions" and exposes a command line interface.


### 1. Configure Python environment

```Bash
cd LambdaReplicaCLI
pip3 install -e .
```

### 2. Install and configure Azure CLI

Because Azure Python SDK cannot fully automate FunctionApp deployment, we use Azure CLI for deployment.

For Mac users:
```Bash
brew update && brew install azure-cli
```

For Linux users, please refer to [Install the Azure CLI on Linux](https://learn.microsoft.com/en-us/cli/azure/install-azure-cli-linux?view=azure-cli-latest&pivots=apt).

After Azure CLI is installed, run:
```Bash
az login --service-principal \
  -u <client_id> \
  -p <client_secret> \
  --tenant <tenant_id>
```

## 3. Configure LambdaReplicaCLI

```Bash
lambda-replica-config
```

Your will need to provide:
* AWS Access Key
* AWS Secret Key
* AWS Role ARN (AWS Lambda functions use this. It must be able to read and write S3 buckets and DynamoDB tables)
* Azure Subscription ID 
* Azure Client ID 
* Azure Tenant ID 
* Azure Client Secret 
* Azure Resource Group
* The path to your GCP Credentials (which is an auto-generated JSON file)

## 4. Run LambdaReplicaCLI

To replicate the object `1M.dat` from the bucket `lambda.replica.test.us-east-1` at `AWS` to 
the container `lambda-replica` of storage account `lreastus` at `Azure`, run:

```Bash
lambda-replica --source-cloud AWS \
--source-bucket lambda.replica.test.us-east-1 \
--object-key 1M.dat \
--destination-cloud Azure \
--destination-bucket lreastus/lambda-replica
```

***
For any question, please contact `shujunyi@gmail.com`.
