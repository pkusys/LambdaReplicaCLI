import os
import shutil
import subprocess
import sys
import zipfile
from pathlib import Path

from lambdareplica.integration import AzureClient, GcpClient, AWSClient
from lambdareplica.setup.setup_credentials import CONFIG_DIR, LOCAL_METADATA_FILE, LOCAL_GOOGLE_METADATA_FILE, \
    read_credentials, update_credentials

BUILD_DIR = os.path.join(CONFIG_DIR, "build")
ZIP_PATH = os.path.join(CONFIG_DIR, "build.zip")

def create_lambda_zip(function_name):
    project_root = Path(__file__).parent.parent.absolute()
    function_root = os.path.join(project_root, 'functions', 'aws', function_name)
    handler_path = os.path.join(function_root, 'handler.py')
    requirements_path = os.path.join(function_root, 'requirements.txt')
    if os.path.exists(BUILD_DIR):
        shutil.rmtree(BUILD_DIR)
    if os.path.exists(ZIP_PATH):
        os.remove(ZIP_PATH)
    os.makedirs(BUILD_DIR)
    try:
        subprocess.check_call([
            sys.executable, "-m", "pip", "install", "--platform", "manylinux2014_x86_64", "--python-version", "3.10",
            "--only-binary=:all:", "-r", requirements_path, "-t", BUILD_DIR, "--quiet"
        ])
    except subprocess.CalledProcessError:
        return False
    except FileNotFoundError:
        return False

    shutil.copy(handler_path, BUILD_DIR)
    build_function_path = os.path.join(BUILD_DIR, 'lambdareplica')
    shutil.copytree(project_root, build_function_path)
    shutil.rmtree(os.path.join(build_function_path, 'cli'))
    shutil.rmtree(os.path.join(build_function_path, 'functions'))
    shutil.rmtree(os.path.join(build_function_path, 'setup'))
    build_credentials_path = os.path.join(BUILD_DIR, 'credentials')
    os.makedirs(build_credentials_path)
    shutil.copy(LOCAL_METADATA_FILE, build_credentials_path)
    shutil.copy(LOCAL_GOOGLE_METADATA_FILE, build_credentials_path)

    with zipfile.ZipFile(ZIP_PATH, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(BUILD_DIR):
            for file in files:
                file_path = os.path.join(root, file)
                archive_name = os.path.relpath(file_path, BUILD_DIR)
                zipf.write(file_path, archive_name)
    return True

def setup_aws(region):
    aws_client = AWSClient(region=region, is_cloud=False)
    table_desc = aws_client.create_dynamodb_table(table_name='multipart',
                                                  key_schema=[{"AttributeName": "Key", "KeyType": "HASH"}],
                                                  attribute_definitions=[{"AttributeName": "Key", "AttributeType": "S"}],
                                                  region=region)
    if not table_desc:
        return False
    is_success = create_lambda_zip('main')
    if not is_success:
        return False
    is_success = aws_client.deploy_lambda(function_name=f'lr_main_{region}', package_path=ZIP_PATH,
                                          handler='handler.lambda_handler', region=region)
    if not is_success:
        return False
    is_success = create_lambda_zip('worker')
    if not is_success:
        return False
    is_success = aws_client.deploy_lambda(function_name=f'lr_worker_{region}', package_path=ZIP_PATH,
                                          handler='handler.lambda_handler', region=region)
    if not is_success:
        return False

    return True

def create_functionapp_zip(function_name):
    project_root = Path(__file__).parent.parent.absolute()
    function_root = os.path.join(project_root, 'functions', 'azure', function_name)
    if os.path.exists(BUILD_DIR):
        shutil.rmtree(BUILD_DIR)
    if os.path.exists(ZIP_PATH):
        os.remove(ZIP_PATH)

    shutil.copytree(function_root, BUILD_DIR)
    build_function_path = os.path.join(BUILD_DIR, 'lambdareplica', 'integration')
    os.makedirs(os.path.join(BUILD_DIR, 'lambdareplica'))
    shutil.copytree(os.path.join(project_root, 'integration'), build_function_path)
    with open(os.path.join(build_function_path, '__init__.py'), "w") as f:
        pass
    build_credentials_path = os.path.join(BUILD_DIR, 'credentials')
    os.makedirs(build_credentials_path)
    shutil.copy(LOCAL_METADATA_FILE, build_credentials_path)
    shutil.copy(LOCAL_GOOGLE_METADATA_FILE, build_credentials_path)

    with zipfile.ZipFile(ZIP_PATH, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(BUILD_DIR):
            for file in files:
                file_path = os.path.join(root, file)
                archive_name = os.path.relpath(file_path, BUILD_DIR)
                zipf.write(file_path, archive_name)

    return True

def setup_azure(region):
    azure_client = AzureClient(is_cloud=False)
    credentials = read_credentials(is_cloud=False)
    result = azure_client.create_cosmosdb_table(account=f'lambda-cosmos-{region}', database='LambdaReplica',
                                                container='multipart',
                                                resource_group=credentials['azure_resource_group'], region=region)
    if not result:
        return False
    if 'azure_cosmos_keys' not in credentials:
        credentials['azure_cosmos_keys'] = {}
    credentials['azure_cosmos_keys'][region] = {"account": f'lambda-cosmos-{region}',
                                                "key": result['key']}
    update_credentials(credentials)

    is_success = create_functionapp_zip('main')
    if not is_success:
        return False
    is_success = azure_client.deploy_functionapp(function_name=f'lambdafuncapp{region}', package_path=ZIP_PATH,
                                                 instance_memory_mb=2048, maximum_instance_count=1000,
                                                 per_instance_concurrency=1,
                                                 resource_group=credentials['azure_resource_group'], region=region)
    if not is_success:
        return False
    is_success = create_functionapp_zip('worker')
    if not is_success:
        return False
    is_success = azure_client.deploy_functionapp(function_name=f'lambdaworkerfuncapp{region}', package_path=ZIP_PATH,
                                                 instance_memory_mb=2048, maximum_instance_count=1000,
                                                 per_instance_concurrency=1,
                                                 resource_group=credentials['azure_resource_group'], region=region)
    if not is_success:
        return False

    return True


def create_cloud_run_zip(function_name):
    project_root = Path(__file__).parent.parent.absolute()
    function_root = os.path.join(project_root, 'functions', 'gcp', function_name)
    if os.path.exists(BUILD_DIR):
        shutil.rmtree(BUILD_DIR)
    if os.path.exists(ZIP_PATH):
        os.remove(ZIP_PATH)

    shutil.copytree(function_root, BUILD_DIR)
    build_function_path = os.path.join(BUILD_DIR, 'lambdareplica', 'integration')
    os.makedirs(os.path.join(BUILD_DIR, 'lambdareplica'))
    shutil.copytree(os.path.join(project_root, 'integration'), build_function_path)
    with open(os.path.join(build_function_path, '__init__.py'), "w") as f:
        pass
    build_credentials_path = os.path.join(BUILD_DIR, 'credentials')
    os.makedirs(build_credentials_path)
    shutil.copy(LOCAL_METADATA_FILE, build_credentials_path)
    shutil.copy(LOCAL_GOOGLE_METADATA_FILE, build_credentials_path)

    with zipfile.ZipFile(ZIP_PATH, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, _, files in os.walk(BUILD_DIR):
            for file in files:
                file_path = os.path.join(root, file)
                archive_name = os.path.relpath(file_path, BUILD_DIR)
                zipf.write(file_path, archive_name)

    return True

def setup_gcp(region):
    gcp_client = GcpClient(is_cloud=False)
    is_success = gcp_client.create_firestore_database(database_name=f'lambdareplica-{region}', region=region)
    if not is_success:
        return False
    is_success = create_cloud_run_zip('main')
    if not is_success:
        return False
    is_success = gcp_client.deploy_cloud_run(function_name=f'lambda-{region}', code_path=ZIP_PATH, max_instances=100,
                                             timeout=600, concurrency=1, cpu='2', memory='1024Mi', region=region)
    if not is_success:
        return False
    is_success = create_cloud_run_zip('worker')
    if not is_success:
        return False
    is_success = gcp_client.deploy_cloud_run(function_name=f'lambda-worker-{region}', code_path=ZIP_PATH,
                                             max_instances=500, timeout=600, concurrency=1, cpu='1', memory='1024Mi',
                                             region=region)
    if not is_success:
        return False

    return True
