from pathlib import Path

from lambdareplica.integration.aws_client import AWSClient
from lambdareplica.integration.azure_client import AzureClient
from lambdareplica.integration.gcp_client import GcpClient

__version__ = "0.3.2"
__root__ = Path(__file__).parent.parent
__all__ = [
    "__version__",
    "__root__",
    # modules
    "AWSClient",
    "AzureClient",
    "GcpClient"
]