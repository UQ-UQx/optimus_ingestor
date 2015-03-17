#SQL Server details
SQL_HOST = 'localhost'
SQL_USERNAME = 'root'
SQL_PASSWORD = ''
#Mongo Server details
MONGO_HOST = 'localhost'
#Cache details - whether to call a URL once an ingestion script is finished
RESET_CACHE = False
RESET_CACHE_URL = 'http://example.com/visualization_reload/'
#Fab - configuration for deploying to a remote server
FAB_HOSTS = []
FAB_GITHUB_URL = 'https://github.com/UQ-UQx/optimus_ingestor.git'
FAB_REMOTE_PATH = '/file/to/your/deployment/location'
#Ignored services
IGNORE_SERVICES = ['extract_sample', 'eventcount', 'daily_count']
#File output
OUTPUT_DIRECTORY = '/tmp'
DATA_PATHS = ['/data/']
EXPORT_PATH = '/Volumes/VMs/export'
#The server where the course information is found
SERVER_URL = 'http://dashboard.ceit.uq.edu.au'
CLICKSTREAM_PREFIX = 'uqx-edx-events-'
DBSTATE_PREFIX = 'UQx-'