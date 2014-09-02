#SQL Server details
SQL_HOST = 'localhost'
SQL_USERNAME = 'root'
SQL_PASSWORD = ''
#Cache details - whether to call a URL once an ingestion script is finished
RESET_CACHE = False
RESET_CACHE_URL = 'http://example.com/visualization_reload/'
#Fab - configuration for deploying to a remote server
FAB_HOSTS = []
FAB_GITHUB_URL = 'https://github.com/UQ-UQx/injestor.git'
FAB_REMOTE_PATH = '/file/to/your/deployment/location'
#Ignored services
IGNORE_SERVICES = ['extractsample', 'personcourse']
#File output
OUTPUT_DIRECTORY = '/tmp'
DATA_PATHS = ['/data/']