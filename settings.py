import os

REDIS_HOST = str(os.getenv('REDIS_HOST') or 'localhost')
REDIS_PORT = int(os.getenv('REDIS_PORT') or 6379)
REDIS_DB = int(os.getenv('REDIS_DB') or 0)
REDIS_FILES_DB = int(os.getenv('REDIS_FILES_DB') or 1)
REDIS_STRUCTURED_DATA_DB = int(os.getenv('REDIS_STRUCTURED_DATA_DB') or 2)

POSTGRES_HOST = str(os.getenv('POSTGRES_HOST') or 'localhost')
POSTGRES_PORT = int(os.getenv('POSTGRES_PORT') or 5432)
POSTGRES_DB = str(os.getenv('POSTGRES_DB') or 'data')
POSTGRES_USER = str(os.getenv('POSTGRES_USER') or 'root')
POSTGRES_PASS = str(os.getenv('POSTGRES_PASSWORD') or 'password')

STRUCTURED_DATA_TOPIC = str(os.getenv('STRUCTURED_DATA_TOPIC') or 'STRUCTURED_DATA_TOPIC')

STRUCTURED_DATA_SCHEMA_PATH = str(os.getenv('STRUCTURED_DATA_SCHEMA_PATH') or './avro_files/structured_data.avsc')

BOOTSTRAP_SERVERS = str(os.getenv('BOOTSTRAP_SERVERS') or 'localhost:9092')
SCHEMA_REGISTRY_URL = str(os.getenv('SCHEMA_REGISTRY_URL') or 'http://localhost:8085')
GROUP_ID = str(os.getenv('GROUP_ID') or 'data_lake')
OFFSET = str(os.getenv('OFFSET') or 'earliest')
ENCODING = str(os.getenv('ENCODING') or 'utf_8')
AVRO_FILES_ENCODING = str(os.getenv('AVRO_FILES_ENCODING') or 'utf-8')
SQL_FILES_ENCODING = str(os.getenv('SQL_FILES_ENCODING') or 'utf-8')