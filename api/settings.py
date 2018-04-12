MONGO_HOST = 'localhost'
MONGO_DB_NAME = 'madkudu'


try:
    from local_settings import *
except ImportError:
    pass
