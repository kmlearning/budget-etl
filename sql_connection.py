import yaml
from sqlalchemy import create_engine
import logging


log = logging.getLogger(__name__)

def get_database():
    try:
        engine = get_connection()
        log.info('Successfully connected to PostgreSQL db')
    except IOError:
        log.exception('Failed to connect to PostgreSQL db')
        return None, 'fail'
    return engine

def get_connection(config_file='sql_config.yaml'):
    """
    Set up database connection from yaml config file
    Input:
        config_file_name: File containing credentials for PG database:
            PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
    """
    with open(config_file, 'r') as file:
        vals = yaml.load(file)
    if not (
        'PGHOST' in vals.keys() and
        'PGPORT' in vals.keys() and
        'PGUSER' in vals.keys() and
        'PGPASSWORD' in vals.keys() and
        'PGDATABASE' in vals.keys()
    ):
        raise Exception('Error in config file: {}'.format(config_file))
    return get_engine(
        vals['PGHOST'], 
        vals['PGPORT'], 
        vals['PGUSER'], 
        vals['PGPASSWORD'],
        vals['PGDATABASE']
    )

def get_engine(host, port, username, password, db):
    """
    Get SQLAlchemy engine using credentials
    Input:
        db: database name
        username: username
        host: hostname of db server
        port: port number
        password:  password for db
    """
    url = 'postgresql://{username}:{password}@{host}:{port}/{db}'.format(
        username = username, 
        password= password, 
        host = host, 
        port = port, 
        db = db
    )
    engine = create_engine(url, pool_size = 50)
    return engine