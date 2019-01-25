import pandas as pd

from latest_check import check_latest
from sql_connection import get_database


ACCOUNT = 'HSBC'
CSVFILE = r'data/hsbc.csv'
SCHEMA = 'budget'
TABLE = 'transactions'

engine = get_database()

headers_in = {
    'transdate': None, 
    'description': 'object', 
    'value': 'float64',
    'truecategory': 'object'
    }

headers_to_keep = [
    'transdate', 
    'value',
    'direction',
    'description', 
    'account', 
    'truecategory'
]

def set_direction(value):
    ''' 
    Returns string of 'in'/'out' based on whether the value is 
    positive/negative respectively
    '''
    if value >= 0:
        direction = 'in'
    else:
        direction = 'out'
    return direction

# Read csv file to df
df = pd.read_csv(CSVFILE, encoding = 'latin-1', names= headers_in.keys(), dtype = headers_in, parse_dates = ['transdate'], thousands=',')

# Add account
df['account'] = ACCOUNT

# Create true_category
df['truecategory'] = None

# Set incoming/outgoing
df['direction'] = df['value'].apply(set_direction)
# Remove pos/negative
df['value'] = df['value'].apply(abs)

# Remove unnecessary headers
df = df[headers_to_keep].sort_values('transdate')

# Get date of latest vendor data for account type
lastdate = check_latest(engine = engine, account = ACCOUNT)

# Filter df to only new since last date, then inset to SQL table
df[df['transdate'] > lastdate].to_sql(name=TABLE, con=engine, schema=SCHEMA, if_exists='append', index=False)