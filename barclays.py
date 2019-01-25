from numpy import isnan
import pandas as pd

from latest_check import check_latest
from sql_connection import get_database


ACCOUNT = 'BarcCC'
CSVFILE = r'data/barclays.csv'
SCHEMA = 'budget'
TABLE = 'transactions'

engine = get_database()

headers_in = {
    'transdate': None, 
    'description': 'object', 
    'pay_type': 'object', 
    'name': 'object', 
    'vendorcategory': 'object', 
    'incoming': 'float64', 
    'value': 'float64',
    'truecategory': 'object'
    }

headers_to_keep = [
    'transdate', 
    'value', 
    'direction',
    'description', 
    'account', 
    'vendorcategory', 
    'truecategory'
]

def set_direction(value):
    ''' 
    Returns string of 'in'/'out' based on whether the value is 
    positive/negative respectively
    '''
    if value <= 0:
        direction = 'in'
    else:
        direction = 'out'
    return direction

# Read csv file to df
df = pd.read_csv(CSVFILE, encoding = 'latin-1', names= headers_in.keys(), dtype = headers_in, parse_dates = ['transdate'])

# Move outgoings into incomings (appear as negative outgoings)
df['value'] = df.apply(lambda row: row['value'] if not isnan(row['value']) else row['incoming'], axis=1)

# Add account
df['account'] = ACCOUNT

# Set incomings/outgoings
df['direction'] = df['value'].apply(set_direction)
# Remove pos/negative
df['value'] = df['value'].apply(abs)

# Remove unnecessary headers
df = df[headers_to_keep].sort_values('transdate')

# Get date of latest vendor data for account type
lastdate = check_latest(engine = engine, account = ACCOUNT)

# Filter df to only new since last date, then inset to SQL table
df[df['transdate'] > lastdate].to_sql(name=TABLE, con=engine, schema=SCHEMA, if_exists='append', index=False)