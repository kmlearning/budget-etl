import pandas as pd


def check_latest(engine, account):
    '''
    Get last date of import and return it
    Arguments:
        engine: 
        account: 
    Returns:
        lastdate: 
    '''
    query = '''
        SELECT
            MAX(transdate) AS lastdate
        FROM
            budget.transactions
        WHERE
            account = %s;
        '''
    # Run query to get newest data date for account
    lastdate = pd.read_sql(query, con = engine, params= [account])
    # Convert datetime type to match df
    lastdate['lastdate'] = pd.to_datetime(lastdate['lastdate'])
    # Get as single value
    lastdate = lastdate['lastdate'].iloc[0]
    if pd.isnull(lastdate):
        lastdate = pd.to_datetime('1900-01-01')
    return lastdate