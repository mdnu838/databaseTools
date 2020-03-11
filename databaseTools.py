import pymysql
from configparser import ConfigParser
configuration = ConfigParser()

DATABASE_CONFIG_FILE = 'database.ini'

def get_db_config(section):
    '''
    This get the database configuration for a given database in dictionary format
    :param section: database name
    :return: database configuration in dictionary format
    '''
    try:
        parser = ConfigParser()
        parser.read(DATABASE_CONFIG_FILE)
        connectionParams = {}
        params = parser.items(section)
        for param in params:
            connectionParams[param[0]] = param[1]
        return connectionParams
    except Execute as e:
        print(e)
        print('Section is meassing')
        return None


def execute_query(query,connectionParams,fetch = False,many=False,record=None):
    '''
    Execute a given sql quary in the database
    :param query: sql query
    :param connectionParams: Database Configuration in Dictionary Format
                            eg:{ 'Host':'host', 'User'='user', 'password'= 'password', 'db'='databaseName'}
    :param fetch: to identify it a fetch or a Execute query
    :param many: To identify it's a single row query or multiple rows
    :param record: if many True then the data for the multiple rows
    :return: if fetch True then the data from the data base,
             if fetch False then True for successful execution of the query,
             else None
    '''

    try:
        connection = pymysql.connect(**connectionParams,
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
        connectionCursor = connection.cursor()
    except:
        print('Database credentials are incorrect ')
        return None
    try:
        if many:
            connectionCursor.executemany(query,record)
            connection.commit()
            return True,connectionCursor.lastrowid
        else:
            connectionCursor.execute(query)
            if fetch:
                rows = connectionCursor.fetchall()
                return rows
            else:
                connection.commit()
                return True
    except Exception as e:
        print(e)
        print('Error in the execution on the Query')
    finally:
        connectionCursor.close()
        connection.close()

def execute_query_from_config(query,databaseName,fetch = False,many=False,record=None):
    '''
    Execute a given sql quary in the database which is already present in the Configuration
    :param query: sql query
    :param databaseName: database section in the database.ini file
    :param fetch: to identify it a fetch or a Execute query
    :param many: To identify it's a single row query or multiple rows
    :param record: if many True then the data for the multiple rows
    :return: if fetch True then the data from the data base,
             if fetch False then True for successful execution of the query,
             else None
    '''
    connectionParams = get_db_config(databaseName)

    try:
        connection = pymysql.connect(**connectionParams,
                                     charset='utf8mb4',
                                     cursorclass=pymysql.cursors.DictCursor)
        connectionCursor = connection.cursor()
    except Exception as e:
        print(e)
        print('Database credentials are incorrect ')
        return None
    try:
        if many:
            connectionCursor.executemany(query, record)
            connection.commit()
            return True, connectionCursor.lastrowid
        else:
            connectionCursor.execute(query)
            if fetch:
                rows = connectionCursor.fetchall()
                return rows
            else:
                connection.commit()
                return True
    except Exception as e:
        print(e)
        print('Error in the execution on the Query')

    finally:
        connectionCursor.close()
        connection.close()




