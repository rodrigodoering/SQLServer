import pyodbc
import pandas as pd
import numpy as np
from functools import wraps

class SQLServer(object):

    def __init__(self, driver=None, server=None, user=None, password=None, dsn=None, database=None, host=None, auth='windows'):
        self.driver = driver
        self.server= server
        self.user = user
        self.password = password
        self.dsn = dsn
        self.database = database
        self.host = host
        self.auth = auth

    def connect(self):
        '''
        The connection wth SQL Server is established via ODBC driver 
        If dsn attribute is passed while instantiating the class, it will force connection via data source name
        Authentication mode (windows or sql) also must be passed when instantiating. By default it's windows
        '''
        self.connected = False
        if self.dsn:
            try:
                if self.auth == 'windows':
                    # connects using only dsn
                    self.connection = pyodbc.connect('DSN='+self.dsn)
                    self.connected = True

                elif self.auth == 'sql':
                    # connects using dsn, user and password
                    self.connection = pyodbc.connect('DSN='+self.dsn+';UID='+self.user+';PWD='+self.password) 
                    self.connected = True
            
                else:   
                    # supports only this two connection presets (windows and sql)
                    print('Pass a valid authentication mode')
            except Exception as e:
                print('Erro: %s' % e)
        else:
            # tries regular pyodb connection passing directly sql params
            try:
                if self.auth == 'windows':
                    # connects using driver, server and database
                    self.connection = pyodbc.connect('DRIVER={'+self.driver+
                                            '};SERVER='+self.server+
                                            ';Trusted_Connection=yes;DATABASE='+self.database)
                    self.connected = True               
                elif self.auth == 'sql':
                    # connects using driver, host, database, user and password
                    self.connection = pyodbc.connect(driver=self.driver,
                                            host=self.host,
                                            database=self.database,
                                            user=self.user,
                                            password=self.password
                                            )
                    self.connected = True
                else:
                    print('Pass a valid authentication mode')
            except Exception as e:
                print('Erro: %s' % e)

        if self.connected:
            # sets cursor
            self.cursor = self.connection.cursor()
            # print current database 
            self.cursor.execute('select db_name()')
            self.current_database = [row[0] for row in self.cursor.fetchall()][0]
            
            if self.dsn:
                print('Connection established with %s\nUsing Database: %s' % (self.dsn, self.current_database))
            else:
                print('Connection established with %s\nUsing Database: %s' % (self.server, self.current_database))
            print('Use set_database(database) method to use a specific database')

    def testConnection(func):
        '''
        Test if it's connected to SQL Server before calling any function that requires database connection
        If not connected it will throw a notificaton
        '''
        # wraps called function
        # decorates all functions that requires database connection
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            # tests if connected attr exists
            if hasattr(self, 'connected'):
                if self.connected:
                    # return wrapped function
                    return func(self, *args, **kwargs)
                else:
                    print('Cannot call %s. There was a problem connecting to SQL Server' % func.__name__)
                    return 
            else:
                print('Connect to server with SQLServer.connect(authentication) method before calling other functions')
                return
        return wrapper

    @testConnection
    def query(self, query, commit=False, return_option='raw'):
        '''
        Execute any T-SQL command passed 
        If commit param is set True, the effects of executed SQL statement will be saved inside the database 
        Else, function will try to extract and store output data according to return_option param
        Return options are: 'raw', 'list' and 'single value'
        If no data was outputed from query, it will return None
        '''
        # execute SQL statement
        self.cursor.execute(query)

        if commit:
            self.connection.commit()
        else:
            try:
                # store data returned from sql server in lists
                # rows are returned from fetchall() func as tuples
                if return_option == 'single value':
                    # query will output a single value
                    output = [row for row in self.cursor.fetchall()]
                    return [value[0] for value in output][0]

                elif return_option == 'list':
                    # return data as list
                    output = [row for row in self.cursor.fetchall()]
                    return [value[0] for value in output]
                
                elif return_option == 'raw':
                    # return raw data as row objects
                    return [row for row in self.cursor.fetchall()]
                
                else:
                    print('return_option param must be "raw", "single value" or "list", returning None')
                    return

            except Exception as e:
                print('Error executing self.query(): %s' % e)
                return 

    @testConnection
    def list_database(self):
        '''
        returns list with existing databases
        '''
        return self.query('SELECT name FROM master.dbo.sysdatabases', return_option='list')

    @testConnection
    def set_database(self, database):
        '''
        USE {datbase} statement
        Verifies is passed database is already in use
        If yes, it will throw a notification
        Else it will access passed database
        '''
        if database == self.current_database:
            print('%s already being used' % database)
        else:
            try:
                # change database
                self.query('USE %s' % database, commit=True)
                # updates current database attribute
                self.current_database = database
                print('Database %s accessed' % database)
            except Exception as e:
                print('An error occured: %s' % e)

    @testConnection
    def detail_table(self, table, dataframe=True):
        '''
        Returns column names, data types and nullable status from passed table
        If dataframe is True (default), info will be returned as dataframe, else it will be outputed as regular python dictionary
        '''
        query = "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'" % table
        columns = ['COLUMN_NAME', 'IS_NULLABLE', 'DATA_TYPE']
        # extract table information
        info = self.query(query)
        # builds dictionary
        data = {}

        for i, column in enumerate(columns):
            data[column] = [row[i] for row in info]

        if dataframe:
            # return as dataframe
            return pd.DataFrame(data)
        else:
            # return as dict
            return data


    @testConnection
    def select(self, table, percent=None, columns=None, condition=None, dataframe=True, schema=None):
        '''
        Outputs data from SQL Select statement using some passed params
        If no param was passed, a simple "SELECT * FROM TABLE" statement will be executed
        Returns output data as a pandas dataframe (default) or as a simple dictionary
        Still needs improvement 
        '''
        # Starts inner function that puts together passed params into sql final statement
        def get_select_statement(table, percent, columns, condition, schema):
            '''
            Builds SQL Statement according to passed params
            percent, columns, condition and schema are customization params for standard select * from table statement
            '''
            select_ = 'select '
            columns_ = '*'
            filter_ = ' from %s' % table
            # check for specifications to build select query with passed params
            if schema:
                # adds table's schema
                table = schema + '.' + table 
                filter_ = ' from %s' % table

            if percent:
                # customize number of returned rows
                select_ = 'select top(%s) percent ' % percent

            if columns:
                # customized returned columns
                columns_ = columns
                columns = columns.replace(' ','').split(",")    
                
            if condition:
                # customize with condition 
                filter_ = filter_ + ' ' + condition

            # set final sql statement
            return select_ + columns_ + filter_, columns 

        try:
            # Execute sql statement and outputs data as 'raw'
            sql_statement, df_columns = get_select_statement(table, percent, columns, condition, schema)
            output_data = self.query(sql_statement)

        except Exception:
            # a probable error here is extracting a table from a database that requires table schema without passing it, raising "invalid object name" SQL error
            # it will attempt to find the table with it's schema
            print('Error while executing query, attempting to query table with schema')
            # find table's schema
            schema = self.query("SELECT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'" % table, return_option='single value')
            # try again passing extracted schema
            sql_statement, df_columns = get_select_statement(table, percent, columns, condition, schema)

            try:
                # tries again to import data now passing valid table schema
                output_data = self.query(sql_statement)

            except Exception as e:
                # if error persists, return Nonetype
                print("Coudn't extract data, returning None")
                print('Raised error: %s' % e)

        if not output_data:
            # checks if any data was outputed from query
            print('No data returned from query, returning None')
            return
        
        # creates empty dictionary to store and structure data 
        final_data = {}

        if not columns:
            # if no column was passed, it will extract all table columns in order to build dataframe 
            # extract table columns to build dataframe
            df_columns = self.detail_table(table).COLUMN_NAME.values.tolist()     

        # structure data in dictionary
        for i, column in enumerate(df_columns):
            final_data[column] = [row[i] for row in output_data] 

        if dataframe:
            # return data as dataframe
            return pd.DataFrame(final_data)
        else:
            # return data as dictionary
            return final_data

    @testConnection
    def list_tables(self, pandaSeries=False, from_database=False, output='dataframe'):
        '''
        Returns list with all existing tables in current database
        If from_database argument is passed, it will call set_database() method and new current database will be setted
        To return to previous database, set_database() method must be called again
        Tables will be returned as pandas.Series or in normal list (default) if pandaSeries param is False
        '''
        if from_database:
            # access passed database
            self.set_database(from_database)
        
        if output == 'dataframe':
            # output table description as dataframe
            return self.select(table='INFORMATION_SCHEMA.TABLES', columns='TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE')
        
        elif output == 'list':
            # output list with table names
            tables = self.select(table='INFORMATION_SCHEMA.TABLES', columns='TABLE_NAME')
            return  tables.TABLE_NAME.values.tolist()
        
        else:
            print('Pass a valid output: "dataframe" or "list"')
            return 

    @testConnection
    def insert(self, df, table):
        '''
        Attempts to insert dataframe into table
        Requires dataframe structure with column names that matches target table's columns 
        Still needs improvement
        '''
        # create sql statement
        insert = 'insert into %s ' % table
        columns = '('+', '.join(df.columns)+')'

        # char values in sql must have quotation marks
        convert_to_char = lambda x: "'%s'" % x

        # make copy of original dataframe to maintain original dataframe 
        df_insert = df.copy()

        for column in df.columns:
            # checks if any datframe column is object type
            if df_insert[column].dtypes == object:
                # if column is object type, quotation marks will be added to column values
                df_insert[column] = df_insert[column].apply(convert_to_char)

        # interates over all dataframe rows
        for i in range(df_insert.shape[0]):
            # store values as strings in list
            values = [str(value) for value in df_insert.iloc[i]]
            # format values as sql strings
            string_values = ' values ('+', '.join(values)+')'
            # create final sql statement
            sql = insert + columns + string_values
            # insert values
            self.query(sql, commit=True)

    @testConnection
    def export_(self, df, file_, file_type, json_orient):
        '''
        Export data from dataframe to specified file path, format and json orientation (if passed)
        Created primarily to be used within export_to_file function, although it might be used separately as well
        '''
        if file_type == 'csv':
            # export as csv
            df.to_csv(file_)

        elif file_type == 'xlsx':
            # export as xlsx
            df.to_excel(file_)

        elif file_type == 'json' and json_orient:
            # export as json
            df.to_json(file_, orient=json_orient)

        elif file_type == 'json' and not json_orient:
            # export as json
            df.to_json(file_)       

        elif file_type == 'txt':
            # export as txt
            columns = df.columns
            with open(file_, 'w') as out:
                df.to_string(out, columns=columns)
        else:
            print('Pass a valid format')
            return

    @testConnection
    def export_to_file(self, tables=None, database='current', file_type='csv', json_orient='index', path=None):
        '''
        Export tables as flat files
        If table is None, all existing tables will be exported
        Table param can be either a single table (string) or a list of tables
        A database different from the current one can be passed
        Format can be "csv" (default), "xlsx", "txt" or "json"
        If format is json, it requires an json_orient param (by default is 'index')
        It will generate one file per table
        '''
        if file_type not in ['csv', 'xlsx', 'txt', 'json']:
            # print notification
            print('Choose a valid format: csv, xlsx, txt or json')
            return

        # format path 
        if path and path[-1] != '\\':
            path += '\\'
        
        if database != 'current':
            # set passed database
            self.set_database(database)

        if not tables:
            # extract list of tables in database
            tables = self.list_tables(pandaSeries=False, output='list')
        
        if type(tables) is list:
            # create list with tables to be exported
            if path:
                # adds path to save table
                files = list(map(lambda table: path + table + '.' + file_type, tables))          
            else:
                files = list(map(lambda table: table + '.' + file_type, tables)) 

            # loop through tableslist
            for file_ , table in zip(files, tables):
                # extract table as dataframe
                print('Exporting table %s as %s' % (table, file_type))
                try:
                    df = self.select(table=table)
                    # export dataframe 
                    self.export_(df, file_, file_type, json_orient)

                except Exception as e:
                    print('Error extracting data from: %s' % table)
                    print('Raised error: %s' % e)

        elif type(tables) is str:
            # same as above but for a single table
            if path:
                file_ = path + tables + '.' + file_type
            else:
                file_ = tables + '.' + file_type
            # extract the dataframe
            df = self.select(table=tables)
            # export data
            self.export_(df, file_, file_type, json_orient)
        else:
            print('tables must be a string or a list of strings')
            return
    
