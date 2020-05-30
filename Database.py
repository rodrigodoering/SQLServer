import pyodbc
import pandas as pd
from functools import wraps
from datetime import datetime


def control_transactions(Obj_SQLServer):
    require_connection = [
        'execute_statement'
        'query_data',
        'list_database',
        'get_servername',
        'get_database',
        'create_database',
        'set_database',
        'detail_table', 
        'select', 
        'list_tables', 
        'insert', 
        'export_to_file'
    ]
    
    class Handler(Obj_SQLServer):

        def __init__(self,*args,**kwargs):
            super().__init__(*args,**kwargs)
        
        def __str__(self):
            info_labels = ['Server:', 'Database:', 'User:', 'DSN:', 'Driver:', 'Connected:']
            info_values = [self.current_server, self.current_database, self.user, self.dsn, self.driver, self.connected]
            repr_values = [
                label +' '+ Handler.handle_db_info(info) + '\n' 
                for label, info in zip(info_labels, info_values)
            ]
            return ''.join(repr_values)

        def __getattribute__(self, name):
            attribute = object.__getattribute__(self, name)
            if not callable(attribute):
                return attribute
            else:
                if attribute.__name__ in require_connection:
                    if self.connected:
                        return attribute
                    else:
                        return Handler.connect_notification
                else:
                    return attribute

        @staticmethod
        def connect_notification(*args, **kwargs):
            print('No connection to SQL Server, use SQLServer.connect(auth)')
            return None

        @staticmethod
        def handle_db_info(db_attr_):
            if isinstance(db_attr_, bool) or db_attr_ is None:
                return str(db_attr_)
            else:
                return db_attr_        
    return Handler


class DatabaseCustomException(Exception):
    def __init__(self, message):
        self.error_str = message
        super(DatabaseCustomException, self).__init__(message)
    
    def error_kind(self):
        if 'Invalid object name' in self.error_str:
            return 'Invalid object error'
        else:
            return self.error_str


@control_transactions
class SQLServer(object):

    def __init__(self, driver=None, server=None, user=None, password=None, dsn=None, database=None):
        self.driver = driver
        self.server= server
        self.user = user
        self.password = password
        self.dsn = dsn
        self.database = database
        self.connected = False
        self.current_database = None
        self.current_server = None



    def connect(self, authentication='sql', autocommit=False):
        conn_values = [self.driver, self.server, self.database, self.user, self.password]
        conn_objs = ['Driver', 'Server', 'Database', 'Uid', 'Pwd']
        if authentication != 'windows' and authentication != 'sql':
            raise Exception("%s isn't a valid authentication mode, pass either 'windows' or 'sql'")
        elif self.dsn:
            dns_str_ = 'DSN=%s' % self.dsn
            uid_pwd_str = ';'.join(
                [
                    '{}={}'.format(obj,val) 
                    for obj, val in zip(conn_objs[-2:], conn_values[-2:]) 
                    if val
                ]
            )

            self.connection_string = '{};{}'.format(dns_str_, uid_pwd_str)
        else:
            self.connection_string = ';'.join(
                [
                    '{}={}'.format(obj,val) 
                    for obj, val in zip(conn_objs, conn_values) 
                    if val
                ]
            )

            if authentication == 'windows':
                self.connection_string += ';Trusted_Connection=Yes'
        try:
            self.connection = pyodbc.connect(self.connection_string, autocommit=autocommit)

        except Exception as e:
            self.conn_error = e
            print('Erro %s' % e)

        else:
            self.connected = True
            # sets cursor
            self.cursor = self.connection.cursor()
            # get current database 
            self.current_database = self.get_database()
            # get current server instance
            self.current_server = self.get_servername()           
            if self.dsn:
                print('Connection established with %s\nUsing Database: %s' % (self.dsn, self.current_database))              
            else:
                print('Connection established with %s\nUsing Database: %s' % (self.server, self.current_database))
            print('Use set_database(database) method to use a specific database')



    def close_connection(self):
        self.connection.close()
        self.connected = False



    def execute_statement(self, statement, commit=True):
        self.cursor.execute(statement)
        if commit:
            self.connection.commit()



    def query_data(self, statement):
        try:
            self.cursor.execute(statement)
            return (row for row in self.cursor.fetchall())
        except pyodbc.ProgrammingError as error:
            print('Problem running statement: {}'.format(statement))
            return None
        except Exception as error:
            print('Unexpected Error executing query: %s' % error)
            print('Returning None')
            return None



    def get_servername(self):
        return next(self.query_data('select @@SERVERNAME'))[0]



    def get_database(self):
        return next(self.query_data('select db_name()'))[0]



    def create_database(self, database_name):
        self.connection.autocommit = True
        self.execute_statement('create database %s' % database_name)
        self.connection.autocommit = False
        print('%s created' % database_name)



    def list_database(self):
        statement = 'SELECT name FROM master.dbo.sysdatabases'
        return [row[0] for row in self.query_data(statement)]



    def set_database(self, database):
        if database == self.current_database:
            print('%s already being used' % database)
        else:
            self.execute_statement('USE %s' % database)
            self.current_database = database
            print('Database %s accessed' % database)



    def detail_table(self, table, dataframe=True):
        sql_statement = "SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s'" % table
        columns = ['COLUMN_NAME', 'IS_NULLABLE', 'DATA_TYPE']
        output_data = (row for row in self.query_data(sql_statement))
        data = [
            {c:v for c,v in zip(columns,tupla)} 
            for tupla in output_data
        ]
        if dataframe:
            return pd.DataFrame(data)
        else:
            return data



    @staticmethod
    def format_select_statement(table, percent, columns, condition, schema):      
        select_ = 'select '
        columns_ = '*'
        filter_ = ' from %s' % table

        if schema:
            table =  '{}.{}'.format(schema, table)  
            filter_ = ' from %s' % table

        if percent:
            select_ = 'select top(%s) percent ' % percent

        if columns:
            if isinstance(columns, str):
                columns_ = columns
                columns = columns.replace(' ','').split(",") 
            elif isinstance(columns, list):
                columns_ = ', '.join(columns)
            else:
                print('Invalid columns structure passed, setting columns to default "*"') 
                
        if condition:
            filter_ = '{} {}'.format(filter_, condition) 
        return '{}{}{}'.format(select_, columns_, filter_) , columns 



    def select(self, table, percent=None, columns=None, condition=None, dataframe=True, schema=None, verbose=True):  
        sql_statement, df_columns = SQLServer.format_select_statement(table, percent, columns, condition, schema)
        output_data = self.query_data(sql_statement)

        if not output_data:
            print('Error while executing query, attempting to query table with schema')
            try:
                schema_statement = "SELECT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'" % table
                print(schema_statement)
                schema = next(self.query_data(schema_statement))[0]
                sql_statement, df_columns = SQLServer.format_select_statement(table, percent, columns, condition, schema)
                output_data = self.query_data(sql_statement)
                if not output_data:
                    print("Coudn't extract data, returning None")
                    return None

            except Exception as e:
                print('Attempt failed, error: %s' % e)
                print('Returning NoneType')
                return None

        data = (row for row in output_data)

        if not columns:
            df_columns = self.detail_table(table).COLUMN_NAME.values.tolist()
        if dataframe:
            return pd.DataFrame(
                (
                    {c:v for c,v in zip(df_columns,tupla)} 
                    for tupla in data
                )
            )
        else:
            if len(df_columns) == 1:
                return [v for row in data for v in row]
            else:
                return [
                    {c:v for c,v in zip(df_columns,tupla)} 
                    for tupla in data
                ]



    def list_tables(self, pandaSeries=False, from_database=False, output='dataframe'):
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
            return None


    def insert(self, df, table):
        # Attempts to insert dataframe into table
        # This function requires dataframe columns to match with database table columns otherwise it wont work
        # Still needs a lot of improvement
        # create sql statement
        insert = 'insert into %s ' % table
        columns = '({})'.format(','.join(df.columns))

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
            insert_stm = '{}{}{}'.format(insert, columns, string_values)
            print('Row %d out of %d' % (i, df_insert.shape[0]))

            try:
                # insert values
                self.execute_statement(insert_stm)
            
            except Exception as e:
                print('Unexpected error occured')
                print('SQL Statement: {}'.format(sql))
                print('Raised Error: {}'.format(e))


    @staticmethod
    def export_(self, df, file_, file_type, json_orient='index'):
        # Export data from dataframe to specified file path, format and json orientation (if passed)
        # Created primarily to be used within export_to_file function, although it might be used separately as well
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
            return None


    def export_to_file(self, tables=None, database='current', file_type='csv', json_orient='index', path=None):
        # Export tables as flat files
        # If table is None, all existing tables will be exported
        # Table param can be either a single table (string) or a list of tables
        # A database different from the current one can be passed
        # Format can be "csv" (default), "xlsx", "txt" or "json"
        # If format is json, it requires an json_orient param (by default is 'index')
        # It will generate one file per table
        if file_type not in ['csv', 'xlsx', 'txt', 'json']:
            # print notification
            print('Choose a valid format: csv, xlsx, txt or json')
            return None

        # format path 
        if path and path[-1] != '\\':
            path += '\\'
        
        if database != 'current':
            # set passed database
            self.set_database(database)

        if not tables:
            # extract list of tables in database
            tables = self.list_tables(pandaSeries=False, output='list')
        
        if isinstance(tables, list):
            # create list with tables to be exported
            if path:
                # adds path to save table
                files = list(map(lambda table: path + table + '.' + file_type, tables))    

            else:
                files = list(map(lambda table: table + '.' + file_type, tables)) 

            # loop through tablelist
            for file_ , table in zip(files, tables):
                # extract table as dataframe
                print('Exporting table %s as %s' % (table, file_type))

                try:
                    df = self.select(table=table)
                    # export dataframe 
                    SQLServer.export_(df, file_, file_type, json_orient)

                except Exception as e:
                    print('Error extracting data from: %s' % table)
                    print('Raised error: %s' % e)

        elif isinstance(tables, str):
            # same as above but for a single table
            if path:
                file_ = path + tables + '.' + file_type
                
            else:
                file_ = tables + '.' + file_type
            # extract the dataframe
            df = self.select(table=tables)
            # export data
            SQLServer.export_(df, file_, file_type, json_orient)

        else:
            print('Tables must be either a string or a list of strings')
            return None
