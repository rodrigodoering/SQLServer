# -*- coding: utf-8 -*-
"""
Spyder Editor

Script para desenvolvimento.
"""

import pyodbc
import TransactSQL
import pandas as pd


def control_transactions(Obj_SQLServer):
    require_connection = [
        'run'
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
        
        ## HANDLER OWN FUNCTIONS FOR NOTIFICATION AND DOCUMENTING 
        ## STATIC - DEFINED AS STATIC METHODS WITHIN CLASS DEF. FOR SIMPLICITY 
        
        @staticmethod
        def connect_notification(*args, **kwargs):
            print('No connection to SQL Server, use SQLServer.connect()')
            return None

        @staticmethod
        def handle_db_info(db_attr_):
            if isinstance(db_attr_, bool) or db_attr_ is None:
                return str(db_attr_)
            else:
                return db_attr_       


        ## MANAGE SQLServer INSTANCE 

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
            """
            First, handler evaluate whether the user is calling for a method or attribute. 
            If not, by definition it wont require connection, therefore no error SHOULD happen even without stablished connection
            
            """
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

            
    return Handler



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


    def connect(self, database=None, authentication='sql', autocommit=False):
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
            
            if database is None:
                print('Use set_database(database) method to use a specific database. Using default database')
            
            elif database in self.list_database():
                self.set_database(database)
            
            else:
                print('Unable to find database %s.\nListing existing databases:\n' % str(database))
                for _db_ in self.list_database():
                    print('- ', _db_)
                print()
            
            if self.dsn:
                print('Connection established with %s\nUsing Database: %s' % (self.dsn, self.current_database))              
            else:
                print('Connection established with %s\nUsing Database: %s' % (self.server, self.current_database))
            


    def close_connection(self):
        self.connection.close()
        self.connected = False


    def run(self, statement, commit=True):
        self.cursor.execute(statement)
        if commit:
            self.connection.commit()


    def query_data(self, statement):
        try:
            self.run(statement, commit=False)
        except pyodbc.ProgrammingError:
            print('Problem running statement: {}'.format(statement))
            return None
        except Exception as error:
            print('Unexpected Error executing query: %s' % error)
            print('Returning None')
            return None
        else:
            return (row for row in self.cursor.fetchall())
        
        
    def query(self, statement):
        try:
            self.run(statement, commit=False)
        except pyodbc.ProgrammingError:
            print('Problem running statement: {}'.format(statement))
            return None
        except Exception as error:
            print('Unexpected Error executing query: %s' % error)
            print('Returning None')
            return None
        else:
            for row in self.cursor.fetchall():
                yield row
        
        
    def queryData(self, statement, dataframe=True):
        output = self.query(statement)
        
        if output is None:
            return None
        
        else:
            if not dataframe:
                return [row for row in output]
            
            else:
                statement = statement.lower().replace('\n', '')
                start = statement.find('select')
                end = statement.find('from')
                columns = statement[start+ len('select') + 1:end - len('from')].replace(' ','').split(',')
                return pd.DataFrame([{col:x for x, col in zip(d, columns)} for d in output])
            
        

    def get_servername(self):
        return next(self.query_data('select @@SERVERNAME'))[0]


    def get_database(self):
        return next(self.query_data('select db_name()'))[0]


    def create_database(self, database_name):
        self.connection.autocommit = True
        self.run('create database %s' % database_name)
        self.connection.autocommit = False
        print('%s created' % database_name)


    def list_database(self):
        statement = 'SELECT name FROM master.dbo.sysdatabases'
        return [row[0] for row in self.query_data(statement)]


    def set_database(self, database):
        if database == self.current_database:
            print('%s already being used' % database)
        else:
            self.run('USE %s' % database)
            self.current_database = database
            print('Database %s accessed' % database)

    
    
    def detail_table(self, table, dataframe=True):
        select_stm = TransactSQL.select(
            table='INFORMATION_SCHEMA.COLUMNS',
            percent=None,
            columns='COLUMN_NAME, IS_NULLABLE, DATA_TYPE',
            condition= "WHERE TABLE_NAME = '%s'" % table,
            schema=None,
            return_cols=False
        )
        output_data = (row for row in self.query_data(select_stm))

        data = [
            {c:v for c,v in zip(['COLUMN_NAME', 'IS_NULLABLE', 'DATA_TYPE'], tupla)} 
            for tupla in output_data
        ]
        if dataframe:
            return pd.DataFrame(data)
        else:
            return data


    def select(self, table, percent=None, columns=None, condition=None, dataframe=True, schema=None, verbose=True):  
        sql_statement, df_columns = TransactSQL.select(table, percent, columns, condition, schema)
        output_data = self.query_data(sql_statement)

        if not output_data:
            print('Error while executing query, attempting to query table with schema')
            try:
                schema_statement = "SELECT TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '%s'" % table
                print(schema_statement)
                schema = next(self.query_data(schema_statement))[0]
                sql_statement, df_columns = TransactSQL.select(table, percent, columns, condition, schema)
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
                ({c:v for c,v in zip(df_columns, tupla)} for tupla in data)
            )
        else:
            if len(df_columns) == 1:
                return [v for row in data for v in row]
            else:
                return [
                    {c:v for c,v in zip(df_columns, tupla)} 
                    for tupla in data
                ]



    def list_tables(self, pandaSeries=False, from_database=False, output='dataframe'):
        if from_database:
            self.set_database(from_database)
        if output == 'dataframe':
            return self.select(table='INFORMATION_SCHEMA.TABLES', columns='TABLE_SCHEMA, TABLE_NAME, TABLE_TYPE')        
        elif output == 'list':
            tables = self.select(table='INFORMATION_SCHEMA.TABLES', columns='TABLE_NAME')
            return  tables.TABLE_NAME.values.tolist()        
        else:
            raise Exception('Pass a valid output: "dataframe" or "list"')


                 
    def batch_dataframe(self, df, batch=None, yield_n_rows=True, verbose=True, dataframe=False):
        rows = df.shape[0]
        if batch is None:
            if rows > 1000:
                batch = 1000
            else:
                batch = rows
                
        partitions = int(rows / batch)
        last_batch = rows - (partitions * batch)
        n_batches = partitions if last_batch == 0 else partitions + 1
        
        for i in range(n_batches):
            s = batch * i
            e = batch * (i + 1) if i != partitions else s + last_batch
            
            if verbose:
                print('Batch %d out of %d available' % (i + 1,n_batches))
            
            data = df.iloc[s:e] if dataframe else df.iloc[s:e].values
            
            if yield_n_rows:
                yield e-s, data
            else:
                yield data
                
               
    
    def store(self, df, table, batch_size=None, update=False, verbose=True):
        insert = TransactSQL.insert(table, df.columns)
        df_insert = df.copy()
        dtypes = self.detail_table(table).set_index('COLUMN_NAME')
        
        for col in df_insert:
            dtype = dtypes.loc[col, 'DATA_TYPE']
            
            if dtype in ('varchar', 'char'):
                df_insert[col] = df_insert[col].apply(TransactSQL.char_format)
                
            elif dtype == 'bit':
                df_insert[col] = df_insert[col].apply(TransactSQL.bit_format)
              
            elif dtype == 'date':
                df_insert[col] = df_insert[col].apply(TransactSQL.date_format)
                
            else:
                df_insert[col] = df_insert[col].apply(TransactSQL.simple_format)
            
            
        for n_rows, batch in self.batch_dataframe(df_insert, batch_size, verbose=verbose):
            insert_stm = '{}{}'.format(insert, TransactSQL.values(batch, n_rows))
            
            try:
                self.run(insert_stm)
            except Exception as e:
                print('Unexpected error occured')
                print('Raised Error: {}'.format(e))
                print('Statement:\n\n', insert_stm)
    


    
    def update(self, table, target_col, ref_col, ref_vals, update_vals, where=None, batch=None):
        df_vals = pd.DataFrame([ref_vals, update_vals]).T
        for n_rows, batch in self.batch_dataframe(df_vals, batch=batch, verbose=False):
            start = TransactSQL.update(table, target_col)
            conditions = TransactSQL.case(ref_col, batch[:, 0], batch[:, 1])
            stm_values = [start, conditions, 'ELSE %s' % target_col]
            
            if where is not None and type(where) is str:
                stm_values.append(where)
                
            stm_values.append('END')
            update_stm = ' '.join(stm_values)
    
            try:
                self.run(update_stm)
            except Exception as e:
                print('Unexpected error occured')
                print('Raised Error: {}'.format(e))
        
        
        
    def export(self, table, columns=None, condition=None, schema=None, database='current', file_type='csv', path=None, **kwargs):        
        if database != 'current':
            self.set_database(database)
        
        try:
            data = self.select(table, columns=columns, condition=condition, schema=schema, dataframe=True)
            
        except Exception as e:
            print('Error extracting data from: %s' % table)
            print('Raised error: %s' % e)
            
        else:
            if path is None:
                file_ = '{}.{}'.format(table, file_type)
                
            elif type(path) is str:
                if path[-1] != '\\':
                    path += '\\'
                file_ = '{}{}.{}'.format(path, table, file_type)
            
            print(file_)
            
            if file_type == 'csv':
                data.to_csv(file_, index=False, **kwargs)
    
            elif file_type == 'xlsx':
                data.to_excel(file_, index=False, **kwargs)
    
            elif file_type == 'json':
                data.to_json(file_, **kwargs)  
    
            elif file_type == 'txt':
                columns = data.columns
                with open(file_, 'w') as out:
                    data.to_string(out, columns=columns, **kwargs)
            else:
                raise Exception('Choose a valid format: csv, xlsx, txt or json')


    

    



    
    
    
    
    
    
    
    
    
    
