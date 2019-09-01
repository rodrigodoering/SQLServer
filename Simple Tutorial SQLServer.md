
Importing `SQLServer`


```python
from SQLServer import *
```

Making instance and connecting to SQL Server via Data Source Name with windows authentication


```python
sql = SQLServer(dsn='DSN_TEST', auth='windows')
```


```python
sql.connect()
```

    Connection established with DSN_TEST
    Using Database: master
    Use set_database(database) method to use a specific database
    

Using `list_database` method to show existing databases


```python
databases = sql.list_database()
databases[:4]
```




    ['master', 'tempdb', 'model', 'msdb']



Using `set_database` method to access desired database


```python
sql.set_database('AdventureWorks2017')
```

    Database AdventureWorks2017 accessed
    

`list_tables` method has two possible output options: __dataframe__ or __list__

as dataframe:


```python
tables_df = sql.list_tables(output='dataframe')
tables.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>TABLE_NAME</th>
      <th>TABLE_SCHEMA</th>
      <th>TABLE_TYPE</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>EmployeePayHistory</td>
      <td>HumanResources</td>
      <td>BASE TABLE</td>
    </tr>
    <tr>
      <th>1</th>
      <td>SalesOrderHeaderSalesReason</td>
      <td>Sales</td>
      <td>BASE TABLE</td>
    </tr>
    <tr>
      <th>2</th>
      <td>SalesPerson</td>
      <td>Sales</td>
      <td>BASE TABLE</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Illustration</td>
      <td>Production</td>
      <td>BASE TABLE</td>
    </tr>
    <tr>
      <th>4</th>
      <td>JobCandidate</td>
      <td>HumanResources</td>
      <td>BASE TABLE</td>
    </tr>
  </tbody>
</table>
</div>



as list:


```python
tables_list = sql.list_tables(output='list')
tables_list[:5]
```




    ['EmployeePayHistory',
     'SalesOrderHeaderSalesReason',
     'SalesPerson',
     'Illustration',
     'JobCandidate']



Using `detail_table` method to show information about a specific table


```python
sql.detail_table('SalesPerson')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>COLUMN_NAME</th>
      <th>DATA_TYPE</th>
      <th>IS_NULLABLE</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>BusinessEntityID</td>
      <td>int</td>
      <td>NO</td>
    </tr>
    <tr>
      <th>1</th>
      <td>TerritoryID</td>
      <td>int</td>
      <td>YES</td>
    </tr>
    <tr>
      <th>2</th>
      <td>SalesQuota</td>
      <td>money</td>
      <td>YES</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Bonus</td>
      <td>money</td>
      <td>NO</td>
    </tr>
    <tr>
      <th>4</th>
      <td>CommissionPct</td>
      <td>smallmoney</td>
      <td>NO</td>
    </tr>
    <tr>
      <th>5</th>
      <td>SalesYTD</td>
      <td>money</td>
      <td>NO</td>
    </tr>
    <tr>
      <th>6</th>
      <td>SalesLastYear</td>
      <td>money</td>
      <td>NO</td>
    </tr>
    <tr>
      <th>7</th>
      <td>rowguid</td>
      <td>uniqueidentifier</td>
      <td>NO</td>
    </tr>
    <tr>
      <th>8</th>
      <td>ModifiedDate</td>
      <td>datetime</td>
      <td>NO</td>
    </tr>
  </tbody>
</table>
</div>



Extracting data from database with specific params using `select` method


```python
df = sql.select(table='SalesPerson', columns='Bonus, SalesYTD, SalesLastYear', condition='where Bonus < 4000', schema='Sales')
df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Bonus</th>
      <th>SalesLastYear</th>
      <th>SalesYTD</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0.0000</td>
      <td>0.0000</td>
      <td>559697.5639</td>
    </tr>
    <tr>
      <th>1</th>
      <td>4100.0000</td>
      <td>1750406.4785</td>
      <td>3763178.1787</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2000.0000</td>
      <td>1439156.0291</td>
      <td>4251368.5497</td>
    </tr>
    <tr>
      <th>3</th>
      <td>2500.0000</td>
      <td>1997186.2037</td>
      <td>3189418.3662</td>
    </tr>
    <tr>
      <th>4</th>
      <td>500.0000</td>
      <td>1620276.8966</td>
      <td>1453719.4653</td>
    </tr>
  </tbody>
</table>
</div>



Exporting a specific table from database as csv format using `export_to_file` method

We used as example a specific single table, however, `tables` param supports also a list of tables to be passed. If no tables are passed, it will be setted as Nonetype (default) and all database tables will be exported.

It's also possible to export tables from another database without needing to manually access the database with `set_database` method. 

Example: 

    sql.export_to_file(tables=['table1', 'table2', ... 'tableN'], database='Desired Database')

`database` by default is 'current'

Moreover, the following file formats are also supported: xlsx, json and txt.

If `file_type` param is passed as __json__, you can also pass json orientation through `json_orient` param


```python
sql.export_to_file(tables='Illustration', format_='csv')
```

    Error while executing query, attempting to query table with schema
    

Checking the data we just exported


```python
Illustration = pd.read_csv('Illustration.csv')
Illustration
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Unnamed: 0</th>
      <th>Diagram</th>
      <th>IllustrationID</th>
      <th>ModifiedDate</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>0</td>
      <td>&lt;!-- Generated by Adobe Illustrator CS -&gt; XAML...</td>
      <td>3</td>
      <td>2014-01-09 13:06:11.780</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1</td>
      <td>&lt;!-- Generated by Adobe Illustrator CS -&gt; XAML...</td>
      <td>4</td>
      <td>2014-01-09 13:06:11.903</td>
    </tr>
    <tr>
      <th>2</th>
      <td>2</td>
      <td>&lt;!-- Generated by Adobe Illustrator CS -&gt; XAML...</td>
      <td>5</td>
      <td>2014-01-09 13:06:11.950</td>
    </tr>
    <tr>
      <th>3</th>
      <td>3</td>
      <td>&lt;!-- Generated by Adobe Illustrator CS -&gt; XAML...</td>
      <td>6</td>
      <td>2014-01-09 13:06:12.043</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>&lt;!-- Generated by Adobe Illustrator CS -&gt; XAML...</td>
      <td>7</td>
      <td>2014-01-09 13:06:12.080</td>
    </tr>
  </tbody>
</table>
</div>



Essentially, `query` method was designed as the most important method. Its the function that actually uses pyodbc connection to interacts with database. It's internally called on pretty much every function demonstrated in this notebook so far.

However, it can be used to perform other more specific taskes which are not contemplated by other functions, here we create a new table.

NOTE: 

Whenever a transaction must be done, use `commit` param as True. If commit is true, this function understands that a transaction must be made, so no data will be returned. If commit is False (default), it understands that some data is expected to be outputed, and return_option ('raw', 'single value' or 'list') param will take action.



```python
create_tb = 'create table tb_test (col_one int not null, col_two int not null, col_three varchar(10) not null)'
sql.query(create_tb, commit=True)
```

Checks if the table has been properly created


```python
sql.detail_table('tb_test')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>COLUMN_NAME</th>
      <th>DATA_TYPE</th>
      <th>IS_NULLABLE</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>col_one</td>
      <td>int</td>
      <td>NO</td>
    </tr>
    <tr>
      <th>1</th>
      <td>col_two</td>
      <td>int</td>
      <td>NO</td>
    </tr>
    <tr>
      <th>2</th>
      <td>col_three</td>
      <td>varchar</td>
      <td>NO</td>
    </tr>
  </tbody>
</table>
</div>



As expected table is empty, therefore no dataframe can be constructed from it


```python
sql.select('tb_test')
```

    No data returned from query, returning None
    

Import pandas and crate a new dataframe so that we can insert some values into our created table


```python
import pandas as pd
insert_dataframe = pd.DataFrame({'col_one':[1,2,3,4], 
                                 'col_two':[3,5,6,6], 
                                 'col_three':['a','b','c','d']})
insert_dataframe
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>col_one</th>
      <th>col_three</th>
      <th>col_two</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>a</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>b</td>
      <td>5</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>c</td>
      <td>6</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>d</td>
      <td>6</td>
    </tr>
  </tbody>
</table>
</div>



Using `insert` method to add our data into __tb_test__. 

NOTE:

For now, this function is very simple and there isn't any procedure to validate if input dataframe columns matches perfectly sql table's columns (regarding column's name, not position). Ergo, make sure that input dataframe has exactly the same columns as database table otherwise it will throw an error.  


```python
sql.insert(insert_dataframe, 'tb_test')
```

Checking if data was properly inserted 


```python
sql.select('tb_test')
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>col_one</th>
      <th>col_three</th>
      <th>col_two</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1</td>
      <td>a</td>
      <td>3</td>
    </tr>
    <tr>
      <th>1</th>
      <td>2</td>
      <td>b</td>
      <td>5</td>
    </tr>
    <tr>
      <th>2</th>
      <td>3</td>
      <td>c</td>
      <td>6</td>
    </tr>
    <tr>
      <th>3</th>
      <td>4</td>
      <td>d</td>
      <td>6</td>
    </tr>
  </tbody>
</table>
</div>


