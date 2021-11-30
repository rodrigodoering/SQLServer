# -*- coding: utf-8 -*-
"""
Created on Mon Jul 12 15:39:15 2021

@author: rodri
"""
import pandas as pd
import numpy as np


def select(table, percent, columns, condition, schema, return_cols=True):      
    select_ = 'SELECT '
    columns_ = '*'
    filter_ = ' FROM %s' % table

    if schema:
        table =  '{}.{}'.format(schema, table)  
        filter_ = ' FROM %s' % table

    if percent:
        select_ = 'SELECT TOP(%s) PERCENT ' % percent

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
    
    if return_cols:
        return '{}{}{}'.format(select_, columns_, filter_) , columns
    else:
        return '{}{}{}'.format(select_, columns_, filter_) 

'''
def select(table, percent, columns, where, order_by, group_by, join, on, schema, return_cols=True):      
    select_ = 'SELECT '
    columns_ = '*'
    filter_ = ' FROM %s' % table

    if schema:
        table =  '{}.{}'.format(schema, table)  
        filter_ = ' FROM %s' % table

    if percent:
        select_ = 'SELECT TOP(%s) PERCENT ' % percent

    if columns:
        if isinstance(columns, str):
            columns_ = columns
            columns = columns.replace(' ','').split(",") 
        elif isinstance(columns, list):
            columns_ = ', '.join(columns)
        else:
            print('Invalid columns structure passed, setting columns to default "*"') 
            
    if where:
        filter_ = '{} {}'.format(filter_, where) 
    
    if return_cols:
        return '{}{}{}'.format(select_, columns_, filter_) , columns
    else:
        return '{}{}{}'.format(select_, columns_, filter_)
    
'''    

def insert(table, columns):
    columns_str = '({})'.format(','.join(columns))
    return 'INSERT INTO %s %s VALUES ' % (table, columns_str)



def update(table, column):
    return 'UPDATE %s SET %s = ' % (table, column)


def char(_str_):
    if _str_ is None:
        return 'NULL'
    elif isinstance(_str_, (int, float, np.float64, np.int64)):
        if np.isnan(_str_):
            return 'NULL'
        else:
            return str(_str_)
    if "'" in _str_:
        _str_ = _str_.replace("'", '')
    
    if _str_ == 'NULL':
        return _str_
    return "'%s'" % _str_



def values(values, n_rows): 
    strings = ('(' + ', '.join([str(value) for value in values[i]]) + ')' for i in range(n_rows))
    return ','.join(strings)


def case(ref_col, when_vals, then_vals):
    when_vals = [char(val) for val in when_vals]
    then_vals = [char(val) for val in then_vals]
    _case_ = 'WHEN {} = {} THEN {}'
    values = ' '.join(
        [
            _case_.format(ref_col, when, then) 
            for when, then in zip(when_vals, then_vals)
        ]
    )
    return '{} {}'.format('CASE', values)
    

def bit_format(x):                                                                                            
    if type(x) is bool:
        return '1' if x else '0'
    elif type(x) is str:
        if x == '1':
            return '1'
        elif x == '0' or x == '-1':
            return '0'
        else:
            return 'NULL'
    else:
        return 'NULL'



def char_format(x):    
    _str_ = str(simple_format(x))
    if _str_ == 'NULL':
        return _str_   
    else:
        if "'" in _str_:
            _str_ = _str_.replace("'", '')

        return "'%s'" % _str_


def date_format(x):
    if pd.isnull(x):
        return 'NULL'
    else:
        return "'%s'" % x


def simple_format(x):
    if x is None:
        return 'NULL'
    elif isinstance(x, (int, float, np.float64, np.int64)):
        if np.isnan(x):
            return 'NULL'
    return str(x)

        