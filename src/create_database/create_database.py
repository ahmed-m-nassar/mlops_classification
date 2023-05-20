import psycopg2
import pandas as pd
import numpy as np
import os
import yaml
import argparse


def read_params_file(file_path):
    '''
    Reads the yaml parameters file 
    '''
    with open(file_path) as yaml_file:
        params = yaml.safe_load(yaml_file)
    return params
    
    
def connect_to_database(host , dbname , user , password , port) :
    '''
    Returns a database connection
    '''
    return  psycopg2.connect(host =  host, dbname = dbname , user  = user ,
                        password = password , port = port)
    
if __name__ == "__main__" :
    params = read_params_file('params.yaml')
    database_connection = connect_to_database(host = params['database']['config']['host'] , 
                                              dbname =  params['database']['config']['dbname'] ,
                                              user = params['database']['config']['user'] ,
                                              password =  params['database']['config']['password'] ,
                                              port =  params['database']['config']['port'] )   
    print(database_connection) 