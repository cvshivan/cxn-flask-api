from cgi import print_directory
from operator import truth
from flask import Flask, json
import pandas as pd
from flask import request, make_response, jsonify

from my_app import index


json_match_datatypes={
    "string":["string","varchar"],
    "object":["varchar"],
    "int":["int","integer"],
    "integer":["int","integer"],
    "float":["float","decimal","number"],
    "decimal":["float","decimal","number"],
    "number":["float","decimal","number"]
}

def check_source_target_column_compatibility(df_mapping):

    match = True

    for row in df_mapping.itertuples(index=True, name='Pandas'):

        s_type = row.source_column_type
        t_type = row.target_column_type

        if s_type in json_match_datatypes:

            li = json_match_datatypes[s_type]

            if t_type not in li:
                print(s_type)
                print(json_match_datatypes[s_type])
                match = False
                break


    return match

def convert_source_column_datatype(df,df_mapping):

    for row in df_mapping.itertuples(index=True, name='Pandas'):

        source_column_name = row.source_column_name
        target_column_name = row.target_column_name

        source_type = row.source_column_type
        target_type = row.target_column_type


        # if s_type != t_type:
            # if 

        # df[source_column_name] = pd.to_numeric(df.pa_2017ID, errors='coerce')
        # df[s_col_name] = pd.to_numeric(df.pa_2017ID, errors='coerce')


    return df

def rename_source_column_to_target_column(df_data,df_mapping):    

    for row in df_mapping.itertuples(index=True, name='Pandas'):

        source_column_name = row.source_column_name
        target_column_name = row.target_column_name

        if source_column_name != target_column_name:

            df_data.rename(columns={source_column_name:target_column_name},inplace=True)

            # df.rename(columns = {'date':'tdate'}, inplace = True)

    return df_data