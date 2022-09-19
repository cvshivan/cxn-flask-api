
# from crypt import methods
from flask import Flask, json
from flask import request, make_response, jsonify
from flask_cors import CORS, cross_origin
import pandas as pd
import pymysql
from sqlalchemy import create_engine, text
# from config import SQLALCHEMY_DATABASE_PEM
# from pyspark.sql.types import *
from decimal import *


import api.db_snowflake as db
import api.connector
import api.file as fl
import api.dataframe_helper as df_helper


from __main__ import app


@app.route('/connector', methods=['GET'])
def connnector():

    args = request.args

    print("args ...")
    print(type(args.to_dict()))

    # cnx = create_engine('mysql+pymysql://root:ClassicMySQL3306@localhost/cxn')
    cnx = create_engine(app.config['SQLALCHEMY_DATABASE_URI'], echo=True)
    df = pd.read_sql('SELECT * FROM connector', cnx)  # read the entire table

    print(df)

    # data = {'name': 'nabin khadka'}

    data = df.to_json()

    response = app.response_class(response=json.dumps(data),
                                  status=200,
                                  mimetype='application/json')

    return response

    # return 'Connector'


@app.route('/list_connector_mapping', methods=['GET'])
def list_connector_mapping():

    # cnx = create_engine('mysql+pymysql://root:ClassicMySQL3306@localhost/cxn')
    cnx = create_engine(app.config['SQLALCHEMY_DATABASE_URI'], echo=True)

    df = pd.read_sql('SELECT * FROM connector_mapping',
                     cnx)  # read the entire table

    print(df)

    data = df.to_json()

    response = app.response_class(response=json.dumps(data),
                                  status=200,
                                  mimetype='application/json')

    # return jsonify(data),200
    return response


# Checkes whether the target_type is allowed list
def validate_target_type(target_type):

    target_types = ['snowflake']

    if target_type.lower() not in target_types:
        return False

    return True

# Checks whether the target table name is the allowed list


def validate_target_table_name(target_table_name):

    target_tables = ['oh', 'warehouse']

    if target_table_name.lower() not in target_tables:
        return False

    return True


def validate_target_structure_snowflake():
    f = 1


def validate_source_structure_csv(connector_id, df):

    # df_derived = pd.DataFrame(columns=['Bracket','Value'])
    # df_derived = pd.DataFrame(columns=['BRACKET','VALUE'])

    df.columns = df.columns.str.lower()

    print(df)

    df_mapping = api.connector.df_connector_mapping_connector_id(connector_id)

    # df_mapping.columns= df_mapping.columns.str.lower()

    print(df_mapping)

    df_derived = pd.DataFrame()

    print(df_derived)

    for ind in df_mapping.index:
        print(df_mapping['source_column_name'][ind],
              df_mapping['source_column_type'][ind])

        col_name = df_mapping['source_column_name'][ind].lower()
        col_type = df_mapping['source_column_type'][ind]

        # df_derived[col_name] =
        # df_derived[df_mapping['SourceColumnType'][ind]] = ''

        if col_type.lower() == 'int64':
            df_derived[col_name] = 0
            # df_derived = df_derived[col_name].astype(col_type,False,'ignore')
            df_derived[col_name] = df_derived[col_name].astype(col_type)
        else:
            df_derived[col_name] = 'object'

    # df_derived.columns= df_derived.columns.str.lower()

    print(df_derived)

    # print('Difference ...')
    # print(df_derived.columns.difference(df.columns))
    # diff = df_derived.columns.difference(df.columns)
    diff_csv = df_derived.columns.difference(df.columns)
    diff_csv_list = diff_csv.tolist()

    diff_mapping = df.columns.difference(df_derived.columns)
    diff_mapping_list = diff_mapping.tolist()


    # print(type(diff))
    # print(diff.tolist())
    if len(diff_csv_list) > 0:
        print('Columns missing in csv - ' + str(diff_csv_list) )
        return False

    if len(diff_mapping_list) > 0:
        print('Columns missing in mapping - ' + str(diff_mapping_list) )
        return False


    return True



@app.route('/connector/<int:connector_id>', methods=['POST'])
@cross_origin()
def connnector_connector_id(connector_id):

    args = request.args

    print("args ...")
    print(type(args.to_dict()))

    file = request.files['file']

    df = fl.fill_file_to_dataframe(file)

    print(df)


    if df is None:
        data = {'message': 'Unsupported Media Type'}
        response = app.response_class(response=json.dumps(data),
                                      status=415,
                                      mimetype='application/json')

        print(response.status_code)
        print(response.message)

        return response


    # return

    # df = pd.read_csv(request.files.get('file'))

    # cnx = create_engine('mysql+pymysql://root:ClassicMySQL3306@localhost/cxn')
    cnx = create_engine(app.config['SQLALCHEMY_DATABASE_URI'], echo=True)

    # df_connector = pd.read_sql('SELECT * FROM connector WHERE connector_id=' + str(connector_id), cnx)  # read the entire table
    stmt = text(
        'SELECT * FROM connector WHERE connector_id=:param_id').bindparams(param_id=connector_id)
    # stmt = text('SELECT * FROM connector WHERE connector_id in(:param_id)'),[{'param_id':connector_id},{'param_id':5}]

    df_connector = pd.read_sql(stmt, cnx)  # read the entire table

    print(df_connector)

    if df_connector.shape[0] == 0:

        data = {'message': 'Connector Definition not found'}
        response = app.response_class(response=json.dumps(data),
                                      status=404,
                                      mimetype='application/json')

        return response

    company_id = df_connector['company_id'].values[0]

    connector_name = df_connector['connector_name'].values[0]
    connector_type = df_connector['connector_type'].values[0]
    connector_description = df_connector['connector_description'].values[0]
    order_to_execute = df_connector['order_to_execute'].values[0]
    extract_method = df_connector['extract_method'].values[0]
    source_type = df_connector['source_type'].values[0]
    source_file_location = df_connector['source_file_location'].values[0]
    source_schema_identification = df_connector['source_schema_identification'].values[0]
    source_url = df_connector['source_url'].values[0]
    source_user_name = df_connector['source_user_name'].values[0]
    source_password = df_connector['source_password'].values[0]
    source_port_no = df_connector['source_port_no'].values[0]
    source_table_sheet = df_connector['source_table_sheet'].values[0]
    source_name_range = df_connector['source_name_range'].values[0]
    target_type = df_connector['target_type'].values[0]
    target_schema_identification = df_connector['target_schema_identification'].values[0]
    target_schema_name = df_connector['target_schema_name'].values[0]
    target_table_name = df_connector['target_table_name'].values[0]
    target_server_url = df_connector['target_server_url'].values[0]
    target_server_port = df_connector['target_server_port'].values[0]
    target_user_name = df_connector['target_user_name'].values[0]
    target_password = df_connector['target_password'].values[0]

    print(company_id)

    if not validate_target_type(target_type):

        data = {'message': 'Target Type not implemented'}
        response = app.response_class(response=json.dumps(data),
                                      status=501,
                                      mimetype='application/json')

        return response

    '''
    if not validate_target_table_name(target_table_name):
        data = {'message': 'Not Implemented'}
        response = app.response_class(response=json.dumps(data),
                                      status=501,
                                      mimetype='application/json')

        return response
    '''

    if not validate_source_structure_csv(connector_id, df):

        data = {
            'message': 'Conflict. Source structure does not match with connector mapping'}
        response = app.response_class(response=json.dumps(data),
                                      status=409,
                                      mimetype='application/json')

        return response

    '''
    if target_table_name.lower() == 'oh':
        ret_val = db.sf_insert_oh_engine(df)
    elif target_table_name.lower() == 'warehouse':
        ret_val = db.sf_insert_warehouse_engine(df)

    else:
        data = {'message': 'Not Implemented for target table'}
        response = app.response_class(response=json.dumps(data),
                                      status=501,
                                      mimetype='application/json')

        return response

    '''


    
    ##############
    
    stmt = text(
        'SELECT * FROM connector_mapping WHERE connector_id=:param_id').bindparams(param_id=connector_id)

    df_mapping = pd.read_sql(stmt, cnx)  # read the entire table

    print(df_mapping)

    if df_mapping.shape[0] == 0:

        data = {'message': 'Mapping data not found'}
        response = app.response_class(response=json.dumps(data),
                                      status=404,
                                      mimetype='application/json')

        return response

    ###############

    if not df_helper.check_source_target_column_compatibility(df_mapping):

        data = {'message': 'Mapping column source and target type not compatible'}
        response = app.response_class(response=json.dumps(data),
                                      status=409,
                                      mimetype='application/json')

        return response



    df_helper.rename_source_column_to_target_column(df,df_mapping)



    ret_val = db.sf_insert_engine(target_table_name,df)

    if ret_val is False:

        data = {'message': 'Expectation Failed. Database operation failed'}
        response = app.response_class(response=json.dumps(data),
                                      status=417,
                                      mimetype='application/json')

        return response

    data = df.to_json()

    response = app.response_class(response=json.dumps(data),
                                  status=200,
                                  mimetype='application/json')

    return response


@app.route('/connector_mapping/<int:connector_id>', methods=['GET'])
def connector_mapping_id(connector_id: int):

    # cnx = create_engine('mysql+pymysql://root:ClassicMySQL3306@localhost/cxn')
    # df = pd.read_sql('SELECT * FROM connectormapping WHERE connectorid=' + str(id) + ' order by slno', cnx) #read the entire table

    df = df_connector_mapping_connector_id(connector_id)
    print(df)

    # data = {'name': 'nabin khadka'}

    data = df.to_json()

    response = app.response_class(response=json.dumps(data),
                                  status=200,
                                  mimetype='application/json')

    return response


def df_connector_mapping_connector_id(connector_id: int):

    # cnx = create_engine('mysql+pymysql://root:ClassicMySQL3306@localhost/cxn')
    cnx = create_engine(app.config['SQLALCHEMY_DATABASE_URI'], echo=True)

    df = pd.read_sql('SELECT * FROM connector_mapping WHERE connector_id=' +
                     str(connector_id) + ' order by slno', cnx)  # read the entire table

    return df
