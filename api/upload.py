
# from crypt import methods
from flask import Flask, json
from flask import request, make_response, jsonify

import pandas as pd
import api.db_snowflake as db
import api.connector

from __main__ import app

@app.route('/Upload', methods=['POST'])
def upload():

    if request.method == 'POST':

        file = request.files['file']

        is_valid = check_valid_file(file)

        if not is_valid:

            data = {'message':'Invalid file'}

            response = app.response_class(response=json.dumps(data),
                        status=400,
                        mimetype='application/json')

            return response


        df = pd.read_csv(request.files.get('file'))


        if not validate_structure(df):

            # return make_response('Failed ...! Structure mismatch ...',400)

            data = {'message':'Failed ...! Structure mismatch'}

            response = app.response_class(response=json.dumps(data),
                        status=400,
                        mimetype='application/json')

            return response


        ret_val = db.sf_insert_wh_engine(df)

        if ret_val != True:

            data = {'message':'File upload failed ...!'}

            response = app.response_class(response=json.dumps(data),
                status=400,
                mimetype='application/json')

            return response




        data = {'message':'File upload success ...!'}

        response = app.response_class(response=json.dumps(data),
                status=200,
                mimetype='application/json')


        return response




def check_valid_file(file):

    ret_val = True

    if file.filename == '':

        ret_val = False


    return ret_val


def validate_structure(df):

    # df_derived = pd.DataFrame(columns=['Bracket','Value']) 
    # df_derived = pd.DataFrame(columns=['BRACKET','VALUE']) 

    df.columns = df.columns.str.lower()

    print(df)

    df_mapping = api.connector.df_connector_mapping_connector_id(1)

    # df_mapping.columns= df_mapping.columns.str.lower()

    print(df_mapping)


    df_derived = pd.DataFrame()

    print(df_derived)    


    for ind in df_mapping.index:
        print(df_mapping['source_column_name'][ind], df_mapping['source_column_type'][ind])

        col_name = df_mapping['source_column_name'][ind].lower()
        col_type = df_mapping['source_column_type'][ind]

        # df_derived[col_name] = 
        # df_derived[df_mapping['SourceColumnType'][ind]] = ''


        if col_type.lower() == 'int64':
            df_derived[col_name]=0
            # df_derived = df_derived[col_name].astype(col_type,False,'ignore')
            df_derived[col_name] = df_derived[col_name].astype(col_type)
        else:
            df_derived[col_name]=''


    # df_derived.columns= df_derived.columns.str.lower()

    print(df_derived)    

    # print('Difference ...')
    # print(df_derived.columns.difference(df.columns))
    diff = df_derived.columns.difference(df.columns)
    diff_list = diff.tolist()

    # print(type(diff))
    # print(diff.tolist())
    if len(diff_list) > 0:   
        return False

    return True        


