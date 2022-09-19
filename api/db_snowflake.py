
# from crypt import methods
from logging import exception
from flask import Flask
from flask import request,make_response, jsonify


import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# for create engine
from snowflake.sqlalchemy import URL
from sqlalchemy import create_engine

# from snowflake.sqlalchemy import URL


# from __main__ import app


USERNAME='cvshivan'
PASSWORD='Chemistry1'
# ACCOUNT='mw11692.ap-southeast-1'
ACCOUNT='KU74463.south-central-us.azure'
WAREHOUSE='COMPUTE_WH'
DATABASE='TRADING'
SCHEMA='PUBLIC'


def sf_connect():

    ctx=snowflake.connector.connect(user=USERNAME,password=PASSWORD,account=ACCOUNT,warehouse=WAREHOUSE,database=DATABASE,schema=SCHEMA,autocommit=True)
    return ctx



def sf_insert_oh_write_pandas(df):

    # ctx=snowflake.connector.connect(user=USERNAME,password=PASSWORD,account=ACCOUNT,warehouse=WAREHOUSE,database=DATABASE,schema=SCHEMA,autocommit=True)
    ctx = sf_connect()

    # print("ctx=" , ctx)

    print (df)

    try:

        cursor = ctx.cursor()

        with ctx as db_conn_sf, db_conn_sf.cursor() as db_cursor_sf:
            # db_cursor_sf.executemany("""
                # INSERT INTO TRADING.PUBLIC.OH (BRACKET,VALUE) VALUES(%s,%s)""", df.values.tolist())

            db_cursor_sf.executemany("""
                INSERT INTO TRADING.PUBLIC.OH (Bracket,Value) VALUES(%s,%s)""", df.values.tolist())


            # success, num_chunks, num_rows, output = write_pandas(conn=db_conn_sf,
                                                        # df=df,
                                                        # table_name='oh',
                                                        # quote_identifiers=False)


            # print('success...')
            # print(success)
            # print('num_chunks...')
            # print(num_chunks)
            # print('num_rows...')
            # print(num_rows)
            # print('output...')
            # print(output)



        cursor.close()

        return 

    except Exception as e:
        print(e)
        print("Failed...! Insert to Snowflake")





def sf_insert_oh_engine(df):


    engine = create_engine(URL(
        account=ACCOUNT,
        user=USERNAME,
        password=PASSWORD,
        warehouse=WAREHOUSE,
        database=DATABASE,      
        schema =SCHEMA,
        autocommit=True
    ))


    # df.to_sql(con = engine, name = 'oh', if_exists='replace', 
                        # other possible values are 'append' and 'fail'
                        # index=False)

    print(df)
    print(df.to_sql(con = engine, 
                        name = 'oh', 
                        if_exists='append', #other possible values are 'append' and 'fail'
                        index=False))


    return True


def sf_insert_warehouse_engine(df):


    engine = create_engine(URL(
        account=ACCOUNT,
        user=USERNAME,
        password=PASSWORD,
        warehouse=WAREHOUSE,
        database=DATABASE,      
        schema =SCHEMA,
        autocommit=True
    ))



    print(df)


    try:
        df.to_sql(con = engine, 
            name = 'warehouse', 
            if_exists='append', #other possible values are 'append' and 'fail'
            index=False)

        return True 
    except Exception as e:

        print(e)
        return False



def sf_insert_engine(table_name,df):


    engine = create_engine(URL(
        account=ACCOUNT,
        user=USERNAME,
        password=PASSWORD,
        warehouse=WAREHOUSE,
        database=DATABASE,      
        schema =SCHEMA,
        autocommit=True
    ))



    print(df)


    try:
        df.to_sql(con = engine, 
            name = table_name, 
            if_exists='append', #other possible values are 'append' and 'fail'
            index=False)

        return True 
    except Exception as e:

        print(e)
        return False



