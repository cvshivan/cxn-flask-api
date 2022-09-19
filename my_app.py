# from tkinter import EXCEPTION
# from crypt import methods

from logging import exception
from flask import Flask
from flask import request, make_response, jsonify
from flask_cors import CORS, cross_origin
# pip install -U flask-cors
import pandas as pd
import snowflake.connector

app = Flask(__name__)
cors = CORS(app)

app.config['CORS_HEADERS'] = 'Content-Type'
app.config['SQLALCHEMY_DATABASE_URI']= 'mysql+pymysql://root:ClassicMySQL3306@localhost/cxn'


USERNAME = 'cvshivan'
PASSWORD = 'Chemistry1'
ACCOUNT = 'ku74463.south-central-us.azure'
WAREHOUSE = 'compute_wh'
DATABASE = 'TRADING'
SCHEMA = 'PUBLIC'


import api.connector
import api.upload
import api.oh
import api.item


@app.route('/')
@cross_origin()
def index():
    return 'Index'


@app.errorhandler(404)
def page_not_found(e):
    return "<h1>404</h1><p>The resource could not be found.</p>", 404


try:
    if __name__ == '__main__':
        app.run(debug=True)
        # app.run(host="localhost", port=8000, debug=True)
except:
    print('Re-run Error')

