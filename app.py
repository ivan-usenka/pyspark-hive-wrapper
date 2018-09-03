from flask import Flask
from flask import request
from api import SparkHiveApiService

app = Flask(__name__)


@app.route('/sql', methods=['POST'])
def execute_sql():
    return SparkHiveApiService.execute_sql(sql_string=request.json['sql'])
