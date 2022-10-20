import json
import pandas as pd
import numpy as np
import requests
import getpass
from pandas_profiling import ProfileReport
import snowflake.connector


class Connections:

    def connect_snowflake(self, table):
        """
        Connecting to the snowflake db
        :return: dataset
        """
        paswrd = getpass.getpass("Password:")
        conn = snowflake.connector.connect(user='user_name', password=str(paswrd), account='my_snowflake_account',
                                           warehouse='warehouse')
        query = '"select * from' + table + '"'
        dataset = pd.read_sql(query, conn)
        return dataset

    def connect_api(self, url, headers=None, data=None, method='GET', params=None, json_data=None, timeout=None):

        """
        Connecting to the API
        :param url: api url
        :param headers: request header
        :param data: data for post call
        :param method: request method
        :param params: request parameters
        :param json_data: request data (json)
        :param timeout: request timeout
        :return: dataset
        """
        dataset = {}

        try:

            if method == 'GET':
                res = requests.get(url, headers=headers, params=params, timeout=timeout)

            elif method == 'POST':
                res = requests.post(url, headers=headers, data=data, json=json_data, params=params, timeout=timeout)

            if res:
                json_obj = json.loads(res.content)
                dataset = pd.read_json(json.dumps(json_obj))
            else:
                print('connect_api failed: status:' + res.status_code + 'status_message: ' + res.text)
                return None
        except Exception as e:
            print("connect_api error: " + str(e))
            pass
        return dataset

    def read_csv(self, filepath, sep=','):
        """
        read csv files
        :return: dataset
        """
        dataset = pd.read_csv(filepath, sep=sep)
        return dataset

    def read_excel(self, filepath, sheet_name=0):
        """
        read excel files
        :return: dataset
        """
        dataset = pd.read_excel(filepath, sheet_name=sheet_name)
        return dataset


class Profiling(Connections):

    def basic_profiling(self):
        df = self.read_csv(filepath='', sep=',')
        profile = ProfileReport(df)
        profile.to_notebook_iframe()

    def explorative_profiling(self):
        df = self.read_csv(filepath='', sep=',')
        profile = ProfileReport(df, explorative=True)
        profile.to_notebook_iframe()






