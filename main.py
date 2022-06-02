import json
import pandas as pd
import numpy as np
import requests
import getpass
from pandas_profiling import ProfileReport
import snowflake.connector


class Connections:

    def connect_snowflake(self, table, columns=None):

        try:
            paswrd = getpass.getpass("Password:")
            conn = snowflake.connector.connect(user='wesanalytics', password=str(paswrd), account='hj05563.us-east-1', warehouse='DW_WES_ANALYTICS')
            if columns is not None:
                query = "select " + str(columns).replace('[', '').replace(']', '').replace("'", '') + " from " + table + " limit 10"
            else:
                query = "select * from " + table + " limit 10"
            dataset = pd.read_sql(query, conn)
            return dataset
        except Exception as e:
            print(e)

    def connect_api(self, con_config):
        # url, headers = None, data = None, method = 'GET', params = None, json_data = None
        """
        :param con_config: connection configuration
        :return: dataset
        """
        dataset = {}

        try:
            headers, data, params, json_data = None, None, None, None
            timeout = 60
            if 'headers' in con_config:
                headers = con_config['headers']
            if 'params' in con_config:
                params = con_config['params']
            if 'timeout' in con_config:
                timeout = con_config['timeout']
            if 'data' in con_config:
                data = con_config['data']
            if 'json_data' in con_config:
                json_data = con_config['json_data']

            if con_config['method'] == 'GET':
                res = requests.get(con_config['url'], headers=headers, params=params, timeout=timeout)

            elif con_config['method'] == 'POST':
                res = requests.post(con_config['url'], headers=headers, data=data, json=json_data, params=params, timeout=timeout)

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
        return dataset.head()

    def read_excel(self, filepath, sheet_name=0):
        """
        read excel files
        :return: dataset
        """
        dataset = pd.read_excel(filepath, sheet_name=sheet_name)
        return dataset


class Profiling(Connections):

    def data_profiling(self, con_type, table=None, filepath=None, con_config=None, columns=None):
        df = {}
        if con_type == 'snowflake':
            df = self.connect_snowflake(table=table, columns=columns)
        elif con_type == 'api':
            df = self.connect_api(con_config=con_config)
        elif con_type == 'csv':
            df = self.read_csv(filepath=filepath)
        elif con_type == 'excel':
            df = self.read_excel(filepath=filepath)

        if len(df):
            profile = ProfileReport(df, explorative=True)
            profile.to_notebook_iframe()

if __name__ == '__main__':
    con = Connections()
    con.connect_snowflake(table='PROD_ODS.SRP.T_S_SRP_EMPBRIDGE__INTERVIEW__C', columns=["ID", "NAME", "CURRENCYISOCODE","CREATEDDATE","EMPBRIDGE__APPLICATION__C"])
