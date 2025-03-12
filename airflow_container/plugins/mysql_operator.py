from airflow.hooks.mysql_hook import MySqlHook
from support_processing import TemplateOperatorDB
import logging
from contextlib import closing
import os

class MySqlOperators:
    def __init__(self, conn_id="mysql"):
        try:
            self.mysqlhook = MySqlHook(mysql_conn_id=conn_id)
            self.mysql_conn = self.mysqlhook.get_conn()
        except:
            logging.error(f"Can't connect to {conn_id} database")
