# -*- coding: utf-8 -*-
"""
@Time: 2021/7/9 17:00
@Auth: LeoLucky(热衷开源的宝藏Boy)
@Project: 
@File: core.py
@IDE: PyCharm
@Email: 568428443@qq.com
@BlogSite: www.fangzengye.com
@motto: 学而知不足&写出简洁,易懂的代码是我们共同的追求
@Version: V1.0.0
@Desc: 与数据库有关
"""
import yaml
import pymysql
import pandas as pd
from sqlalchemy import create_engine
import os

class Connector(object):
    """
    连接数据库进行增删改查操作
    """
    def __init__(self, server_name: str, remote: bool = False):
        """
        默认本地服务器
        :param server_name: 服务器名称, 服务器信息在yaml文件查看
        :param remote: 是否远程访问
        """
        self.remote = remote
        self.server_name = server_name
        base_dir = os.path.abspath(os.path.dirname(__file__))
        if self.remote:
            """远程连接数据库环境"""
            self.path = base_dir + "/DatabaseInfoSettings/db-dev.yaml"
        else:
            """服务器环境"""
            self.path = base_dir + "/DatabaseInfoSettings/db.yaml"

        """创建sql引擎"""
        database_info = self.__read_database_info(self.path)[server_name]
        self.__host = database_info["host"]
        self.__port = database_info["port"]
        self.__user = database_info["username"]
        self.__passwd = database_info["password"]
        self.engine = pymysql.connect(host=self.__host, port=self.__port, user=self.__user, passwd=self.__passwd)

    @staticmethod
    def __read_database_info(config_path):
        with open(config_path, 'r', encoding='utf-8') as fp:
            cont = fp.read()
        return yaml.safe_load(cont)

    def colse_engine(self):
        """关闭引擎"""
        self.engine.close()

    def show_database(self, like: str = None) -> pd.DataFrame:
        """
        展示该服务器拥有的数据库
        :param like: 模糊查找参数
        :return:
        """
        if like:
            query_sql = "SHOW DATABASES LIKE '%{}%'".format(like)
        else:
            query_sql = "SHOW DATABASES"
        result = pd.read_sql(sql=query_sql, con=self.engine)
        return result

    def show_table(self, database_name: str, like: str = None) -> pd.DataFrame:
        """
        展示该数据库的所有表
        :param like: 模糊查找表名
        :param database_name: 数据库名
        :return:
        """
        if like:
            query_sql = "SHOW TABLES FROM `{}` LIKE '%{}%'".format(database_name, like)
        else:
            query_sql = "SHOW TABLES FROM `{}`".format(database_name)
        result = pd.read_sql(sql=query_sql, con=self.engine)
        return result

    def query(self, query_sql: str) -> pd.DataFrame:
        """
        :param query_sql: 查询语句
        :return:
        """
        result = pd.read_sql(sql=query_sql, con=self.engine)
        return result

    def save_to_database(self,
             df: pd.DataFrame,
             database_name: str,
             table_name: str,
             schema=None,
             if_exists: str = "fail",
             index=True,
             index_label=None,
             chunksize=None,
             dtype=None,
             method=None,):
        """将数据保存sql
        df: 表
        database_name: 保存在数据库的名称
        table_name: 保存表名
        """
        engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(self.__user, self.__passwd, self.__host,
                                                                       self.__port, database_name))
        df.to_sql(name=table_name, con=engine, schema=schema, if_exists=if_exists, index=index, index_label=index_label,
                  chunksize=chunksize, dtype=dtype, method=method)
