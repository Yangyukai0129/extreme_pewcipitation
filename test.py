import pandas as pd
import glob
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os

# 配置 MySQL 连接
username = 'root'
password = 'C502S502'
host = 'localhost'
port = '3306'  # 默认 MySQL 端口
database = 'rainfall'

# 创建 SQLAlchemy 引擎
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')

project_dir = './test.sql'
with open(project_dir, 'r') as file:
    query = file.read()

pd = pd.read_sql(query,engine)

print(pd)