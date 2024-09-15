import pandas as pd
import glob
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError

# 配置 MySQL 连接
username = 'root'
password = 'C502S502'
host = 'localhost'
port = '3306'  # 默认 MySQL 端口
database = 'rainfall'

# 创建 SQLAlchemy 引擎
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')

# 假设所有的CSV文件都放在同一个文件夹中
file_paths = glob.glob('./觀測_日資料_臺南市_降雨量/*.csv')

# 初始化一个空的DataFrame来存储合并的数据
combined_df = pd.DataFrame()

# 遍历每个CSV文件
for file_path in file_paths:
    df = pd.read_csv(file_path)


    # 检查并重命名可能冲突的列
    if 'Rainfall' in df.columns:
        df.rename(columns={'Rainfall': 'Rainfall_existing'}, inplace=True)

    # 执行 melt 操作
    df_melted = df.melt(id_vars=['LON', 'LAT'], var_name='Date', value_name='Rainfall')

    # 将 Date 列转换为 datetime 格式
    df_melted['Date'] = pd.to_datetime(df_melted['Date'], errors='coerce').dt.date

    # 将 Rainfall 列转换为浮点数
    df_melted['Rainfall'] = pd.to_numeric(df_melted['Rainfall'], errors='coerce')

    # 过滤掉 Date 列值为 'Unnamed: 368' 和 'Unnamed: 367' 的行
    df_melted = df_melted[~df_melted['Date'].astype(str).isin(['Unnamed: 368', 'Unnamed: 367'])]

    # 将 DataFrame 合并到 combined_df 中
    if combined_df.empty:
        combined_df = df_melted
    else:
        # 基于LON、LAT、Date列进行合并
        combined_df = pd.merge(combined_df, df_melted, on=['LON', 'LAT', 'Date'], how='outer', suffixes=('', '_duplicate'))

        # 处理重复列：选择非重复的降雨量数据
        if 'Rainfall_duplicate' in combined_df.columns:
            combined_df['Rainfall'] = combined_df[['Rainfall', 'Rainfall_duplicate']].sum(axis=1)
            combined_df.drop(columns=['Rainfall_duplicate'], inplace=True)

# 批次大小
batch_size = 10000

# 分批次插入数据
try:
    with engine.connect() as conn:
        trans = conn.begin()
        try:
            for i in range(0, len(combined_df), batch_size):
                df_batch = combined_df.iloc[i:i + batch_size]
                df_batch.to_sql('tainan_city', con=conn, if_exists='append', index=False)
            trans.commit()
            print("Data has been successfully saved to MySQL.")
        except SQLAlchemyError as e:
            trans.rollback()
            print(f"Error: {e}")
except SQLAlchemyError as e:
    print(f"Connection Error: {e}")