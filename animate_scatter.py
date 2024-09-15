from sqlalchemy import create_engine
import pandas as pd
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
import matplotlib.animation as animation


print('2333')
# 配置 MySQL 连接
username = 'root'
password = 'C502S502'
host = 'localhost'
port = '3306'  # 默认 MySQL 端口
database = 'rainfall'

# 从 MySQL 数据库抓取数据
engine = create_engine(f'mysql+pymysql://{username}:{password}@{host}:{port}/{database}')

print('8')
# 假设你的表名是 `rainfall_table`，并且需要抓取 `LAN`, `LAT`, `Date`, `Rainfall` 列
#query = "SELECT LON, LAT, Date, Rainfall FROM rainfall.new_taipei_city join rainfall.keelung_city on new_taipei_city.date=keelung_city.date where Date between '2022-01-01' and '2022-12-31' AND Rainfall !='-99.9' AND Rainfall >= '80'"
#query = "SELECT LON, LAT, Date, Rainfall FROM rainfall.taipei_city where Date between '2022-10-01' and '2022-10-15' AND Rainfall !='-99.9' UNION all SELECT LON, LAT, Date, Rainfall FROM rainfall.keelung_city where Date between '2022-10-01' and '2022-10-15' AND Rainfall !='-99.9'UNION all SELECT LON, LAT, Date, Rainfall FROM rainfall.taoyuan_city where Date between '2022-10-01' and '2022-10-15' AND Rainfall !='-99.9'"
query = '''
           SELECT * FROM rainfall.pingtung_county where Date between '2022-01-01' and '2022-12-31' and Rainfall != '-99.9' and Rainfall >= '10';
        '''



df = pd.read_sql(query, engine)
print('9')
# 确保 Date 列是 datetime 类型
df['Date'] = pd.to_datetime(df['Date']).dt.month
LON = df['LON']
LAT = df['LAT']
rainfall = df['Rainfall']
time = df['Date']

print(df['Date'])

fig, ax = plt.subplots()
sc = ax.scatter(LON, LAT, c=rainfall, cmap='PiYG')
plt.colorbar(sc, label='Rainfall (mm)')

def update(frame):
    mask = time == frame
    sc.set_array(rainfall[mask])
    ax.scatter(LON, LAT, c=rainfall, cmap='PiYG')
    ax.set_title(f'month: {frame}')
    return sc,

plt.title('Rainfall Distribution')
plt.xlabel('Longtitude')
plt.ylabel('Latitude')

ani = animation.FuncAnimation(fig, update, interval=200, frames=np.unique(time), repeat=True)
# ani.save('animation.gif', fps=10)   # 儲存為 gif

plt.show()


