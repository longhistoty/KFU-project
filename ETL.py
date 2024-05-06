import kaggle # pip install kaggle
from sqlalchemy import create_engine, DateTime  # pip install sqlalchemy
import pandas as pd # pip install pandas
from clickhouse_driver import Client  # pip install clickhouse-driver
import time

def get_data_kaggle():
    path = 'C:\\Users\\Temny\\Desktop\\test' # указать путь к папке, откуда будем тащить данные (желательно в одну папку, где будет скрипт)
    dataset = "nikhil1e9/netflix-stock-price" # название датасета
    kaggle.api.dataset_download_files(dataset, path=path, unzip=True) # загружает данные, предварительно нужно скачать api файл


def load_data_postgres(name, conn):
    table = pd.read_csv(name)
    table = table.rename(columns={'Adj Close': 'Adj_Close'})
    table['stock_name'] = name[:-10]
    types = {
        'Date': DateTime()
    }
    table.to_sql(name = name[:-10], con = conn, schema= 'public', if_exists='replace', index=False, dtype = types)



def main():
    NAME = "name"
    USER=  "user"
    PASSWORD = "1234"
    HOST= "localhost"
    PORT = 5432

    conn_string = 'postgresql+psycopg2://{}:{}@{}:{}/{}'.format( 
        USER,  
        PASSWORD,  
        HOST,  
        PORT,  
        NAME 
    )
    conn = create_engine(conn_string)
    get_data_kaggle()
    tables = ('AMAZON_daily.csv', 'APPLE_daily.csv', 'GOOGLE_daily.csv', 'META_daily.csv','NETFLIX_daily.csv')

    for table in tables:
        load_data_postgres(table, conn)

    data = transform_data(conn,tables)
    load_data_ch(data, conn)
    
def load_data_ch(data, conn):
    data.to_sql(name = 'stock_prices', con = conn, schema= 'public', if_exists='replace', index=False)
    client = Client(host='localhost', port=9000, user='user', password='1234', database='default')
    data = data.to_dict(orient='records')
    client.execute('TRUNCATE TABLE stock_prices')
    client.execute('INSERT INTO stock_prices (Date,Open,High,Low,Close,Adj_Close,Volume,stock_name) VALUES', data)
    client.disconnect()


def transform_data(conn,tables):
    transform_data = pd.DataFrame()
    for table in tables:
        query = f''' select * from public."{table[:-10]}"'''
        data = pd.read_sql_query(query, conn)
        transform_data = pd.concat([transform_data, data], ignore_index=True)

    return transform_data


main()