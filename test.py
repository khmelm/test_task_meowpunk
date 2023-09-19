import pandas as pd
import sqlite3

from datetime import datetime
from memory_profiler import profile


class DataProcessor:
    """Создаем класс"""
    def __init__(self, database_name, csv_client_path, csv_server_path, desired_date):
        self.database_name = database_name
        self.csv_client_path = csv_client_path
        self.csv_server_path = csv_server_path
        self.desired_date = desired_date

    """Функция для подключения к БД"""
    def get_conn(self):
        return sqlite3.connect(self.database_name)

    """Функция для извлечения данных из БД"""
    def extract_cheater_data(self, table_name):
        conn = self.get_conn()
        query = f'SELECT * FROM {table_name}'
        return pd.read_sql_query(query, conn)

    """Функция для извлечения данных из csv-файлов"""
    def extract_csv_data(self, csv_file_path):
        return pd.read_csv(csv_file_path)

    """Функция для преобразования строковых данных в timestamp"""
    def str_to_timestamp(self, date_str):
        date_obj = datetime.strptime(date_str, '%Y-%m-%d %H:%M:%S')
        return date_obj.timestamp()

    """Функция для извлечения данных из csv-файлов по интересующей дате"""
    def filter_data_by_date(self, df, date):
        date_obj = pd.to_datetime(date)
        start_of_day = date_obj.replace(hour=0, minute=0, second=0).timestamp()
        end_of_day = date_obj.replace(hour=23, minute=59, second=59).timestamp()
        return df[(df['timestamp'] >= start_of_day) & (df['timestamp'] <= end_of_day)]

    """Функция для извлечения и загрузки данных об игрока за определенную дату, у которых ban_time - это предыдущие сутки или раньше относительно timestamp"""
    @profile
    def process_data(self):
        cheater_df = self.extract_cheater_data('cheaters')
        client_df = self.extract_csv_data(self.csv_client_path)
        server_df = self.extract_csv_data(self.csv_server_path)

        filtered_client_df = self.filter_data_by_date(client_df, self.desired_date)
        filtered_server_df = self.filter_data_by_date(server_df, self.desired_date)
        
        #объединяем данные по error_id
        merged_df = filtered_server_df.merge(filtered_client_df, on='error_id', how='inner')
        del filtered_client_df
        del filtered_server_df
        #переименовываем колонки у фрейма для читабельности
        merged_df = merged_df.rename(columns={'timestamp_x': 'timestamp', 'description_x': 'json_server', 'description_y': 'json_client'})
        
        #из таблицы читеров вытаскиваем данные, которые есть в прошлом запросе
        needed_cheater_df = cheater_df.merge(merged_df[['player_id']], on='player_id', how='inner')
        needed_cheater_df['ban_time'] = needed_cheater_df['ban_time'].apply(self.str_to_timestamp) #полученный ban-time переводим в timestamp 

        #выгружаем данные только тех, которые находятся вбане последние сутки от timestamp(server.csv)
        cheater_1_day_df = merged_df.merge(
            needed_cheater_df,
            on='player_id',
            how='inner',
        ).loc[
            lambda x: abs(x['timestamp'] - x['ban_time']) <= 86400
        ]

        result_df = merged_df[~merged_df['player_id'].isin(cheater_1_day_df['player_id'])].drop(columns=['timestamp_y'])
        result_df.to_sql('empty_table', con=self.get_conn(), if_exists='replace', index=False)
        return result_df

if __name__ == "__main__":
    desired_date = '2021-03-02'
    processor = DataProcessor('cheaters.db', '/client.csv', '/server.csv', desired_date)
    result_df = processor.process_data()

#Cреднее потребление памяти - 509 MiB