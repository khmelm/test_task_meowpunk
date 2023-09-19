# Тестовое задание на позицию Дата-Инженер

![Python](https://img.shields.io/badge/Python-3.x-blue) ![Pandas](https://img.shields.io/badge/Pandas-1.x-orange) ![SQL](https://img.shields.io/badge/SQL-green)

C полным текстом тестового задания можно ознакомится в файле task.txt

Создан класс, который:

1. Выгружает данные из client.csv и server.scv за определенную дату.
2. Объединяет данные из этих таблиц по error_id.
3. Исключает из выборки записи с player_id, которые есть в таблице cheaters, но только в том случае если, у player_id ban_time - это предыдущие сутки или раньше относительно timestamp из server.scv
4. Выгружает данные в базу данных SQLite. В ней находятся следующие данные:
   - timestamp из server.csv
   - player_id из client.csv
   - error_id из сджойненных server.csv и client.csv
   - json_server поле json из server.csv
   - json_client поле json из client.csv

Для проверки код необходимо:

- скачать файлы server.csv, client.csv и cheater.db
- cоздать таблицу empty_table в cheater.db

```sql
 CREATE TABLE IF NOT EXISTS empty_table (
    timestamp DATETIME,
    player_id INTEGER,
    event_id INTEGER,
    error_id INTEGER,
    json_server TEXT,
    json_client TEXT
);
```

- запустить test.py
