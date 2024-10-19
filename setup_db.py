import sqlite3
from datetime import datetime, timedelta

conn = sqlite3.connect('batch_example.db')
cursor = conn.cursor()




cursor.execute('''
CREATE TABLE IF NOT EXISTS source_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data TEXT NOT NULL,
    sale_date DATE NOT NULL
)
''')

cursor.execute('''
CREATE TABLE IF NOT EXISTS target_table (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    data TEXT NOT NULL,
    sale_date DATE NOT NULL,
    processed_date DATE NOT NULL
)
''')

conn.commit()


today = datetime.today()
for i in range(1,31):
  sale_date = today - timedelta(days=i)
  data = f'Data {i}'
  cursor.execute('INSERT INTO source_table (data, sale_date) VALUES (?, ?)', (data, sale_date.strftime('%Y-%m-%d')))
  
conn.commit()
conn.close()

