import sqlite3
from datetime import datetime, timedelta

def run_monthly_batch():
  # データベースに接続
  conn = sqlite3.connect('batch_example.db')
  cursor = conn.cursor()

  # 直近7日間の日付を計算
  end_date = datetime.today()
  start_date = end_date - timedelta(days=7)

  start_str = start_date.strftime('%Y-%m-%d')
  end_str = end_date.strftime('%Y-%m-%d')

  print(f'バッチ処理開始: {start_str} から {end_str} のデータを処理します。')

  # source_tableから直近7日間のデータを抽出
  cursor.execute('''
      SELECT id, data, sale_date
      FROM source_table
      WHERE sale_date BETWEEN ? AND ?
  ''', (start_str, end_str))

  rows = cursor.fetchall()

  print(f'抽出したデータ件数: {len(rows)}')

  # target_tableにデータを挿入
  processed_date = end_str  # 処理日として終了日を使用
  for row in rows:
    _, data, sale_date = row
    cursor.execute('''
      INSERT INTO target_table (data, sale_date, processed_date) VALUES (?, ?, ?)
      ''', (data, sale_date, processed_date))

  conn.commit()
  conn.close()

  print('バッチ処理完了: データをtarget_tableに反映しました。')

if __name__ == '__main__':
  print('実行')
  run_monthly_batch()
