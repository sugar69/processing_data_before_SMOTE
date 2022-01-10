# import csv
# import sqlite3
#
# csv_filepath = './cyphar_docker.csv'
# db_filepath = './cyphar_docker.db'
#
# # Connectionオブジェクトをメモリ上に作成してDBに接続
# db = sqlite3.connect(db_filepath)
# # Cursorオブジェクトを作成(DB上の処理対象の行を示す)
# c = db.cursor()
#
# # cyphar_dockerという名前のtableを作成
# c.execute("""CREATE TABLE cyphar_docker
#  (name CHAR(15) NOT NULL,
#  atk INTEGER NOT NULL,
#  hp INTEGER NOT NULL);""")
#
# # CSVをロードして中身をDBに挿入
# with open(csv_filepath, 'r') as f:
#     b = csv.reader(f)
#     header = next(b)
#     for t in b:
#         # tableに各行のデータを挿入
#         c.execute('INSERT INTO cyphar_docker VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);', t)
#
# # cyphar_docker tableの内容を表示
# c.execute('SELECT * FROM cyphar_docker;')
# print(c.fetchall())
#
# # DBの変更を反映(コミット)
# db.commit()
#
# # DBとの接続を閉じる
# db.close()

import sqlite3
from os.path import join, split
from glob import glob
import pandas as pd

csv_path = r"./"
db_path = r"./shishir-a412ed_docker.db"

def insert_csv():
    # CSV ファイルのディレクトリ取得
    csv_files = glob(join(csv_path, "./shishir-a412ed_docker.csv"))
    print(csv_files)

    for csv in csv_files:
        print("2")

        table_name = split(csv)[1] # テーブル名作成

        table_name = table_name[0:-4] # テーブル名から拡張子を削除

        df = pd.read_csv(csv, dtype=object) # CSV 読込

        with sqlite3.connect(db_path) as conn:
            df.to_sql(table_name, con=conn) # SQLiteにCSVをインポート

insert_csv()