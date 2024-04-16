import mysql.connector
import pandas as pd

# データベース接続設定
MYSQL_SERVER = 'tech0-db-step4-studentrdb-8.mysql.database.azure.com'
MYSQL_USER = 'tech0gen5student'
MYSQL_PASSWORD = 'vY7JZNfU8'
MYSQL_DB = 'otazunedb'
TABLE_NAME = 'tourreservation'

# MySQLデータベースに接続
conn = mysql.connector.connect(
    host=MYSQL_SERVER,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB
)

# データベースからデータを読み込むSQLクエリの実行
query = f"SELECT * FROM {TABLE_NAME}"  # テーブル名を使用
data = pd.read_sql_query(query, conn)

# データフレームの最初の数行を表示し、構造とカラム名を確認
print(data.head(), data.columns)

# 与えられた参加者の名前の情報を整形する関数を定義
def format_participant_info(id):
    # データフレームで指定されたIDの行をフィルタリング
    participant_info = data[data['ID'] == id].iloc[0]

    formatted_info = f"""
    {participant_info['RequestedDate']}に、{participant_info['Country']}出身の{participant_info['CustomerName']}さんが、
    大人{participant_info['AdultParticipants']}名、
    5~12才のお子様{participant_info['ChildrenParticipants5To12']}名、
    0~4才のお子様{participant_info['ChildrenParticipantsUnder4']}名で、
    「{participant_info['TourName']}」に参加されます。
    {participant_info['CustomerName']}さんは「{participant_info['Requests']}」というご要望をお持ちです。
    他にどのような体験が提案できると、{participant_info['CustomerName']}さんの日本旅行がより良いものにできそうでしょうか？
    """
    
    return formatted_info.strip()

# 全てのIDに対して情報を整形し表示
for id_value in data['ID'].unique():
    try:
        print(format_participant_info(id_value))
    except IndexError:
        print(f"ID {id_value} のデータが見つかりませんでした。")

# データベース接続を閉じる
conn.close()