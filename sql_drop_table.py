import mysql.connector
import pandas as pd

# データベース接続設定
MYSQL_SERVER = 'tech0-db-step4-studentrdb-8.mysql.database.azure.com'
MYSQL_USER = 'tech0gen5student'
MYSQL_PASSWORD = ''
MYSQL_DB = 'otazunedb'
TABLE_NAME = 'tourreservation'

# データベースに接続
conn = mysql.connector.connect(
    host=MYSQL_SERVER,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DB,
    raise_on_warnings=True
)
cursor = conn.cursor()

# 既存のテーブルを削除して新しいテーブルを作成
cursor.execute(f"DROP TABLE IF EXISTS {TABLE_NAME}")
create_table_query = f"""
CREATE TABLE {TABLE_NAME} (
    ID INT AUTO_INCREMENT PRIMARY KEY,
    TourName VARCHAR(255),
    RequestedDate DATE,
    RequestedTime VARCHAR(255),
    AdultParticipants INT,
    ChildrenParticipants5To12 INT,
    ChildrenParticipantsUnder4 INT,
    WheelchairAccess INT,
    OtherRemarks TEXT,
    CouponCode VARCHAR(100),
    CustomerName VARCHAR(255),
    BirthDate DATE,
    Country VARCHAR(255),
    Email VARCHAR(255),
    PhoneNumber VARCHAR(255),
    Questions TEXT,
    Requests TEXT
);
"""
cursor.execute(create_table_query)

# CSVファイルを読み込む
csv_data = pd.read_csv('OTAZUNEdatabase - シート1.csv')
csv_data = csv_data.replace({pd.NA: None, '-': None})

# 日付データの形式を変換
csv_data['Requested date'] = pd.to_datetime(csv_data['Requested date'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
csv_data['Date of birth'] = pd.to_datetime(csv_data['Date of birth'], format='%m/%d/%Y').dt.strftime('%Y-%m-%d')

# データをデータベースに挿入
for index, row in csv_data.iterrows():
    insert_query = f"""
    INSERT INTO {TABLE_NAME} (
        TourName, RequestedDate, RequestedTime, AdultParticipants, 
        ChildrenParticipants5To12, ChildrenParticipantsUnder4, WheelchairAccess, 
        OtherRemarks, CouponCode, CustomerName, BirthDate, Country, Email, 
        PhoneNumber, Questions, Requests
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    cursor.execute(insert_query, (
        row['Tour name'], row['Requested date'], row['Requested time'],
        row['Number of participants(Adult)'],
        row['Number of participants(Children (Age 5-12))'],
        row['Number of participants(Children (Under 4))'],
        row['Remarks（Including wheelchair users）'],
        row['Remarks（Others）'],
        row['Coupon Codes'], row['Name'], row['Date of birth'],
        row['Country'], row['E-mail'], row['Phone no'],
        row['Questions'], row['Requests']
    ))

# 変更をコミットし、接続を閉じる
conn.commit()
conn.close()