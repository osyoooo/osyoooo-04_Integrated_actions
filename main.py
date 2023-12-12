import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe
from google.oauth2.service_account import Credentials
from oauth2client.service_account import ServiceAccountCredentials

# Google APIへのアクセスにはOAuth 2.0という認証プロトコルが使用されており、scope呼ばれる権限の範囲を使ってアクセスを制御
scope = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]

# 環境変数から認証用のjsonyoを読み込む
credentials_json = json.loads(os.getenv('CREDENTIALS_JSON'))
credentials = Credentials.from_service_account_file(credentials_json, scopes=scope)

#認証情報を取得
gc = gspread.authorize(credentials)

# 指定されたスプレッドシートとシート名からDataFrameを作成する関数
def get_dataframe_from_sheet(spreadsheet, sheet_name):
    worksheet = spreadsheet.worksheet(sheet_name)
    data = worksheet.get_all_values()
    return pd.DataFrame(data[1:], columns=data[0])

# スプレッドシートのIDを環境変数から取得
SPREADSHEET_KEY = os.getenv('SPREADSHEET_KEY')

# 指定されたスプレッドシートとシート名からDataFrameを作成する関数
def get_dataframe_from_sheet(spreadsheet, sheet_name):
    worksheet = spreadsheet.worksheet(sheet_name)
    data = worksheet.get_all_values()
    return pd.DataFrame(data[1:], columns=data[0])

# スプレッドシートのIDを指定して開く
spreadsheet = gc.open_by_key(SPREADSHEET_KEY)

# NPCシートとREPシートのデータをDataFrameに変換
df_bukken = get_dataframe_from_sheet(spreadsheet, 'suumo_bukkenn')
df_address = get_dataframe_from_sheet(spreadsheet, 'suumo_address_db')

#物件コードをキーにして物件情報と正しい住所情報を横結合する
merge_df = pd.merge(df_bukken, df_address, on='Bc_code')

#nameが空白行を削除　物件の掲載が終了しているため
merge_df = merge_df[merge_df['name'] != '']

# 'Place ID', '間取り', '階' でグループ化し、'Bc_code' の値を横結合
grouped = merge_df.groupby(['Lat','Lng', '間取り', '階'])['Bc_code'].agg(';'.join).reset_index()

#　A000001から１ずつ増えていくユニークコードを作成して追加
grouped['Unique_ID'] = ['A' + str(i).zfill(6) for i in range(1, len(grouped) + 1)]

# 必要なカラムのみを選択
grouped_sp = grouped[['Unique_ID', 'Bc_code']]

worksheet = spreadsheet.worksheet('Integrated_code')

# ワークシートの内容をクリア
worksheet.clear()

#　ワークシートに書き込み
set_with_dataframe(worksheet, grouped_sp)

###### EOF
