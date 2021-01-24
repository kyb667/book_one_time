import common
from datetime import datetime, date, timedelta
from dateutil.relativedelta import relativedelta
import pandas as pd

CSV_FOLER = common.makeCsvFoler
SEND_EMAIL_CONNECTION = common.getConnection()

def make_csv(today):
    yesterday = (today - timedelta(common.ONEDAY)).strftime('%Y-%m-%d')
    with SEND_EMAIL_CONNECTION.cursor() as db_cursor:
        db_cursor.execute(""" 
            SELECT a.*
            FROM applicationbook a,
                (SELECT user_id, title, author, price, publisher, max(update_num) update_num
                FROM applicationbook
                WHERE to_char(created_at, 'YYYY-MM-DD') = '{0}' or to_char(update_at, 'YYYY-MM-DD') = '{0}'
                GROUP BY user_id, title, author, price, publisher) r
            WHERE a.user_id = r.user_id
            AND a.title = r.title
            AND a.author = r.author
            AND a.price = r.price
            AND a.publisher = r.publisher
            AND a.update_num = r.update_num
        """.format(yesterday))
        result = db_cursor.fetchall()
        if result:
            df = pd.DataFrame(result)
            df.columns = common.APPLICATIONBOOK_COL
            df.to_csv(CSV_FOLER+'/applicationbook'+ '.csv', index=False)


def delete_data(today):
    target_date = (today - relativedelta(months=common.DELETE_DATA_MONTH)).strftime('%Y-%m-%d')
    with SEND_EMAIL_CONNECTION.cursor() as db_cursor:
        db_cursor.execute(""" 
            DELETE
            FROM applicationbook
            WHERE to_char(COALESCE(update_at, created_at), 'YYYY-MM-DD') = '{0}' 
        """.format(target_date))