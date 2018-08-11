#!/usr/bin/env python
# -*- coding: utf-8 -*-

import psycopg2

class DB():
    def __init__(self, dbname):
        # データベースに接続
        self.conn = psycopg2.connect(host="localhost", dbname=dbname, user="postgres", password="qazwsxedc")
        self.cur = self.conn.cursor()

    # データベースを新規作成
    def create_db(self, new_dbname):
        # ※データベース新規作成時は、dbname="postgres"などの既存のデータベースに接続すること
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.cur = self.conn.cursor()
        self.cur.execute('CREATE DATABASE %s' % new_dbname)
        self.conn.commit()

    # SQL実行処理 (CREATE, DROP, DELETE, UPDATE,..など)
    def execute_sql(self, sql):
        self.cur.execute(sql)
        self.conn.commit()

    # リスト型データの挿入処理
    # 例 insert('INSERT INTO twiurl (idol_group_id,twitter_name,twitter_url,account_type) VALUES (%s,%s,%s,%s)', [1002, 'myidol', 'https://hoge.com', 'other'])
    def insert(self, sql, list):
        self.cur.execute(sql, list)
        self.conn.commit()

    # リスト型データの取得処理
    # 例 select('SELECT ota_follow_id, follow_num FROM otafollow WHERE idol_group_id = %s ORDER BY follow_num DESC LIMIT 100' % (idol_group_id))
    def select(self, sql):
        self.cur.execute(sql)

        # tupleをlistに変換した形式で返す [(a,b),(c,d),..] => [[a,b],[c,d],..]
        rows = [list(i) for i in self.cur.fetchall()]

        # 1次元のリストならネストを削除 [[1],[2],[3]] => [1, 2, 3]
        if len(rows) >= 1:
            if len(rows[0]) == 1:
                values = list()
                for l in rows:
                    values.append(l[0])
                return values

        return rows

    # データベースを閉じる処理
    def close(self):
        self.cur.close()
        self.conn.close()

if __name__ == '__main__':

    """ データベース新規作成時に実行
    db = DB("postgres")
    db.create_db("iddata")
    db.close()
    """



