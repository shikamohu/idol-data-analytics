#!/usr/bin/env python
# -*- coding: utf-8 -*-

import tweepy
from database import DB
import datetime
import collections
import time
import os
from os.path import join, dirname
from dotenv import load_dotenv

class TwitterCrawl():
    def __init__(self):
        # TODO : dotenvで別途ファイルを作成し、そこから参照する。.envはgitignoreにする
        # OAuth keys
        dotenv_path = join(dirname(__file__), '.env')
        load_dotenv(dotenv_path)

        self.consumer_key = os.environ.get("CONSUMER_KEY")
        self.consumer_secret = os.environ.get("CONSUMER_SECRET")
        self.access_key = os.environ.get("ACCESS_KEY")
        self.access_secret = os.environ.get("ACCESS_SECRET")

        self.auth = tweepy.OAuthHandler(self.consumer_key, self.consumer_secret)
        self.auth.set_access_token(self.access_key, self.access_secret)

        self.api = tweepy.API(self.auth, wait_on_rate_limit=True)

    # DBからアイドルグループのTwitterのscreen_nameをリスト形式で取得 ex. [[2, 'countrygirls_uf'], [5, 'JuiceJuice_uf'],..]
    def _select_idol_group_screen_name(self, start_idol_group_id=0):
        db = DB('iddata')

        # リスト形式でアイドルグループのURLを取得 ex. [[2, 'https://twitter.com/countrygirls_uf'], [5, 'https://twitter.com/JuiceJuice_uf'],..]
        urls = db.select("SELECT idol_group_id, url FROM idol_group_twitter_url WHERE account_type = 'official' AND idol_group_id >= %s ORDER BY idol_group_id ASC" % (start_idol_group_id))
        db.close()
        print(urls)

        # 不要部分を削除し、URLをUserIDに変換 ex. [[2, 'countrygirls_uf'], [5, 'JuiceJuice_uf'],..]
        screen_names = [[url[0], url[1].replace('https://twitter.com/', '').replace('http://twitter.com/', '').replace('/', '').replace('?lang=ja', '')] for url in urls]

        return screen_names

    # 現在の日付を返すメソッド
    def _date_now(self):
        date_now = datetime.datetime.now().strftime('%Y-%m-%d')
        return date_now

    # 登録したアイドルグループのTwitterフォロワー数をDBに挿入
    def idol_group_follower_num(self):

        db = DB('iddata')

        screen_names = self._select_idol_group_screen_name()

        # フォロワー数を取得してリストを作成
        follower_nums = list()
        for screen_name in screen_names:
            try:
                # フォロワー数リストに[idol_group_id, screen_name, フォロワー数, 取得日]を挿入
                row = [screen_name[0], screen_name[1], self.api.get_user(screen_name[1]).followers_count, self._date_now()]
                follower_nums.append(row)

            # 例外処理
            except tweepy.error.TweepError:
                # 存在しないTwitterのscreen_nameを表示
                print("存在しないscreen_nameです : ", screen_name[0], screen_name[1])
                print("フォロワー数 : 0")

                # フォロワー数 : 0を挿入
                row = [screen_name[0], screen_name[1], 0, self._date_now()]
                follower_nums.append(row)

        # DBにフォロワー数を挿入 [idol_group_id, screen_name, フォロワー数, 取得日]
        for follower_num in follower_nums:
            print(follower_num)
            db.insert('INSERT INTO idol_group_twitter_follower_num (idol_group_id, screen_name, follower_num, recode_date) VALUES (%s,%s,%s,%s)', follower_num)

        db.close()


    # 登録したアイドルグループのオタベクトルをDBに挿入
    def idol_group_otavector(self, start_idol_group_id=0):

        # 処理前の時刻
        t1 = time.time()

        db = DB('iddata')
        screen_names = self._select_idol_group_screen_name(start_idol_group_id)

        # オタベクトルをdictで取得してDBに挿入
        for screen_name in screen_names:

            print("idol_group_id : %d screen_name : %s のオタベクトルを取得します" % (screen_name[0], screen_name[1]))

            # 1つのアイドルグループのTwitterのフォロワーuser_idを300件取得
            follower_ids = tweepy.Cursor(self.api.followers_ids, screen_name=screen_name[1]).items(300)

            # フォロワーがフォローしているuser_idのリスト
            follower_follow_userids = list()

            # フォローuser_idを取得したフォロワーのカウント
            count = 0

            # 1つのアイドルグループのフォロワーがフォローしているuser_idを1000件まですべて取得
            for follower_id in follower_ids:

                # 1人のフォロワーのuserオブジェクトを取得する
                follower = self.api.get_user(user_id=follower_id)

                # 鍵垢のフォロワーはスキップする
                if follower.protected is True:
                    continue

                # 1人のフォロワーがフォローしているuser_idをリストに追加
                try:
                    follower_follow_userids.extend(tweepy.Cursor(self.api.friends_ids, id=follower_id).items(1000))
                    count += 1
                # 例外処理
                except tweepy.error.TweepError as e:
                    print(e.reason)

                # 100人のフォロワーのフォローuser_idを取得したらループを抜ける
                if count >= 100:
                    break

            # オタがフォローしているuser_id (key) と重複数 (value) の辞書を作成 ex. {901547836 : 100, 21548745 : 88, ...}
            otavector = collections.Counter(follower_follow_userids)
            print(otavector)

            # データベースにオタベクトルを挿入
            for key, value in otavector.items():
                db.insert('INSERT INTO idol_group_twitter_otavector (idol_group_id, screen_name, follow_userid, follow_num, recode_date) VALUES (%s,%s,%s,%s,%s)',[screen_name[0], screen_name[1], key, value, self._date_now()])
            print("idol_group_id : %d screen_name : %s のフォロワー %d 人のオタベクトルを取得しました" % (screen_name[0], screen_name[1], count))

            # 処理後の時刻
            t2 = time.time()

            # 経過時間を表示
            elapsed_time = t2 - t1
            print(f"経過時間：{elapsed_time}\n")

        db.close()


def create_table():
    """ テーブル作成SQL
    "CREATE TABLE idol_group_twitter_follower_num (idol_group_id integer, screen_name varchar(255), follower_num integer, recode_date date) WITH OIDS;"
    """
    db = DB("iddata")
    #db.execute_sql("CREATE TABLE idol_group_twitter_follower_num (idol_group_id integer, screen_name varchar(255), follower_num integer, recode_date date) WITH OIDS;")
    db.execute_sql("CREATE TABLE idol_group_twitter_otavector (idol_group_id integer, screen_name varchar(255), follow_userid varchar(255), follow_num integer, recode_date date) WITH OIDS;")
    db.close()


if __name__ == '__main__':

    #create_table()
    crawl = TwitterCrawl()
    crawl.idol_group_otavector(410)






