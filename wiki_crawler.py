#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import re
import difflib
from database import DB
from urllib.parse import urljoin

class WikiCrawl():
    def __init__(self):
        pass

    def idol_group_wiki_namecheck(self):
        pass

    # アイドルグループのwikipediaのURLを取得
    def idol_group_wiki_url(self):
        db = DB('iddata')
        # 女性アイドルグループのURL
        base_url = 'https://ja.wikipedia.org/wiki/%E6%97%A5%E6%9C%AC%E3%81%AE%E5%A5%B3%E6%80%A7%E3%82%A2%E3%82%A4%E3%83%89%E3%83%AB%E3%82%B0%E3%83%AB%E3%83%BC%E3%83%97%E3%81%AE%E4%B8%80%E8%A6%A7'
        r = requests.get(base_url)
        content = r.content
        soup = BeautifulSoup(content, 'html.parser')

        # すべての該当クラスの<div>タグをリストで返す ex. [<div class="hoge">～</div>, <div>～</div>,...,<div>～</div>]
        divs = soup.find_all('div', class_='div-col columns column-count column-count-2')

        # 各<div>タグの要素から<a>タグを抜き出し、グループコード,グループ名,URLを抜き出す(80,90年代はパス）
        for div in divs[2:]:
            idol_groups = div.find_all('a')
            for idol_group in idol_groups:

                # 相対パスを絶対パスに変換して取得
                url = urljoin(base_url, idol_group.get('href'))
                name = idol_group.text

                # データベースに登録済みか確認
                pass_url = list()
                pass_url.extend(db.select('SELECT url FROM idol_group_wiki_url;'))
                pass_url.extend(db.select('SELECT url FROM not_idol_group_wiki_url;'))
                if url in pass_url:
                    continue

                # idol_group_idを設定
                max_id = db.select('SELECT MAX(idol_group_id) FROM idol_group_name;')[0]
                if max_id is None:
                    id = 1
                else:
                    id = max_id + 1

                # データベースへの登録処理
                print(id, name, url)
                command = input('新規アイドルグループに登録しますか？ (y/n/skip) >>')
                if command is 'y':
                    db.insert('INSERT INTO idol_group_name (idol_group_id, idol_group_name) VALUES (%s,%s)', [id, name])
                    db.insert('INSERT INTO idol_group_wiki_url (idol_group_id, url) VALUES (%s,%s)', [id, url])
                    print('登録しました')
                elif command is 'n':
                    db.insert('INSERT INTO not_idol_group_wiki_url (not_idol_group_name, url) VALUES (%s,%s)', [name, url])
                    print('URLを除外リストに挿入しました')
                else:
                    print('スキップしました')

        db.close()

    # アイドルグループのtwitterのURLを取得
    def idol_group_twitter_url(self):
        db = DB('iddata')
        id_name_wikiurls = db.select('SELECT N.idol_group_id, N.idol_group_name, W.url FROM idol_group_name AS N INNER JOIN idol_group_wiki_url AS W ON N.idol_group_id = W.idol_group_id')
        for id_name_wikiurl in id_name_wikiurls:

            # wikipediaの個別アイドルグループのURLをクロール
            res = requests.get(id_name_wikiurl[2])
            content = res.content
            soup = BeautifulSoup(content, 'html.parser')

            # twitter.comを含む<a>タグをlistで取得
            twitter_a = soup.find_all('a', href=re.compile("twitter.com"))

            # <a>タグからTwitterURLとツイッター名を取得しlist化
            twitter_name_urls = list()
            for a in twitter_a:
                twitter_url = a.get('href')
                twitter_name = a.text

                # すでにURLがDBに登録されていたらスキップ
                db_twitter_url = db.select("SELECT url FROM idol_group_twitter_url WHERE idol_group_id = %s AND url = '%s'" % (id_name_wikiurl[0], twitter_url))
                if len(db_twitter_url) > 0:
                    continue

                # URLに特定の文字列が含まれていれば、スキップ
                if '/status' in twitter_url:
                    continue

                twitter_name_urls.append([twitter_name, twitter_url])

            # 追加twitter_name_urlsが空ならスキップ
            if len(twitter_name_urls) == 0:
                continue

            # twitter_name_urlsリストの各先頭にtargetsリストのマッチ度を挿入
            targets = ["公式", "運営", "オフィシャル", "スタッフ", "staff","OFFICIAL"]
            targets.append(id_name_wikiurl[1])
            for i, name_url in enumerate(twitter_name_urls):
                match_ratio = 0
                for target in targets:
                    match_ratio += difflib.SequenceMatcher(None, name_url[0], target).ratio()
                twitter_name_urls[i].insert(0, match_ratio)

            # [[match_ratio, idol_group_name, twitter_url],..]のリストをマッチ度の高い順にソート
            twitter_match_name_urls = twitter_name_urls
            twitter_match_name_urls.sort(reverse=True)
            print(id_name_wikiurl[0], id_name_wikiurl[1])
            print('データベースのTwitterURLリスト')
            print(db.select('SELECT idol_group_id, twitter_name, url, account_type FROM idol_group_twitter_url WHERE idol_group_id = %s' % id_name_wikiurl[0]))
            print('追加するTwitterURLリスト')
            print(twitter_match_name_urls)


            command = input('%sをofficialにしますか？(y/n) >>' % (twitter_match_name_urls[0]))
            if command is 'y':
                # データベース内の特定アイドルグループのtwitterURLのアカウントタイプをすべてotherに更新
                db.execute_sql("UPDATE idol_group_twitter_url SET account_type = 'other' WHERE idol_group_id = %s" % id_name_wikiurl[0])

                # TwitterURLを挿入
                for count, match_name_url in enumerate(twitter_match_name_urls):

                    # マッチ度が高いURLはofficialにして挿入
                    if count == 0:
                        db.insert('INSERT INTO idol_group_twitter_url (idol_group_id, twitter_name, url, account_type) VALUES (%s,%s,%s,%s)', [id_name_wikiurl[0], match_name_url[1], match_name_url[2], 'official'])
                        continue
                    db.insert('INSERT INTO idol_group_twitter_url (idol_group_id, twitter_name, url, account_type) VALUES (%s,%s,%s,%s)', [id_name_wikiurl[0], match_name_url[1], match_name_url[2], 'other'])
                print('officialで登録しました')
            else:
                # TwitterURLをotherにして挿入
                for match_name_url in twitter_match_name_urls:
                    db.insert(
                        'INSERT INTO idol_group_twitter_url (idol_group_id, twitter_name, url, account_type) VALUES (%s,%s,%s,%s)', [id_name_wikiurl[0], match_name_url[1], match_name_url[2], 'other'])
                print('otherで登録しました')

def create_table():
    """ データベース作成SQL
    "CREATE DATABASE iddata;"
    """
    """ テーブル作成SQL
    "CREATE TABLE idol_group_name (idol_group_id integer PRIMARY KEY, idol_group_name varchar(255)) WITH OIDS;"
    "CREATE TABLE idol_group_wiki_url (idol_group_id integer PRIMARY KEY, url varchar(255)) WITH OIDS;"
    "CREATE TABLE not_idol_group_wiki_url (not_idol_group_name varchar(255), url varchar(255)) WITH OIDS;"
    "CREATE TABLE idol_group_twitter_url (idol_group_id integer, twitter_name varchar(255), url varchar(255), account_type varchar(255)) WITH OIDS;"
    """
    db = DB('iddata')
    db.execute_sql("CREATE TABLE idol_group_name (idol_group_id integer PRIMARY KEY, idol_group_name varchar(255)) WITH OIDS;")
    db.execute_sql("CREATE TABLE idol_group_wiki_url (idol_group_id integer PRIMARY KEY, url varchar(255)) WITH OIDS;")
    db.execute_sql("CREATE TABLE not_idol_group_wiki_url (not_idol_group_name varchar(255), url varchar(255)) WITH OIDS;")
    db.execute_sql("CREATE TABLE idol_group_twitter_url (idol_group_id integer, twitter_name varchar(255), url varchar(255), account_type varchar(255)) WITH OIDS;")
    db.close()

if __name__ == '__main__':

    #create_table()
    crawl = WikiCrawl()
    #crawl.idol_group_wiki_url()
    crawl.idol_group_twitter_url()




