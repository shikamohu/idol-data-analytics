#!/usr/bin/env python
# -*- coding: utf-8 -*-

from iddata import DB
import numpy as np
import pandas as pd
from matplotlib import pyplot as plt
import matplotlib as mpl
from sklearn.decomposition import PCA
from sklearn import datasets

# フォントを設定
font = {"family":"IPAexGothic"}
mpl.rc('font', **font)

def main():

    dataset = datasets.load_iris()
    print(dataset)
    print(type(dataset))

    features = dataset.data
    print(type(features))
    targets = dataset.target

    # 主成分分析する
    pca = PCA(n_components=2)
    pca.fit(features)

    # 分析結果を元にデータセットを主成分に変換する
    transformed = pca.fit_transform(features)

    # 主成分をプロットする
    for label in np.unique(targets):
        plt.scatter(transformed[targets == label, 0],
                    transformed[targets == label, 1])
    plt.title('principal component')
    plt.xlabel('pc1')
    plt.ylabel('pc2')

    # 主成分の寄与率を出力する
    print('各次元の寄与率: {0}'.format(pca.explained_variance_ratio_))
    print('累積寄与率: {0}'.format(sum(pca.explained_variance_ratio_)))

    # グラフを表示する
    plt.show()



def idol_pca():
    db = DB()
    # アイドルグループのidリストを取得
    # 262,361だけ異常値なので外した
    idol_group_ids = db.select('SELECT DISTINCT idol_group_id FROM idol_group_twitter_otavector WHERE idol_group_id NOT IN(262,361) ORDER BY idol_group_id')
    print(idol_group_ids)

    # アイドルグループの名前リストを取得
    idol_group_names = list()
    for id in idol_group_ids:
        idol_group_names.append(db.select('SELECT idol_group_name FROM idol_group_name WHERE idol_group_id = %s' % (id))[0])

    # 空のdataflameを作成
    df = pd.DataFrame()

    # pandasのdataflame用にcolumnsとdataを作成
    for idol_group_id in idol_group_ids:
        # フォロワー数のかぶりが2以上のものを選択
        otavector = db.select('SELECT follow_userid, follow_num FROM idol_group_twitter_otavector WHERE idol_group_id = %s AND follow_num > 1 ORDER BY follow_num DESC' % (idol_group_id))
        follow_ids = list()
        follow_nums = list()

        for o in otavector:
            follow_ids.append(o[0])
            follow_nums.append(o[1])

        #record = pd.DataFrame(follow_nums, columns=ota_follow_ids, index=["idol_group_id"])
        s = pd.Series(follow_nums,index=follow_ids, name=idol_group_id)
        df = df.append(s)

    # NANを0に置き換え
    df = df.fillna(0)

    print(df)
    ndarray = df.values
    print(ndarray)
    print(type(ndarray))
    #df.to_csv("otavector.csv")

    # 主成分分析する
    pca = PCA(n_components=2)
    pca.fit(ndarray)

    # 分析結果を元にデータセットを主成分に変換する
    transformed = pca.fit_transform(ndarray)

    print(type(transformed))
    print(transformed)

    # 主成分をプロットする
    for t in transformed:
        plt.scatter(t[0], t[1])
    plt.title('principal component')
    plt.xlabel('pc1')
    plt.ylabel('pc2')

    # グループ名をプロットする
    for (name, t) in zip(idol_group_names, transformed):
        plt.annotate(name, xy=(t[0], t[1]))
    plt.tight_layout()

    # グラフを表示する
    plt.show()


if __name__ == '__main__':
    print(mpl.matplotlib_fname())
    #print(numpy.__file__)
    #main()
    idol_pca()

