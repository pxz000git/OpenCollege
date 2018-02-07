#! /usr/bin/env python
# -*- coding:utf-8 -*-
import sys

sys.path.append("../")
import jieba
import re

from pyspark.mllib.feature import Word2Vec
from pyspark.ml.feature import Word2Vec
from pyspark.sql import SparkSession


def find_art_bykey(word, sort_value, coll1, coll2):
    '''
    根据关键字查找一系列排序的（保存在数据库表中）文章
    :param word:
    :param sort_value:
    :param coll1:
    :param coll2:
    :return:
    '''
    row_val = 'words'
    word_dict = coll_distin(coll1, row_val)
    if word not in word_dict.keys():
        print("没有找到该关键字!!!")
    else:
        if coll2.find({"keyword": {"$regex": word}}).count() > 0:
            cursor = coll2.find({'keyword': word}).sort('value', sort_value).limit(10)
            for s in cursor:
                print(s['keyword'], s['value'], s['title'], s['articy'])


def coll_distin(coll, row_val):
    '''
    集合内容去重
    :param coll:
    :return:
    '''
    word_dict = {}
    for item in coll.find():
        for kw_1 in item.get(row_val):
            if kw_1 not in word_dict:
                # 去除重复关键字
                word_dict[kw_1] = 1
    return word_dict


def stop_words(stopwords, item):
    '''
    处理文档，过滤停用词
    :param stopwords:
    :param item:
    :return:返回过滤停用词后的列表
    '''
    with open(stopwords, 'r') as f:
        stop_words = f.read()
        regex = r'^[\u4e00-\u9fa5_a-zA-Z]+$'
        content = []
    for i in jieba.cut(item.get('articy').replace(',', '').strip(), cut_all=False):
        if i not in stop_words and i != '\t' and '\n' and re.match(regex, i):
            content.append(i)
    word_list = list(set(content))
    return word_list


def w_v(word_list):
    spark = SparkSession \
        .builder \
        .master('local[2]') \
        .appName("w2v") \
        .getOrCreate()
    docmentDF = spark.createDataFrame([(word_list,), (word_list,)], ["sentence"])
    # vectorSize可以控制向量的维度
    word2Vec = Word2Vec(vectorSize=10, minCount=1, inputCol='sentence', outputCol='model')
    model = word2Vec.fit(docmentDF)
    return model
