#!/usr/bin/python
# -*- coding: utf-8 -*-
# author:pxz

import sys
import os
from imp import reload

reload(sys)
sys.path.append("../")

from ParseChat.conf import settings
from pyspark.ml.feature import Word2Vec
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.clustering import KMeans, LDA
from pyspark.ml.feature import CountVectorizer
import jieba

sourcePath = settings.SOURCEPATH['sourcePath']
stopPath = settings.STOPPATH['stopPath']

conf = SparkConf().setMaster('local').setAppName("APP")
sc = SparkContext(conf=conf)
spark = SparkSession(sc)


def loadData():
    df = spark.read.csv(sourcePath, header=True)
    df = df.select(df.number.alias("id"), df.context.alias("content"))
    print(df.show(truncate=False))
    # # df = df.selectExpr('number as id', 'context as content')
    rdd = df.rdd.combineByKey(createCombiner, mergeValue, mergeCombiners)
    df = spark.createDataFrame(rdd, ['id', 'content'])
    # 参数是函数,函数应用于RDD的每一个(一行)元素,返回一个新的RDD
    rdd = df.rdd.map(tokenizer)
    df = spark.createDataFrame(rdd, ['id', 'content']).cache()
    return df


def createCombiner(value):
    return value


def mergeValue(acc, value):
    return acc + value


def mergeCombiners(acc1, acc2):
    return acc1 + acc2


def stop_word():
    '''
    :return: 停用词列表
    '''
    with open(stopPath, "r") as f:
        stop_words = f.read().split('\n')
    return list(set(stop_words))


stop_words = stop_word()


def tokenizer(row):
    '''
    分词并且过滤停用词
    :return:[row.id, result]
    '''
    result = list()
    row2 = ''.join(row.content.split())  # 去除空格字符
    cut_words = jieba.cut(row2)
    # 过滤停用词
    for word in cut_words:
        if word not in stop_words:
            result.append(word)
    return [row.id, result]


def w2v():
    '''
    word2vector处理
    :return: df_word2vec 词向量
    '''
    df = loadData()
    word2Vec = Word2Vec(vectorSize=3, inputCol="content", outputCol="features")
    model_word2Vec = word2Vec.fit(df)
    df_word2vec = model_word2Vec.transform(df)
    return df_word2vec.cache()


def kemans():
    '''
    KMEANS 聚类，可设置不同数目聚类中心
    :return:
    '''
    df_word2vec = w2v()
    km = KMeans(featuresCol="features", k=3)
    model_km = km.fit(df_word2vec)
    # print(model_km.clusterCenters())
    df_km = model_km.transform(df_word2vec).select("id", "prediction")
    df_km.cache()
    df_km.show(truncate=False)


def count2Vector():
    '''
    根据统计词频转化成向量
    :return: df_countv
    '''
    df = loadData()
    countv = CountVectorizer(inputCol="content", outputCol="features")
    model_countv = countv.fit(df)
    df_countv = model_countv.transform(df).cache()
    df_countv.show(truncate=True)
    return df_countv


def lda():
    '''
    由词频分类
    :return:
    '''
    df = count2Vector()
    lda = LDA(featuresCol='features', k=3, seed=1, optimizer="em")
    lad_model = lda.fit(df)
    df_show = lad_model.describeTopics()
    print(df_show)


if __name__ == "__main__":
    # kemans()
    lda()
