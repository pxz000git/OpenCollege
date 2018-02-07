#! /usr/bin/env python
# -*- coding:utf-8 -*-
import os

from pyspark.sql import SparkSession

os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
from pyspark.ml.feature import Word2Vec


def spark_context():
    spark = SparkSession \
        .builder \
        .master('local[2]') \
        .appName("w2v") \
        .getOrCreate()
    # spark = SparkSession \
    #     .builder \
    #     .appName("myApp") \
    #     # .config("spark.mongodb.input.uri", "mongodb://175.102.18.112:27018/OpenCollege_kw.art2vec_syn") \
    #     # .config("spark.mongodb.output.uri", "mongodb://175.102.18.112:27018/OpenCollege_kw.VECTORS_syn") \
    #     .getOrCreate()
    sent = ("a f f a c b f f f").split()
    doc = spark.createDataFrame([(sent,)], ["sentence"])
    print(doc.collect())
    word2Vec = Word2Vec(vectorSize=3, seed=10, inputCol="sentence", outputCol="model")
    model = word2Vec.fit(doc)
    a = int(sent.__len__())
    for item in model.getVectors().take(a):
        # collection3.save({"word_vec": {"word": item['word'], "vector": str(item['vector'])}})
        print({"word_vec": {"word": item['word'], "vector": str(item['vector'])}})


if __name__ == "__main__":
    spark_context()
