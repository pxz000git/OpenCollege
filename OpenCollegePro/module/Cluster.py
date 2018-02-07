#!/usr/bin/env python
# _*_ coding:utf-8 _*_
# autho:pxz

from pyspark.sql import SparkSession


def spark_context():
    spark = SparkSession \
        .builder \
        .appName("myApp") \
        .config("spark.mongodb.input.uri", "mongodb://175.102.18.112:27018/OpenCollege_kw.art2vec_syn") \
        .config("spark.mongodb.output.uri", "mongodb://175.102.18.112:27018/OpenCollege_kw.VECTORS_syn") \
        .getOrCreate()

    # people =spark.createDataFrame([("Bilbo Baggins", 50), ("Gandalf", 1000), ("Thorin", 195), ("Balin", 178), ("Kili", 77), ("Dwalin", 169), ("Oin", 167), ("Gloin", 158), ("Fili", 82), ("Bombur", None)], ["name", "age"])
    # people.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").save()

    df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
    print(type(df.take(5).fit))
    # print(df.take(5)[1])
    print((df.take(5)[1])['words'])


# df.show()

if __name__ == '__main__':
    spark_context()
