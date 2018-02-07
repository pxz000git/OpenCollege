#!/usr/bin/env python
# _*_ coding:utf-8 _*_
# autho:pxz
import sys

sys.path.append("../")

import os

os.environ["PYSPARK_PYTHON"] = "/usr/local/bin/python3"
from module.MongoOperator import MongoOperat
from conf import settings

host = settings.host
port = settings.port
# 设置数据库
DB_NAME_SOUR = settings.DATABASES_NAME['DB_NAME_SOUR']
DB_NAME_TOP = settings.DATABASES_NAME['DB_NAME_TOP']
DB_KW_ART = settings.DATABASES_NAME['DB_KW_ART']

# 设置数据表
OUT_WEB_DATA = settings.TABLE_NAME['OUT_WEB_DATA']
ALL_WEB_DATA = settings.TABLE_NAME['ALL_WEB_DATA']
LDA_DATA = settings.TABLE_NAME['LDA_DATA']
KEYWORD_DATA = settings.TABLE_NAME['KEYWORD_DATA']
KW_ART = settings.TABLE_NAME['KW_ART']
ART_VECTOR = settings.TABLE_NAME['ART_VECTOR']
VECTORS = settings.TABLE_NAME['VECTORS']

filer = settings.FILER
# print(filer)
sort_value = settings.SORT_VALUE
stopwords = settings.STOP_WORDS
db = MongoOperat(host, port, DB_NAME_SOUR, DB_NAME_TOP, DB_KW_ART,
                 OUT_WEB_DATA, ALL_WEB_DATA, LDA_DATA, KEYWORD_DATA,KW_ART, ART_VECTOR, VECTORS)


def run():
    pass
    # db.DBinsert(filer,LDA_DATA)                           #关键字插入（临时）
    # db.DBtransfer(OUT_WEB_DATA,ALL_WEB_DATA)              #把抓取的源数据进行分类
    # db.data_count(LDA_DATA,ALL_WEB_DATA,KEYWORD_DATA)     #词频统计
    db.kw_art_count(LDA_DATA, ALL_WEB_DATA, KW_ART)       #文章主题关联
    # word = input("请输入查询的关键字:")
    # word = str(word)
    # word = "教育"
    # db.find_art_bykey(word,sort_value,LDA_DATA,KW_ART)    #根据关键字查找文章
    # db.doc_word(stopwords, KW_ART, ART_VECTOR)            # 文档生成分词
    # db.word_vec(ART_VECTOR,VECTORS)                        #分词2词向量


if __name__ == '__main__':
    run()
