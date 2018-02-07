#!/usr/bin/env python
# _*_ coding:utf-8 _*_
# autho:pxz
import sys

sys.path.append("../")

# 设置主题数量
k = 200

# 排序,1升序，-1降序
SORT_VALUE = -1

host = '175.102.18.112'
port = 27018

# 关键字文件
FILER = '../data/kw' + str(k) + '.txt'
# 停用词
STOP_WORDS = '../data/stopwords.txt'
# 数据库表
'''
        "HangZhouLLL",
        "HaoKeWang",
        "JS-Study",
        "MoocCnOnline",
        "MoocOpen",
        "SHLLL",
        "WanFang",
        "WangYiYunCourse",
        "ZheJiangLLL",
        "all_courses",
        "all_courses_edu",
        "all_courses_jour",
        "all_courses_syn",
        "haodaxue",
        "laonianren",
        "zhihuishu",
        "zhiwang"
'''
# 分类表：教育、期刊、综合性大学、科研
class_table = 'syn'  # edu,jour,syn,mooc

# 数据库名
DATABASES_NAME = {
    'DB_NAME_SOUR': 'OpenCollege',  # 数据抓取,数据分析源库
    'DB_NAME_TOP': 'openCourseOut_em',  # LDA关键字提取及分析结果库
    'DB_KW_ART': 'OpenCollege_kw',  # 关键字排序库
}
# 数据表名
TABLE_NAME = {
    'OUT_WEB_DATA': 'MoocCnOnline',  # 数据分析源表
    'ALL_WEB_DATA': 'all_courses_' + class_table,  # 数据源分类表
    'KW_ART': 'art_' + class_table,  # 数据关键字排序表
    'LDA_DATA': 'LDA_course_html_out_' + str(k),  # 关键字提取表
    'KEYWORD_DATA': 'keyWord_result_' + str(k),  # 关键字对应文章次数表
    'ART_VECTOR': 'art2vec_' + class_table,  # 文档词向量结果表
    'VECTORS': 'VECTORS_' + class_table,
}

# 教育_edu
# OUT_WEB_DATA_EDU =[ 'zhihuishu',
#                     'haodaxue',
#                     'WangYiYunCourse',
#                     'HaoKeWang',
#                     'MoocCnOnline',
#                     'MoocOpen'
#                     ]
# 期刊_jour
# OUT_WEB_DATA_JOUR =[
#     'zhiwang',
#     'WanFang'
# ]
# 综合性大学网院_syn
# OUT_WEB_DATA_JOUR = [
#     'laonianren'
# ]
