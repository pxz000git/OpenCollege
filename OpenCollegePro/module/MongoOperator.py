# _*_ coding:utf-8 _*_
# autho:pxz
import sys
from imp import reload

reload(sys)
sys.path.append("../")

import pymongo as pm
from module import DealWord


class MongoOperat:
    def __init__(self, host, port, db_name_sour, db_name_top, db_kw_art, OUT_WEB_DATA,
                 ALL_WEB_DATA, LDA_DATA, KEYWORD_DATA, KW_ART, ART_VECTOR, VECTORS):
        '''
        设置mongodb的地址，端口以及默认访问的集合，后续访问中如果不指定collection，则访问这个默认的
        :param host: 端口
        :param port: 地址
        :param db_name: 数据库
        :param OUT_WEB_DATA: 某网站爬虫数据表
        :param ALL_WEB_DATA: 所有网站爬虫数据表
        :param LAD_DATA: 算法生成数据表
        :param KEYWORD_DATA:生成结果数据表
        :param ART_VECTOR:文档词向量表
        :param VECTORS:词向量表
        '''
        self.host = host
        self.port = port
        self.OUT_WEB_DATA = OUT_WEB_DATA
        self.ALL_WEB_DATA = ALL_WEB_DATA
        self.LAD_DATA = LDA_DATA
        self.KEYWORD_DATA = KEYWORD_DATA
        self.KW_ART = KW_ART
        self.ART_VECTOR = ART_VECTOR
        self.VECTORS = VECTORS
        # 建立数据库连接
        self.client = pm.MongoClient(host=host, port=port)
        # 选择相应的数据库名称
        self.db_sour = self.client.get_database(db_name_sour)
        self.db_top = self.client.get_database(db_name_top)
        self.db_kw_art = self.client.get_database(db_kw_art)

    def DBinsert(self, filer, LDA_DATA):
        '''
        临时-->插入关键字数据文件
        :param LDA_DATA:
        :return:
        '''
        collection1 = self.db_top.get_collection(LDA_DATA)
        f = open(filer, 'r')
        data = f.read()
        list_data1 = []
        list_data2 = []
        for item in data.strip().split('Topic  :'):
            list_data1.append(item.strip())
        for kv in list_data1:
            list_data2.append(kv.strip().split('\n'))
        for i in list_data2:
            collection1.save({"words": i})
        f.close()

    def DBtransfer(self, OUT_WEB_DATA, ALL_WEB_DATA):
        '''
        把OUT_WEB_DATA集合中的数据转换到ALL_WEB_DATA集合中
        :param OUT_WEB_DATA:
        :param ALL_WEB_DATA:
        :return:
        '''
        collection1 = self.db_sour.get_collection(OUT_WEB_DATA)
        collection2 = self.db_sour.get_collection(ALL_WEB_DATA)
        for item in collection1.find():
            collection2.save({'title': item.get('title'), 'html': item.get('html'), 'time': item.get('startTime')})

    def data_count(self, LDA_DATA, ALL_WEB_DATA, KEYWORD_DATA):
        '''
        把LAD_DATA集合中的关键字数据经与ALL_WEB_DATA文章集合词频统计后(对应文章的次数)K-V存储>到KEYWORD_DATA集合
        :param LAD_DATA:
        :param ALL_WEB_DATA:
        :param KEYWORD_DATA:
        '''
        collection1 = self.db_top.get_collection(LDA_DATA)
        collection2 = self.db_sour.get_collection(ALL_WEB_DATA)
        collection3 = self.db_top.get_collection(KEYWORD_DATA)
        row_val = 'words'
        word_dict = DealWord.coll_distin(collection1, row_val)
        for kw in word_dict.keys():
            kw = str(kw)
            print(kw)
            i = collection2.find({"html": {"$regex": kw}}).count()
            time = collection2.find({"time": {"$regex": 'time'}}).count()
            collection3.save({"keywords": kw, "value": i, "time": time})

    def kw_art_count(self, LDA_DATA, ALL_WEB_DATA, KW_ART):
        '''
        根据每段文章或网页对主题的包含程度（例如关键字出现频率）做一个主题到文章
        的关联数据表
        :param KEYWORD_DATA:
        :param ALL_WEB_DATA:
        :return:
        '''
        collection1 = self.db_top.get_collection(LDA_DATA)
        collection2 = self.db_sour.get_collection(ALL_WEB_DATA)
        collection3 = self.db_kw_art.get_collection(KW_ART)
        row_val = 'words'
        word_dict = DealWord.coll_distin(collection1, row_val)
        for kw in word_dict.keys():
            kw = str(kw)
            for art in collection2.find():
                i = str(art.get('html')).count(kw)
                if i == 0:
                    continue
                collection3.save({"keyword": kw, "value": i, "title": art.get('title'), "articy": art.get('html')})

    def find_art_bykey(self, word, sort_value, LDA_DATA, KW_ART):
        '''
        根据关键字查找一系列排序的（保存在数据库表中）文章
        :param word: 关键字
        :param sort_value: 排序方式
        :param LDA_DATA: 关键字表
        :param KW_ART: 文章表
        :return:
        '''
        collection1 = self.db_top.get_collection(LDA_DATA)
        collection2 = self.db_kw_art.get_collection(KW_ART)
        DealWord.find_art_bykey(word, sort_value, collection1, collection2)

    def doc_word(self, stopwords, KW_ART, ART_VECTOR):
        '''
        文档生成文档分词
        :param stopwords: 停用词
        :param KW_ART: 文档表
        :param ART_VECTOR: 分词文档表
        :return: None
        '''
        collection1 = self.db_kw_art.get_collection(KW_ART)
        collection2 = self.db_kw_art.get_collection(ART_VECTOR)
        for item in collection1.find():
            word_list = DealWord.stop_words(stopwords, item)
            print(word_list)
            # collection2.save({"words": word_list})

    def word_vec(self, ART_VECTOR, VECTORS):
        '''
        文档生成词向量
        :param KW_ART:
        :param ART_VECTOR:
        :return:
        '''
        collection1 = self.db_kw_art.get_collection(ART_VECTOR)
        collection2 = self.db_kw_art.get_collection(VECTORS)
        for item in collection1.find().batch_size(10):
            word_list = item.get('words')
            model = DealWord.w_v(word_list)
            a = int(word_list.__len__())
            for row in model.getVectors().take(a):
                collection2.save({"word_vec": {"word": row['word'], "vector": str(row['vector'])}})
                # print({"word_vec": {"word": row['word'], "vector": str(row['vector'])}})
