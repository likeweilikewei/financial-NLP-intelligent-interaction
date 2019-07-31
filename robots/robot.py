#!/usr/bin/env python
# -*-coding=utf-8-*-

"""
author:lkw
date:2018.1.8
email:a18829040692@163.com
function:给定一个问题进行相应的回答
class:如下
class_one:选股
class_two:个股指标
class_three:个股行情
class_four:个股投顾建议
class_five:百科
class_six:娱乐聊天
class_seven:研报
"""

import hashlib
import json
import os
import queue
import time

import jieba.analyse
import jieba.posseg
import requests
from robots.RedisManager import RedisManager
from robots.corrector.cn_spell_test import corrector
from robots.dicts.bai_ke import *
from robots.dicts.bk import *
from robots.dicts.dicts import *
from robots.dicts.gg import *
from robots.dicts.gx import *
from robots.dicts.zb import *
from gensim import corpora
from gensim import models
from gensim import similarities

from robots.settings import LoggingEmail
from robots.settings import error_logger
from robots.settings import logger
from robots.settings import redisManager

# from nltk.stem.lancaster import LancasterStemmer

jieba.load_userdict(os.path.join(os.path.dirname(__file__), r'corpus/custom_dict'))


class Handler:
    """
    处理函数，用于统一语句处理接口
    """
    def __init__(self, _class_number_index):
        """
        构造函数
        :param _class_number_index: 类别下标，类别一的下标是0
        """
        self._class_number_index = _class_number_index

    def __callback(self, prefix, name, *args):
        """
        查看函数是否存在，存在就返回，不存在则不做处理
        :param prefix: 函数名的前半句
        :param name: 函数名的后半句
        :param args: 函数的参数
        :return: 函数的处理结果
        """
        method = getattr(self, prefix+name, None)
        if hasattr(method, '__call__'):  # python3用hasattr代替了if callable(method)
            answer = method(*args)
            return answer

    def generate_api_function(self):
        """
        调用问题类的处理函数
        :return:类别处理函数
        """
        classes = ['class_one', 'class_two', 'class_three', 'class_four', 'class_five',
                   'class_six']
        _name = classes[self._class_number_index]
        class_function = self.__callback('generate_api_function_', _name)
        return class_function


class ClassBasicFunction:
    """
    处理函数的基类
    """
    def __init__(self, _cut_input, _input_words, _page, _pagesize, _technical_flag, _technical_len):
        """
        构造函数，利用子类进行初始化
        :param _cut_input: 分词后的用户输入
        :param _input_words: 用户输入原话，主要用于提取数字
        :param _page: 用户请求的页号
        :param _pagesize: 用户请求的页面大小
        :param _technical_flag: 是否含有技术指标的标志，True：有。False:无。
        :param _technical_len: 含有的技术指标的个数
        """
        self._technical_flag = _technical_flag
        self._technical_len = _technical_len
        self._cut_input = _cut_input
        self._input_word = _input_words
        self._page = _page
        self._pagesize = _pagesize

    @staticmethod
    def index_query(name, indication, indication_type):
        redis_setting = {'host': '127.0.0.1', 'port': 6379, 'password': '123456'}
        if indication_type == 1:
            """redis index low frequency set"""
            redis_setting['db'] = 7
            __redis_manager = RedisManager(redis_setting)
            logger.info('label_index_type: indication type is low frequency.')
        else:
            """redis index high frequency set"""
            redis_setting['db'] = 10
            __redis_manager = RedisManager(redis_setting)
            logger.info('label_index_type: indication type is high frequency.')
        try:
            _result = __redis_manager.hget(name=name, key=indication)
            if not _result:
                _result = __redis_manager.hmget(name=name, keys=indication)
        except:
            _result = __redis_manager.hmget(name=name, keys=indication)
        print(_result)
        return _result

    @staticmethod
    def indexes_query(name, indications, indication_types):
        """
        从redis的高频和低频库中查询一只股票的多个指标
        :param name: 股票参数
        :param indications: 指标参数列表
        :param indication_types: 指标高低频类型列表，高频指标从高频库中查询，低频指标从低频库中查询，1：低频，2:高频。
        :return: 结果列表
        同时需要对输出结果进行类型转换，如果结果为None符号注意输出None
        """
        redis_setting = {'host': '127.0.0.1', 'port': 6379, 'password': '123456'}
        if indication_types == 1:
            """redis index low frequency set"""
            redis_setting['db'] = 7
            __redis_manager = RedisManager(redis_setting)
            logger.info('label_index_type: indication type is low frequency.')
        else:
            """redis index high frequency set"""
            redis_setting['db'] = 10
            __redis_manager = RedisManager(redis_setting)
            logger.info('label_index_type: indication type is high frequency.')
        try:
            _result = __redis_manager.hget(name=name, key=indications)
            if not _result:
                _result = __redis_manager.hmget(name=name, keys=indications)
        except:
            _result = __redis_manager.hmget(name=name, keys=indications)
        print(_result)
        return _result

    @staticmethod
    def get_number_from_str_list(words):
        """
        从分词后语句中提取数值,注意去除了%，5%提取成5
        :param words: 分词后的语句
        :return: 数值列表
        number_list:存放数值的列表
        negative_number_flag:负数的标志，必须是-的前面不是数字、带有负的情况下，前者是为了避免和区间混淆
        num_tmp:遍历的前一个word
        word:词列表中的一个词语
        word_tmp:在有%的情况下暂时存放不带%的数值
        """
        number_list = []
        negative_number_flag = False
        num_tmp = None
        for word in words:
            if '%' in word:
                word_tmp = word.strip('%')
                # words = float(words) / 100
                word_tmp = float(word_tmp)
                if negative_number_flag:
                    negative_number_flag = False
                    number_list.append(-word_tmp)
                else:
                    number_list.append(word_tmp)
                # 这里是为了让1%也记录为一个数字
                word = 1
            else:
                try:
                    if negative_number_flag:
                        number = float(word)
                        number_list.append(-number)
                        negative_number_flag = False
                    else:
                        number = float(word)
                        number_list.append(number)
                except:
                    if word in ['负']:
                        negative_number_flag = True
                    if word in ['-']:
                        try:
                            float(num_tmp)
                        except:
                            negative_number_flag = True
            num_tmp = word
        return number_list

    def select_stocks(self):
        """
        处理class_one,根据基本指标、财务指标、技术指标在板块里选股
        :return:回答
        提取redis api的请求参数
        range_type:筛选类型，范围：A, 大于：B，小于：C
        range_value:筛选值，数值要一一对应
        indication:指标名，要和指标类型一一对应
        indication_type_list:标记每一个指标是属于低频还是高频，方便判断是不是高频加低频的情况
        indication_type：标记是低频还是高频，或者是低频加高频，
        1：低频指标，2:高频指标，3：高频+低频指标，这个类别决定了用高频还是低频库
        如果是低频用低频库，如果是高频用高频库，如果是高频+低频用高频库
        indication_high_low:存放高频加低频情况下的所有低频指标，因为这种情况下使用高频库，
        为了使低频指标也能取到，因此在这里存放低频指标传到后端进行处理。
        indication_name:存放指标中文名称
        block_type:板块类型，14902:指数板块，14903：行业板块，14904：概念板块
        block:板块代码
        block_name:板块名称
        """
        params = {'range_type': [], 'range_value': [], 'indication': [],
                  'indication_type_list': set(), 'indication_high_low': [], 'indication_name': [],
                  'block_type': [], 'block': [], 'block_name': []}
        for word in self._cut_input:
            """板块参数提取"""
            if word in index_code.keys():
                params['block'].append(index_code[word])
                params['block_type'].append('index:14902')
                params['block_name'].append(word)
            if word in industry_code.keys():
                params['block'].append(industry_code[word])
                params['block_type'].append('industry:14903')
                params['block_name'].append(word)
            if word in conception_code.keys():
                params['block'].append(conception_code[word])
                params['block_type'].append('conception:14904')
                params['block_name'].append(word)
            if word in region_code.keys():
                params['block'].append(region_code[word])
                params['block_type'].append('region:14905')
                params['block_name'].append(word)

            """指标参数提取"""
            if word in operator_reflection.keys():
                params['range_type'].append(operator_reflection[word])
            if word in high_frequency_index.keys():
                params['indication_name'].append(word)
                params['indication'].append(high_frequency_index[word])
                params['indication_type_list'].add(2)
                params['indication_type'] = 2
            if word in low_frequency_index.keys():
                if word in typical_technical_index:
                    params['range_type'].append('D')
                params['indication_name'].append(word)
                params['indication'].append(low_frequency_index[word])
                params['indication_type_list'].add(1)
                params['indication_high_low'].append(low_frequency_index[word])
                params['indication_type'] = 1
        if {1, 2}.issubset(params['indication_type_list']):
            params['indication_type'] = 3

        """提取数值参数,返回所有数值的列表"""
        number_list = self.get_number_from_str_list(jieba.lcut(self._input_word))
        logger.info('从语句中提取的数值参数是：{}'.format(number_list))

        # 异常处理
        # 当同时没有指标和板块的时候判错，就算block_name为空，params = {'type': ''}, 所以不用检测key的方法判断
        __block = False
        __indication = False
        print(params['block_name'])
        if params['block_name']:
            logger.info('识别出的板块：{}'.format(params['block_name']))
            __block = True
        if params['indication_name']:
            logger.info('识别出的指标：{}'.format(params['indication_name']))
            __indication = True
        if not __block and not __indication:
            print(__block)
            print(__indication)
            logger.info('1.选股，没有指标和板块名。')
            error_logger.info('1.选股，没有指标和板块名。')
            return {'answer': '您可能想选股，请输入完整，例如：可转债里涨跌幅低于1的股票。', 'type': 'sentence'}

        # 当板块、板块类型、板块名长度不一样的时候出错
        if __block:
            if len(params['block']) != len(params['block_type']) or len(params['block_type']) != len(params['block_name']):
                logger.info('该问题的回答是：{}\n\n'.format({'answer': '1.选股，板块名和板块参数的数目不对应,\
                板块：{}， 板块名：{}， 板块类型：{}'.format(params['block'], params['block_name'], params['block_type']),
                                                     'type': 'sentence'}))
                error_logger.info('该问题的回答是：{}\n\n'.format({'answer': '1.选股，板块名和板块参数的数目不对应,\
                板块：{}， 板块名：{}， 板块类型：{}'.format(params['block'], params['block_name'], params['block_type']),
                                                     'type': 'sentence'}))
                return {'answer': '您可能想板块和指标选股选股，请输入完整，例如：一带一路成交股数最高的股票。', 'type': 'sentence'}

        # 当参数和关系词不对应的时候判错
        if __indication:
            _len_A = params['range_type'].count('A')
            _len_B1 = params['range_type'].count('B1')
            _len_C1 = params['range_type'].count('C1')
            len_num = _len_A + len(params['range_type']) - _len_B1 - _len_C1
            if len(number_list) != len_num:
                logger.info('1.选股，参数和大小关键词不对应，提取的关键词列表是：{}， '
                            '参数列表是：{}'.format(params['range_type'], number_list))
                error_logger.info('1.选股，参数和大小关键词不对应，提取的关键词列表是：{}， '
                                  '参数列表是：{}'.format(params['range_type'], number_list))
                return {'answer': '您可能想板块和指标选股指标选股，请输入完整，例如：雄安新区成交股最高的股票。', 'type': 'sentence'}

        """生成后端redis api的请求参数"""
        _api_params = {}
        _api_params['page'] = self._page
        _api_params['pagesize'] = self._pagesize

        """指标api参数"""
        if __indication:
            _api_params['indication'] = ','.join(params['indication'])
            _api_params['range_type'] = ','.join(params['range_type'])
            _api_params['indication_high_low'] = ','.join(params['indication_high_low'])
            _api_params['indication_type'] = params['indication_type']

        """板块api参数"""
        if __block:
            _api_params['block'] = ','.join(params['block'])
            _api_params['block_type'] = ','.join(params['block_type'])

        if __indication:
            """针对含有A的情况要用','连接两个参数，要用';'连接A和B、B和B"""
            _range_value_new = []
            _range_type_new = []
            """python没有赋值，只有引用，不加[:]的话，_range_type_list_tmp.pop()也会将params['range_type']弹出"""
            _range_type_list_tmp = params['range_type'][:]
            _number_list_tmp = number_list
            while _range_type_list_tmp:
                _type_tmp = _range_type_list_tmp.pop()
                if _type_tmp == 'A':
                    _new_type = 'A'
                    _after_value = _number_list_tmp.pop()
                    _before_value = _number_list_tmp.pop()
                    _new_value = '{},{}'.format(_before_value, _after_value)
                elif _type_tmp == 'B1':
                    _new_type = 'B1'
                    _new_value = '-9999999999999999'
                elif _type_tmp == 'C1':
                    _new_type = 'C1'
                    _new_value = '999999999999999999'
                elif _type_tmp == 'D':
                    _new_type = 'B'
                    _new_value = '0.5'
                else:
                    _new_type = _type_tmp
                    _value = _number_list_tmp.pop()
                    _new_value = '{}'.format(_value)
                _range_value_new.append(_new_value)
                _range_type_new.append(_new_type)

            if len(_range_value_new) == len(params['range_type']):
                _range_value_new.reverse()
                _range_type_new.reverse()
                _api_params['range_value'] = ';'.join(_range_value_new)
                _api_params['range_type'] = ','.join(_range_type_new)
            else:
                logger.info('1.选股，参数和大小关键词不对应，提取的关键词列表是：{}， '
                            '参数列表是：{}'.format(_range_type_list_tmp, _range_value_new))
                error_logger.info('1.选股，参数和大小关键词不对应，提取的关键词列表是：{}， '
                                  '参数列表是：{}'.format(_range_type_list_tmp, _range_value_new))
                return {'answer': '您可能想板块和指标选股指标选股，请输入完整，例如：白马股涨跌幅最高的股票。',
                        'type': 'sentence'}

        """通过redis api进行查询"""
        logger.info('该问题的redis api请求参数：{}'.format(_api_params))
        _url = 'http://127.0.0.1:5003/index_block_filter/'
        _redis_result_tmp = requests.get(_url, params=_api_params)
        try:
            _redis_result = _redis_result_tmp.json()
            logger.info('该问题的redis api请求结果：{}\n\n'.format(_redis_result))
        except Exception as e:
            logger.info('该问题的回答是：{}\n\n'.format({'answer': '1.选股，查询后端redis出错，出错原因：{},\
            板块：{}， 板块名：{}， 板块类型：{}'.format(params['block'], params['block_name'], params['block_type'], e),
                                                 'type': 'sentence'}))
            error_logger.info('该问题的回答是：{}\n\n'.format({'answer': '1.选股，查询后端redis出错，出错原因：{},\
            板块：{}， 板块名：{}， 板块类型：{}'.format(params['block'], params['block_name'], params['block_type'], e),
                                                 'type': 'sentence'}))
            return {'answer': '您可能想板块和指标选股选股，请输入完整，例如：房地产业换手率最高的股票。', 'type': 'sentence'}

        """将后端数据组合成前端适宜的数据"""
        stocks_info = _redis_result['info']
        stocks_info_new = []
        for stock_info in stocks_info:
            stock_info_new = {}
            stock_info_new['code'] = stock_info['code']
            stock_info_new['name'] = stock_info['name']
            stock_info_new['price'] = stock_info['price']
            stock_info_new['inc'] = stock_info['inc']
            stock_info_new['indication_value'] = stock_info['indication_value']
            stocks_info_new.append(stock_info_new)

        """构建前端返回字典"""
        answer = {}
        answer['answer'] = stocks_info_new
        answer['indication_name'] = params['indication_name']
        answer['block_name'] = params['block_name']
        answer['type'] = 'form'
        return answer

    def stock_index(self):
        """
        处理class_two,根据个股和指标诊股，返回个股最新的指标值
        :return:回答
        """
        parameter_dict = {'stock_name': None, 'indication_name': None, 'indication_type': 1, 'indication': None}
        """查询语句中是否有个股名称和指标名称，若有则生成股票代码和指标在redis里的变量"""
        for __word in self._cut_input:
            if __word in ashare_reflection.keys():
                parameter_dict['stock_name'] = __word
            if __word in high_frequency_index.keys():
                parameter_dict['indication_name'] = __word
                parameter_dict['indication_type'] = 2
                # parameter_dict['indication'] = high_frequency_index[__word]
            if __word in low_frequency_index.keys():
                parameter_dict['indication_name'] = __word
                parameter_dict['indication_type'] = 1
                # parameter_dict['indication'] = low_frequency_index[__word]
        stock_name = parameter_dict['stock_name']
        indicators_name = parameter_dict['indication_name']
        indication_type = parameter_dict['indication_type']

        """异常处理"""
        class_four_results_dict = {}
        if not stock_name:
            class_four_results_dict['answer'] = '2.个股加指标，没有个股关键词'
            class_four_results_dict['type'] = 'sentence'  # 标记这条回答是语句，直接显示
            error_logger.info(class_four_results_dict['answer']+'\n\n')
            return class_four_results_dict
        if not indicators_name:
            class_four_results_dict['answer'] = '2.个股加指标，没有指标关键词'
            class_four_results_dict['type'] = 'sentence'  # 标记这条回答是语句，直接显示
            error_logger.info(class_four_results_dict['answer']+'\n\n')
            return class_four_results_dict

        """映射参数并且查询redis，如果语句中含有股票名称和指标的话进行下一步处理，否则返回"""
        if stock_name and indicators_name:
            stock_parameter = ashare_reflection[stock_name]
            if indication_type == 2:
                indicators_parameter = high_frequency_index[indicators_name]  # 映射成参数
            else:
                indicators_parameter = low_frequency_index[indicators_name]
            redis_word = 'stkRealTimeState:{}_{}'.format(stock_parameter, '14901')
            logger.info('该问题请求redis的集合是：'+redis_word+'key是：'+indicators_parameter)
            class_four_results = self.index_query(name=redis_word, indication=indicators_parameter,
                                                  indication_type=indication_type)
            logger.info('该问题的redis请求结果是：{}'.format(class_four_results))
            if class_four_results:  # 查询阿里云的redis中是否有这只股票的这个指标，若没有则返回：'这只股票没有这个值'
                print('resutl: {}'.format(class_four_results))
                class_four_results = str(class_four_results, encoding="utf-8")  # flask不识别byte类型，转化成str
                if indicators_name in ratio_index:
                    class_four_answer = '{}的{}是{}%'.format(stock_name, indicators_name, class_four_results)  # 生成回答语句
                else:
                    class_four_answer = '{}的{}是{}'.format(stock_name, indicators_name, class_four_results)  # 生成回答语句
                class_four_results_dict['answer'] = class_four_answer
                class_four_results_dict['type'] = 'sentence'  # 标记这条回答是语句，前端直接显示
            else:
                class_four_results_dict['answer'] = '您可能想查询个股指标，这个股票没有这个值，请查询其他值，例如：乐视网的跌停价是多少。'
                class_four_results_dict['type'] = 'sentence'  # 标记这条回答是语句，直接显示
                error_logger.info(class_four_results_dict['answer']+'\n\n')
        return class_four_results_dict

    def stocks_indexes(self):
        """
        处理class_two,根据个股和指标诊股，返回个股最新的指标值,个股和指标都可以是多个
        例如：工商银行平安银行的市盈率和市销率、建设银行的市值是多少？
        回答应该是工商银行的市盈率是2、市销率是3，平安银行的市盈率是1、市销率是4，建设银行的市值是1000000。
        这里采用的算法是利用两个队列，遇到指标就将（指标，指标高低频类型，指标名）进指标队列，
        遇到个股且指标队列为空就将（个股，个股名）入个股队列，遇到个股且指标队列
        不为空，就出栈所有的指标和个股，将每个个股都对应所有的指标进行查询，然后将个股入个股队列。
        :return:回答
        """
        # 存储所有的个股和指标
        stock_index = []
        for __word in self._cut_input:
            if __word in ashare_reflection.keys() or __word in high_frequency_index.keys() or __word in low_frequency_index.keys():
                stock_index.append(__word)
        stock_index.append(-1)

        # 异常判断
        stocks_indexes_answer = {}
        stock_index_len = len(stock_index)
        if stock_index_len < 3 or (stock_index_len - 1) % 2 != 0:
            stocks_indexes_answer['answer'] = '2.个股加指标，没有个股或指标关键词'
            stocks_indexes_answer['type'] = 'sentence'  # 标记这条回答是语句，直接显示
            error_logger.info(stocks_indexes_answer['answer']+'\n\n')
            return stocks_indexes_answer

        # 进行队列的判断
        stock_queue = queue.Queue()
        index_queue = queue.Queue()
        redis_results = []
        for __i in stock_index:
            if __i in ashare_reflection.keys() and index_queue.empty():
                stock_queue.put((ashare_reflection[__word], __word))
            elif __i in high_frequency_index.keys():
                index_queue.put(high_frequency_index[__i], 2, __i)
            elif __i in low_frequency_index.keys():
                index_queue.put((low_frequency_index[__i], 1, __i))
            elif __i in ashare_reflection.keys() and not index_queue.empty():
                index_tmp = []
                stock_tmp = []
                while not index_queue.empty():
                    index_info = index_queue.get()
                    index_tmp.append(index_info)
                while not stock_queue.empty():
                    stock_info = stock_queue.get()
                    stock_tmp.append(stock_info)
                for stock_info in stock_tmp:
                    for index_info in index_tmp:
                        redis_word = 'stkRealTimeState:{}_{}'.format(stock_info[0], '14901')
                        logger.info('该问题请求redis的集合是：' + redis_word + 'key是：' + index_info[0])
                        redis_result = self.index_query(name=redis_word, indication=index_info[0],
                                                        indication_type=index_info[1])
                        redis_result = str(redis_result, encoding="utf-8")  # flask不识别byte类型，转化成str
                        logger.info('该问题的redis请求结果是：{}'.format(redis_result))
                        redis_results.append((stock_info[1], index_info[2], redis_result))

        redis_results.append(('end', 'end', 'end'))

        for redis_word in redis_results:
            if redis_word[2] != 'end':
                pass

        parameter_dict = {'stock_name': None, 'indication_name': None, 'indication_type': 1, 'indication': None}
        """查询语句中是否有个股名称和指标名称，若有则生成股票代码和指标在redis里的变量"""
        for __word in self._cut_input:
            if __word in ashare_reflection.keys():
                parameter_dict['stock_name'] = __word
            if __word in high_frequency_index.keys():
                parameter_dict['indication_name'] = __word
                parameter_dict['indication_type'] = 2
                # parameter_dict['indication'] = high_frequency_index[__word]
            if __word in low_frequency_index.keys():
                parameter_dict['indication_name'] = __word
                parameter_dict['indication_type'] = 1
                # parameter_dict['indication'] = low_frequency_index[__word]
        stock_name = parameter_dict['stock_name']
        indicators_name = parameter_dict['indication_name']
        indication_type = parameter_dict['indication_type']

        """异常处理"""
        class_four_results_dict = {}
        if not stock_name:
            class_four_results_dict['answer'] = '2.个股加指标，没有个股关键词'
            class_four_results_dict['type'] = 'sentence'  # 标记这条回答是语句，直接显示
            error_logger.info(class_four_results_dict['answer']+'\n\n')
            return class_four_results_dict
        if not indicators_name:
            class_four_results_dict['answer'] = '2.个股加指标，没有指标关键词'
            class_four_results_dict['type'] = 'sentence'  # 标记这条回答是语句，直接显示
            error_logger.info(class_four_results_dict['answer']+'\n\n')
            return class_four_results_dict

        """映射参数并且查询redis，如果语句中含有股票名称和指标的话进行下一步处理，否则返回"""
        if stock_name and indicators_name:
            stock_parameter = ashare_reflection[stock_name]
            if indication_type == 2:
                indicators_parameter = high_frequency_index[indicators_name]  # 映射成参数
            else:
                indicators_parameter = low_frequency_index[indicators_name]
            redis_word = 'stkRealTimeState:{}_{}'.format(stock_parameter, '14901')
            logger.info('该问题请求redis的集合是：'+redis_word+'key是：'+indicators_parameter)
            class_four_results = self.index_query(name=redis_word, indication=indicators_parameter,
                                                  indication_type=indication_type)
            logger.info('该问题的redis请求结果是：{}'.format(class_four_results))
            if class_four_results:  # 查询阿里云的redis中是否有这只股票的这个指标，若没有则返回：'这只股票没有这个值'
                print('resutl: {}'.format(class_four_results))
                class_four_results = str(class_four_results, encoding="utf-8")  # flask不识别byte类型，转化成str
                if indicators_name in ratio_index:
                    class_four_answer = '{}的{}是{}%'.format(stock_name, indicators_name, class_four_results)  # 生成回答语句
                else:
                    class_four_answer = '{}的{}是{}'.format(stock_name, indicators_name, class_four_results)  # 生成回答语句
                class_four_results_dict['answer'] = class_four_answer
                class_four_results_dict['type'] = 'sentence'  # 标记这条回答是语句，前端直接显示
            else:
                class_four_results_dict['answer'] = '您可能想查询个股指标，这个股票没有这个值，请查询其他值，例如：乐视网的跌停价是多少。'
                class_four_results_dict['type'] = 'sentence'  # 标记这条回答是语句，直接显示
                error_logger.info(class_four_results_dict['answer']+'\n\n')
        return class_four_results_dict

    def analyze_stock(self):
        """
        处理class_three,根据个股返回行情
        :return:回答
        """
        """参数提取"""
        parameter_dict = {'stock_name': None}
        for __word in self._cut_input:
            if __word in ashare_reflection.keys():
                parameter_dict['stock_name'] = __word
        stock_name = parameter_dict['stock_name']

        """异常处理"""
        if not stock_name:
            logger.info('该问题的回答是：{}\n\n'.format({'answer': '3.查询个股行情，没有检测到个股名', 'type': 'sentence'}))
            error_logger.info('该问题的回答是：{}\n\n'.format({'answer': '3.查询个股行情，没有检测到个股名', 'type': 'sentence'}))
            return {'answer': '您可能想查询个股行情，没有检测到个股名，请输入完整，例如：中国中铁行情',
                    'type': 'sentence'}

        """查询redis的个股行情"""
        stock_parameter = ashare_reflection[stock_name]  # 映射成参数
        redis_word = 'stkRealTimeState:{}_{}'.format(stock_parameter, '14901')
        logger.info('该问题请求redis的集合是：'+redis_word+'key是：nMatch&riseAndFallRate&riseAndFallAmount')
        price, inc, inc_value = self.index_query(name=redis_word,
                                                 indication=['nMatch', 'riseAndFallRate', 'riseAndFallAmount'],
                                                 indication_type=2)
        try:
            price = float(price)
            inc = float(inc)
            inc_value = float(inc_value)
        except Exception as e:
            logger.info('3.查询个股行情，查询现价、涨跌幅、涨跌值出错：{}'.format(e))
            error_logger.info('3.查询个股行情，查询现价、涨跌幅、涨跌值出错：{}'.format(e))
            return {'answer': '您可能想查询个股行情，没有检测到个股名，请输入完整，例如：中国中铁行情',
                    'type': 'sentence'}

        """构建前端返回列表"""
        stock_market_list = []
        stock_market_dict = {'code': stock_parameter, 'name': stock_name, 'price': price,
                             'inc': inc, 'inc_value': inc_value}
        stock_market_list.append(stock_market_dict)
        class_five_result_dict = {}
        class_five_result_dict['answer'] = stock_market_list
        class_five_result_dict['type'] = 'label'
        logger.info('该问题的回答是：{}'.format(class_five_result_dict))
        return class_five_result_dict

    def specific_analyze_stock(self):
        """
        处理class_four,技术面、资金面、财务面、机构推荐/研报、新闻、公告、简况层面分析个股
        是诊股类的细化
        :return:回答
        """
        return {'answer': '4.,技术面、资金面、财务面、机构推荐/研报、新闻、公告、简况层面分析个股，答案生成99%', 'type': 'sentence'}

    def indicators_teach(self):
        """
        处理class_five,根据名词进行百科解释
        :return:回答
        """
        """判断是否存在百科名词"""
        parameter_dict = {'indicators_teach_word': None}
        for __word in self._cut_input:
            if __word in bai_ke:
                parameter_dict['indicators_teach_word'] = __word
        indicators_teach_word = parameter_dict['indicators_teach_word']

        """异常处理"""
        if not indicators_teach_word:
            logger.info('该问题的回答是：{}\n\n'.format({'answer': '5.名词百科，没有发现百科名。', 'type': 'sentence'}))
            error_logger.info(
                '该问题的回答是：{}\n\n'.format({'answer': '5.名词百科，没有发现百科名。', 'type': 'sentence'}))
            return {'answer': '您可能想百科金融知识，没有发现百科名，请输入完整，例如：。', 'type': 'sentence'}

        """查询redis,映射成参数"""
        redis_key = indicators_teach_word
        redis_name = 'indicators_teach'
        redis_result_seven_tmp = redisManager.hget(name=redis_name, key=redis_key)
        try:
            redis_result_seven = redis_result_seven_tmp.decode("utf-8")
            # print('word type:{}'.format(type(redis_result_seven)))
        except Exception as e:
            logger.info('5.百科查询，查询redis时出错：{}'.format(e))
            error_logger.info('5.百科查询，查询redis时出错：{},原语句：{}'.format(e, redis_result_seven_tmp))
            return {'answer': '您可能想百科金融知识，没有发现百科名，请输入完整，例如：公司型基金的解释。', 'type': 'sentence'}
        logger.info('redis的请求参数是：{} ，请求结果是：{}'.format(redis_key, redis_result_seven))

        """构建返回列表"""
        class_seven_result = {}
        class_seven_result['answer'] = redis_result_seven
        class_seven_result['type'] = 'sentence'
        return class_seven_result

    def play_and_chat(self):
        """
        处理class_six,不含股票相关词汇，进行聊天娱乐模式
        :return:回答
        """
        # 判断是否有聊天关键字
        if '小霞' in self._cut_input:
            _input = self._input_word[2:]
        else:
            _input = self._input_word
        logger.info('欧蜜拉得到的问题是：{}'.format(_input))
        time_now = time.time()
        timestamp = int(round(time_now * 1000))  # 毫秒级时间戳
        timestamp_str = str(timestamp)
        appSecret = '22cd5040b9d347f2b338eeb46f1770be'
        appkey = "80ccf2fdeba243f49c014af42f571e25"
        api = 'nli'
        sign_str = appSecret + "api=" + api + "appkey=" + appkey + "timestamp=" + timestamp_str + appSecret
        hl = hashlib.md5()
        hl.update(sign_str.encode(encoding='utf-8'))
        sign = hl.hexdigest()
        dict_small = {"input_type": int(1), "text": _input}
        json_small = json.dumps(dict_small)
        rq_dict = {"data": json_small, "data_type": "stt"}
        rq = json.dumps(rq_dict)
        url_olami = 'https://cn.olami.ai/cloudservice/api'
        params = {'appkey': str(appkey), 'api': str(api),
                  'sign': str(sign),
                  'timestamp': int(timestamp),
                  'rq': rq}
        logger.info('欧蜜拉得到的参数是：{}'.format(params))
        answer = requests.get(url_olami, params=params)
        answer_json_all = answer.json()
        logger.info('欧蜜拉给出的回答是：{}'.format(answer_json_all))
        try:
            answer_json = answer_json_all['data']['nli'][0]['data_obj'][0]['content']
        except Exception as e:
            answer_json = answer_json_all['data']['nli'][0]['desc_obj']['result']
        logger.info('该问题的回答是：{}'.format(answer_json))
        return {'answer': answer_json, 'type': 'sentence'}

    def research_report(self):
        return {'answer': '7.研报，答案生成99%', 'type': 'sentence'}


class ClassFonction(Handler, ClassBasicFunction):
    """
    <分类计算模块>
    a)小类的处理类，生成返回的回答
    """
    def __init__(self, _class_number_index, _cut_input, _input_words, _page, _pagesize, _technical_flag, _technical_len):
        """
        构造函数
        :param _class_number_index:用户输入的类别索引
        :param _cut_input:分词后的用户输入
        :param _input_words:用于提取参数的语句
        :param _page:页号，从1开始
        :param _pagesize:页大小，决定一次返回多少条股票条目
        """
        Handler.__init__(self, _class_number_index=_class_number_index)
        ClassBasicFunction.__init__(self, _cut_input=_cut_input, _input_words=_input_words,
                                    _page=_page, _pagesize=_pagesize, _technical_flag=_technical_flag,
                                    _technical_len=_technical_len)

    def generate_api_function_class_one(self):
        """
        类别一处理函数
        :return: 回答
        """
        answer_one = self.select_stocks()
        return answer_one

    def generate_api_function_class_two(self):
        """
        类别二处理函数
        :return: 回答
        """
        answer_two = self.stock_index()
        return answer_two

    def generate_api_function_class_three(self):
        """
        类别三处理函数
        :return: 回答
        """
        answer_three = self.analyze_stock()
        return answer_three

    def generate_api_function_class_four(self):
        """
        类别四处理函数
        :return: 回答
        """
        answer_four = self.specific_analyze_stock()
        return answer_four

    def generate_api_function_class_five(self):
        """
        类别五处理函数
        :return: 回答
        """
        answer_five = self.indicators_teach()
        return answer_five

    def generate_api_function_class_six(self):
        """
        类别六处理函数
        :return: 回答
        """
        answer_six = self.play_and_chat()
        return answer_six

    def generate_api_function_class_seven(self):
        """
        类别七处理函数
        :return: 回答
        """
        answer_seven = self.research_report()
        return answer_seven


class ErrorDecorate:
    """
    <错误信息模块>
    a)装饰错误信息，加类名
    b)错误时将错误信息记录进日志
    """
    def __init__(self, _logger=logger, _error_logger=error_logger):
        self._logger = _logger
        self._error_logger = _error_logger

    def error_decorate(self, _error_flag, _error_message):
        """
        当有错误的时候，装饰加上类名
        :param _error_flag: 错误标志
        :param _error_message: 错误信息
        :return: 装饰后的错误信息
        """
        if _error_flag:
            _error_message = self.__class__.__name__ + _error_message
        return _error_message

    def error_logger(self, _error_flag, _error_message):
        """
        当有错误时，记录进总日志和错误日志
        :param _error_flag: 错误标志
        :param _error_message: 错误信息
        :return: None
        """
        if _error_flag:
            self._logger.info(_error_message)
            self._error_logger.info(_error_message)


class OriginalWordPreprocessing(ErrorDecorate):
    """
    <原语句处理模块>
    a)用户输入的原语句预处理,包括：
    1.大写字母小写化; 2.纠错; 3.词性分词; 4.判断技术指标关键词;
    5.原语句的基本处理流程; 6.生成技术指标模块的参数; 7.生成映射模块的参数。
    b)处理成技术指标模块和映射模块所需要的参数信息,具体是：
    技术指标模块：是否含有技术指标词的标志、技术指标词列表和原语句
    映射模块：jieba词性分词后的生成器
    """
    def __init__(self, _input_word):
        """
        [原语句处理模块的构造函数]
        a)初始化原语句处理模块的参数
        :param _input_word:用户输入原语句
        参数信息：
        _input_word:原语句; _input_word_lower:小写字母大写化的输入;
        _input_word_correction:纠错后的输入; _word_and_flag:词性分词后的输入;
        _error_flag:是否发生错误的标志; _error_message:处理中发生的错误信息;
        _technical_index_flag:是否含有技术指标名的标志; _technical_index:技术指标名列表。
        """
        ErrorDecorate.__init__(self)
        self._input_word = _input_word
        self._input_word_lower = None
        self._input_word_correction = None
        self._word_and_flag = None
        self._error_flag = False
        self._error_message = None
        self._technical_index_flag = False
        self._technical_index = []
        self._word_and_flag_tmp = None

    @LoggingEmail()
    def upper_to_lower(self):
        """
        输入字母小写变大写，例如macd->MACD
        :return: None
        """
        if isinstance(self._input_word, str):
            self._error_flag = False
            self._error_message = None
            self._input_word_lower = self._input_word.upper()
        elif not self._error_flag:
                self._error_flag = True
                self._error_message = '.upper_to_lower运行出错：获取的用户输入不是str类型,无法将大写字母小写化。'

    @LoggingEmail()
    def word_error_correction(self):
        """
        原语句纠错，例如工伤银行->工商银行。
        :return: None
        """
        if isinstance(self._input_word_lower, str):
            self._error_flag = False
            self._error_message = None
            self._input_word_correction = corrector.correct(sentence=self._input_word_lower)
        elif not self._error_flag:
                self._error_flag = True
                self._error_message = '.word_error_correction运行出错：大写字母小写化后的输入不是str类型，\
                                       可能是None,请检查用户输入是否正确获取。'

    @LoggingEmail()
    def word_cut(self):
        """
        词性分词
        :return: None
        """
        if isinstance(self._input_word_lower, str):
            self._word_and_flag_tmp = jieba.lcut(self._input_word_lower)
            self._word_and_flag = jieba.posseg.cut(self._input_word_lower)
            # print('_input_word_lower: {}'.format(self._input_word_lower))
            # for i, j in self._word_and_flag:
            #     print(i, j)
        elif not self._error_flag:
                self._error_flag = True
                self._error_message = '.word_cut运行出错：纠错后的输入不是str,可能是None,请检查\
                                       word_error_correction纠错模块是否正常运行。'

    @LoggingEmail()
    def judge_keyword(self):
        """
        判断输入语句中是不是在技术选股
        如果是则调用技术模块进行处理，否则调用映射模块进行处理
        记录技术指标的类别，方便技术指标模块进行匹配，
        具体的是：ma:1,kdj:2,boll:3,macd:4,放量:5.
        :return: None
        """
        # print('_technical flag:{}'.format(self._technical_index_flag))
        if self._word_and_flag_tmp:
            # print('word_and_flag : True')
            self._technical_index_flag = False
            self._technical_index = []
            __count_tmp = 0
            for _word in self._word_and_flag_tmp:
                # print('word:{}, flag:{}'.format(_word, _flag))
                if _word in technical_index_word:
                    __count_tmp += 1
                    if _word in technical_class_word.keys():
                        self._technical_index.append(technical_class_word[_word])
                    if __count_tmp >= 2:
                        self._technical_index_flag = True
        elif not self._error_flag:
                self._error_flag = True
                self._error_message = '.judge_keyword运行出错：jieba词性分词后的结果为空，请检查word_cut词性分词模块。'
        # print('_technical flag after:{}'.format(self._technical_index_flag))

    @LoggingEmail()
    def original_word_preprocessing(self):
        """
        原语句处理模块的处理流程控制程序
        :return: 错误标志和错误信息、是否含有技术指标的标志
        """
        self.upper_to_lower()
        # self.word_error_correction()
        self.word_cut()
        self.judge_keyword()
        self._error_message = self.error_decorate(self._error_flag, self._error_message)
        self.error_logger(_error_flag=self._error_flag, _error_message=self._error_message)
        logger.info('用户问题词性分词后：{}\n用户问题预处理后：{}\n是否是技术指标选股:{}'.
                    format(self._word_and_flag_tmp, self._input_word_lower, self._technical_index_flag))
        return self._technical_index_flag

    @LoggingEmail()
    def generate_technical_index_class_parameter(self):
        """
        生成技术指标模块所需的参数，
        如果主函数检查到_technical_index_flag为真，则调用技术指标模块来生成映射模块的参数
        否则直接调用generate_word_map_class_parameter来生成映射模块的参数。
        :return: 是否是技术指标选股、纠错后的原语句、技术指标的类别
        """
        return {'_input_word_correction': self._input_word_correction, '_technical_index': self._technical_index,
                '_word_and_flag': self._word_and_flag}

    @LoggingEmail()
    def generate_word_map_class_parameter(self):
        """
        生成映射模块所需的参数
        :return: 词性分析的结果、用来提取数值的用户输入原语句的纠错语句
        """
        # print('word_and_flag:{}\ninput_parameter: {}'.format(self._word_and_flag, self._input_word_lower))
        return {'_word_and_flag': self._word_and_flag, '_input_parameter': self._input_word_lower}


class TechnicalIndex:
    def __init__(self):
        pass

    def index_word_preprocessing(self):
        pass

    def keyword_classification(self):
        pass

    def tf_idf(self):
        pass

    def generate_classification_word(self):
        pass

    def generate_standard_word(self):
        pass

    def generate_word_map_class_parameter(self):
        """
        记得传技术指标的标志
        记得词性里追加 ('。。。', 'zb'), ('大于', 'gx')
        提取参数的语句末尾追加：, 0.5
        :return:
        """
        pass


class WordMap(ErrorDecorate):
    """
    <映射模块>
    a)分类词映射
    将原语句处理模块词性分词后的结果进行映射，具体的是词性是gg->个股，zb->指标，bk->板块，gx->关系。
    以减少语料库的压力和提高分类准确率
    b)标准词映射
    将上述结果映射为指标标准词，具体的是多少钱->现价，工行->工商银行等。
    以在分类后计算能正确提取出变量。
    c)提供分词预处理模块的参数
    具体的是映射后的分类词列表
    d)提供分类计算模块的参数
    具体的是映射后的标准词列表和提取数值的原语句，在没有技术指标选股的情况下,是原语句处理模块的原输入语句，
    在有的情况下是技术指标数字变大写后的输入，例如5日->五日后的输入。
    """
    def __init__(self, _word_and_flag, _input_parameter, _technical_classification_word=None,
                 _technical_standard_word=None, _technical_flag=False, _technical_len=0):
        """
        构造函数
        :param _word_and_flag: 词性分词后的结果，带词性
        :param _technical_classification_word: 技术指标分类词
        :param _technical_standard_word: 技术指标标准词
        :param _input_parameter: 用于提取数值的原语句
        """
        ErrorDecorate.__init__(self)
        self._word_and_flag = _word_and_flag
        self._input_parameter = _input_parameter
        self._technical_classification_word = _technical_classification_word
        self._technical_standard_word = _technical_standard_word
        self._technical_flag = _technical_flag
        self._technical_len = _technical_len
        self._classification_word = []
        self._standard_word = []
        self._error_flag = False
        self._error_message = None

    @LoggingEmail()
    def classification_standard_word(self):
        """
        进行分类词和标准词的映射
        :return: None
        标准词添加指标、普通关系、'区间'、板快、个股信息。
        分类词添加'只标', '观系', '区间', '板快', '个鼓'。
        这里写错是为了不占用指标等词的词性
        """
        self._classification_word = []
        self._standard_word = []

        # 判断区间的方法：指标和指标之间若有两个数字则为区间，为一个数字则为普通关系
        # __count_m：数字计数
        # __gx:关系收集
        __count_m = 0
        __gx = []
        for _word, _flag in self._word_and_flag:
            # print(__count_m)
            print(_word, _flag)
            if _flag == 'gg':
                self._classification_word.append('个鼓')
                self._standard_word.append(gg_to_full[_word])
            elif _flag == 'zb':
                if zb_to_full[_word] in typical_technical_index:
                    self._classification_word.append('观系')
                if __count_m == 2:
                    self._classification_word.append('区间')
                    self._standard_word.append('区间')
                elif __count_m == 1 and __gx:
                    self._classification_word.append('观系')
                    self._standard_word.append(__gx[0])
                self._classification_word.append('只标')
                self._standard_word.append(zb_to_full[_word])
                __count_m = 0
                __gx = []
            elif _flag == 'bk':
                self._classification_word.append('板快')
                self._standard_word.append(bk_to_full[_word])
            elif _flag == 'gx':
                if _word in no_number_operator.keys():
                    self._classification_word.append('观系')
                    self._standard_word.append(_word)
                else:
                    __gx.append(_word)
            elif _flag == 'm' and _word not in ['多少', '几点'] or _word in ['5', '1', '2', '3', '4', '6', '7', '8', '9', '0']:
                __count_m += 1
            else:
                self._classification_word.append(_word)

        # 针对最后的区间判断，当最后一个指标后有两个数字就判断为区间
        if __count_m == 1 and __gx:
            self._classification_word.append('观系')
            self._standard_word.append(__gx[0])
        elif __count_m == 2:
            self._classification_word.append('区间')
            self._standard_word.append('区间')

        logger.info('\n映射后的分类词：{}\n映射后的标准词：{}'.format(self._classification_word, self._standard_word))
        self._error_flag = False
        self._error_message = None

        """在这里完成输出的检查，保证从这里输出的只有正确答案或者错误提示"""
        if not self._classification_word and not self._error_flag:
            self._error_flag = True
            self._error_message = '.classification_standard_word运行出错：处理后的分类词为空，\
                                   请首先检查_word_and_flag是否有值。'
        if not self._standard_word and not self._error_flag:
            self._error_flag = True
            self._error_message = '.classification_standard_word运行出错，处理后的标准词为空，\
                                   请首先检查_word_and_flag是否有值。'

    @LoggingEmail()
    def word_map(self):
        """
        映射模块的处理流程控制程序
        :return: 运行出错标志、出错信息
        """
        self._error_flag = False
        self._error_message = None
        self.classification_standard_word()
        self._error_message = self.error_decorate(self._error_flag, self._error_message)
        self.error_logger(_error_flag=self._error_flag, _error_message=self._error_message)

    @LoggingEmail()
    def generate_cut_word_preprocessing_class_parameter(self):
        """
        生成分词预处理模块的参数
        :return: 分类词列表、标准词、用于提取参数的原语句
        """
        self.word_map()
        return {'_classification_word': self._classification_word, '_standard_word': self._standard_word,
                '_input_parameter': self._input_parameter, '_technical_flag': self._technical_flag,
                '_technical_len': self._technical_len}


class Preprocessing:
    """
    <预处理模块>
    包含常见的文本和分词后的预处理，包括：
    a)去标点符号
    b)词干化
    c)去低频词
    d)分词
    """
    @staticmethod
    def remove_punctuation(_cut_inputs):
        """
        去除标点符号
        :param _cut_inputs:分词后的语料，
        格式：[['词语','词语','词语'],['词语','词语','词语']...]
        :return:去除标点后的语料
        """
        _punctuations = [',', '.', ':', ';', '?', '(', ')', '[', ']', '&', '!', '*', '@', '#', '$', '%',
                         '。', '，', '：', '；', '？', '（', '）', '【', '】', '！', '、']
        _cut_inputs_filtered = [[_word for _word in _document if _word not in _punctuations] for _document in _cut_inputs]
        return _cut_inputs_filtered

    @staticmethod
    def stems(_cut_inputs):
        """
        词干化,针对英语,会自动将英语转化为小写
        :param _cut_inputs:分词后的语料，
        格式：[['词语','词语','词语'],['词语','词语','词语']...]
        :return:词干化的语料
        """
        # _st = LancasterStemmer()
        # _cut_inputs_stemmed = [[_st.stem(_word) for _word in _document] for _document in _cut_inputs]
        # return _cut_inputs_stemmed
        pass

    @staticmethod
    def remove_low_freq_word(_cut_inputs):
        """
        去除低频词
        :param _cut_inputs:分词后的语料，
        格式：[['词语','词语','词语'],['词语','词语','词语']...]
        :return:高频词
        """
        if _cut_inputs:
            """转换成一个list"""
            all_stems = sum(_cut_inputs, [])
            stems_once = set(stem for stem in set(all_stems) if all_stems.count(stem) == 0)
            """其中set是集合，all_stems.count是集合统计每个词语的出现的次数"""
            texts = [[stem for stem in text if stem not in stems_once] for text in _cut_inputs]
        else:
            texts = _cut_inputs
        return texts

    @staticmethod
    def cut_word(_file, _classes):
        """
        对一行一句的文本进行分词，如果不是则要先分句。
        :param _file: 一个文本
        :param _classes: 语料的类别，对第一第二类只收集关键词
        :return: 分词后的文本，是一个列表
        """
        __single_corpus = []
        for __line in Preprocessing.file_lines(_file):
            __result = jieba.posseg.cut(__line)

            # 读取文本方法：统一将普通关系映射为'关系'，将'较高'这类关系也映射为'关系'，将区间符号表示映射为'区间'。
            # __count_m：数字计数
            # __gx:关系收集
            # 当为选股和个股指标的时候只存储关键名词，以消除无关词对分类的影响
            __count_m = 0
            __single_corpus_tmp = []
            for _word, _flag in __result:
                # if _classes == 'class_two':
                    # print(_word, _flag)
                if _flag == 'gg':
                    __single_corpus.append('个鼓')
                elif _flag == 'zb':
                    __single_corpus.append('只标')
                    if __count_m == 2:
                        __single_corpus.append('区间')
                    elif __count_m == 1:
                        __single_corpus.append('观系')
                    __count_m = 0
                    if zb_to_full[_word] in typical_technical_index:
                        __single_corpus.append('观系')
                elif _flag == 'bk':
                    __single_corpus.append('板快')
                elif _flag == 'gx':
                    if _word in no_number_operator.keys():
                        __single_corpus.append('观系')
                elif _flag == 'm' and _word not in ['多少', '几点'] or _word in ['5', '1', '2', '3', '4', '6', '7', '8', '9']:
                    __count_m += 1
                elif _classes not in ['class_one', 'class_two']:
                    __single_corpus_tmp.append(',' + _word)
            if __count_m == 1:
                __single_corpus.append('观系')
            elif __count_m == 2:
                __single_corpus.append('区间')

            # 对于其他的词用结巴关键词提取取出停用词和标点符号
            __line_tmp = ''.join(__single_corpus_tmp)
            __line_word = jieba.analyse.extract_tags(__line_tmp)
            if __line_word:
                # print(__line_word)
                __single_corpus.extend(__line_word)
        print(__single_corpus)
        return __single_corpus

    @staticmethod
    def file_lines(__file):
        """
        对文本进行迭代
        :param __file: 文本
        :return: 文本的一行
        """
        for __line in __file:
            yield __line


class CutWordPreprocessing(Preprocessing, ErrorDecorate):
    """
    <分词预处理模块>
    a)对用户输入后的分类词进行预处理，包括去标点符号。
    """
    def __init__(self, _classification_word, _standard_word, _input_parameter, _technical_flag, _technical_len):
        """
        构造函数
        :param _classification_word: 用户输入处理后的分类词
        :param _standard_word: 标准词
        :param _input_parameter: 用于提取参数的语句
        :param _technical_flag: 是否含有技术指标的标志，True：有，False：没有。
        :param _technical_len: 技术指标长度
        """
        # print('classification:{}'.format(_classification_word))
        ErrorDecorate.__init__(self)
        self._technical_flag = _technical_flag
        self._technical_len = _technical_len
        self._classification_word = _classification_word
        self._standard_word = _standard_word
        self._input_parameter = _input_parameter
        self._classification_word_new = None
        self._error_flag = False
        self._error_message = None

    @LoggingEmail()
    def cut_word_preprocessing(self):
        """
        分词预处理模块的处理流程的控制程序
        包括正确性判断机制，如果出错，则返回出错标志和出错信息，
        否则返回正确标志和None
        :return: 是否成功的标志和信息
        """
        # print(self._classification_word)
        self._classification_word_new = self.remove_punctuation([self._classification_word])[0]

        """结果正确性判断"""
        if isinstance(self._classification_word_new[0], str):
            self._error_flag = False
            self._error_message = None
        else:
            self._error_flag = True
            self._error_message = '.cut_word_preprocessing函数运行出错，分词预处理后的结果错误，\
                                   请检查函数输入是否有值和格式正确。'
        self._error_message = self.error_decorate(self._error_flag, self._error_message)
        self.error_logger(_error_flag=self._error_flag, _error_message=self._error_message)

    @LoggingEmail()
    def generate_classify_class_parameter(self):
        """
        生成分类模块的参数
        :return: 分类模块的参数，预处理后的分类词、标准词、用于提取参数的原语句
        """
        self.cut_word_preprocessing()
        return {'_classification_word': self._classification_word_new, '_standard_word': self._standard_word,
                '_input_parameter': self._input_parameter, '_technical_flag': self._technical_flag,
                '_technical_len': self._technical_len}


class ClassifyHandler(Preprocessing, ErrorDecorate):
    """
    <分类处理模块>
    a)将用户的输入预处理并输入分类模型，得到类别下标
    """
    def __init__(self):
        """
        构造函数
        __index:模型索引
        __dictionary:模型字典
        __lsi:模型
        __tf_idf:tf_idf模型
        __classification_word:分类词
        __new_word:预处理后的输入
        _error_flag:错误标志
        _error_message:错误信息
        _class_number:类别索引，从0开始
        """
        ErrorDecorate.__init__(self)
        self.__index = None
        self.__dictionary = None
        self.__lsi = None
        self.__tf_idf = None
        self.__classification_word = None
        self.__new_word = None
        self._error_flag = False
        self._error_message = None
        self.__class_number = None

    @LoggingEmail()
    def user_cut_input_preprocessing(self):
        """
        用户输入预处理
        :return: None
        """
        self.__new_word = self.remove_punctuation(_cut_inputs=[self.__classification_word])[0]

    @LoggingEmail()
    def put_in_model(self):
        """
        将用户输入放入分类模型
        :return: None
        """
        # 正确性判断
        if not self.__new_word and not self._error_flag:
            self._error_flag = True
            self._error_message = '.user_cut_input_preprocessing函数运行失败，用户分词失败，请检查输入是否有异常。'

        # 词袋处理
        __word_bow = self.__dictionary.doc2bow(self.__new_word)
        # 计算tf_idf值
        __word_tf_idf = self.__tf_idf[__word_bow]
        # 计算lsi值
        _word_lsi = self.__lsi[__word_tf_idf]
        __word_similarity = self.__index[_word_lsi]
        self.__class_number = int(__word_similarity[0][0])
        # 当不含关键词时候，定义为娱乐聊天类
        if __word_similarity[0][1] == 0:
            self.__class_number = int(7)
        logger.info('\n问题的分类结果是：{}\n问题的分类是：{}'.format(__word_similarity, self.__class_number+1))

        # 正确性判断
        # print('class number:{}'.format(self.__class_number))
        # print('class number type: {}'.format(type(self.__class_number)))
        if not isinstance(self.__class_number, int) and not self._error_flag:
            self._error_flag = True
            self._error_message = '.put_in_model函数运行失败，没有得到的分类类别，请检查输入和处理过程是否有问题。'

    @LoggingEmail()
    def classify_handler(self, _index, _dictionary, _lsi, _tf_idf, _classification_word):
        """
        分类处理模块处理流程控制程序，兼顾正确性检查
        :param _index: 模型索引
        :param _dictionary: 模型字典
        :param _lsi: 模型
        :param _tf_idf: tf_idf模型
        :param _classification_word: 分类词
        :return: 错误标志、错误信息
        """
        self.__index = _index
        self.__dictionary = _dictionary
        self.__lsi = _lsi
        self.__tf_idf = _tf_idf
        self.__classification_word = _classification_word
        self.__class_number = None
        self._error_message = None
        self._error_flag = False
        self.user_cut_input_preprocessing()
        self.put_in_model()
        self._error_message = self.error_decorate(_error_flag=self._error_flag, _error_message=self._error_message)
        self.error_logger(_error_flag=self._error_flag, _error_message=self._error_message)

    @LoggingEmail()
    def generate_classify_class_parameter(self):
        """
        生成分类模块的参数
        :return: 输入的类别索引
        """
        return self.__class_number


class ClassifyModel(Preprocessing, ErrorDecorate):
    """
    <分类模型生成模块>
    在分类模块里判断要是分类模型不存在则调用这个模块生成模型，包括：
    a)语料读取
    b)训练模型
    c)保存模型
    """
    def __init__(self):
        """
        构造函数
        project_path:项目路径
        robot_corpus_path:语料库路径
        robot_dictionary_path:模型词典路径
        robot_index_path:模型索引路径
        robot_model_path:模型路径
        corpus_preprocessed:预处理后的语料
        corpus:分词后的原始语料
        _error_flag:错误标志
        _error_message:错误信息
        dictionary:模型字典
        index:模型索引
        lsi:模型
        tf_idf:tf_idf模型
        """
        ErrorDecorate.__init__(self)
        self.project_path = os.path.dirname(__file__)
        self.robot_corpus_path = os.path.join(self.project_path, 'corpus')
        self.robot_dictionary_path = os.path.join(self.project_path, 'model\RobotDictionary')
        self.robot_index_path = os.path.join(self.project_path, 'model\RobotIndex')
        self.robot_model_path = os.path.join(self.project_path, 'model\RobotModel')
        self.robot_tf_idf_path = os.path.join(self.project_path, 'model\RobotTfIdf')
        self.corpus_preprocessed = None
        self.corpus = []
        self._error_flag = False
        self._error_message = None
        self.dictionary = None
        self.index = None
        self.lsi = None
        self.tf_idf = None

    @LoggingEmail()
    def read_corpus(self):
        """
        读取训练语料库
        :return: 分词后的语料库，格式是:[['词语','词语','词语'],['词语','词语','词语']...]
        """
        __classes = ['class_one', 'class_two', 'class_three', 'class_four', 'class_five',
                     'class_six']
        self.corpus = []
        for i in range(len(__classes)):
            _file = open(os.path.join(self.robot_corpus_path, __classes[i]), 'r', encoding='utf-8')
            __single_corpus = self.cut_word(_file=_file, _classes=__classes[i])
            self.corpus.append(__single_corpus)
            _file.close()

    @LoggingEmail()
    def corpus_preprocessing(self):
        """
        分词后的语料预处理
        具体包括去除符号
        :return:预处理后的语料
        """
        self.corpus_preprocessed = None
        self.corpus_preprocessed = self.remove_punctuation(_cut_inputs=self.corpus)

        # 正确性判断
        if not self.corpus_preprocessed:
            self._error_flag = True
            if not self.corpus:
                self._error_message = '.read_corpus函数运行出错，从语料库读出来的corpus为空，请检查语料读取函数。'
            else:
                self._error_message = '.corpus_preprocessing函数运行出错，语料预处理后的结果为空，请检查预料预处理函数。'
        else:
            self._error_flag = False
            self._error_message = None

    @LoggingEmail()
    def train_lsi_model(self):
        """
        训练LSI模型
        :return: 返回模型、字典、索引
        """
        # 先将所有的词按字典进行编号
        self.dictionary = corpora.Dictionary(self.corpus_preprocessed)
        _corpus = [self.dictionary.doc2bow(text) for text in self.corpus_preprocessed]
        # doc2bow(): 将collection words 转为词袋，用两元组(word_id, word_frequency)表示
        self.tf_idf = models.TfidfModel(_corpus)
        _corpus_tf_idf = self.tf_idf[_corpus]
        # lsi[corpus]计算和各类的相似度，哪类概率大就属于那一类
        self.lsi = models.LsiModel(_corpus_tf_idf)
        _corpus_lsi = self.lsi[_corpus_tf_idf]
        self.index = similarities.Similarity(output_prefix=None, corpus=_corpus_lsi, num_features=10000, num_best=7)

    @LoggingEmail()
    def save_lsi_model(self):
        """
        保存训练的模型
        :return: None
        """
        if self.dictionary and self.index and self.lsi:
            self.dictionary.save(self.robot_dictionary_path)
            self.lsi.save(self.robot_model_path)
            self.index.save(self.robot_index_path)
            self.tf_idf.save(self.robot_tf_idf_path)
            self._error_flag = False
            self._error_message = None
        elif not self._error_flag:
            self._error_flag = True
            self._error_message = '.train_lsi_model函数运行出错，检测到dictionary、index、lsi其中有运行失败的。'

    @LoggingEmail()
    def class_model(self):
        """
        分类模型生成模块的处理流程的控制程序
        :return: 错误标志，错误信息
        """
        self._error_flag = False
        self._error_message = None
        self.read_corpus()
        self.corpus_preprocessing()
        self.train_lsi_model()
        self.save_lsi_model()
        self.error_decorate(_error_flag=self._error_flag, _error_message=self._error_message)
        self.error_logger(_error_flag=self._error_flag, _error_message=self._error_message)

    @LoggingEmail()
    def generate_classify_handler_class_parameter(self):
        """
        生成分类处理模块的参数
        :return: 模型索引、模型字典、模型
        """
        self.class_model()
        return self.index, self.dictionary, self.lsi, self.tf_idf


class Classify(ClassifyModel, ClassifyHandler, ErrorDecorate):
    """
    <分类模块>
    负责接受分类词并输入类别模型判断所属类别的下标
    a)加载模型，模型不存在则重新训练并保存模型
    b)输入模型得到类别索引
    """
    def __init__(self, _classification_word, _standard_word, _input_parameter, _technical_flag, _technical_len):
        """
        构造函数
        :param _classification_word: 分词预处理模块传过来的分类词
        _standard_word:标准词
        _input_parameter:用于提取参数的原语句
        _index:模型索引
        _dictionary:模型字典
        _lsi:模型
        _tf_idf:tf_idf模型
        _class_number:分类下标
        __error_flag:错误标志
        __error_message:错误信息
        """
        ClassifyModel.__init__(self)
        ClassifyHandler.__init__(self)
        ErrorDecorate.__init__(self)
        self._classification_word = _classification_word
        self._standard_word = _standard_word
        self._input_parameter = _input_parameter
        self._technical_flag = _technical_flag
        self._technical_len = _technical_len
        self._index = None
        self._dictionary = None
        self._lsi = None
        self._tf_idf = None
        self._class_number = None
        self._error_flag_classify = False
        self._error_message_classify = None

    @LoggingEmail()
    def generate_lsi_model(self):
        """
        生成lsi模型
        :return: None
        """
        self._index, self._dictionary, self._lsi, self._tf_idf = self.generate_classify_handler_class_parameter()

    @LoggingEmail()
    def load_lsi_model(self):
        """
        加载lsi模型
        :return: None
        """
        try:
            self._lsi = models.LsiModel.load(self.robot_model_path)
            self._dictionary = corpora.Dictionary.load(self.robot_dictionary_path)
            self._index = similarities.Similarity.load(self.robot_index_path)
            self._tf_idf = models.TfidfModel.load(self.robot_tf_idf_path)
            logger.info('LSI模型是否存在：存在。')
        except:
            logger.info('LSI模型是否存在：不存在。')
            self.generate_lsi_model()

        # 正确性判断
        if not self._lsi and not self._dictionary and not self._tf_idf and not self._index and not self._error_flag_classify:
            self._error_flag_classify = True
            self._error_message_classify = '.load_lsi_model函数运行出错，模型没有加载正确，请检查分类模型加载模块。'

    @LoggingEmail()
    def get_class_number(self):
        """
        得到输入的分类下标
        :return: None
        """
        self.classify_handler(_classification_word=self._classification_word,
                              _index=self._index, _dictionary=self._dictionary,
                              _tf_idf=self._tf_idf, _lsi=self._lsi)
        self._class_number = self.generate_classify_class_parameter()

        # 正确性判断
        if not isinstance(self._class_number, int) and not self._error_flag_classify:
            self._error_flag_classify = True
            self._error_message_classify = '.get_class_number函数运行错误，得到的分别函数不是数字，请检查类别索引函数。'

    @LoggingEmail()
    def classify(self):
        """
        分类模块的处理流程控制程序，兼顾正确性判断
        :return:错误标志、错误信息
        """
        self._error_flag_classify = False
        self._error_message_classify = None
        self.load_lsi_model()
        self.get_class_number()
        self._error_message_classify = self.error_decorate(_error_message=self._error_message_classify,
                                                           _error_flag=self._error_flag_classify)
        self.error_logger(_error_flag=self._error_flag_classify, _error_message=self._error_message_classify)

    @LoggingEmail()
    def generate_class_function_class_patameter(self):
        """
        生成分类计算模块所需的参数
        :return: 输入的类别下标、标准词、用于提取参数的原语句
        """
        self.classify()
        return {'_class_number_index': self._class_number, '_cut_input': self._standard_word,
                '_input_words': self._input_parameter, '_technical_flag': self._technical_flag,
                '_technical_len': self._technical_len}


def robots(_input_word, _page, _pagesize):
    """
    用户问题的唯一入口
    :param _input_word: 用户的问题，格式是str
    :param _page: 页号
    :param _pagesize: 页大小
    :return: 返回一个dict, 格式是{'answer':str}
    """
    # 调用原语句处理模块
    logger.info('用户问题：{}'.format(_input_word))
    original_handler = OriginalWordPreprocessing(_input_word=_input_word)
    _technical_flag = original_handler.original_word_preprocessing()

    # 在含有技术指标时候调用技术指标模块进行处理，否则直接调用映射模块
    # print('technical_flag: {}'.format(_technical_flag))
    if not _technical_flag:
        original_result = original_handler.generate_word_map_class_parameter()
    else:
        original_result = original_handler.generate_technical_index_class_parameter()

    # print('original_result: {}'.format(original_result))
    # for i, j in original_result['_word_and_flag']:
    #     print(i, j)
    # 调用映射模块
    word_map_handler = WordMap(**original_result)
    word_map_result = word_map_handler.generate_cut_word_preprocessing_class_parameter()
    # print('word_map_result: {}'.format(word_map_result))

    # 调用分词预处理模块
    cut_word_handler = CutWordPreprocessing(**word_map_result)
    cut_word_result = cut_word_handler.generate_classify_class_parameter()

    # 调用分类模块
    classify_handler = Classify(**cut_word_result)
    classify_result = classify_handler.generate_class_function_class_patameter()

    # 调用分类计算模块
    classify_result['_page'] = _page
    classify_result['_pagesize'] = _pagesize
    class_function_handler = ClassFonction(**classify_result)
    answer = class_function_handler.generate_api_function()
    return answer


if __name__ == '__main__':
    # 换手率最高的股票目前没有换手率指标
    class_one_list = ['现价大于10的股票', '涨跌幅大于5%的股票',  '涨跌幅最高的股票',
                      '振幅大于10的股票', '成交额最高的股票', '开盘价从高到低的股票',
                      '换手率最高的股票', '营业外收入最高的股票', '固定资产比率比较高的股票',
                      '非流动资产比率最低的股票', '短期借款从高到低的股票', '资产减值损失大于10000000的股票',
                      '营业总成本较低，利润总额较高的股票', '净资产收益率大于1，资本回报率大于2的股票',
                      '市盈率区间在0到10，市销率在2到15之间，营业收入增长率大于2的股票',
                      '涨跌幅大于2、营业成本较低、商誉较高、经营活动产生的现金流量净额较高的股票',
                      '舟山新区股票', '装饰园林概念的股票', '房地产业有哪些股票',
                      '征信概念有哪些股票？', '智能物流有哪些股票？', '智能电网概念有哪些股票？',
                      '稀土永磁有哪些股票？', '新三板有哪些股票？', '区块链概念有哪些股票？',
                      '舟山新区股票', '装饰园林概念的股票', '房地产业有哪些股票',
                      '区块链里现价低于10的股票', '三网融合利润总额较高的股票',
                      '中朝经济特区流通市值最高的股票', '丝绸之路中资本回报率大于1的股票',
                      '5G板块里销售净利率大于5的股票']
    class_two_list = [
        '工商银行的现价是多少？', '工行的市盈率是多少？', '601398的涨跌幅是多少？',
        '乐视网的跌停价是多少？', '东土科技的涨跌幅多少？', '贵州茅台的管理费用',
        '华大基因的每股现金流量净额', '尚品宅配的每股公积金是多少', '长春高新的净利润现金含量',
        '寒锐钴业的期末现金及现金等价物余额', '兆易创新的商誉是多少', '富瀚微的负债合计',
        '科大智能的换手率多少？', '国美通讯的流通A股数多少？', '羚锐制药的自由流通股本多少？',
        '九鼎投资的开盘价是多少？', '美丽生态的跌停价是多少？', '国机汽车的收盘价是多少？',
        '光明乳业的换手率是多少？'
        ]
    class_three_list = [
        '科大讯飞行情', '中公高科行情怎么样？', '中国中铁行情', '新天然气行情',
        '同济科技行情怎么样', '量子高科能买吗', '吉比特的行情', '北方稀土怎么样',
        '欧派家居的行情', '亿联网络的行情', '鸿特科技的行情', '汉森制药前景怎么样',
        '乐视怎么样？', '科伦药业咋样？', '瑞普生物赚钱吗？', '石大胜华的新闻', '天广中茂的财报',
        '南卫股份的公告', '百隆东方的K线', '同洲电子的走势', '西部黄金的日度'
    ]
    class_four_list = [
        '华远地产的机构推荐', '会畅通讯的研报', '模塑科技的机构评级', '巴安水务资金分析', '创业黑马财务面分析',
        '开创国际技术分析', '傲农生物的新闻', '国创高新最新消息', '洪都航空的资讯', '福瑞股份的公告',
        '汇嘉时代的简况', '贵糖股份目标价位', '保变电气升值空间', '京运通推荐评级', '恒泰实达最大涨幅',
        '阿石创专家评价', '闻泰科技净流入资金是多少', '永清环保盈利能力怎么样', '农业银行财务点评', '大丰实业技术面怎么样',
        '绿盟科技盈利情况', '广汇汽车资金状况', '平高电气资金点评', '美的集团财务状况', '石大胜华资产能力',
    ]
    class_five_list = [
                        '中国银行的百科', '动态市盈率的百科', '咸鸭蛋理论百科', '定量管理思想百科',
                        '贝尔宾团队角色理论百科', '蓝筹股是什么意思', 'MACD的解释', 'KDJ指标是什么意思',
                        '量比的百科', '内盘外盘是什么意思', '公司型基金的解释', '融资融券的百科',
                        '岛形反转的解释', 'KD指标死叉的百科', '顾比倒数线是什么意思', '威尔逊5000指数的百科',
                        '道琼斯中国指数系列的意思', '每笔手数的解释', '前方受阻红三线是什么意思',
                        'MA指标的百科', '期货的百科']
    class_six_list = ['讲个故事', '讲个笑话', '今天星期几', '征信概念有哪些股票？', '智能物流有哪些股票？',
                      '智能电网概念有哪些股票？', '稀土永磁有哪些股票？', '新三板有哪些股票？', '区块链概念有哪些股票？',
                      '虚拟现实概念里涨跌幅最高的股票', '新能源汽车成交额最高的股票', '盐湖提锂现价最高的股票',
                      '智慧医疗换手率最高的股票', '特斯拉成交额最高的股票', '无感支付里涨跌幅低于1的股票']

    class_one = class_one_list + class_six_list

    input_words = []
    hard_question = [
        '工商银行', 'pe在0.5-100.1的、收盘价较高、涨跌幅在-10.1%~16.66%的股票', '平安银行的换手率',
        '振幅什么意思', '最近什么股票涨幅比较多', '沪深300板块里涨跌幅大于-1的股票有哪些',
        '沪深300的国家队有哪些', '涨跌幅小于-1%的股票', '换手率是什么意思', '市盈率什么意思',
        '阳光照明的现价', '湖北宜化的热点指数', '上海有哪些股票',
        '房地产业有哪些股票', '沪深300有哪些股票', '国家队有哪些股票', '银之杰的市净率是多少',
        '中海油服的市净率多少', '上证50市盈率小于12%的股票', '上证50', '超图软件怎么样',
        '讲个故事', '五天均线连续五天上涨的股票', '市盈率的百科', '工商银行的市盈率',
        '沪深300有哪些股票', '湖北医药板块市盈率大于0的股票', '涨跌幅大于2的股票'
        ]
    # input_word = hard_question[-1]
    # input_word = class_one[1]  # 选择一个语句模拟输入
    input_word = class_two_list[18]  # 选择一个语句模拟输入
    # input_word = class_three_list[10]  # 选择一个语句模拟输入
    # input_word = class_four_list[11]  # 选择一个语句模拟输入
    # input_word = class_five_list[19]  # 选择一个语句模拟输入
    # input_word = class_six_list[20]  # 选择一个语句模拟输入
    page = 1
    pagesize = 10
    answer_dict = robots(_input_word=input_word, _page=page, _pagesize=pagesize)  # input_word格式：str
