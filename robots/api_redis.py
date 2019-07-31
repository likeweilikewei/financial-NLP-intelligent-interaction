#! /user/bin/env python
# -*- coding=utf-8 -*-

"""
author: lkw
email: a18829040692@163.com
date:2017.11.22
function: redis query api
"""

import pandas as pd
import redis
from robots.RedisManager import RedisManager
from flask import Flask, jsonify, request

from robots.settings import api_redis_logger
from robots.settings import redisManagerFrom

app = Flask(__name__)


class FilterRedis:

    def __init__(self, _request):
        """
        collecting parameter from url
        :param _request:url parameter
        """
        self.kwargs = _request.args.to_dict()
        """
        类别代码,逗号分割.可以不传,和板块数一一对应
        14901:个股  14902:指数  14903:行业 14904:概念
        例如：block_type=14902,14903
        """
        self.block_type = self.kwargs.get('block_type')
        """
        板块代码，逗号分割，可以不传,和板块类型一一对应
        例如：block=000001,BX
        """
        self.block = self.kwargs.get('block')
        """
        indication：指标代码，逗号分割，可以不传，和筛选值和筛选范围一一对应
        range_type：筛选类型，A:范围，B:大于，C:小于，大于从最小开始返回，小于从最大开始返回,逗号分割，可以不传
        但是要注意和筛选类型和指标一一对应，可以有多个,A类型要对应两个筛选值，之间用逗号分割，不同筛选类型的值用分号分割
        range_value：筛选值，两个是一个范围用','分割，其他用';'分割，可以不传
        例如：indication=priceBack_10,bollGapDecrease,maShort,pe&range_type=B,B,B,A&range_value=0;0;0;30,100
        page:页号，从1开始的整数，可以不传，默认大小是1
        pagesize:一页的大小，整数，决定了返回多少条，可以不传，默认大小是10
        """
        self.indication = self.kwargs.get('indication')
        self.range_value = self.kwargs.get('range_value')
        self.range_type = self.kwargs.get('range_type')
        if self.kwargs.get('page'):
            self.page = int(self.kwargs.get('page'))  # 前端的请求页面数
        else:
            self.page = 1
        if self.kwargs.get('pagesize'):
            self.pagesize = int(self.kwargs.get('pagesize'))  # 前端的请求页面的大小
        else:
            self.pagesize = 10

        """
        indication_type:标记所用的指标类型，1：低频指标，2:高频指标，3：高频+低频指标，这个类别决定了用高频还是低频库
        如果是低频用低频库，如果是高频用高频库，如果是高频+低频用高频库。可以不传，不传的话默认使用低频库。
        进行板块筛选的时候也可以传。
        indication_high_low:标记高频加低频情况下的低频指标，可以不传，当使用低频和高频指标混合筛选股票时必须传。
        使用逗号分割
        """

        if self.kwargs.get('indication_type'):
            self.indication_type = int(self.kwargs.get('indication_type'))
        else:
            self.indication_type = 1
        self.indication_high_low = self.kwargs.get('indication_high_low')

        """申明整理后的变量，保存参数的列表，方便后面处理"""
        self.indication_high_low_index_list = []
        self.indication_high_low_list = []
        self.block_types_list = []
        self.block_list = []
        self.indication_list = []
        self.range_value_list = []
        self.range_type_list = []
        """保存参数列表长度"""
        self.len_indication_high_low_list = 0
        self.len_block_types_list = 0
        self.len_block_list = 0
        self.len_indication_list = 0
        self.len_range_value_list = 0
        self.len_range_type_list = 0
        self.len_return = 0

        """
        定义公共变量
        判别是使用高频库还是低频库，redisManager:低频库，redisManagerFrom:高频库，默认使用高频
        block_set:存放redis的交集数据
        """
        self.block_set = 'tempCodes'
        self.redis_manager = None

    def clear_data(self):
        """
        1.整理json数据，将各种逗号和分号进行拆分成列表
        sorting parameters
        :return: None
        """
        if self.indication_high_low:
            self.indication_high_low_list = self.indication_high_low.split(',')
            self.len_indication_high_low_list = len(self.indication_high_low_list)
            api_redis_logger.info('1.0 整理json数据, 低频指标: {}'.format(self.indication_high_low_list))
        if self.block_type:
            self.block_types_list = self.block_type.split(',')
            self.len_block_types_list = len(self.block_types_list)
            api_redis_logger.info('1.0 整理json数据, 板块类型: {}'.format(self.block_types_list))
        if self.block:
            self.block_list = self.block.split(',')
            self.len_block_list = len(self.block_list)
            api_redis_logger.info('1.0 整理json数据, 板块参数列表: {}'.format(self.block_list))
        if self.indication:
            self.indication_list = self.indication.split(',')
            self.len_indication_list = len(self.indication_list)
            api_redis_logger.info('1.0 整理json数据，指标: {}'.format(self.indication_list))
        if self.range_value:
            __range_value_tmp = self.range_value.split(';')
            for __i in __range_value_tmp:
                __range_value_small = __i.split(',')
                __range_value_small = list(map(float, __range_value_small))
                self.range_value_list.append(__range_value_small)
            api_redis_logger.info('1.0 整理json数据，指标值: {}'.format(self.range_value_list))
            self.len_range_value_list = len(self.range_value_list)
        if self.range_type:
            self.range_type_list = self.range_type.split(',')
            self.len_range_type_list = len(self.range_type_list)
            api_redis_logger.info('1.0 整理json数据, 关系类型: {}'.format(self.range_type_list))

    def label_index_type(self):
        """
        2.确认高低频集合
        1：低频指标用低频集合
        2：高频指标用高频集合
        3：高频加低频指标用高频集合
        judge to use which redis set
        :return: None
        """
        CREDIS = {}
        CREDIS['host'] = '118.31.76.163'
        CREDIS['port'] = 6379
        CREDIS['password'] = 'K5pMwOrXgPlRc4MZ'
        if self.indication_type == 1:
            """redis index low frequency set"""
            CREDIS['db'] = 7
            api_redis_logger.info('2.1 确认高低频集合，选择低频集合。')
        elif self.indication_type == 2 or self.indication_type == 3:
            """redis index high frequency set"""
            CREDIS['db'] = 10
            api_redis_logger.info('2.1 确认高低频集合，选择高频集合。')

            # 由于db10里面的板块名没有前缀，因此在这里做一个区分
            self.block_types_list = list(map(lambda x: x[-5:], self.block_types_list))
        else:
            """redis index low frequency set"""
            CREDIS['db'] = 7
            api_redis_logger.info('2.1 确认高低频集合，出错，没有指标类型。'
                                  .format(self.indication_type))
        self.redis_manager = RedisManager(CREDIS)

    def generate_redis_set_all_index(self):
        """
        3.板块筛选后的整体指标集合
        generate intersection of blocks in set
        :return: None
        """
        # api_redis_logger.info('3.1 板块筛选后的整体指标集合，len block list: {}'.format(self.len_block_list))
        self.redis_manager.zrem(self.block_set)
        # _tmp_set = []
        if self.len_block_list >= 2:
            _tmp_set_one = self.block_types_list[0] + self.block_list[0]
            _tmp_set_two = self.block_types_list[1] + self.block_list[1]
            print(_tmp_set_one, _tmp_set_two)
            self.redis_manager.zinterstore(self.block_set, (_tmp_set_one, _tmp_set_two))
            for i in range(self.len_block_list-2):
                self.redis_manager.zinterstore(self.block_set,
                                               (self.block_set, self.block_types_list[i+2]+self.block_list[i+2]))
            # _tmp_set.append(_tmp_set_one+','+_tmp_set_two)
        elif self.len_block_list == 1:
            _tmp_set_one = self.block_types_list[0] + self.block_list[0]
            print(_tmp_set_one)
            self.redis_manager.zinterstore(self.block_set, (_tmp_set_one, _tmp_set_one))
            # _tmp_set.append(_tmp_set_one+','+_tmp_set_one)
        else:
            self.block_set = '14901robot'
        """some stock does not have turnOver"""
        print(self.block_set)
        if self.len_indication_list != 0:
            if 'turnOver' in self.indication_list:
                self.redis_manager.zinterstore('tempCodes', (self.block_set, 'turnOver'))
                self.block_set = 'tempCodes'
        api_redis_logger.info('3.1 板块筛选后的整体指标集合，使用的redis集合self.block_set: {}'.format(self.block_set))
        # api_redis_logger.info('finished get all index destination set!')

    def generate_redis_set(self):
        """
        4.板块筛选后的目标指标集合
        return universal hashing sort
        :return: sorted list
        """
        # 高频的时候用高频的参数名
        if self.indication_type == 2 or self.indication_type == 3:
            get_list = ["stkRealTimeState:*->shrCd", "stkRealTimeState:*->shrNm"]
            __by_index = "stkRealTimeState:*->nMatch"
        else:
            # 低频的时候用低频的参数名
            get_list = ["stkRealTimeState:*->code", "stkRealTimeState:*->cname"]
            __by_index = "stkRealTimeState:*->close"

        # 如果含有指标名需要返回指标
        if self.len_indication_list != 0:
            get_list.extend(["stkRealTimeState:*->" + self.indication_list[i] for i in range(self.len_indication_list)])

        # 使用redis的跨集合排序功能得到股票代码、股票名称、指标值，采用用收盘价升序
        print("block set:{}".format(self.block_set))
        print("by index:{}".format(__by_index))
        print("get list:{}".format(get_list))
        _results = self.redis_manager.sort(self.block_set, __by_index,
                                           get_list, None, None, False, True)

        # 日志记录
        api_redis_logger.info('4.1 板块筛选后的目标指标集合，结果: {}'.format(_results))
        return _results

    def hash_sorted_to_pandas(self, _results):
        """
        5. 将数据从查询redis后的列表转化为df
        have the sorted list to df
        :param _results: sorted list
        :return: sorted df
        """
        # code:带后缀的股票代码
        # name:股票名
        # price:股价
        # inc:涨跌幅
        # rate:目标指标，格式：[[指标1列表], [指标2列表], ...]
        self.len_return = self.len_indication_list + 2
        code = []
        name = []
        rate = []

        # api_redis_logger.info('5.1 将数据从查询redis后的列表转化为df sorted list : {}'.format(_results))
        e_tmp = None
        e_tmp_2 = None
        for _i in range(int(len(_results) / self.len_return)):
            if _results[self.len_return * _i]:
                code.append(_results[self.len_return * _i].decode(encoding='utf-8'))
            else:
                code.append(_results[self.len_return * _i])
            if _results[self.len_return * _i + 1]:
                name.append(_results[self.len_return * _i + 1].decode(encoding='utf-8'))
            else:
                name.append(_results[self.len_return * _i + 1])

            """generate every rate list"""
            if _i == 0:
                for _j in range(self.len_indication_list):
                    try:
                        rate.append([_results[self.len_return * _i + _j + 2].decode(enconding='utf-8')])
                    except Exception as e:
                        rate.append([_results[self.len_return * _i + _j + 2]])
                        if not e_tmp:
                            e_tmp = True
                            api_redis_logger.info('5.1 将数据从查询redis后的列表转化为df，生成目标指标列表时出错: {}'.format(e))
            else:
                for _j in range(self.len_indication_list):
                    try:
                        rate[_j].append(_results[self.len_return * _i + _j + 2].decode(encoding='utf-8'))
                    except Exception as e_2:
                        rate[_j].append(_results[self.len_return * _i + _j + 2])
                        if not e_tmp_2:
                            e_tmp_2 = True
                            api_redis_logger.info('5.2 将数据从查询redis后的列表转化为df，生成目标指标列表时出错: {}'.format(e_2))
        # api_redis_logger.info('rate: {}'.format(rate))
        # api_redis_logger.info('len of target set and target index is: {}'.format(len(code)))

        if self.len_indication_list:
            return_dict_tmp = {'code': code, 'name': name}
            return_rate_dict_tmp = {'rate{}'.format(i): rate[i] for i in range(self.len_indication_list)}
            return_dict = {**return_dict_tmp, **return_rate_dict_tmp}
            """
            #test the len of index
            print('return_dict:{}'.format(return_dict))
            print('type of return_dict:{}'.format(type(return_dict)))
            print('len of code: {}'.format(len(code)))
            print('len of name: {}'.format(len(name)))
            print('len of price: {}'.format(len(price)))
            print('len of inc: {}'.format(len(inc)))
            print('rate0: {}'.format(return_dict['rate0']))
            print('len of rate0: {}'.format(len(return_dict['rate0'])))
            """
            return_df = pd.DataFrame(return_dict)
            for __i in range(self.len_indication_list):
                return_df[['rate{}'.format(__i)]] = return_df[['rate{}'.format(__i)]].astype(float)
        else:
            return_dict = {'code': code, 'name': name}
            return_df = pd.DataFrame(return_dict)
        api_redis_logger.info('5.3 将数据从查询redis后的列表转化为df，结果是: {}'.format(return_df))
        # self.test_check_codes(test_code_list=code)

        # 去除code为None的元素
        return_df = return_df[(True ^ return_df['code'].isin([None]))]
        return return_df

    def pandas_filter(self, data_df, _range_type, _range_value, filter_index):
        """
        6.指标筛选
        filter stocks meeting conditions
        :param data_df: sorted dict
        :param _range_type: range type list
        :param _range_value: range value list
        :param filter_index: 需要筛选的指标，这里之所以不直接用filter_index代替len_range_type_list
        是因为需要后者对应列名rate{},在筛选指标里的就进行筛选，否则不进行筛选
        :return:stock df meeting conditions
        如果有高频指标加低频指标则应该不对低频指标进行筛选
        """
        if self.len_range_type_list != self.len_indication_list:
            api_redis_logger.info('6.1 指标筛选, 出错，len_range_type_list is not equal to \
            len_indication_list,please check input.')
            return None, 1
        """
        # test list index if turn out IndexError:list index out of range
        print('_data_df: {}'.format(_data_df))
        print('_range_value: {}'.format(_range_value))
        """
        # print('adata_df:{}'.format(data_df))
        for __i in range(self.len_range_type_list):
            if self.indication_list[__i] not in filter_index:
                continue
            if _range_type[__i] == 'A':
                # 针对区间指标筛选
                data_df = data_df[data_df['rate{}'.format(__i)] >= _range_value[__i][0]]
                data_df = data_df[data_df['rate{}'.format(__i)] <= _range_value[__i][1]]
                data_df.sort_values(by=['rate{}'.format(__i)],
                                    ascending=[True], inplace=True)
                # print('type:{}, value:{}~{}, data:\n{}\n'.format(_range_type[__i], _range_value[__i][0],
                #                                                  _range_type[__i][1], data_df))
            elif _range_type[__i] == 'B':
                # 针对大于的筛选
                data_df = data_df[data_df['rate{}'.format(__i)] > _range_value[__i][0]]
                data_df.sort_values(by=['rate{}'.format(__i)],
                                    ascending=[True], inplace=True)
                # print('type:{}, value:{}~{}, data:\n{}\n'.format(_range_type[__i], _range_value[__i][0],
                #                                                  999999999999, data_df))
            elif _range_type[__i] == 'C':
                # 针对小于的筛选
                data_df = data_df[data_df['rate{}'.format(__i)] < _range_value[__i][0]]
                data_df.sort_values(by=['rate{}'.format(__i)],
                                    ascending=[False], inplace=True)
                # print('type:{}, value:{}~{}, data:\n{}\n'.format(_range_type[__i], -999999999999,
                #                                                  _range_value[__i][0], data_df))
            elif _range_type[__i] == 'B1':
                # 针对较低取中位数以下的值
                __rate_median = data_df.median(axis=0)['rate{}'.format(__i)]
                # print(__rate_median)
                data_df = data_df[data_df['rate{}'.format(__i)] > _range_value[__i][0]]
                data_df = data_df[data_df['rate{}'.format(__i)] <= __rate_median]
                data_df.sort_values(by=['rate{}'.format(__i)],
                                    ascending=[True], inplace=True)
                # print('type:{}, value:{}~{}, data:\n{}\n'.format(_range_type[__i], _range_value[__i][0],
                #                                                  __rate_median, data_df))
            elif _range_type[__i] == 'C1':
                # 针对较高的取中位数以上的值
                __rate_median = data_df.median(axis=0)['rate{}'.format(__i)]
                data_df = data_df[data_df['rate{}'.format(__i)] < _range_value[__i][0]]
                data_df = data_df[data_df['rate{}'.format(__i)] >= __rate_median]
                data_df.sort_values(by=['rate{}'.format(__i)],
                                    ascending=[False], inplace=True)
                # print('type:{}, value:{}~{}, data:\n{}\n'.format(_range_type[__i], __rate_median,
                #                                                  _range_value[__i][0], data_df))
            else:
                # 异常处理
                api_redis_logger.info('6.1 指标筛选, 出错，pandas filter error,关系词里有非A B C B1 C1的元素：{}\n\n'.format(_range_type[__i]))
                _messages = {'block': self.block_list, 'indication': self.indication_list, 'info': []}
                return _messages
            # print('sdata_df:{}'.format(data_df))

        return data_df, 2

    def handle_high_low_indication(self, data_df):
        """
        7.处理高频加低频筛选
        专门处理indication_type=3的情况，这是使用的是高频库，但是低频的指标没有值
        也就没有办法进行筛选了，这里我们先将高频的指标进行筛选，然后生成一个集合，
        然后从低频库里找出符合低频指标的集合，然后集合做交集就是目标集合
        :param data_df: 高频库筛选后的dataFrame集合
        :return: 进过低频筛选后的最终集合
        """
        data_df_tmp = data_df
        # print('data_df:{}'.format(data_df))
        connection_pool = redis.ConnectionPool(host='118.31.76.163', port=6379, db=7, password='K5pMwOrXgPlRc4MZ')
        redis_manager_tmp = redis.StrictRedis(connection_pool=connection_pool)
        pipe = redis_manager_tmp.pipeline()
        _index_list = []
        _indication_list = []
        # print(self.indication_high_low_list)

        for _redis_key in self.indication_high_low_list:
            # print('_redis_key:{}'.format(_redis_key))
            for _row in zip(data_df, data_df.index, data_df['code']):
                # print(_row)
                _redis_name = 'stkRealTimeState:{}_14901'.format(_row[2][:6])
                # print(_redis_name)
                pipe.hget(name=_redis_name, key=_redis_key)
                _index_list.append(_row[1])
                _indication_list.append(_redis_key)
        _results = pipe.execute()
        print('获取的低频数据是：{}'.format(_results))
        print('获取的低频指标的长度是：{}'.format(len(_results)))
        """开始一个一个更新低频数据"""
        if len(_results) != len(_index_list) or len(_index_list) != len(_indication_list):
            api_redis_logger.info('7.1 处理高频加低频筛选, 在处理高频加低频情况下，去取低频数据时取到的索引、指标、\
            值的长度不一致，索引：{}，指标：{}，值：{}'.format(_index_list, _indication_list, _results))
        else:
            for _i in range(len(_results)):
                _result = _results[_i]
                if _result:
                    _value = float(_results[_i].decode(encoding='utf-8'))
                else:
                    _value = _results
                data_df_tmp.ix[_index_list[_i],
                               'rate{}'.format(self.indication_list.index(_indication_list[_i]))] = _value
        api_redis_logger.info('7.1 处理高频加低频筛选, 高频加低频补足低频指标后的数据为：{}'.format(data_df_tmp))
        return data_df_tmp

    def handle_high_low_new(self, data_df):
        """
        7.处理高频加低频筛选
        专门处理indication_type=3的情况，这是使用的是高频库，但是低频的指标没有值
        也就没有办法进行筛选了，这里我们先将高频的指标进行筛选，然后生成一个集合，
        然后从低频库里找出符合低频指标的集合，然后集合做交集就是目标集合
        :param data_df: 高频库筛选后的dataFrame集合
        :return: 进过低频筛选后的最终集合
        """
        connection_pool = redis.ConnectionPool(host='118.31.76.163', port=6379, db=7, password='K5pMwOrXgPlRc4MZ')
        redis_manager_tmp = redis.StrictRedis(connection_pool=connection_pool)
        pipe = redis_manager_tmp.pipeline()
        _df_index = data_df.index
        for _row in zip(_df_index, data_df['code']):
            _redis_name = 'stkRealTimeState:{}_14901'.format(_row[1][:6])
            pipe.hmget(name=_redis_name, keys=self.indication_high_low_list)
        _results = pipe.execute()
        print('获取的低频数据是：{}'.format(_results))
        print('获取的低频指标的长度是：{}'.format(len(_results)))

        # 开始一个一个更新低频数据
        # 获取所有的低频指标在指标列表里的下标，方便更新插入相应rate
        _low_index = []
        for _index in self.indication_high_low_list:
            _low_index.append(self.indication_list.index(_index))

        # 异常处理
        if len(_results) != len(data_df.index):
            api_redis_logger.info('7.1 处理高频加低频筛选, 查询出的低频指标的数量错误。')
        else:
            for _i in range(len(_results)):
                for _j in range(self.len_indication_high_low_list):
                    _result = _results[_i][_j]
                    if _result:
                        _value = float(_result.decode(encoding='utf-8'))
                    else:
                        _value = None
                    data_df.ix[_df_index[_i], 'rate{}'.format(_low_index[_j])] = _value
            # 删除有None的指标项的行
            data_df.dropna(inplace=True)

        api_redis_logger.info('7.1 处理高频加低频筛选, 高频加低频补足低频指标后的数据为：{}'.format(data_df))
        return data_df

    def pandas_page(self, data_df):
        """
        8.分页
        用来进行分页的处理，返回指定的条目
        :param data_df: 所有的符合条件的集合
        :return: 指定页号和页大小的df
        """
        _page = self.page
        _pagesize = self.pagesize
        _data_df = data_df
        if _data_df.shape[0] <= (_page-1)*_pagesize:
            api_redis_logger.info('8.1 分页, there is no more data that meet condition.')
            return None, 3
        else:
            # api_redis_logger.info('data df all: {}'.format(_data_df))
            _filter_df = _data_df[(_page-1)*_pagesize:_page*_pagesize]
            # api_redis_logger.info('page range: {}~{}'.format((_page-1)*_pagesize, _page*_pagesize))
            api_redis_logger.info('8.1 分页, data df filter: {}'.format(_filter_df))
            return _filter_df, 4

    def pandas_to_json(self, _pandas_df):
        """
        9.df转json
        translate df to json
        :param _pandas_df:stock df meeting conditions
        :return:stock dict meeting conditions
        """
        # api_redis_logger.info('_pandas_df: {}'.format(_pandas_df))
        if self.len_range_type_list != self.len_indication_list:
            api_redis_logger.info('9.1 df转json, len_range_type_list is not equal to len_indication_list,please check input.len of type is {},\
                        and len of indication is {}'.format(self.len_range_type_list, self.len_indication_list))
            _message_false = {'block': self.block_list, 'indication': self.indication_list, 'info': []}
            return _message_false
        _info_list = []
        """df to json"""
        try:
            for _, _row in _pandas_df.iterrows():
                tmp_row = pd.Series(_row)
                # api_redis_logger.info('tmp_row: {}'.format(tmp_row))
                _info_dict = {}
                _info_dict['code'] = tmp_row['code'][:6]
                _info_dict['name'] = tmp_row['name']

                """get real time price and inc,in high frequency code is 6 char,in low frequency code is 9"""
                __tmp_key = 'stkRealTimeState:{}_14901'.format(tmp_row['code'][:6])

                # 从高频库里面获取实时行情数据
                __price, __inc, __status, __preclose = redisManagerFrom.hmget(name=__tmp_key,
                                                                              keys=['nMatch', 'riseAndFallRate',
                                                                                    'stockStatus', 'nPreClose'])
                __price = float(__price)
                __inc = float(__inc)
                __status = float(__status)
                __preclose = float(__preclose)

                # __status=0 正常。__status=2 停牌。
                # 停牌有可能是盘中停牌，这种情况是有price的。
                # status=1 正常。status=2 停牌，这时候现价是昨日/最近收盘价。status=3 盘中停牌。
                """label the stock status,1:normal,2:盘中 suspended,3:suspended"""
                if __status == 0:
                    _info_dict['status'] = 1
                    _info_dict['price'] = __price
                    _info_dict['inc'] = __inc
                elif __status == 2:
                    if __price == 0:
                        _info_dict['status'] = 2
                        _info_dict['price'] = __preclose
                        _info_dict['inc'] = 0
                    else:
                        _info_dict['status'] = 3
                        _info_dict['price'] = __price
                        _info_dict['inc'] = __inc

                """if has indication parameter"""
                indication_value = []
                for __i in range(self.len_indication_list):
                    value_tmp = tmp_row['rate{}'.format(__i)]
                    # _info_dict[self.indication_list[__i]] = value_tmp
                    indication_value.append(value_tmp)
                _info_dict['indication_value'] = indication_value
                _info_list.append(_info_dict)
        except Exception as e:
            api_redis_logger.info('9.1 df转json, 出错，pandas_to_json failed:{}'.format(e))
        api_redis_logger.info('9.1 df转json, 结果: {}'.format(_info_list))
        _message = {'block': self.block_list, 'indication': self.indication_list, 'page': self.page,
                    'pagesize': self.pagesize, 'info': _info_list}
        return _message

    def redis_api(self):
        """
        scheduler function
        :return:standard output dict
        """
        api_redis_logger.info('\n\n0. get in scheduler function')
        self.clear_data()
        self.label_index_type()
        self.generate_redis_set_all_index()
        _results = self.generate_redis_set()
        # print('_result:{}'.format(_results))

        """包含了异常处理，保证结果不为空，一下步骤才能正常进行"""
        if _results:
            # 将redis hash结构转化为df
            _return_dfs = self.hash_sorted_to_pandas(_results=_results)
            # print('_return_dfs:{}'.format(_return_dfs))
        else:
            api_redis_logger.info('4.2 板块筛选后的目标指标集合,出错,返回的结果：{}.\n\n'.format(_results))
            _messages = {'block': self.block_list, 'indication': self.indication_list, 'info': []}
            return _messages

        # 指标筛选股票，针对高低频搭配需要先高频筛选，查询低频指标，低频筛选。其他情况直接进行相应的筛选
        if not _return_dfs.empty:
            if self.indication_type == 3:

                # 计算混合情况下需要筛选的高频指标
                _high_index = [x for x in self.indication_list if x not in self.indication_high_low_list]

                # 对于这种情况，为了之后不对所有的股票查询低频指标，可以先筛选出符合条件的高频指标
                _return_dfs, _ = self.pandas_filter(data_df=_return_dfs, _range_type=self.range_type_list,
                                                    _range_value=self.range_value_list,
                                                    filter_index=_high_index)

                # 然后对低频指标进行查询
                api_redis_logger.info('6.1 指标筛选, 高频指标筛选后的数据为：{}'.format(_return_dfs))
                _return_dfs = self.handle_high_low_new(data_df=_return_dfs)

                # 然后对低频指标进行筛选
                _filter_dfs, _number = self.pandas_filter(data_df=_return_dfs, _range_type=self.range_type_list,
                                                          _range_value=self.range_value_list,
                                                          filter_index=self.indication_high_low_list)
            else:
                # 对低频或者高频指标进行筛选,如果只是板块选股，以下函数会直接返回原始df
                _filter_dfs, _number = self.pandas_filter(data_df=_return_dfs, _range_type=self.range_type_list,
                                                          _range_value=self.range_value_list,
                                                          filter_index=self.indication_list)
            # print('_filer_dfs:{}'.format(_filter_dfs))
        else:
            # 如果板块筛选后转df结果为空则报错
            api_redis_logger.info('hash sorted to pandas error,返回的结果：{}.\n\n'.format(_return_dfs))
            _messages = {'block': self.block_list, 'indication': self.indication_list, 'info': []}
            return _messages

        # 指标筛选后的错误处理
        if _number == 1:
            # 如果指标列表和指标值列表长度不一样则报错
            api_redis_logger.info('len_range_type_list is not equal to len_indication_list,please check input.\n\n')
            _messages = {'block': self.block_list, 'indication': self.indication_list, 'info': []}
            return _messages

        # 指标筛选后进行分页处理
        api_redis_logger.info('6.2 指标筛选, 最终结果，pandas筛选后的数据为：{}'.format(_filter_dfs))
        _filter_page, _number_page = self.pandas_page(data_df=_filter_dfs)

        # 分页异常处理，3是没有更多数据了，4是正常
        if _number_page == 3:
            api_redis_logger.info('7.1 分页, 出错，there no more data that meet the condition.\n\n')
            _messages = {'block': self.block_list, 'indication': self.indication_list, 'info': []}
            return _messages

        # 如果正确就将df转化为json
        elif _number_page == 4:
            # print(_filter_page)
            _pandas_to_json_dict = self.pandas_to_json(_pandas_df=_filter_page)
            api_redis_logger.info('7.1 分页, 结果，pandas to dict: {}\n\n'.format(_pandas_to_json_dict))
            return _pandas_to_json_dict

    def test_check_codes(self, test_code_list):
        """test, look which code not in all codes."""
        __code_test = test_code_list
        __codes = self.redis_manager.zrange('14901robot', 0, -1, False, False)
        for __i in range(len(__codes)):
            try:
                __codes[__i] = __codes[__i].decode(encoding='utf-8')[:6]
            except Exception as e:
                api_redis_logger.info('change all codes to utf-8 error: {}'.format(e))
        api_redis_logger.info('code before: {}'.format(__codes))
        for __j in range(len(__code_test)):
            try:
                __code_test[__j] = __code_test[__j][:6]
            except Exception as e:
                api_redis_logger.info('change all code to utf-8 error: {}'.format(e))
        print('codes: {}'.format(__codes))
        print('code: {}'.format(test_code_list))
        for __k in range(len(__codes)):
            if __codes[__k] not in __codes:
                print('{} not in all codes'.format(__codes[__k]))


@app.route('/demo/', methods=['GET', 'POST'], endpoint='demo')
def demo():
    message = ['robot_api']
    info = {'message': message}
    return jsonify(info)


# @robot_api.route('/index_block_filter/', endpoint='stock_index_block_filter')
# @robot_api.route('/index_block_filter/', methods=['GET', 'POST'])
@app.route('/index_block_filter/', methods=['GET', 'POST'])
def stock_index_block_filer():
    # print(request)
    filter_redis_handler = FilterRedis(_request=request)
    message = filter_redis_handler.redis_api()
    return jsonify(message)

if __name__ == '__main__':
    # app.run(debug=True)
    app.run(host='0.0.0.0', port=5003)
