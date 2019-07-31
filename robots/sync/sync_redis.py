#! /user/bin/env python
# -*- coding=utf-8 -*-

"""
author:lkw
date:2017.6.20
function:sync mysql、other redis data to redis
"""

import time
from datetime import date
from datetime import datetime
from datetime import timedelta

import numpy as np
import pandas as pd
from robots.dicts.bk import *
from robots.settings import sync_redis_logger

from robots.settings import engine
from robots.settings import f0
from robots.settings import f11
from robots.settings import f4
from robots.settings import redisManager
from robots.settings import redisManagerFrom


class SyncData:
    """get data from mysql、other redis,and merge data to redis"""

    def __init__(self, _day, _engine):
        self.day = _day
        self.year = _day.strftime('%Y')
        self.engine = _engine

    def read_valuation(self):
        """
        read datg from valuation in mysql
        :return: data frame
        """
        valuation_df = pd.read_sql("""SELECT * FROM valuation_{} WHERE date = '{}'""".
                                   format(self.year, self.day), self.engine)
        return valuation_df

    def read_technichal(self):
        """
        read data from technical in mysql
        :return:data frame
        """
        technical_df = pd.read_sql("""SELECT * FROM technical_{} WHERE date = '{}'""".
                                   format(self.year, self.day), self.engine)
        return technical_df

    def read_basic(self):
        """
        read data from basic in mysql
        :return: data frame
        """
        base_df = pd.read_sql("""SELECT code, name FROM basic""", self.engine)
        return base_df

    @staticmethod
    def data_mining(_valuation_df, _technical_df, _base_df):
        """
        data mining
        :param _valuation_df: valuation data frame
        :param _technical_df:technical data frame
        :param _base_df:basic data frame
        :return:mining data
        """
        if not _valuation_df.empty and not _technical_df.empty:
            for col in ['create_time', 'update_time']:
                del _valuation_df[col]
                del _technical_df[col]
            for col in ['change_date']:
                del _valuation_df[col]
            valuation_df = _valuation_df.replace(999999999.0000, np.nan)
            _df = _technical_df.merge(valuation_df, on=['code', 'date'], how='outer')
            _df = _df.merge(_base_df, on=['code'])
            _df = _df.rename(columns={'name': 'cname'})
            _df[_df.select_dtypes(['int64']).columns] = _df[_df.select_dtypes(['int64']).columns].astype('uint8')
            _df[_df.select_dtypes(['float64']).columns] = _df[_df.select_dtypes(['float64']).columns].astype('float32')
            _df[_df.select_dtypes(['object']).columns] = _df[_df.select_dtypes(['object']).columns].astype(str)
            _df.index = _df['code']
            return _df
        elif _valuation_df.empty:
            sync_redis_logger.info('get nothing from valuation in mysql!')
            return None
        else:
            sync_redis_logger.info('get nothing from technical in mysql!')
            return None

    @staticmethod
    def merge_redis(_df):
        """
        merge data to redis
        cost 30 hour
        :param _df: input data frame
        :return: None
        """
        keys = _df.columns.values.tolist()
        for idx, row in _df.iterrows():
            for key in keys:
                print('name:', idx[:6], 'key:', key, 'value:', row[key])
                redisManager.hset(name='stkRealTimeState:{}_14901'.format(idx[:6]),
                                  key=key, value=row[key])

    @staticmethod
    def merge_mysql_index_data_to_redis(_df):
        """
        merge data to redis by pipeline
        cost 90 seconds
        :param _df:input data frame
        :return:None
        """
        redisManager.delete_by_list(redisManager.keys(name='stkRealTimeState:*'))
        pipes = redisManager.pipeline()
        sync_redis_logger.info('start merge mysql data to redis, get in mysql __pipe')
        [pipes.hmset(name='stkRealTimeState:{}_14901'.format(idx[:6]), mapping=row.to_dict()) for idx, row in _df.iterrows()]
        pipes.execute()
        sync_redis_logger.info('finished merge mysql data to redis, finished execute mysql __pipe')

    @staticmethod
    def merge_redis_data_to_redis():
        """
        merge redis 10 to redis 7
        :return: None
        """
        sync_redis_logger.info('start merge redis 10 data to redis 7')
        cnt = 0
        __pipe = redisManager.pipeline()
        for key in redisManagerFrom.keys('stkRealTimeState:*_14901'):
            key = str(key)[2:-1]  # 二进制转换为字符串
            # if key[:4] == '1490' or key[:17] == 'stkRealTimeState:':
            if key[:17] == 'stkRealTimeState:':
                data_type = redisManagerFrom.type(key)
                data_type = str(data_type)[2:-1]  # 二进制转换为字符串

                if data_type == 'string':
                    values = redisManagerFrom.get(key)
                    __pipe.set(key, values)

                elif data_type == 'list':
                    values = redisManagerFrom.lrange(key, 0, -1)
                    __pipe.lpush(key, values)

                elif data_type == 'set':
                    values = redisManagerFrom.smembers(key)
                    __pipe.sadd(key, values)

                elif data_type == 'hash':
                    # keys = redisManagerFrom.hkeys(key)
                    keys = ['shrCd', 'shrNm', 'nMatch', 'riseAndFallRate']
                    for key_in in keys:
                        value = redisManagerFrom.hget(key, key_in)
                        if value:
                            __pipe.hset(key, key_in, value)

                elif data_type == 'zset':
                    value_score = redisManagerFrom.zrange(key, 0, -1, True, True)
                    try:
                        # __pipe = redisManager.pipeline()
                        for value, score in value_score:
                            __pipe.zadd(key, score, value)
                        # __pipe.execute()
                    except Exception as e:
                        sync_redis_logger.info('sync data from redis 10 to redis 7 failed :{}'.format(e))

                else:
                    sync_redis_logger.info('in redis 10 has a unknow type:{}'.format(data_type))
                cnt = cnt + 1
        __pipe.execute()
        sync_redis_logger.info('finished merge redis 10 data to redis 7, total data:{}'.format(cnt))

    @staticmethod
    def merge_14901_robot_low():
        """
        merge redis set 14901robot where stores all 14901 codes, like 002864_14901
        :return:None
        """
        sync_redis_logger.info('start merge redis set 14901codes.')
        redisManager.zrem('14901robot')
        _stocks = redisManager.keys('stkRealTimeState:*_14901')
        __pipe = redisManager.pipeline()
        for _stock in _stocks:
            __pipe.zadd('14901robot', 0, str(_stock)[-13:-1])
        __pipe.execute()
        sync_redis_logger.info('finished merge redis set 14901 codes.')

    @staticmethod
    def merge_14901_robot_high():
        """
        同步高频库里面的14901robot,因为在高频库里14901codes是set,因此程序导入到14901robot zset中，
        方便数据接口做交集的时候方便
        :return:
        """
        sync_redis_logger.info('开始同步高频库里面的14901robot.')
        redisManagerFrom.__delitem__(name='14901robot')
        __stocks = redisManagerFrom.smembers(name='14901codes')
        __pipe = redisManagerFrom.pipeline()
        for __stock in __stocks:
            __stock = str(__stock, encoding='utf-8')
            __pipe.zadd('14901robot', 0, __stock)
        __pipe.execute()
        sync_redis_logger.info('同步高频库里面的14901robot完成.')

    @staticmethod
    def get_max_tradedate_from_now():
        """
        get max trade date from now
        :return: newest trade date
        """
        _now = datetime.now()
        _now_str = f4(_now)
        _trade_days = pd.read_sql("SELECT trade_days FROM calendar WHERE exchange='SSE' \
                                          AND trade_days < '{}' ORDER BY trade_days DESC LIMIT 1".format(_now_str),
                                  engine)
        _max_date = f4(_trade_days['trade_days'].values[0])
        return _max_date

    @staticmethod
    def merge_indicators_teach():
        """
        merge indicators teach word in mysql to redis 7
        :return: None
        """
        sync_redis_logger.info('start merge indicators teach to redis.')
        redisManager.hdel_set(name='indicators_teach')
        pipe_tmp = redisManager.pipeline()
        _bai_ke = pd.read_sql("SELECT term, content FROM bai_ke_data", engine)
        for _, bai_ke in _bai_ke.iterrows():
            # print('name: {}, bai_ke: {}'.format(bai_ke['term'], bai_ke['content'].strip()))
            pipe_tmp.hset(name='indicators_teach', key=bai_ke['term'], value=bai_ke['content'].strip())
        pipe_tmp.execute()
        sync_redis_logger.info('finished merge indicators teach to redis.')

    def merge_14902_14903_14904_codes(self):
        """
        merge 14902 14903 14904 redis set
        :return: None
        """
        # 删除14902 14903 14904开头的集合
        SyncData.zrem_all_1490(_pattern='index:', _position=1)
        SyncData.zrem_all_1490(_pattern='industry:', _position=1)
        SyncData.zrem_all_1490(_pattern='conception', _position=1)

        sync_redis_logger.info('start merge 14902 14903 14904 data to redis.')
        __pipe = redisManager.pipeline()

        # 找到当前的交易日期，只查询正在上市的结果
        _max_date = self.get_max_tradedate_from_now()
        _gn = pd.read_sql("SELECT code, gn_code FROM conseption WHERE list_date <= '{}' \
                          AND delist_date >= '{}'".format(_max_date, _max_date), engine)
        _hy = pd.read_sql("SELECT code, hy_code FROM industry WHERE start_date <= '{}' \
                                  AND end_date >= '{}'".format(_max_date, _max_date), engine)
        _ix = pd.read_sql("SELECT code, category FROM indexs WHERE list_date <= '{}' \
                                  AND delist_date >= '{}'".format(_max_date, _max_date), engine)

        # 得到个股全集并且处理后将代码加上后缀，因为mysql里面的code都是带后缀的，方便下面和redis个股全集做交集
        _codes_14901 = redisManager.zrange(name='14901robot', start=0, end=-1, desc=False, withscores=False)
        _codes_14901_suffix = []
        for __code in _codes_14901:
            __code_tmp = __code.decode()
            _codes_14901_suffix.append(f0(__code_tmp[:6]))

        # merge conception code to redis 7
        for __gn_codes, __row in _gn.groupby('gn_code'):
            redisManager.zrem('conception:14904' + __gn_codes)
            __intersection_gn_codes = set(_codes_14901_suffix).intersection(set(__row['code'].values))
            for __gn_code in __intersection_gn_codes:
                __pipe.zadd("conception:14904" + __gn_codes, 0, f11(__gn_code) + "_14901")

        # merge industry code to redis 7
        for __hy_codes, __row in _hy.groupby('hy_code'):
            redisManager.zrem("industry:14903" + __hy_codes)
            __intersection_hy_codes = set(_codes_14901_suffix).intersection(set(__row['code'].values))
            for __hy_code in __intersection_hy_codes:
                __pipe.zadd("industry:14903" + __hy_codes, 0, f11(__hy_code) + "_14901")

        # merge index code to redis 7
        for __ix_codes, __row in _ix.groupby('category'):
            redisManager.zrem("index:14902" + f11(__ix_codes))
            __intersection_ix_codes = set(_codes_14901_suffix).intersection(set(__row['code'].values))
            for __ix_code in __intersection_ix_codes:
                __pipe.zadd("index:14902" + f11(__ix_codes), 0, f11(__ix_code) + "_14901")
        __pipe.execute()
        sync_redis_logger.info('finished merge 14902 14903 14904 data to redis.')

    @staticmethod
    def merge_14905_codes_high():
        """
        同步高频库里面的地域信息，注意高频里是不带前缀的
        :return:
        """
        sync_redis_logger.info('开始同步高频库里面的地域信息.')
        SyncData.zrem_all_1490(_pattern='14905', _position=1, _redis_manager=redisManagerFrom)
        _time0 = time.time()
        sync_redis_logger.info('高频库里面的地域信息删除完毕.')
        _dy = pd.read_sql("SELECT code, region FROM regionals", engine)
        _codes_14901 = redisManagerFrom.zrange(name='14901robot', start=0, end=-1, desc=False, withscores=False)
        _codes_14901_suffix = list(map(lambda x: f0(x.decode()[:6]), _codes_14901))

        # 开始同步14905
        __pipe = redisManagerFrom.pipeline()
        for _dy_name, row in _dy.groupby('region'):
            # 将乱码筛选出去
            if not _dy_name or not isinstance(_dy_name,str) or len(row['code'].values[0])<9 or row['code'].values[0][-2]!='S':
                continue
            _redis_dy = '14905' + region_code[_dy_name]

            # 和redis里的股票全集做交集
            _dy_stocks = set(_codes_14901_suffix).intersection(set(row['code'].values))

            # 开始更新redis
            for _dy_stock in _dy_stocks:
                __pipe.zadd(_redis_dy, 0, f11(_dy_stock) + '_14901')
        __pipe.execute()

        # 日志记录
        sync_redis_logger.info('同步高频库里面的地域信息完成.')
        sync_redis_logger.info('同步高频库地域信息耗时:{} sec'.format(time.time() - _time0))

    @staticmethod
    def merge_14905_codes_low():
        """
        同步地域板块的信息
        :return:
        """
        SyncData.zrem_all_1490(_pattern='region:', _position=1)
        _time0 = time.time()
        sync_redis_logger.info('start merge 14905 data to redis.')
        _dy = pd.read_sql("SELECT code, region FROM regionals", engine)
        _codes_14901 = redisManager.zrange(name='14901robot', start=0, end=-1, desc=False, withscores=False)
        _codes_14901_suffix = list(map(lambda x: f0(x.decode()[:6]), _codes_14901))

        # 开始同步14905
        __pipe = redisManager.pipeline()
        count = 0
        for _dy_name, row in _dy.groupby('region'):
            # 将乱码筛选出去
            if not _dy_name or not isinstance(_dy_name,str) or len(row['code'].values[0])<9 or row['code'].values[0][-2]!='S':
                continue
            count += 1
            _redis_dy = 'region:14905' + region_code[_dy_name]
            # 和redis里的股票全集做交集
            _dy_stocks = set(_codes_14901_suffix).intersection(set(row['code'].values))

            # 开始更新redis
            for _dy_stock in _dy_stocks:
                __pipe.zadd(_redis_dy, 0, f11(_dy_stock) + '_14901')
        __pipe.execute()

        # 日志记录
        sync_redis_logger.info('finished merge 14905 data to redis.')
        sync_redis_logger.info('merge 14905 data to redis cost:{} sec'.format(time.time() - _time0))

    @staticmethod
    def zrem_all_1490(_pattern, _position=1, _redis_manager=redisManager):
        """
        在更新之前删除所有的14902、14903、14904、14905的信息
        以防止旧的信息干扰
        _pattern:删除的模式前缀
        _position:决定模糊匹配的位置
        _redis_manager:决定是哪一个库里面进行操作
        1：前缀
        2：中缀
        3：后缀
        :return:
        """
        if _position == 1:
            __key = _pattern + '*'
            __word = '删除{}前缀的完毕。'.format(_pattern)
        elif _position == 2:
            __key = '*' + _pattern + '*'
            __word = '删除{}中缀的完毕。'.format(_pattern)
        else:
            __key = '*' + _pattern
            __word = '删除{}后缀的完毕。'.format(_pattern)
        __sets = _redis_manager.keys(__key)
        for _set in __sets:
            _set = str(_set, encoding="utf-8")
            if _redis_manager == redisManager:
                _redis_manager.zrem(name=_set)
            else:
                _redis_manager.__delitem__(_set)
        sync_redis_logger.info(__word)

    def sync_datas(self):
        """
        sync data,get data and merge data
        :return: None
        """
        start_time_zero = time.time()
        # self.merge_indicators_teach()
        start_time_one = time.time()
        sync_redis_logger.info('merge indicators teach to redis cost:' + str(start_time_one - start_time_zero) + ' sec')
        valuation_df_tmp = self.read_valuation()
        technical_df_tmp = self.read_technichal()
        base_df_tmp = self.read_basic()
        df_tmp = self.data_mining(_valuation_df=valuation_df_tmp, _technical_df=technical_df_tmp, _base_df=base_df_tmp)
        start_time_two = time.time()
        self.merge_mysql_index_data_to_redis(_df=df_tmp)
        end_time = time.time()
        sync_redis_logger.info('get data from mysql cost:' + str(start_time_two - start_time_one) + ' sec')
        sync_redis_logger.info('merge mysql data to redis cost :' + str(end_time - start_time_two) + ' sec')
        """
        现在是从高频库里直接查询，所以现在不用从高频库里更新股价和涨跌幅
        # self.merge_redis_data_to_redis()
        """
        end_time_two = time.time()
        # 同步低频库里面的14901robot
        self.merge_14901_robot_low()
        # 同步高频库里面的14901robot
        self.merge_14901_robot_high()

        end_time_three = time.time()
        self.merge_14902_14903_14904_codes()
        end_time_four = time.time()

        # 更新低频的db7地域信息
        self.merge_14905_codes_low()
        # 更新高频的db10地域信息
        self.merge_14905_codes_high()
        """
        sync_redis_logger.info('get data from redis 10 and merge data to redis 7 cost: ' + str(end_time_two - end_time) + ' sec')
        """
        sync_redis_logger.info('merge data to redis 14901Codes cost:' + str(end_time_three - end_time_two) + ' sec')
        sync_redis_logger.info('merge data to redis 14901 14902 14904 cost:' + str(end_time_four - end_time_three) + ' sec')
        sync_redis_logger.info('sync data cost all:' + str(end_time_four-start_time_zero) + ' sec')
        sync_redis_logger.info('finished data sync!\n\n')


class GetDay:
    """judge newest day to sync"""
    @staticmethod
    def get_today():
        """
        get today
        :return: today date
        """
        today = time.strftime("%Y-%m-%d", time.localtime())
        return today

    @staticmethod
    def get_yesterday():
        """
        get yesterday
        :return: yesterday date
        """
        today = date.today()
        oneday = timedelta(days=1)
        yesterday = today - oneday
        yesterday = yesterday.isoformat()
        return yesterday

    @staticmethod
    def get_mysql_day():
        """
        get newest day in mysql
        :return: newest day in mysql
        """
        technical_year = time.strftime("%Y", time.localtime())
        valuation_day_df = pd.read_sql("""SELECT date FROM valuation_{} ORDER BY date DESC LIMIT 1"""
                                       .format(technical_year), engine)
        if valuation_day_df.empty:
            sync_redis_logger.info('valuation table has no data! check mysql valuation_{}'.format(technical_year))
            return None
        else:
            sync_redis_logger.info('valuation table has data.')
            valuation_day = valuation_day_df.iloc[0][0]

        technical_day_df = pd.read_sql("""SELECT date FROM technical_{} ORDER BY date DESC LIMIT 1"""
                                       .format(technical_year), engine)
        if technical_day_df.empty:
            sync_redis_logger.info('technical table has no data! check mysql technical_{}'.format(technical_year))
            return None
        else:
            sync_redis_logger.info('technical table has data.')
            technical_day = technical_day_df.iloc[0][0]

        if valuation_day > technical_day:
            mysql_day = technical_day
        else:
            mysql_day = valuation_day
        return mysql_day

    def get_min_day(self):
        """
        get newest day
        :return: newest day
        """
        today = self.get_today()
        yesterday = self.get_yesterday()
        mysql_day = self.get_mysql_day()
        if not mysql_day:
            return 'no'
        if today == mysql_day:
            sync_redis_logger.info('newest day:{}'.format(mysql_day))
            return today
        elif yesterday == mysql_day:
            sync_redis_logger.info('newest day:{}'.format(yesterday))
            return yesterday
        else:
            sync_redis_logger.info('newest day:{}'.format(mysql_day))
            return mysql_day

if __name__ == '__main__':
    day_handler = GetDay()
    day = day_handler.get_min_day()
    """如果mysql没有数据就不做redis的同步"""
    if day != 'no':
        sync_handler = SyncData(_day=day, _engine=engine)
        sync_handler.sync_datas()
        # sync_handler.merge_14905_codes_low()
        # sync_handler.merge_14905_codes_low()
