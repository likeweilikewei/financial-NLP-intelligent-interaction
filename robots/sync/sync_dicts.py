#! /user/bin/env python
# -*- coding=utf-8 -*-

"""
author:lkw
date:2017.9.27
function:同步板块信息
email:a18829040692@163.com
"""
import os
from datetime import datetime

import pandas as pd
from robots.dicts.bk import *
from robots.settings import f11

from robots.settings import engine
from robots.settings import f4


class SyncDicts:
    def __init__(self):
        self.project_path = os.path.dirname(__file__)
        self.dicts_path = os.path.join(self.project_path, 'dicts\\bk')
        self.handler = open(self.dicts_path, 'w+', encoding='utf-8')
        self.bk_available_path = os.path.join(self.project_path, 'dicts\\bk_available')
        self.available_handler = open(self.bk_available_path, 'w+', encoding='utf-8')

    def close(self):
        self.handler.close()
        self.available_handler.close()

    @staticmethod
    def write(key_value, bk_type, handler):
        print(key_value)
        print(type(key_value))
        if not key_value or not bk_type:
            return None

        __count = 0
        for _key, _value in key_value:
            __count += 1
            if __count == 1:
                handler.write('\n' + bk_type + " = {" + "\n'{}': '{}', ".format(_key, _value))
            elif __count < 4:
                handler.write("'{}': '{}', ".format(_key, _value))
            elif __count % 4 == 0:
                handler.write("'{}': '{}',\n".format(_key, _value))
            else:
                handler.write("'{}': '{}', ".format(_key, _value))
        handler.write("}\n")

    def merge_dicts(self):
        """
        从mysql更新dicts中的板块信息，不过需要手动完成
        :return:
        """
        _max_date = self.get_max_tradedate_from_now()
        _gn = pd.read_sql("SELECT gn_code, industry FROM conseption WHERE list_date <= '{}' \
                          AND delist_date >= '{}'".format(_max_date, _max_date), engine)
        _hy = pd.read_sql("SELECT hy_code, industry FROM industry WHERE start_date <= '{}' \
                                  AND end_date >= '{}'".format(_max_date, _max_date), engine)
        _ix = pd.read_sql("SELECT category, name FROM indexs WHERE list_date <= '{}' \
                                  AND delist_date >= '{}'".format(_max_date, _max_date), engine)
        _dy = pd.read_sql("SELECT region FROM regionals", engine)

        # 当mysql有新的板块时，将板块名：板块参数以字典的形式保存起来，之后手动合并到dicts文件里
        # 检查概念更新
        key_value = []
        key_value_all = []
        key_value_standard = []
        key_value_standard_new = []
        # 更新
        for __gn_code, __row in _gn.groupby('gn_code'):
            key_value_standard.append((__row['industry'].values[0], __row['industry'].values[0]))
            key_value_all.append((__row['industry'].values[0], __gn_code))
            if __gn_code not in conception_code.values():
                key_value_standard_new.append((__row['industry'].values[0], __row['industry'].values[0]))
                key_value.append((__row['industry'].values[0], __gn_code))
        self.write(key_value=key_value, bk_type='conception_new', handler=self.handler)
        self.write(key_value=key_value_standard, bk_type='conception_standard_all', handler=self.handler)
        self.write(key_value=key_value_all, bk_type='conception_all', handler=self.handler)
        self.write(key_value=key_value_standard_new, bk_type='conception_standard_new', handler=self.handler)

        # 检查行业更新
        key_value = []
        key_value_all = []
        key_value_standard = []
        key_value_standard_new = []
        # 检查更新
        for __hy_code, __row in _hy.groupby('hy_code'):
            # 保存所有行业到参数的映射
            key_value_all.append((__row['industry'].values[0], __hy_code))
            # 保存行业标准化字典
            key_value_standard.append((__row['industry'].values[0], __row['industry'].values[0]))
            # 保存更新的行业到参数的映射
            if __hy_code not in industry_code.values():
                key_value.append((__row['industry'].values[0], __hy_code))
                # 保存更新的行业标准化字典
                key_value_standard_new.append((__row['industry'].values[0], __row['industry'].values[0]))
        # 开始写入本地文件
        self.write(key_value=key_value, bk_type='industry_new', handler=self.handler)
        self.write(key_value=key_value_all, bk_type='industry_all', handler=self.handler)
        self.write(key_value=key_value_standard, bk_type='industry_standard_all', handler=self.handler)
        self.write(key_value=key_value_standard_new, bk_type='industry_standard_new', handler=self.handler)

        # 检查指数更新
        key_value = []
        key_value_all = []
        key_value_standard = []
        key_value_standard_new = []
        # 更新
        for __ix_code, __row in _ix.groupby('category'):
            key_value_standard.append((__row['name'].values[0], __row['name'].values[0]))
            key_value_all.append((__row['name'].values[0], f11(__ix_code)))
            if f11(__ix_code) not in index_code.values():
                key_value.append((__row['name'].values[0], f11(__ix_code)))
                key_value_standard_new.append((__row['name'].values[0], __row['name'].values[0]))
        self.write(key_value=key_value, bk_type='index_new', handler=self.handler)
        self.write(key_value=key_value_all, bk_type='index_all', handler=self.handler)
        self.write(key_value=key_value_standard, bk_type='index_standard_all', handler=self.handler)
        self.write(key_value=key_value_standard_new, bk_type='index_standard_new', handler=self.handler)

        # 检查地域更新
        key_value = []
        key_value_all = []
        key_value_standard = []
        key_value_standard_new = []
        for __dy_code, __row in _dy.groupby('region'):
            key_value_standard.append((__row['region'].values[0], __row['region'].values[0]))
            key_value_all.append((__row['region'].values[0], __dy_code))
            if __dy_code not in region_code.keys():
                key_value.append((__row['region'].values[0], __dy_code))
                key_value_standard_new.append((__row['region'].values[0], __row['region'].values[0]))
        self.write(key_value=key_value, bk_type='region_new', handler=self.handler)
        self.write(key_value=key_value_all, bk_type='region_all', handler=self.handler)
        self.write(key_value=key_value_standard, bk_type='region_standard_all', handler=self.handler)
        self.write(key_value=key_value_standard_new, bk_type='region_standard_new', handler=self.handler)

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

    def check_old_bk_standard_available(self):
        """
        检查本地板块中可用的标准化映射,方便跟新板块的时候复用原有的字典
        :return:
        """
        # 更新指数
        _available = []
        for _key, _value in index_to_full.items():
            if _value in index_all.keys():
                _available.append((_key, _value))
        self.write(key_value=_available, bk_type='index_available', handler=self.available_handler)

        # 更新行业
        _available = []
        for _key, _value in industry_to_full.items():
            if _value in industry_all.keys():
                _available.append((_key, _value))
        self.write(key_value=_available, bk_type='industry_available', handler=self.available_handler)

        # 更新概念
        _available = []
        for _key, _value in conception_to_full.items():
            if _value in conception_all.keys():
                _available.append((_key, _value))
        self.write(key_value=_available, bk_type='conception_available', handler=self.available_handler)

        # 更新地域
        _available = []
        for _key, _value in region_to_full.items():
            if _value in region_all.keys():
                _available.append((_key, _value))
        self.write(key_value=_available, bk_type='region_available', handler=self.available_handler)

if __name__ == '__main__':
    sync_handler = SyncDicts()
    # sync_handler.merge_dicts()
    sync_handler.check_old_bk_standard_available()
    sync_handler.close()
