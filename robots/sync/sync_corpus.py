#!/usr/bin/env python
# -*-coding=utf-8-*-

"""
author:lkw
date:2017.10.14
function:更新语料库
email:a18829040692@163.com
"""

import os
from robots.dicts.bai_ke import bai_ke
from robots.dicts.zb import *


class SyncCorpus:
    def __init__(self):
        self.corpus_path = os.path.join(os.path.dirname(__file__), 'corpus')

    def class_one(self):
        """
        生成指标选股的语料
        :return:
        """
        first_word = ['板快里', '', ]
        second_word = ['只标']
        middle_word = ['区间在0到0', '不低于0', '不高于0', '不高过0', '不低过0', '低于0', '高于0',
                       '超过0', '不超过0', '大于0', '不大于0',
                       '少于0', '不少于0', '小于0', '不小于0', '多于0', '不多于0', '0到0之间',
                       '排名', '排行', '最高', '最低', '比较高',
                       '比较低', '较高', '较低', '很低', '很高', '异常高', '异常低', '非常高',
                       '非常低', '特别高', '特别低', '小于等于0',
                       '大于等于0', '降序', '升序', '范围在0到0', '取值在0到0', '从低到高',
                       '从小到大', '从高到低', '从大到小', '低出0',
                       '高出0', '没超过0', '没低于0', '没多于0', '没高于0', '没低过0',
                       '没少于0', '没小于0', '没大于0', '没低过', '没高过',
                       '不超出0', '在0到0间', '在0~-0', '大于0小于0', '小于0大于0', '0到0',
                       '0至0', '在0和0之间', '在0到0间',
                       '在0与0之间', '0-0之间', '0,0区间内', '0、0范围内的', '在0之上',
                       '在0上面', '0以上', '0以下', '0下面', '0之下']
        end_word = ['的股票', '的股票有哪些', '的', '']
        __f = open(os.path.join(self.corpus_path, 'class_one'), 'w', encoding='utf-8')
        for i in first_word:
            for j in second_word:
                for k in middle_word:
                    for h in end_word:
                        word = i + j + k + h + '\n'
                        # word2 = '板块' + h + '\n'
                        # word3 = '指标' + h + '\n'
                        __f.write(word)
                        # __f.write(word2)
                        # __f.write(word3)
        __f.close()

    def class_two(self):
        """
        生成个股指标的语料
        :return:
        """
        first_word = ['个鼓', '个鼓的']
        second_word = ['只标']
        three_word = ['是多少', '的值', '']
        __f = open(os.path.join(self.corpus_path, 'class_two'), 'w', encoding='utf-8')
        for i in first_word:
            for j in second_word:
                for k in three_word:
                    words = i + j + k + '\n'
                    __f.write(words)
        __f.close()

    def class_three_old(self):
        """
        生成个股行情的语料
        :return:
        """
        first_word = ['个鼓']
        end_word = ['行情', '的行情', '的走势', '的K线', 'k线', '分时', '日K', '周K', '月K', '日度',
                    '分钟线', '的公告', '的资讯', '的新闻',
                    '的情况', '的财报', '简况']
        f = open(os.path.join(self.corpus_path, 'class_three'), 'w+', encoding='utf-8')
        for i in first_word:
            for j in end_word:
                word = i + j + '\n'
                f.write(word)
        f.close()

    def class_three(self):
        """
        生成诊股的语料
        包括行情、个股详情等信息。是一个个股总的信息界面。
        :return:
        """
        first_word = ['个鼓']
        end_word = ['行情', '的行情', '的走势', '的K线', 'K线图', '分时', '日K', '周K', '月K', '日度',
                    '分钟线', '建议', '投顾建议', '好不好', '会涨吗', '会上涨吗', '前景', '未来怎么样',
                    '的情况', '怎么样', '值得入手吗', '能买吗？', '值得投资吗', '目标价位', '涨幅空间',
                    '能涨吗', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '', '',
                    '能赚钱不', '上涨潜力', '综合分析', '分析', 'K线图', '分时', '日K', '周K', '月K', '日度',
                    '分钟线', '建议', '投顾建议', '好不好', '会涨吗', '会上涨吗', '前景', '未来怎么样',
                    '的情况', '怎么样', '值得入手吗', '能买吗？', '值得投资吗'
                    ]
        f = open(os.path.join(self.corpus_path, 'class_three'), 'w+', encoding='utf-8')
        for i in first_word:
            for j in end_word:
                word = i + j + '\n'
                f.write(word)
        f.close()

    def class_four_old(self):
        """
        个股投顾建议的语料
        :return:
        """
        first_word = ['个鼓', '个鼓的']
        second_word = ['建议', '投顾建议', '好不好', '怎么样', '值得入手吗', '咋样', '能买吗？',
                       '值得投资吗', '目标价位', '涨幅空间', '推荐评级', '能涨吗？',
                       '会上涨吗？', '前景', '未来怎么样', '机构评价', '专家评价', '评级怎么样',
                       '能赚钱不', '资金面怎么样', '基本面', '财务面状况', '上涨潜力', '综合分析', '分析',

                       '股价模拟走势', '相似K线', '筹码成本分析', '支撑位/压力位', '市场热度',
                       '市场热度评分？', '人气指数', '消息测评', '机构调研',
                       '主力强度', '主力强度评分', '主力资金', '主力成本', '龙虎榜', '融资买入',

                       '资金分布图', '走势分析', '走势分析评分', '市场表现',
                       '板快潜力？', '价值评估', '价值评估评分', '估值',
                       '成长情况', '盈利情况', '资产状况', '现金', '牛人解读(投顾)', '牛人解读评分', '牛人热度', '最新解读'
                       ]
        __f = open(os.path.join(self.corpus_path, 'class_four'), 'w+', encoding='utf-8')
        for i in first_word:
            for j in second_word:
                word = i + j + '\n'
                __f.write(word)
        __f.close()

    def class_four(self):
        """
        个股具体诊股分析的语料
        :return:
        """
        first_word = ['个鼓', '个鼓的']
        # 依次包括机构/研报 、资金分析、财务面分析、技术面分析、个股新闻、个股公告、个股简况
        second_word = ['机构推荐', '研判', '研报', '个股研判', '涨幅空间', '目标价', '机构评级',
                       '推荐次数', '综合评分', '目标价位', '升值空间', '推荐评级', '最大涨幅',
                       '研究报告', '机构调研', '推荐', '机构评价', '专家评价', '评级怎么样',
                       '最新推荐',

                       '资金分析', '资金面分析', '资金怎么样', '资金状况', '流入资金', '散户流入', '散户流出',
                       '流出资金多少', '多少流入', '多少流出', '净流入资金是多少', '主力流出', '最近五日资金流向',
                       '资金点评', '资金评价', '资金情况', '资金状态', '资金分布情况', '主力流入', '最新资金流向',
                       '5日资金流向', '资金分布图',

                       '财务面分析', '财务面', '财务分析', '财务状况', '财务情况', '财务评分', '财务好不好',
                       '财务怎么样', '财务能打多少分', '盈利情况', '盈利能力怎么样', '盈利状况', '盈利能打多少分',
                       '成长', '成长情况', '成长能力', '成长打分', '成长状况', '资产', '资产能力', '资产情况',
                       '资产状况', '资产打分', '资产分数', '估值', '估值打分', '估值能力', '估值情况', '估值分析',
                       '现金', '现金分析', '现金情况', '现金能力', '现金状况', '财务点评', '财务指标', '财务指标情况',
                       '利润表', '负债表', '资产负债表', '现金流量表', '同行财务对比', '行业财务情况比较',

                       '技术分析', '技术面分析', '技术评分', '技术面情况', '技术面好不好', '技术面怎么样', '触发的技术指标',

                       '新闻', '最新消息', '资讯',

                       '公告',

                       '简况', '公司名称', '上市日期', '发行价格', '发行数量', '每手股数'
                       ]
        __f = open(os.path.join(self.corpus_path, 'class_four'), 'w+', encoding='utf-8')
        for i in first_word:
            for j in second_word:
                word = i + j + '\n'
                __f.write(word)
        __f.close()

    def class_five(self):
        """
        生成百科的语料
        :return:
        """
        bai_kes = bai_ke + ['板快', '个鼓']
        second_word = ['的解释', '的百科', '是什么意思', '的资料', '的介绍', '', '是指什么']
        __f = open(os.path.join(self.corpus_path, 'class_five'), 'w+', encoding='utf-8')
        for i in bai_kes:
            for j in second_word:
                word = i + j + '\n'
                __f.write(word)
        __f.close()

    def class_seven(self):
        """
        生成研报模块的语料
        :return:
        """
        __f = open(os.path.join(self.corpus_path, 'class_seven'), 'w+', encoding='utf-8')
        first_word = ['个鼓', '个鼓的']
        second_word = ['分析', '市场分析', '价值',
                       '最新推荐', '推荐', '评级', '市场评级',
                       '评分', '分数', '研究报告', '研报', '推荐次数', '行业报告']
        for i in first_word:
            for j in second_word:
                word = i + j + '\n'
                __f.write(word)
        one_word = ['板快', '板快行业的']
        two_word = ['股票推荐', '研究', '涨跌幅', '研报', '开盘价', '收盘价', '振幅', '交易额']
        for i in one_word:
            for j in two_word:
                word = i + j + '\n'
                __f.write(word)
        __f.write('热门的股票推荐\n')
        __f.write('热门的股票\n')
        __f.write('热门的股票有哪些\n')
        __f.write('很火的股票\n')
        __f.write('好的股票\n')
        __f.write('哪些股票能赚钱\n')
        __f.write('大家关注哪些股票\n')
        __f.write('大家关心哪些股票\n')
        __f.write('主流的股票\n')
        __f.write('人们都在看的股票\n')
        __f.write('看好的股票\n')

        __f.write('推荐次数最多的股票\n')
        __f.write('推荐次数很多的股票\n')
        __f.write('推荐次数多的股票\n')
        __f.write('热捧的股票\n')
        __f.write('建议次数最多的股票\n')

        __f.write('最热门的行业推荐\n')
        __f.write('最热门的行业\n')
        __f.write('好的的行业推荐\n')
        __f.write('很火的行业\n')
        __f.write('哪些行业能赚钱\n')
        __f.write('最被关注的行业\n')
        __f.write('人们关心哪些行业\n')
        __f.write('主流的行业\n')

        __f.write('推荐次数最多的行业\n')
        __f.write('推荐次数很多的行业\n')
        __f.write('推荐次数多的行业\n')
        __f.write('热捧的行业\n')
        __f.write('建议次数最多的行业\n')
        __f.write('看好的行业\n')

        __f.write('最新的研究报告\n')
        __f.write('最新的研报\n')
        __f.write('股票最新的情况\n')
        __f.write('股票最新的消息\n')
        __f.write('建议次数最多的行业\n')
        __f.write('看好的行业\n')

        __f.write('最准的分析师\n')
        __f.write('最好分析师推荐\n')
        __f.write('分析师的排名\n')
        __f.write('分析师有哪些\n')
        __f.write('研报最多的分析师\n')
        __f.write('评分最高的研究员\n')
        __f.write('分析师怎么样')

        __f.close()


def sync_corpus():
    """
    同步所有的语料的信息
    :return:
    """
    sync_handler = SyncCorpus()
    sync_handler.class_one()
    sync_handler.class_two()
    sync_handler.class_three()
    sync_handler.class_four()
    sync_handler.class_five()
    # sync_handler.class_seven()

if __name__ == '__main__':
    sync_corpus()
