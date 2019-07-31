#! /user/bin/env python
# -*- coding=utf-8 -*-

"""
author:lkw
date:2017.9.18
function:同步jieba的自定义词典
email:a18829040692@163.com
"""
import os
from robots.dicts.bai_ke import *
from robots.dicts.bk import *
from robots.dicts.dicts import *
from robots.dicts.gg import *
from robots.dicts.gx import *
from robots.dicts.zb import *


class SyncCustom:
    def __init__(self):
        self.project_path = os.path.dirname(__file__)
        self.custom_dict_path = os.path.join(self.project_path, 'corpus\custom_dict')
        self.handler = open(self.custom_dict_path, 'w+', encoding='utf-8')

    def close(self):
        self.handler.close()

    def write(self, words):
        for word in words:
            self.handler.write(word + '\n')

    def write_posseg(self, words, poss):
        for word in words:
            self.handler.write(word + ' {}\n'.format(poss))

    def sync_gg(self):
        gg = list(gg_to_full.keys())
        # gg.append('个鼓')
        if '只标' in gg:
            gg.remove('只标')
        if '板快' in gg:
            gg.remove('板快')
        if '观系' in gg:
            gg.remove('观系')
        self.write_posseg(words=gg, poss='gg')

    def sync_zb(self):
        zb = list(zb_to_full.keys())
        # zb.append('只标')
        if '个鼓' in zb:
            zb.remove('个鼓')
        if '板快' in zb:
            zb.remove('板快')
        if '观系' in zb:
            zb.remove('观系')
        self.write_posseg(words=zb, poss='zb')

    def sync_bk(self):
        bk = list(bk_to_full.keys())
        # bk.append('板快')
        if '只标' in bk:
            bk.remove('只标')
        if '个鼓' in bk:
            bk.remove('板快')
        if '观系' in bk:
            bk.remove('观系')
        self.write_posseg(words=bk, poss='bk')

    def sync_gx(self):
        gx = list(operator_reflection.keys())
        gx.append('观系')
        if '只标' in gx:
            gx.remove('只标')
        if '板快' in gx:
            gx.remove('板快')
        if '个鼓' in gx:
            gx.remove('个鼓')
        self.write_posseg(words=gx, poss='gx')

    def sync_dicts(self):
        dicts = technical_index_word.copy()
        if '只标' in dicts:
            dicts.remove('只标')
        if '板快' in dicts:
            dicts.remove('板快')
        if '观系' in dicts:
            dicts.remove('观系')
        if '个鼓' in dicts:
            dicts.remove('个鼓')
        self.write(words=dicts)

    def sync_bai_ke(self):
        bai_kes = [word for word in bai_ke if word not in list(zb_to_full.keys())]
        if '只标' in bai_kes:
            bai_kes.remove('只标')
        if '板快' in bai_kes:
            bai_kes.remove('板快')
        if '观系' in bai_kes:
            bai_kes.remove('观系')
        if '个鼓' in bai_kes:
            bai_kes.remove('个鼓')

        # 移除和板块、指标冲突的百科名
        if '上证50指数' in bai_kes:
            bai_kes.remove('上证50指数')
        self.write(words=bai_kes)

    def sync_corpus(self):
        corpus = corpus_word
        if '只标' in corpus:
            corpus.remove('只标')
        if '板快' in corpus:
            corpus.remove('板快')
        if '观系' in corpus:
            corpus.remove('观系')
        if '个鼓' in corpus:
            corpus.remove('个鼓')
        self.write(words=corpus)


def sync_custom():
    sync_handler = SyncCustom()
    sync_handler.sync_bai_ke()
    sync_handler.sync_bk()
    sync_handler.sync_dicts()
    sync_handler.sync_gg()
    sync_handler.sync_gx()
    sync_handler.sync_zb()
    sync_handler.sync_corpus()
    sync_handler.close()

sync_custom()
