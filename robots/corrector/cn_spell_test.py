# -*- coding: utf-8 -*-
import sys
import os
sys.path.append(os.path.dirname(__file__))
from cn_spell import Corrector
import jieba
pwd_path = os.path.abspath(os.path.dirname(__file__))
usr_dict_path = os.path.join(pwd_path, 'data/cn/usrdict.txt')

jieba.initialize()
jieba.load_userdict(usr_dict_path)

#error_sentence_1 = '教收你好！分析下工伤银行和索飞亚这类高增长股票，如何去判定出现业绩拐点？例如老板前34季度增长放缓，毛利下降。这个是受到地产调控影响还是行业内部竞争加剧？对于这些高曾长股票，应如何给予合适古值？'
#error_sentence_1 = '你觉得瞬网科技飞适合常期投资吗'
#error_sentence  = ['教收你好！分析下工伤银行和索飞亚这类高增长股票，如何去判定出现业绩拐点？例如老板前34季度增长放缓，毛利下降。这个是受到地产调控影响还是行业内部竞争加剧？对于这些高曾长股票，应如何给予合适古值？','你觉得瞬网科技飞适合常期投资吗',
#                  '科大迅飞的市盈是多少','5日均线回采10日均线的股票有哪些']
# error_sentence = '5日均线回采10日均线的股票有哪些'
corrector = Corrector()
# correct_sent, details = corrector.correct(error_sentence)
# print("original sentence:{} => correct sentence:{}".format(error_sentence, correct_sent))

