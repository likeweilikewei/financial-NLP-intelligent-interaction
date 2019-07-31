#!/usr/bin/env python
# -*-coding=utf-8-*-

# import os

# project_path = os.path.dirname(__file__)
# corrector_path = os.path.join(project_path, 'corrector', 'cn_spell_test')
# print(corrector_path)
from robots.corrector.cn_spell_test import corrector

# error_sentence = '5日均线回采10日均线的股票有哪些 工杭'
error_sentence = None
correct_sent, details = corrector.correct(error_sentence)
print("original sentence:{} => correct sentence:{}".format(error_sentence, correct_sent))
# print(corrector)
