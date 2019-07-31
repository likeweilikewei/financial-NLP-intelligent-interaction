# -*- coding: utf-8 -*-
import os
import jieba
import codecs
from pypinyin import lazy_pinyin
from data.cn.same_pinyin import similar_char
from data.cn.key_char import key_char

pwd_path = os.path.abspath(os.path.dirname(__file__))
word_file_path = os.path.join(pwd_path, 'data/cn/word_dict.txt')
char_file_path = os.path.join(pwd_path, 'data/cn/char_set.txt')
same_stroke_path = os.path.join(pwd_path, 'data/cn/same_stroke.txt')
same_pinyin_path = os.path.join(pwd_path, 'data/cn/same_pinyin.txt')
PUNCTUATION_LIST = "。，,、？：；{}[]【】“‘’”《》/！%……（）<>@#$~^￥%&*\"\'=+-"

class Corrector(object):
    def construct_dict(self,path):
        word_freq = {}
        with codecs.open(path, 'r', encoding='utf-8') as f:
            for line in f:
                info = line.split()
                word = info[0]
                freq = int(info[1])
                word_freq[word] = freq
        return word_freq
    def edit_distance_word(self,word):
        """
        编辑距离为1
        """

        splits = [(word[:i], word[i:]) for i in range(len(word) + 1)]
        transposes = [L + R[1] + R[0] + R[2:] for L, R in splits if len(R) > 1]
        replaces = [L + c + R[1:] for L, R in splits if R and R[0] in similar_char.keys() for c in similar_char[R[0]]]
        inserts = [L + c + R for L, R in splits for c in key_char]
        return set(transposes + inserts + replaces)

    def known(self,words,word_freq):
        return set(word for word in words if word in word_freq)

    def candidates(self,word,word_freq):
        candidates_1_order = []
        candidates_2_order = []
        candidates_3_order = []
        error_pinyin = lazy_pinyin(word)
        candidate_words = list(self.known(self.edit_distance_word(word),word_freq))
        for candidate_word in candidate_words:
            candidata_pinyin = lazy_pinyin(candidate_word)
            overlap = [w for w in candidata_pinyin if w in error_pinyin]
            if len(overlap) > 1:
                candidates_1_order.append(candidate_word)
            elif len(overlap) > 0:
                candidates_2_order.append(candidate_word)
            else:
                candidates_3_order.append(candidate_word)
        return candidates_1_order, candidates_2_order, candidates_3_order

    def correct_word(self,word,word_freq):
        c1_order, c2_order, c3_order = self.candidates(word,word_freq)
        if c1_order:
            return max(c1_order, key=word_freq.get)
        elif c2_order:
            return max(c2_order, key=word_freq.get)
        elif c3_order:
            return max(c3_order, key=word_freq.get)
        else:
            return word

    def segment(self,sentence):
        return jieba.lcut(sentence)

    def correct(self,sentence):
        correct_sentence = ''
        locations, wrong_words, right_words = [], [], []
        seg_words = self.segment(sentence)
        word_freq = self.construct_dict(word_file_path)
        for word in seg_words:
            corrected_word = word
            if word not in PUNCTUATION_LIST:
                if word not in word_freq.keys():
                    corrected_word = self.correct_word(word,word_freq)
                    loc = sentence.find(word)
                    locations.append(loc)
                    wrong_words.append(word)
                    right_words.append(corrected_word)
            correct_sentence += corrected_word
        correct_detail = zip(locations, wrong_words, right_words)
        return correct_sentence, correct_detail


















