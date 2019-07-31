#!/usr/bin/env python
# -*-coding=utf-8-*-

import random

from flask import Flask, jsonify, request
from robots.robot import robots

# from api_redis import robot_api
from robots.settings import logger, good_logger, bad_logger

app = Flask(__name__)
# app.register_blueprint(robot_api, url_prefix='/api/hq')
# app.register_blueprint(robot_api)


@app.route('/requset_question/', methods=['GET', 'POST'])
def request_questions():
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
        '乐视怎么样？', '科伦药业咋样？', '瑞普生物赚钱吗？', '石大胜华的分钟线', '天广中茂的月度',
        '南卫股份投顾建议', '百隆东方的K线', '同洲电子的走势', '西部黄金的日度'
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

    random.shuffle(class_one_list)
    random.shuffle(class_two_list)
    random.shuffle(class_three_list)
    random.shuffle(class_five_list)
    random.shuffle(class_six_list)
    class_two_list.extend(class_three_list)
    result_dict = []
    result_dict.append(class_one_list[0])
    result_dict.append(class_two_list[0])
    result_dict.append(class_three_list[0])
    result_dict.append(class_five_list[0])
    result_dict.append(class_six_list[0])
    logger.info('从语料库里随机取出的问题是：{}\n'.format(result_dict))
    query_result_dict = {}
    query_result_dict['questions'] = result_dict
    query_result_dict['respondCode'] = '000'
    query_result_dict['respondMessage'] = '成功'
    query_result_dict['type'] = 'list'
    logger.info('返回给API的数据是：{}\n'.format(query_result_dict))
    return jsonify(query_result_dict)


@app.route('/questions/', methods=['GET', 'POST'])
def question_to_answer():
    words = request.values.get('question')
    page = request.values.get('page')
    pagesize = request.values.get('pagesize')
    result_dict = robots(_input_word=words, _page=page, _pagesize=pagesize)
    query_result_dict = {}
    query_result_dict['type'] = result_dict['type']
    query_result_dict['answer'] = result_dict['answer']
    if 'indication_name' in result_dict.keys():
        query_result_dict['indication_name'] = result_dict['indication_name']
    if 'block_name' in result_dict.keys():
        query_result_dict['block_name'] = result_dict['block_name']
    query_result_dict['respondCode'] = '000'
    query_result_dict['respondMessage'] = '成功'
    logger.info('返回的API接口数据是：{}\n\n'.format(query_result_dict))
    return jsonify(query_result_dict)


@app.route('/evaluation/', methods=['GET', 'POST'])
def evaluate():
    question = request.values.get('question')
    answer = request.values.get('answer')
    answer = '{}'.format(answer)
    evaluation = request.values.get('evaluation')
    evaluation = int(evaluation)
    if evaluation:
        good_logger.info('\n'+'好回答'+'\n'+question+'\n'+answer+'\n')
    else:
        bad_logger.info('\n'+'坏回答'+'\n'+question+'\n'+answer+'\n')
    query_result_dict = {}
    query_result_dict['respondCode'] = '000'
    query_result_dict['respondMessage'] = '成功'
    return jsonify(query_result_dict)

if __name__ == '__main__':
    # app.run(debug=True)
    app.run(host='0.0.0.0', port=5002)
