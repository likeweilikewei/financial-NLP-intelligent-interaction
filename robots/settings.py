#! /user/bin/env python
# -*- coding=utf-8 -*-

import datetime
import sys
import os
import traceback
import logging
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
from logging.handlers import TimedRotatingFileHandler
from contextlib import contextmanager
import pandas as pd
import redis
from sqlalchemy import create_engine
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import sessionmaker
from robots.RedisManager import RedisManager

"""
redis配置
"""
"""redis index low frequency set"""
CREDIS={}
CREDIS['host'] = '127.0.0.1'
CREDIS['db'] = 7
CREDIS['port'] = 6379
CREDIS['password'] = '123456'
redisManager = RedisManager(CREDIS)

"""redis index high frequency set"""
# FREDIS={}
# FREDIS['host'] = '127.0.0.1'
# FREDIS['db'] = 10
# FREDIS['port'] = 6379
# FREDIS['password'] = '123456'
# redisManagerHigh = RedisManager(FREDIS)
# redisManagerFrom = redis.Redis(host='127.0.0.1', port=6379, db=10, password='123456')
redisManagerFrom = redis.StrictRedis(connection_pool=redis.ConnectionPool(host='127.0.0.1',
                                                                          port=6379, db=10,
                                                                          password='123456'))

"""
日志配置
"""
# 当前目录
# dirname：返回文件的路径，pardir:返回当前文件的目录的父目录的表示，通常是.., __file__指向当前的文件名
# 因此os.path.dirname(__file__), os.path.pardir)返回父目录的路径，abspath返回绝对路径。
ROOT_PATH = os.path.abspath(os.path.join(os.path.dirname(__file__), os.path.pardir))
# 返回当前的项目目录, 这个jiuniu_robot_lsi_class在上传的时候改为robot
PROJECT_PATH = os.path.join(ROOT_PATH, 'robots')
# 普通的全部运行日志
LOG_PATH = os.path.join(PROJECT_PATH, 'log', 'robot_all.log')
# 用户踩的日志
BAD_LOG_PATH = os.path.join(PROJECT_PATH, 'log', 'bad_answer.log')
# 用户赞的日志
GOOD_LOG_PATH = os.path.join(PROJECT_PATH, 'log', 'good_answer.log')
# 系统检测到的错误收集日志
ERROR_LOG_PATH = os.path.join(PROJECT_PATH, 'log', 'robot_error.log')
# mysql同步redis的日志
MYSQL_TO_REDIS_LOG_PATH = os.path.join(PROJECT_PATH, 'log', 'sync_redis.log')
# api的redis日志
API_REDIS_LOG_PATH = os.path.join(PROJECT_PATH, 'log', 'api_redis.log')
# 将project插入到path中
sys.path.append(ROOT_PATH)


def getLogger(logname, name, print_flag=True):
    # 创建一个logger
    lg = logging.getLogger(name)
    lg.setLevel(logging.DEBUG)

    # 创建一个handler，用于写入日志文件
    fh = logging.FileHandler(logname, encoding='utf-8')
    fh.setLevel(logging.DEBUG)

    # 定义handler的输出格式
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)

    # 给logger添加handler
    lg.addHandler(fh)

    # 再创建一个handler，用于输出到控制台
    if print_flag:
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        ch.setFormatter(formatter)
        lg.addHandler(ch)
    return lg

logger = getLogger(LOG_PATH, 'robot_all')
bad_logger = getLogger(BAD_LOG_PATH, 'bad_answer')
good_logger = getLogger(GOOD_LOG_PATH, 'good_answer')
error_logger = getLogger(ERROR_LOG_PATH, 'robot_error')
sync_redis_logger = getLogger(MYSQL_TO_REDIS_LOG_PATH, 'sync_redis')
api_redis_logger = getLogger(API_REDIS_LOG_PATH, 'api_redis')


"""
mysql配置
"""
SQL_PATH = 'mysql+mysqldb://root:123456@127.0.0.1:3306/quant_new?charset=utf8'
"""
-pool_recycle, 默认为-1, 推荐设置为7200, 即如果connection空闲了7200秒, 自动重新获取, 以防止connection被db server关闭. 
-pool_size=5, 连接数大小，默认为5，正式环境该数值太小，需根据实际情况调大
-max_overflow=10, 超出pool_size后可允许的最大连接数，默认为10, 这10个连接在使用过后, 不放在pool中, 而是被真正关闭的. 
-pool_timeout=30, 获取连接的超时阈值, 默认为30秒
"""
engine = create_engine(SQL_PATH, pool_recycle=1, pool_size=30, max_overflow=0, pool_timeout=60)
"""
数据库设计的难点之一，是session生命周期的管理问题。sqlalchemy提供了一个简单的session管理机制，
即scoped session。它采用的注册模式。所谓的注册模式，简单来说，是指在整个程序运行的过程当中，
只存在唯一的一个session对象。
介绍到这里，我们知道scoped session本质上是一个全局变量。可是，如果直接把session定义成全局变量，
在多线程的环境下，会造成线程同步的问题。为此，scoped session在默认情况下，采用的线程本地化存储方式。
也就是说，每个线程的session对象是不同的。这样，不同线程对数据库的操作不会相互影响。
"""
Session = scoped_session(sessionmaker(bind=engine, autoflush=False, autocommit=False))


@contextmanager
def auto_session():
    session = Session()
    try:
        yield session
        session.commit()
    except Exception as e:
        print(str(traceback.print_exc()))
        session.rollback()
    finally:
        session.close()


def query(*args):
    with auto_session() as session:
        return session.query(*args)


def add(row):
    with auto_session() as session:
        session.add(row)


def merge(row):
    with auto_session() as session:
        session.merge(row)


"""可重用的格式函数"""


def f0(x):
    """返回带后缀的股票代码"""
    return '{}.SH'.format(x) if x.startswith('60') else '{}.SZ'.format(x)


def f1(x):
    if isinstance(x, list):
        return ['%.3f' % i for i in x]
    elif isinstance(x, pd.Series):
        return ['%.3f' % i for i in list(x)]
    return '%.3f' % x


def f2(x):
    try:
        encoding = chardet.detect(str(x)).get('encoding')
        x = x.decode(encoding)
    except Exception as e:
        logger.info('f2 decode {}'.format(e.message))
    finally:
        return x


def f3(x):
    return ','.join([str(i) for i in x])


def f4(x):
    """将时间转化为字符串'%Y-%m-%d'"""
    if isinstance(x, datetime.datetime) or isinstance(x, datetime.date):
        return x.strftime('%Y-%m-%d')
    return x


def f5(x):
    if isinstance(x, datetime.datetime):
        return x.strftime('%Y-%m-%d %H:%M:%S')
    return x


def f6(x):
    if isinstance(x, list):
        res = []
        for xx in x:
            if not '++' in xx:
                res.append(float(xx))
        return res
    return float(xx)


def f7(x):
    if not x: return x
    if isinstance(x, basestring):
        x = str(x)
        if '.' in x:
            return float(x)
        if x.isdigit():
            return int(x)
    return x


def f8(x):
    return pd.isnull(x)


def f9(x):
    if isinstance(x, str):
        return datetime.datetime.strptime(x, '%Y-%m-%d')
    return x


def f10(x):
    return x.strftime('%Y%m%d')


def f11(x):
    """返回字符串股票代码的纯数字"""
    return x[:6]


def f14(x):
    """返回股票代码的纯数字"""
    return str(x)[:6]


class LoggingEmail:
    def __init__(self, _log_name_all='robot_all', _log_name_error='robot_error'):
        self._log_name_all = _log_name_all
        self._log_name_error = _log_name_error
        self._log_path_project = os.path.dirname(__file__)
        self._log_path_all = os.path.join(self._log_path_project, 'log', '{}.log'.format(self._log_name_all))
        self._log_path_error = os.path.join(self._log_path_project, 'log', '{}.log'.format(self._log_name_error))

    def __call__(self, func):
        self.func = func

        def real_func(*args, **kwargs):
            flag, message, _result = self.log(func=func, arg=args, kwarg=kwargs)
            if not flag:
                self.email(message=message)
            return _result
        return real_func

    @staticmethod
    def get_logger(log_name, log_path, print_flag=False):
        """创建一个logger"""
        logger_decorator = logging.getLogger(log_name)
        logger_decorator.setLevel(logging.DEBUG)

        """创建一个handler,用于写入日志,每天一个日志，backupCount=0决定不删除旧日志，如果为1，则保留一天"""
        handler_decorator = TimedRotatingFileHandler(log_path,
                                                     when="d",
                                                     interval=5,
                                                     backupCount=0)

        """定义handler的格式"""
        formatter = logging.Formatter('%(asctime)s  - %(levelname)s - %(message)s')
        handler_decorator.setFormatter(formatter)

        """给logger添加handler"""
        logger_decorator.addHandler(handler_decorator)

        """添加handler输出到控制台"""
        if print_flag:
            handler_out = logging.StreamHandler()
            handler_out.setLevel(logging.DEBUG)
            handler_out.setFormatter(formatter)
            logger_decorator.addHandler(handler_out)
        return logger_decorator

    def log(self, func, arg, kwarg):
        if self._log_name_all == 'robot_all':
            _logger_all = logger
        else:
            _logger_all = self.get_logger(log_name=self._log_name_all, log_path=self._log_path_all, print_flag=True)
        if self._log_name_error == 'robot_error':
            _logger_error = error_logger
        else:
            _logger_error = self.get_logger(log_name=self._log_name_error, log_path=self._log_path_error, print_flag=True)

        """记录日志"""
        try:
            # _logger_all.info("运行的函数是: %s\n参数是: %s, %s" % (func.__name__, arg, kwarg))
            _result = func(*arg, **kwarg)
            # _logger_all.info('运行的结果是：{}\n\n'.format(_result))
            return True, None, _result
        except:
            message = '函数 {} 运行出错: {}'.format(func.__name__, traceback.format_exc()) + '\n'
            _logger_all.info(message)
            _logger_error.info(message)
            return False, message, 'ResultError!'

    @staticmethod
    def email(message):
        my_sender = 'xxx@qq.com'  # 发件人邮箱账号
        my_pass = 'xxx'  # 发件人邮箱密码
        my_user = 'xxx@qq.com'  # 收件人邮箱账号，我这边发送给自己

        msg = MIMEText(message, 'plain', 'utf-8')
        msg['From'] = formataddr(["罗伯特·德尼罗", my_sender])  # 括号里的对应发件人邮箱昵称、发件人邮箱账号
        msg['To'] = formataddr(["罗伯特·德尼罗", my_user])  # 括号里的对应收件人邮箱昵称、收件人邮箱账号
        msg['Subject'] = "聊天神器运行出错"  # 邮件的主题，也可以说是标题

        server = smtplib.SMTP_SSL("smtp.qq.com", 465)  # 发件人邮箱中的SMTP服务器，端口是25
        server.login(my_sender, my_pass)  # 括号中对应的是发件人邮箱账号、邮箱密码
        server.sendmail(my_sender, [my_user, ], msg.as_string())  # 括号中对应的是发件人邮箱账号、收件人邮箱账号、发送邮件
        server.quit()  # 关闭连接
