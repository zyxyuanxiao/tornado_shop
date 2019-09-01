# -*- coding: utf-8 -*-
# @Author: YouShaoPing
# @Date:   2018-12-11 14:13:58
# @Last Modified by:   YouShaoPing
# @Last Modified time: 2018-12-12 14:12:29

from loguru import logger

# trace
# debug
# info
# success
# warning
# error
# critical


formats = {
    0: "{time:YYYY-MM-DD HH:mm:ss zz} {level: <8} {file.path}:{function}:{line} pid-{process} {message}",
    1: "{time:YYYY-MM-DD HH:mm:ss zz} {level: <8} {file.path}:{function}:{line} tid-{thread} {message}",
    2: "<green>{time:YYYY-MM-DD HH:mm:ss zz}</green> <level>{level: <8}</level>|"
    "<cyan>{name}:{file.path}:{function}:{line} pid-{process}</cyan> - {message}",
    3: "<green>{time:YYYY-MM-DD HH:mm:ss zz}</green> <level>{level: <8}</level>|"
    "<cyan>{name}:{file.path}:{function}:{line} tid-{thread}</cyan> - {message}",
    4: "{time:x zz} {level: <8} {file.path}:{line} pid-{process} {message}",
    5: "{time:x zz} {level: <8} {file.path}:{line} tid-{thread} {message}",
    6: "{message}"
}


def get_logger(sink, format=0, name='root', file_level=None, backtrace=False, delay=True, **kwargs):
    """
        sink : |file-like object|_, |str|, |Path|, |function|_, |Handler| or |class|
        level: |int| or |str| 日志等级trace->critical会记录>=level日志
        file_level： |str|大写日志等级只记录这个等级日志
        backtrace: bool日志会追踪错误栈
        serialize: bool Json格式
        enqueue: bool 多线程，多进程安全必须设置为True

        如果sink是文件 kwargs里面可以设置以下参数：
        rotation: |str|, |int|, |time|, |timedelta| or |function| 日志回滚参数设置
        retention: |str|, |int|, |timedelta| or |function| 日志清理参数设置
        compression: |str| or |function| 日志压缩格式
        delay: bool 日志是否在开始生成还是记录第一条日志生成
        mode：str 写文件模式
        参数和使用详情请参照:https://git.silvrr.com/risk-backend/risk-document/wikis/akpi-logging
    """
    if isinstance(format, int):
        if format < 0 or format > len(formats):
            raise Exception('format beyond formats')
        format = formats['format']
    elif isinstance(format, str):
        format = format
    else:
        raise Exception('format val error')
    filter = lambda record: record["extra"].get("name") == name
    if file_level:
        filter = lambda record: record["extra"].get("name") == name and record['level'] == file_level
    logger.start(sink, format=format, filter=filter, backtrace=False, delay=False, **kwargs)
    return logger.bind(name=name)


def turn_off_default_logger():
    logger.stop(0)
