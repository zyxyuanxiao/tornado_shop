# coding=utf-8
import asyncio

import logging
import logging.config
import logging.handlers
import os
import sys
import tornado.httpserver
import tornado.options
import tornado.gen
import tornado.web
import tornado.platform.asyncio
import urllib.parse
from tornado.log import access_log
from conf import system_config
import traceback
import signal
from src.user import LoginApi,SignupApi
from src.website import HomeApi
from src.course import CourseApi,CourseDetailApi
from src.pay import PayApi,PayResultApi

# logging.config.fileConfig(system_config.get_logger_conf_path())

class ErrorHandle:
    def __init__(self):
        pass

    def handle(self, record):
        pass

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")

def log_request(handler):
    if handler.get_status() < 400:
        log_method = access_log.info
    elif handler.get_status() < 500:
        log_method = access_log.warning
    else:
        log_method = access_log.error
    request_time = 1000.0 * handler.request.request_time()
    remote_ip = handler.request.headers.get('X-Real-IP', '')
    _request_summary = "%s %s (%s)" % (handler.request.method, handler.request.uri, remote_ip)

    message_dict = {
        'method': handler.request.method,
        'remote_ip': remote_ip,
        'request_uri': handler.request.uri,
        'status_code': handler.get_status(),
        'request_time': request_time
    }
    log_method("%d %s %.2fms" % (handler.get_status(), _request_summary, request_time), extra=message_dict)

def make_app():
    application = tornado.web.Application([
        (r"/", MainHandler),
        (r"/user/login", LoginApi),
        (r"/user/signup", SignupApi),
        (r"/website/main", HomeApi),
        (r"/website/course", CourseApi),
        (r"/website/coursedetail", CourseDetailApi),
        (r"/website/pay", PayApi),
        (r"/website/pay_result", PayResultApi),
    ])
    return application


if __name__ == "__main__":
    # init()
    port = int(sys.argv[1])
    logging.info('%s feature process init end ...', port)
    logging.info('%s tornado running...', port)
    logging.info('%s  async_conn.loop running...', port)

    # 信号处理
    def stop_callback(signum, frame):
        logging.info("%s received signum %d, process stopping", sys.argv[1], signum)
        asyncio.get_event_loop().stop()
        logging.info("% tornado stopped", sys.argv[1])
        sys.exit(0)

    signal.signal(signal.SIGTERM, stop_callback)    # 终止信号

    app = make_app()
    app.settings["log_function"] = log_request
    http_server = tornado.httpserver.HTTPServer(app, decompress_request=False)
    http_server.listen(port)

    loop = asyncio.get_event_loop()
    loop.run_forever()

