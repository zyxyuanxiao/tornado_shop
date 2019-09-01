# coding: utf8
import tornado
from tornado import web
import time
import logging
from conn_utils.redis_conn_cluster import RedisClusterConnector

# key
white_list = "feature:ip_whitelist"
user_feature_request = "feature:user/feature"


class BaseHandler(tornado.web.RequestHandler):
    # _redis_conn = RedisClusterConnector.connect()
    async def options(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "*")
        self.set_status(204)
        self.finish()

    @staticmethod
    def success_ret():
        now_time = int(time.time()*1000)
        ret = {"success": "true", "sysTime": now_time}
        return ret

    @staticmethod
    def success_ret_with_data(data):
        ret = BaseHandler.success_ret()
        ret["data"] = data
        return ret

    @staticmethod
    def error_ret():
        now_time = int(time.time() * 1000)
        ret = {"success": "false", "sysTime": now_time}
        return ret

    @staticmethod
    def error_ret_with_errmsg(errmsg):
        ret = BaseHandler.error_ret()
        ret["errMsg"] = errmsg
        return ret

    @staticmethod
    def error_ret_with_errcode(errcode):
        ret = BaseHandler.error_ret()
        ret["errCode"] = errcode
        return ret

    @staticmethod
    def error_ret_with_errmsg_and_errcode(errmsg, errcode):
        ret = BaseHandler.error_ret()
        ret["errMsg"] = errmsg
        ret["errCode"] = errcode
        return ret

    async def prepare(self):
        self.set_header("Access-Control-Allow-Origin", "*")
        self.set_header("Access-Control-Allow-Headers", "*")
        self._certify_sign(self)

    @staticmethod
    def _certify_sign(request):
        pass
        # if request.request.method =="OPTIONS":
        #     request.write("ok")
        # res = BaseHandler._redis_conn.sismember("ip_whitelist", ip)
        # if not res:
        #     logging.warning('Unauthorized:\n\thost:{},request:{}'.format(ip, request.uri))
        #     raise web.HTTPError(401)

if __name__ == '__main__':
    import requests
    # re=requests.post("http://127.0.0.1:8000/user/login",data={"phone_number":13729805358,"login_method":"phone verify"})
    # re=requests.post("http://127.0.0.1:8000/user/login",data={"phone_number":13729805358,"login_method":"phone number","sms_code":224331})
    # re = requests.post("http://127.0.0.1:8000/user/signup",json={"phone_number": 13729805358, "signup_method": "phone verify"})
    re = requests.post("http://127.0.0.1:8000/user/signup",json={"phone": 13729805358, "signup_method": "phone number","code":107721})
    print(re.text)