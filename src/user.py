import json
import logging
import random
import aiohttp
import re
from base_handler import BaseHandler
from conn_utils.mysql_conn.async_mysql_conn_pool import AsyncMysqlConn
from conn_utils.redis_conn_cluster import RedisClusterConnector
from aliyunsdkcore.client import AcsClient
from aliyunsdkcore.request import CommonRequest
from tornado import escape
import time
import requests
qcode = None
tip = 1
ctime=None
ticket_dict = {}
USER_INIT_DICT = {}
ALL_COOKIE_DICT = {}

client = AcsClient('LTAIW2njpzYPpEZV', 'xUwDppwYAqFw2JMJ0ALolm9JYJKrN8', 'cn-hangzhou')

class LoginApi(BaseHandler):

    async def post(self):
        print("coming a request")
        args = escape.json_decode(self.request.body)
        print(args)
        login_method = args.get("login_method", None)
        try:
            if login_method=="phone number":
                result =await LoginFunc.phone_login(args)
            elif login_method=="phone verify":
                result =await LoginFunc.sms_verify(args)
            elif login_method=="webchat":
                # 用户扫码登陆
                result = await LoginFunc.wetcht_login(args)
            elif login_method =="webchat_check":
                result = await LoginFunc.wetchat_check(args)
            else:
                result = await LoginFunc.normal_login(args)
        except Exception as e:
            logging.exception(e)
            self.write(BaseHandler.error_ret())
        else:
            self.write(BaseHandler.success_ret_with_data(result))

class SignupApi(BaseHandler):

    async def post(self):
        print("new signup")
        args = escape.json_decode(self.request.body)
        print(args)
        login_method = args.get("signup_method", None)
        try:
            if login_method=="phone number":
                result =await SignupFunc.phone_login(args)
            elif login_method=="phone verify":
                result =await SignupFunc.sms_verify(args)
            elif login_method=="webchat":
                # 用户扫码登陆
                result = await LoginFunc.wetcht_login(args)
            else:
                result = await SignupFunc.normal_login(args)
        except Exception as e:
            logging.exception(e)
            self.write(BaseHandler.error_ret())
        else:
            self.write(BaseHandler.success_ret_with_data(result))

class LoginFunc(object):

    @staticmethod
    async def normal_login(request):
        data = {"msg": "success", "reason": None, "code": 1000}
        username = request.get("username",None)
        password = request.get("password", None)
        async with AsyncMysqlConn("read") as conn:
            cnt =await conn.select_one_value("select count(1) from t_user where username=%s and passwd=%s",[username,password])
        if not cnt:
            data["msg"] = "用户名或者密码错误"
            data["code"] = "1001"
        return data

    @staticmethod
    async def wetcht_login(request):
        global ctime
        data = {"msg": "success", "reason": None, "code": 1000}
        access_token = request.get("access_token", None)
        # 判断用户是否已注册
        # 根据access_token登陆
        ctime = time.time()  # 时间窗，用于生成请求url
        response = requests.get(
            url='https://login.wx.qq.com/jslogin?appid=wx782c26e4c19acffb&fun=new&lang=zh_CN&_=%s' % ctime
            # r后面一般是时间窗，redirect_url=xxx完成操作后跳转url，可以删除
        )
        code = re.findall('uuid = "(.*)";', response.text)
        global qcode
        qcode = code[0]
        data['msg']=qcode
        return data

    @staticmethod
    async def wetchat_check(request):
        global qcode
        global tip
        global ctime
        data = {"msg": "success", "reason": None, "code": 408}
        r1 = requests.get(
            url='https://login.wx.qq.com/cgi-bin/mmwebwx-bin/login?loginicon=true&uuid=%s&tip=%s&sr=-1767722401&_=%s' % (
            qcode, tip, ctime,)  # 传请求二维码的参数
        )
        # 这时向微信请求，pending多久看微信什么时候返回
        if 'window.code=408' in r1.text:
            print('无人扫码')
            data["msg"]="waiting"
        elif 'window.code=201' in r1.text:  # 已扫码，返回头像url给前端，再继续监听同一个url看是否确认
            data['code'] = 201
            avatar = re.findall("window.userAvatar = '(.*)';", r1.text)[0]
            data['msg'] = avatar
            tip = 0  # 修改一下请求url的参数
        elif 'window.code=200' in r1.text:  # 已确认
            ALL_COOKIE_DICT.update(r1.cookies.get_dict())  # 更新第一次确认的cookie，可能有用
            redirect_url = re.findall('window.redirect_uri="(.*)";', r1.text)[0]  # 不同设备重定向url可能不一样
            redirect_url = redirect_url + "&fun=new&version=v2&lang=zh_CN"  # 新的重定向url添加后缀去请求用户数据
            r2 = requests.get(url=redirect_url)
            # 获取凭证
            from bs4 import BeautifulSoup
            soup = BeautifulSoup(r2.text, 'html.parser')
            for tag in soup.find('error').children:  # 找到所有的登陆凭证
                ticket_dict[tag.name] = tag.get_text()  # 字典类型，引用类型，修改值不用global
            ALL_COOKIE_DICT.update(r2.cookies.get_dict())  # 更新重定向的cookie，可能有用
            data['code'] = 200
            user_info_url = "https://wx2.qq.com/cgi-bin/mmwebwx-bin/webwxinit?r=-1780597526&lang=zh_CN&pass_ticket=" + \
                            ticket_dict['pass_ticket']
            user_info_data = {
                'BaseRequest': {
                    'DeviceID': "e459555225169136",  # 这个随便写，没获取过
                    'Sid': ticket_dict['wxsid'],
                    'Skey': ticket_dict['skey'],  # 全部在用户凭证里
                    'Uin': ticket_dict['wxuin'],
                }
            }
            r3 = requests.post(
                url=user_info_url,
                json=user_info_data,  # 不能data，否则只能拿到key，value传不了
            )
            r3.encoding = 'utf-8'  # 编码
            user_init_dict = json.loads(r3.text)  # loads将text字符串类型转为字典类型
            ALL_COOKIE_DICT.update(r3.cookies.get_dict())  # 再次保存cookie，这样就包含了以上所有流程的cookie
            # USER_INIT_DICT 已声明为空字典，内存地址已有，添加值不修改地址，但赋值会改变地址，比如=123，之前要声明global即可。
            # USER_INIT_DICT['123']=123,    USER_INIT_DICT.update(user_init_dict)两种做法都没改变地址
            USER_INIT_DICT.update(user_init_dict)
        return data

    @staticmethod
    async def phone_login(request):
        data = {"msg": "success", "reason": None, "code": 1000}
        phone_number = request.get("phone", None)
        sms_code = request.get("code", None)
        async with AsyncMysqlConn("read") as conn:
            cnt =await conn.select_one_value("select count(1) from tmp.t_user where phone_number=%s",[phone_number])
        if not cnt:
            data["msg"]="not register"
            data["code"]=1001
        else:
            _redis_conn = RedisClusterConnector.connect()
            redis_code = _redis_conn.get("phone_{}_sms_code".format(phone_number))
            if not int(redis_code)==int(sms_code):
                data["msg"] = "verify code is wrong"
                data["code"] = 1004
        return data

    @staticmethod
    async def sms_verify(request):
        data={"msg":"success","reason":None,"code":1000}
        phone_number = request.get("phone_number", None)
        async with AsyncMysqlConn("read") as conn:
            cnt =await conn.select_one_value("select count(1) from tmp.t_user where phone_number=%s",[phone_number])
        if not cnt:
            data["msg"]="not register"
            data["code"]=1001
        else:
            sms_code = int("".join([str(random.randint(0,9)) for _ in range(6)]))
            _redis_conn = RedisClusterConnector.connect()
            _redis_conn.set("phone_{}_sms_code".format(phone_number),sms_code)
            ret = await LoginFunc.send_sms_code(phone_number,sms_code)
            if not ret["Code"]=="OK":
                data["msg"] = "thirdpart sms server error"
                data["code"] = 1003
        return data

    @staticmethod
    async def send_sms_code(phone_number,sms_code):
        # 调用第三方接口发生短信
        return sms_code_send(phone_number,sms_code)

class SignupFunc(object):

    @staticmethod
    async def normal_login(request):
        data = {"msg": "success", "reason": None, "code": 1000}
        username = request.get("username",None)
        password = request.get("password", None)
        phone = request.get("phone", None)
        async with AsyncMysqlConn("master") as conn:
            cnt = await conn.select_one_value("select count(1) from tmp.t_user where phone_number=%s", [phone])
            if cnt:
                data["msg"]="this phone had regisiter"
                data["code"]=1005
            else:
                await conn.insert_one("insert into t_user (username,passwd,phone_number) values(%s,%s,%s)",[username,password,phone])
        return data

    @staticmethod
    async def wetcht_login(request):
        access_token = request.get("access_token", None)
        # 判断用户是否已注册
        # 获取扫码用户信息
        # 写表
        return

    @staticmethod
    async def phone_login(request):
        data = {"msg": "success", "reason": None, "code": 1000}
        phone_number = request.get("phone", None)
        sms_code = request.get("code", None)
        async with AsyncMysqlConn("read") as conn:
            cnt =await conn.select_one_value("select count(1) from tmp.t_user where phone_number=%s",[phone_number])
        if cnt:
            data["msg"]="this phone had regisiter"
            data["code"]=1005
        else:
            _redis_conn = RedisClusterConnector.connect()
            redis_code = _redis_conn.get("phone_{}_sms_code".format(phone_number))
            print(redis_code)
            if not int(redis_code)==int(sms_code):
                data["msg"] = "verify code is wrong"
                data["code"] = 1004
            else:
                async with AsyncMysqlConn("master") as conn:
                    await conn.insert_one("insert into t_user (username,passwd,phone_number) values(%s,%s,%s)",
                                          ["test", "test", phone_number])
        return data

    @staticmethod
    async def sms_verify(request):
        data={"msg":"success","reason":None,"code":1000}
        phone_number = request.get("phone_number", None)
        async with AsyncMysqlConn("read") as conn:
            cnt =await conn.select_one_value("select count(1) from tmp.t_user where phone_number=%s",[phone_number])
        if cnt:
            data["msg"] = "this phone had regisiter"
            data["code"] = 1005
        else:
            sms_code = int("".join([str(random.randint(0,9)) for _ in range(6)]))
            _redis_conn = RedisClusterConnector.connect()
            _redis_conn.set("phone_{}_sms_code".format(phone_number),sms_code)
            ret = await LoginFunc.send_sms_code(phone_number,sms_code)
            if not ret["Code"]=="OK":
                data["msg"] = "thirdpart sms server error"
                data["code"] = 1003
                logging.exception(ret)
        return data

    @staticmethod
    async def send_sms_code(phone_number,sms_code):
        # 调用第三方接口发生短信
        return sms_code_send(phone_number,sms_code)

async def fetch(session, url,data,json,headers):
    async with session.post(url,data=data,json=json,headers=headers) as response:
        if response.status==504:
            logging.info("request return 504 timeout,start to jump off nginx to request")
            async with session.post(url=url
                    , data=data, json=json) as second_response:
                if second_response.status!=200:
                    logging.info(await second_response.text())
                    raise Exception("get feature error")
                return await second_response.text()

        if response.status!=200:
            logging.info(await response.text())
            raise Exception("get feature error")
        return await response.text()

async def async_request(url,data=None,json=None,headers=None):
    async with aiohttp.ClientSession() as session:
        return await fetch(session,url,data,json,headers)

def sms_code_send(phone_number,sms_code):
    request = CommonRequest()
    request.set_accept_format('json')
    request.set_domain('dysmsapi.aliyuncs.com')
    request.set_method('POST')
    request.set_protocol_type('https')  # https | http
    request.set_version('2017-05-25')
    request.set_action_name('SendSms')

    request.add_query_param('RegionId', "cn-hangzhou")
    request.add_query_param('PhoneNumbers', str(phone_number))
    request.add_query_param('SignName', "路飞vue")
    request.add_query_param('TemplateCode', "SMS_173251941")
    request.add_query_param('TemplateParam', json.dumps({"code":sms_code}))

    response = client.do_action(request)
    return json.loads(str(response, encoding='utf-8'))

if __name__ == '__main__':
    sms_code_send(13729805358,123456)