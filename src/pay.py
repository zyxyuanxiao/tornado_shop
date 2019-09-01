import json
import logging
import time
from conn_utils.mysql_conn.async_mysql_conn_pool import AsyncMysqlConn
from base_handler import BaseHandler
from tornado import escape
from conn_utils.pay import AliPay

class PayApi(BaseHandler):
    async def post(self):
        ret = {'code': 100, 'data': None}
        args = escape.json_decode(self.request.body)
        course_id = args.get("course_id", None)
        price = args.get("price", None)
        alipay = aliPay()
        # 对购买的数据进行加密
        async with AsyncMysqlConn("read") as conn:
            title = await conn.select_one_value("select title from t_course where id=%s",[course_id] )
        out_trade_no = "x2" + str(time.time())  # 随机生成的订单号
        # 1. 在数据库创建一条数据：状态（待支付）
        # 2. 创建加密的数据，然后放到url发送给支付宝
        query_params = alipay.direct_pay(
            subject=title,  # 商品简单描述
            out_trade_no=out_trade_no,  # 商户订单号，支付完成后支付宝会返回订单号
            total_amount=price,  # 交易金额(单位: 元 保留俩位小数)
        )
        pay_url = "https://openapi.alipaydev.com/gateway.do?{}".format(query_params)
        ret['data']=pay_url
        self.write(BaseHandler.success_ret_with_data(ret))

def aliPay():
    # 沙箱测试地址：https://openhome.alipay.com/platform/appDaily.htm?tab=info
    # 支付相关配置
    APPID = "2016092200570009"
    NOTIFY_URL = "http://127.0.0.1:8000/website/update_order"   # 需公网IP
    RETURN_URL = "http://127.0.0.1:8000/website/pay_result"
    PRI_KEY_PATH = "conf/keys/app_private_2048.txt"
    PUB_KEY_PATH = "conf/keys/alipay_public_2048.txt"

    obj = AliPay(
        appid=APPID,
        app_notify_url=NOTIFY_URL,  # 如果支付成功，支付宝会向这个地址发送POST请求（校验是否支付已经完成）
        return_url=RETURN_URL,  # 如果支付成功，重定向回到你的网站的地址。
        alipay_public_key_path=PUB_KEY_PATH,  # 支付宝公钥
        app_private_key_path=PRI_KEY_PATH,  # 应用私钥
        debug=True,  # 默认False,
    )
    return obj

class PayResultApi(BaseHandler):
    async def get(self):
        params = self.request.arguments
        data={}
        for k,v in params.items():
            data[k]=v[0].decode()
        sign = data.pop('sign', None)  # 获取sign对应的签名，通过verify函数检验
        alipay = aliPay()
        status = alipay.verify(data, sign)  # true or false
        if status:
            self.redirect('http://127.0.0.1:8080/payresult')
        else:
            self.write("支付异常")

    async def post(self):
        args = escape.json_decode(self.request.body)
        post_dict = {}
        for k, v in args.items():
            post_dict[k] = v[0]  # 解析字典
        alipay = aliPay()
        sign = post_dict.pop('sign', None)
        status = alipay.verify(post_dict, sign)
        if status:
            # 这一步去数据库修改订单状态
            out_trade_no = post_dict.get('out_trade_no')
            print(out_trade_no)