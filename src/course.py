import json
import logging
from conn_utils.mysql_conn.async_mysql_conn_pool import AsyncMysqlConn

from base_handler import BaseHandler
from tornado import escape

class CourseApi(BaseHandler):
    async def post(self):
        ret = {'code': 100, 'data': None}
        try:
            async with AsyncMysqlConn("read") as conn:
                data =await conn.select_many("select * from t_course",)
            ret['data']=data
        except Exception as e:
            logging.exception(e)
            self.write(BaseHandler.error_ret())
        else:
            self.write(BaseHandler.success_ret_with_data(ret))

class CourseDetailApi(BaseHandler):
    async def post(self):
        args = escape.json_decode(self.request.body)
        course_id = args.get("id", None)
        ret = {'code': 100, 'data': None}
        try:
            async with AsyncMysqlConn("read") as conn:
                data =await conn.select_one("select * from t_course_detail where course_id=%s",[course_id])
                tmp = []
                if data.get("recommend_courses"):
                    for i in data["recommend_courses"].split(","):
                        title = await conn.select_one_value("select title from t_course where id=%s",[int(i)])
                        tmp.append({"id":int(i),"title":title})
                data["recommend_courses"]=tmp
            ret['data']=data
        except Exception as e:
            logging.exception(e)
            self.write(BaseHandler.error_ret())
        else:
            self.write(BaseHandler.success_ret_with_data(ret))