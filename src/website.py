import json
import logging

from base_handler import BaseHandler


class HomeApi(BaseHandler):
    async def get(self):
        self.write("welcome to home page")


