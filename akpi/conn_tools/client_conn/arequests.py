#!/usr/bin/env python
# -*- coding: utf-8 -*-

# Copyright (python) ,2018-2036, AKULAKU Tech. Co,Ltd
# @Author: YouShaoPing
# @Version: 0.0.1
# @Date:   2018-11-24 15:16:43
# @Last Modified by:   YouShaoPing
# @Last Modified time: 2018-12-07 10:46:59

import threading
from aiohttp import ClientSession


class Arequests(object):
    """docstring for Arequests"""

    _instance_lock = threading.Lock()

    def __init__(self):
        pass

    def __new__(cls, *args, **kwargs):
        if not hasattr(Arequests, "_instance"):
            with Arequests._instance_lock:
                if not hasattr(Arequests, "_instance"):
                    Arequests._instance = object.__new__(cls)
        return Arequests._instance

    async def get(self, url, allow_redirects=True, **kwargs):
        async with ClientSession(**kwargs) as session:
            async with session.get(url) as resp:
                return resp.text()
