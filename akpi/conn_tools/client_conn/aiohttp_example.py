import json
import asyncio
import aiohttp

from aiohttp.formdata import FormData


async def get():
    cookies = {'cookies_are': 'working'}
    headers = {'content-type': 'application/json'}
    async with aiohttp.ClientSession(cookies=cookies, headers=headers) as session:
        params = 'key=value+1'
        params = [('key', 'value1'), ('key', 'value2')]
        params = {'key1': 'value1', 'key2': 'value2'}
        # proxy_auth = aiohttp.BasicAuth('user', 'pass')
        # proxy = "http://some.proxy.com"
        async with session.get('http://httpbin.org/get', params=params, headers=headers, timeout=60) as resp:
            # proxy_auth=proxy_auth,
            # proxy=proxy,
            print(resp.status)
            print(resp.url)
            print(await resp.text())


async def post():
    async with aiohttp.ClientSession() as session:
        # 表单模式(转码)
        payload = {'key1': 'value1', 'key2': 'value2'}
        # 表单模式(不转码)
        payload = json.dumps(payload)
        # 上传小文件
        # files = {'file': open('test.py', 'rb')}
        # 设置文件名称
        data = FormData()
        data.add_field('file',
                       open('test.py', 'rb'),
                       filename='test.py',
                       content_type='application/vnd.ms-excel')
        async with session.post('http://httpbin.org/post', data=data) as resp:
            print(resp.status)
            print(await resp.text())
            print(resp.url)


async def post_file():
    @aiohttp.streamer
    async def file_sender(writer, file_name=None):
        with open(file_name, 'rb') as f:
            chunk = f.read(2**16)
            while chunk:
                await writer.write(chunk)
                chunk = f.read(2**16)
    async with aiohttp.ClientSession() as session:
        async with session.post('http://httpbin.org/post',
                                data=file_sender(file_name='test.py')) as resp:
            print(await resp.text())
            print(resp.url)


async def put():
    async with aiohttp.ClientSession() as session:
        data = {'key1': 'value1', 'key2': 'value2'}
        async with session.put('http://httpbin.org/put', data=data) as resp:
            print(resp.status)
            print(await resp.json())
            print(resp.url)


async def delete():
    async with aiohttp.ClientSession() as session:
        params = {'key1': 'value1', 'key2': 'value2'}
        async with session.put('http://httpbin.org/delete', params=params) as resp:
            print(resp.status)
            print(await resp.text())
            print(resp.url)


async def head():
    async with aiohttp.ClientSession() as session:
        async with session.put('http://httpbin.org/get') as resp:
            print(resp.headers)
            print(resp.url)
            print(resp.cookies)


async def options():
    async with aiohttp.ClientSession() as session:
        async with session.options('http://httpbin.org/get') as resp:
            print(resp.headers)
            print(resp.url)
            print(resp.cookies)


async def patch():
    async with aiohttp.ClientSession() as session:
        async with session.patch('http://httpbin.org/patch', data=b'data') as resp:
            print(resp.headers)
            print(resp.url)
            print(resp.cookies)

loop = asyncio.get_event_loop()
asyncio.ensure_future(get(), loop=loop)
asyncio.ensure_future(post(), loop=loop)
asyncio.ensure_future(put(), loop=loop)
asyncio.ensure_future(head(), loop=loop)
asyncio.ensure_future(options(), loop=loop)
asyncio.ensure_future(patch(), loop=loop)
asyncio.ensure_future(delete(), loop=loop)
asyncio.ensure_future(post_file(), loop=loop)
loop.run_forever()
