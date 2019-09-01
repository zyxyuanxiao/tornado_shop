import logging
import gc
import sys
sys.path.insert(0,"/home/ubuntu/data/code_develop/feature-server")
from contextlib import contextmanager
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor,ProcessPoolExecutor
from risk_platform.mysql_conn.async_conn.async_mysql_conn_pool import AsyncMysqlConn
from risk_platform.mysql_conn.async_conn.async_bussiness_conn_pool import AsyncBussinessMysqlConn
from risk_platform.mysql_conn.async_conn.async_feature_data_conn_pool import AsyncFeatureDataMysqlConn
from risk_platform.mysql_conn.async_conn.async_model_conn_pool import AsyncModelMysqlConn
from risk_platform.mysql_conn.async_conn.async_db4_conn_pool import AsyncDb4MysqlConn
from risk_platform.mysql_conn.async_conn.async_akuloan_conn_pool import AsyncAkuloanMysqlConn
from risk_platform.mysql_conn.mysql_conn_pool import MysqlConn
from risk_platform.mysql_conn.bussiness_conn_pool import BussinessMysqlConn
from risk_platform.mysql_conn.feature_data_conn_pool import FeatureDataMysqlConn
from risk_platform.mysql_conn.model_conn_pool import ModelMysqlConn
from risk_platform.mysql_conn.db4_conn_pool import db4MysqlConn
from risk_platform.mysql_conn.akuloan_conn_pool import AkuloanMysqlConn
from risk_platform.utils.log_config import set_logger


@contextmanager
def func_time_calculate():
    start_time = time.perf_counter()
    # func = ((sys.argv[0]).rsplit('/',1)[1]).split('.')[0]
    try:
        yield
    finally:
        # SocketConn.error_send()
        logging.info("mission completed! taken time {}".format(time.perf_counter()-start_time))

def mysql_initial(db_list,max_workers):
    for db in db_list:
        db._db_pools={}
        db.config_pool({"max_connections": max_workers*2, "min_cached": max_workers//2, "max_cached": max_workers},
                       {"max_connections": max_workers * 2, "min_cached": max_workers // 2, "max_cached": max_workers})


async def mutli_coro_run(max_workers,task_func,start_pos,max_pos,step,**kwargs):
    if not task_func:
        raise ValueError("must pass a run func")
    AsyncMysqlConn._db_pools = {}
    AsyncMysqlConn.config_pool(master_config={"max_connections": max_workers*2, "min_cached": max_workers//2, "max_cached": max_workers},
                    risk_config={"max_connections": max_workers*2, "min_cached": max_workers//2, "max_cached": max_workers},
                    tidb_config={"max_connections": max_workers*2, "min_cached": max_workers//2, "max_cached": max_workers}
                          )
    mysql_initial([BussinessMysqlConn, FeatureDataMysqlConn, AkuloanMysqlConn, ModelMysqlConn, db4MysqlConn,
                   AsyncBussinessMysqlConn, AsyncFeatureDataMysqlConn, AsyncDb4MysqlConn, AsyncAkuloanMysqlConn,
                   AsyncModelMysqlConn], max_workers)
    # pool = ThreadPoolExecutor(max_workers=max_workers)
    # pool_submit = pool.submit
    tasks=[]
    # coro_lock = asyncio.Semaphore(max_workers)
    while start_pos<=max_pos:
        end_pos = start_pos+step
        # task = asyncio.ensure_future(task_func(start_pos,end_pos))
        # async with coro_lock:
        task = task_func(start_pos,end_pos)
        start_pos = end_pos + 1
        tasks.append(task)
        if len(tasks)==max_workers:
            await asyncio.wait(tasks)
            tasks=[]
    if tasks:
        await asyncio.wait(tasks)
    # pool.shutdown()
    # await asyncio.gather(*tasks)

def mutli_thread_run(max_workers,task_func,start_pos,max_pos,step,**kwargs):
    if not task_func:
        raise ValueError("must pass a run func")
    MysqlConn._db_pools = {}
    MysqlConn.config_pool(master_config={"max_connections": max_workers*2, "min_cached": max_workers//2, "max_cached": max_workers},
                    risk_config={"max_connections": max_workers*2, "min_cached": max_workers//2, "max_cached": max_workers},
                    tidb_config={"max_connections": max_workers*2, "min_cached": max_workers//2, "max_cached": max_workers}
                          )
    mysql_initial([BussinessMysqlConn, FeatureDataMysqlConn, AkuloanMysqlConn, ModelMysqlConn, db4MysqlConn,
                   AsyncBussinessMysqlConn,AsyncFeatureDataMysqlConn,AsyncDb4MysqlConn,AsyncAkuloanMysqlConn,AsyncModelMysqlConn],max_workers)
    pool = ThreadPoolExecutor(max_workers=max_workers)
    pool_submit = pool.submit
    while start_pos<=max_pos:
        end_pos = start_pos+step
        pool_submit(task_func,start_pos,end_pos)
        start_pos = end_pos + 1
    pool.shutdown()

def mutli_thread_pool(flag=1,max_workers=2,*args,**kwargs):
    AsyncMysqlConn._db_pools = {}
    AsyncModelMysqlConn._db_pools = {}
    AsyncDb4MysqlConn._db_pools = {}
    AsyncBussinessMysqlConn._db_pools={}
    AsyncFeatureDataMysqlConn._db_pools={}
    AsyncAkuloanMysqlConn._db_pools={}
    AsyncMysqlConn.config_pool(master_config={"max_connections": max_workers*2, "min_cached": max_workers/2, "max_cached": max_workers},
                    risk_config={"max_connections": max_workers*2, "min_cached": max_workers/2, "max_cached": max_workers},
                    tidb_config={"max_connections": max_workers*2, "min_cached": max_workers/2, "max_cached": max_workers}
                          )
    AsyncBussinessMysqlConn.config_pool(business_read_config={"max_connections": max_workers*2, "min_cached": max_workers/2, "max_cached": max_workers},
                                   business_master_config={"max_connections": max_workers*2, "min_cached": max_workers/2, "max_cached": max_workers})
    AsyncFeatureDataMysqlConn.config_pool(feature_data_master_config={"max_connections": max_workers*2, "min_cached": max_workers/2, "max_cached": max_workers},
                                     feature_data_read_config={"max_connections": max_workers*2, "min_cached": max_workers/2, "max_cached": max_workers})
    AsyncModelMysqlConn.config_pool(master_config={"max_connections": max_workers*2, "min_cached": max_workers/2, "max_cached": max_workers},
                               read_config={"max_connections": max_workers*2, "min_cached": max_workers/2, "max_cached": max_workers})
    AsyncDb4MysqlConn.config_pool(master_config={"max_connections": max_workers*2, "min_cached": max_workers/2, "max_cached": max_workers},
                             read_config={"max_connections": max_workers*2, "min_cached": max_workers/2, "max_cached": max_workers})
    AsyncAkuloanMysqlConn.config_pool(
        master_config={"max_connections": max_workers * 2, "min_cached": max_workers / 2,
                               "max_cached": max_workers},
        read_config={"max_connections": max_workers * 2, "min_cached": max_workers / 2,
                             "max_cached": max_workers})
    if flag==1:
        pool = ThreadPoolExecutor(max_workers=max_workers)
    else:
        pool = ProcessPoolExecutor(max_workers=max_workers)
    return pool

def mutli_process_run(max_workers,task_func,start_pos,max_pos,step,**kwargs):
    if not task_func:
        raise ValueError("must pass a run func")
    logging.info("mission start {} process,func name is {},start from {} to {},step is {}".format(max_workers,task_func.__name__,start_pos,max_pos,step))
    with func_time_calculate():
        process_pool = ProcessPoolExecutor(max_workers=max_workers)
        pool_submit = process_pool.submit
        while start_pos<=max_pos:
            end_pos = start_pos+step
            pool_submit(new_func,task_func,start_pos,end_pos)
            start_pos = end_pos + 1
        process_pool.shutdown()


def new_func(func,start_pos,end_pos):
    AsyncMysqlConn._db_pools = {}
    AsyncModelMysqlConn._db_pools = {}
    AsyncDb4MysqlConn._db_pools = {}
    AsyncBussinessMysqlConn._db_pools={}
    AsyncFeatureDataMysqlConn._db_pools={}
    AsyncAkuloanMysqlConn._db_pools={}
    AsyncMysqlConn.config_pool(master_config={"max_connections": 3, "min_cached": 1, "max_cached": 2})
    AsyncMysqlConn.config_pool(risk_config={"max_connections": 3, "min_cached": 1, "max_cached": 2})
    AsyncBussinessMysqlConn.config_pool(business_read_config={"max_connections": 3, "min_cached": 1, "max_cached": 2},
                                   business_master_config={"max_connections": 3, "min_cached": 1, "max_cached": 2})
    AsyncFeatureDataMysqlConn.config_pool(feature_data_master_config={"max_connections": 3, "min_cached": 1, "max_cached": 2},
                                     feature_data_read_config={"max_connections": 3, "min_cached": 1, "max_cached": 2})
    AsyncModelMysqlConn.config_pool(master_config={"max_connections": 3, "min_cached": 1, "max_cached": 2},
                               read_config={"max_connections": 3, "min_cached": 1, "max_cached": 2})
    AsyncDb4MysqlConn.config_pool(master_config={"max_connections": 3, "min_cached": 1, "max_cached": 2},
                             read_config={"max_connections": 3, "min_cached": 1, "max_cached": 2})
    AsyncAkuloanMysqlConn.config_pool(
        master_config={"max_connections":3, "min_cached": 1,
                               "max_cached": 2},
        read_config={"max_connections": 3, "min_cached": 1,
                             "max_cached": 2})
    func(start_pos,end_pos)

def coroutine_run(thread_max_workers,task_func,start_pos,end_pos,thread_step):
    # new_loop = asyncio.new_event_loop()
    # asyncio.set_event_loop(new_loop)
    coro = mutli_coro_run(thread_max_workers,task_func,start_pos,end_pos,thread_step)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(coro)

def mutli_process_and_thread_run(process_max_workers,thread_max_workers,task_func,start_pos,max_pos,process_step,thread_step,**kwargs):
    if not task_func:
        raise ValueError("must pass a run func")
    with func_time_calculate():
        process_pool = ProcessPoolExecutor(max_workers=process_max_workers)
        while start_pos<=max_pos:
            end_pos = start_pos+process_step
            p = process_pool.submit(mutli_thread_run,thread_max_workers,task_func,start_pos,end_pos,thread_step)
            # process_pool.submit(coroutine_run,thread_max_workers,task_func,start_pos,end_pos,thread_step)
            # p.add_done_callback(SocketConn.error_sum)
            start_pos = end_pos + 1
        process_pool.shutdown()

def mutli_process_and_coro_run(process_max_workers,thread_max_workers,task_func,start_pos,max_pos,process_step,thread_step,**kwargs):
    if not task_func:
        raise ValueError("must pass a run func")
    with func_time_calculate():
        process_pool = ProcessPoolExecutor(max_workers=process_max_workers)
        while start_pos<=max_pos:
            end_pos = start_pos+process_step
            # p = process_pool.submit(mutli_thread_run,thread_max_workers,task_func,start_pos,end_pos,thread_step)
            process_pool.submit(coroutine_run,thread_max_workers,task_func,start_pos,end_pos,thread_step)
            # p.add_done_callback(SocketConn.error_sum)
            start_pos = end_pos + 1
        process_pool.shutdown()

def original_process_and_thread_run(process_max_workers,thread_max_workers,task_func,start_pos,max_pos,process_step,thread_step,**kwargs):
    from multiprocessing import Process
    with func_time_calculate():
        task_list = []
        while start_pos<=max_pos:
            end_pos = start_pos+process_step
            task = Process(target=mutli_thread_run, args=(thread_max_workers,task_func,start_pos,end_pos,thread_step))
            task_list.append(task)
            task.start()
            start_pos = end_pos + 1
            if len(task_list)>=process_max_workers:
                while len(task_list)>=process_max_workers:
                    for i in task_list:
                        if not i.is_alive():
                            i.terminate()
                            task_list.remove(i)
        while task_list:
            for i in task_list:
                if not i.is_alive():
                    task_list.remove(i)


class task_run(object):
    mode_dict = {
        1: mutli_thread_run,
        2: mutli_process_run,
        3: mutli_process_and_thread_run,
        4: mutli_process_and_coro_run,
    }

    def __init__(self,log_path,error_log=True,kibana=True,level=logging.INFO):
        # set_logger(log_path,error_log,kibana,level)
        set_logger(log_path, logger=logging.getLogger(), level=level, error_log=error_log, reset=True)

    def run_mode(self,mode):
        func = self.mode_dict.get(mode,None)
        if not func:
            raise KeyError("task run has no this mode")
        return func

async def test(start,end):
    await asyncio.sleep(1)
    print(start,end)

if __name__ == '__main__':
    func = task_run("timer_task/test.log").run_mode(3)
    func(5,3,test,1,1000,200-1,100-1)