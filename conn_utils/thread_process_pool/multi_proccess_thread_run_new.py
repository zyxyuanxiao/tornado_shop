import logging
from logging import handlers
import time
import functools
import objgraph
import signal
import operator
import gc
import types,inspect
from importlib import import_module
from contextlib import contextmanager
from pythonjsonlogger.jsonlogger import JsonFormatter
import argparse

from system_config import system_config
from risk_platform.thread_process_pool.multi_proccess_thread_run import mutli_process_and_thread_run,mutli_thread_run,mutli_process_run,mutli_process_and_coro_run

parser = argparse.ArgumentParser(description='Timer Task Run')
parser.add_argument('-o', dest='logfile', action='store',metavar='log path',help='output file for log')
parser.add_argument('-mo', dest='mode', action='store',metavar='mode choice',help='choose which task run mode',choices=['1','2','3','4','-1','-2','-3','-4'])
parser.add_argument('-p', dest='proccss_num', action='store',metavar='process number',help='set how many process number task run')
parser.add_argument('-t', dest='thread_num', action='store',metavar='thread number',help='set how many thread number on one process task run')
parser.add_argument('-f', dest='task_func', action='store',metavar='task function',help='string type,detail module path of task function')
parser.add_argument('-B', dest='start', action='store',metavar='start id',help='task start from where')
parser.add_argument('-T', dest='stop', action='store',metavar='stop id',help='task stop from where')
parser.add_argument('-ps', dest='process_step', action='store',metavar='process step',help='task process run step')
parser.add_argument('-ts', dest='thread_step', action='store',metavar='thread step',help='task thread run step')
shell_args = parser.parse_args()

def module_parser(path):
    m_path, funcname = path.rsplit('.',1)
    m = import_module(m_path)
    return getattr(m,funcname)

@contextmanager
def func_time_calculate():
    start_time = time.time()
    try:
        yield
    finally:
        logging.info("mission completed! taken time {}".format(time.time()-start_time))

class LoggerMeta(type):
    def __init__(cls,clsname, bases, clsdict):
        super(LoggerMeta,cls).__init__(clsname, bases, clsdict)
        for n,name in enumerate(clsdict["_fields"]):
            setattr(cls,name,property(operator.itemgetter(n)))

    def __new__(cls, clsname, bases, clsdict):
        instance = super(cls,cls).__new__(cls,clsname, bases, clsdict)
        if "_fields" not in clsdict:
            raise Exception("_fields must be defined in {}".format(cls.__name__))
        return instance

    def __call__(cls, *args, **kwargs):
        logger = logging.getLogger()
        logger.handlers = []
        logger.setLevel(logging.INFO)
        logging.getLogger("py2neo.batch").setLevel(logging.WARNING)
        logging.getLogger("py2neo.cypher").setLevel(logging.WARNING)
        logging.getLogger("httpstream").setLevel(logging.WARNING)
        logging.getLogger("elasticsearch").setLevel(logging.ERROR)
        return super(LoggerMeta,cls).__call__(*args, **kwargs)

class BaseHandler(object):
    fmt = "%(asctime)s %(levelname)s %(filename)s:%(lineno)d" \
          " pid-%(process)d %(message)s"
    date_fmt = "%Y-%m-%d %H:%M:%S"
    base_path = system_config.get_logger_base_path()
    def __init__(self):
        self.formatter = logging.Formatter(self.fmt, self.date_fmt)
    def __get__(self, instance, owner):
        pass
    def __set__(self, instance, value):
        if not isinstance(value,str):
            raise TypeError("expected string type")
    def set_format(self,handler):
        handler.setFormatter(self.formatter)
        handler.setLevel(self.level)
        logger = logging.getLogger()
        logger.addHandler(handler)

class ErrorLevel(object):
    level = logging.ERROR

class InfoLevel(object):
    level = logging.INFO

class UploadHandler(ErrorLevel,BaseHandler):

    def __init__(self):
        self.formatter = JsonFormatter(self.fmt, self.date_fmt)

    def __set__(self, instance, value):
        super(self.__class__,self).__set__(instance, value)
        if value.find("/")!=-1:
            value = value.split("/")[-1]
        handler = handlers.TimedRotatingFileHandler(filename=self.base_path + "kibana_timer/"+ value,when="D",interval=1,backupCount=3)
        self.set_format(handler)

class ErrorHandler(ErrorLevel,BaseHandler):
    def __set__(self, instance, value):
        super(self.__class__,self).__set__(instance, value)
        handler = handlers.WatchedFileHandler(filename=self.base_path + "timer_task/error_message.log")
        self.set_format(handler)

class NomalHandler(InfoLevel,BaseHandler):
    def __set__(self, instance, value):
        super(self.__class__,self).__set__(instance, value)
        handler = handlers.WatchedFileHandler(filename=self.base_path+value)
        self.set_format(handler)

def mode_choice(func=None,process_max_workers=8,thread_max_workers=3,task_func=None,start_pos=1,max_pos=None,process_step=100000-1,thread_step=2000-1):
    if not func:
        return functools.partial(mode_choice, process_max_workers=process_max_workers,thread_max_workers=thread_max_workers,
                                 task_func=task_func,start_pos=start_pos,max_pos=max_pos,process_step=process_step,thread_step=thread_step)
    @functools.wraps(func)
    def wraps(fun,*args,**kwargs):
        if len(args)>1:
            fun(*args)
        else:
            fun(process_max_workers=process_max_workers,thread_max_workers=thread_max_workers,
                                 task_func=args[0],start_pos=start_pos,max_pos=max_pos,process_step=process_step,thread_step=thread_step)
    return wraps

class MultiMethod:
    def __init__(self,name):
        self._methods = {}
        self.__name__=name

    def register(self, mode):
        def wraps(func):
            self._methods[mode] = func
            return self
        return wraps

    def __call__(self, *args):
        types = tuple(len(args))
        types = 2 if types>1 else types
        meth = self._methods.get(types, None)
        if meth:
            return meth(*args)
        else:
            raise TypeError('No matching method for types {}'.format(types))

    def __get__(self, instance, cls):
        if instance is not None:
            return types.MethodType(self, instance)
        else:
            return self

class TaskRun(list,object,metaclass=LoggerMeta):
    upload_handler = UploadHandler()
    nomal_handler = NomalHandler()
    error_handler = ErrorHandler()
    patch = MultiMethod("choice_run")
    _fields = ["t","p","p&t","p&c"]

    def __init__(self,path):
        if shell_args.logfile:
            path = shell_args.logfile
        self.upload_handler = path
        self.error_handler = path
        self.nomal_handler = path
        super(self.__class__,self).__init__([mutli_thread_run,mutli_process_run,mutli_process_and_thread_run,mutli_process_and_coro_run])

    def start(self,mode,*args):
        if shell_args.mode:
            mode = int(shell_args.mode)
            if mode>0:
                if not shell_args.task_func:
                    pos = 2 if args[0]>=3 else 1
                    shell_args.task_func = args[pos]
                else:
                    shell_args.task_func = module_parser(shell_args.task_func)
                params = [(getattr(shell_args, x, None)) for x in ["proccss_num", "thread_num","start", "stop", "process_step", "thread_step"]]
                params = [int(x) for x in params if x]
                params.insert(2 if mode>2 else 1,shell_args.task_func)
                args = (y for y in params if y)
        if mode<0:
            mode = - mode
        self.choice_run(self[mode-1],*args)

    @staticmethod
    @mode_choice(process_max_workers=3,thread_max_workers=3)
    @patch.register(1)
    def choice_run(fun,task_func):
        pass

    @staticmethod
    @mode_choice
    @patch.register(2)
    def choice_run(fun,*args):
        pass



# def objgraph_analysis(signum, frame):
#     logging.error("receive singal {}".format(signum))
#     objgraph.show_growth()


if __name__=="__main__":
    TaskRun(shell_args.logfile).start(1)
    # pass