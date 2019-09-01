import threading
import redis
from conf import system_config


class RedisClusterConnector(object):
    hosts = system_config.redis_cluster_aws().split(",")
    host_dict_tuple = []
    lock = threading.Lock()
    rc = None

    def __init__(self):
        pass

    def __del__(self):
        pass

    @classmethod
    def connect(cls):
        if cls.rc is None:
            with cls.lock:
                if cls.rc is None:
                    cls.rc = redis.Redis(host=cls.hosts[0].split(":")[0],port=6379)
        return cls.rc

if __name__ == '__main__':
    RedisClusterConnector.connect()