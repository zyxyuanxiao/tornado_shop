from configparser import ConfigParser
import os

config_parser = ConfigParser()
config_file = os.path.dirname(os.path.abspath(__file__)) + "/system.conf"
config_parser.read(config_file)

def get_logger_conf_path():
    return config_parser.get("logger", "conf_path")

def redis_cluster_aws():
    return config_parser.get("redis_cluster_aws", "hosts")

def get_mysql_server_ip():
    return config_parser.get("mysql", "host")


def get_mysql_server_user():
    return config_parser.get("mysql", "user")


def get_mysql_passwd():
    return config_parser.get("mysql", "passwd")


def get_mysql_default_db():
    return config_parser.get("mysql", "db")


def get_mysql_charset():
    return config_parser.get("mysql", "charset")


def get_mysql_master_server_ip():
    return config_parser.get("mysql", "master_host")


def get_mysql_master_server_user():
    return config_parser.get("mysql", "master_user")


def get_mysql_master_passwd():
    return config_parser.get("mysql", "master_passwd")


def get_mysql_master_default_db():
    return config_parser.get("mysql", "master_db")

def get_kafka_bootstrap_server(kf_type='installment'):
    if kf_type == 'risk':
        return config_parser.get("kafka", "risk.bootstrap.servers")
    else:
        return config_parser.get("kafka", "bootstrap.servers")