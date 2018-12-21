#coding: utf-8

import os
import logging
import time
import ujson
import threading
import random
import requests
from pyutil.program.conf2 import Conf
from pyutil.program import metrics2 as metrics
from pyutil.consul.bridge import translate_one

CONF = Conf(os.path.join(os.path.dirname(__file__), 'qap.conf'))

def init_metrics():
    metrics.init(CONF)
    metrics.define_timer("call.success.latency.us", "us")
    metrics.define_timer("call.error.latency.us", "us")
    metrics.define_counter("call.success.throughput", "req")
    metrics.define_counter("call.error.throughput", "req")
    metrics.define_tagkv("to", ["data.olap.qap"])
    metrics.define_tagkv("method", ["parse",
                                    "validate",
                                    "explain"])
    metrics.define_tagkv("from_cluster", ["default"])
    metrics.define_tagkv("to_cluster", ["default"])

init_metrics()

logging.getLogger("urllib3").setLevel(logging.WARNING)
logger = logging.getLogger("qap")
logger.setLevel(logging.WARNING)

TIMEOUT = 2

QAP_STAGES = [
    "parse",
    "validate",
    "explain"
]

CLUSTER_TO_QAP = {
    "PROD": "data.olap.qap.service.lf",
    "TEST": "data.olap.qap_test.service.lf",
    "TOB": "qap-tob",
    "i18n": "data.olap.qap_i18n.service.maliva"
}

class QAPClient(object):

    def __init__(self, max_retries=1, conn_timeout=TIMEOUT, cluster="PROD"):
        self.max_retries = max_retries
        self._conn_timeout = conn_timeout
        self.cluster = cluster
        self.lock = threading.Lock()

    def _get_url(self, service):
        server_list = translate_one(service)
        host_port = random.choice(server_list)
        return "http://%s:%s/" % (host_port[0], host_port[1])

    def _get_response(self, lookup):
        with self.lock:
            retries = self.max_retries
            while retries >= 0:
                try:
                    retries -= 1
                    headers = {'Content-Type': 'application/json'}
                    lookup = ujson.dumps(lookup)
                    url = self._get_url(CLUSTER_TO_QAP.get(self.cluster, "data.olap.qap"))
                    r = requests.post(url, data=lookup, headers=headers, timeout=self._conn_timeout)
                    content = r.content
                    assert r.status_code == 200, "http status(%d) != 200 : %s" % (
                        r.status_code, content
                    )
                    return content
                except Exception as e:
                    logger.warn("%s %s", url, e)
    def _process_hql(self, lookup):
        start = time.time()
        response = self._get_response(lookup)
        end = time.time()
        latency = (end - start) * 1000000
        if response:
            res = ujson.loads(response)
            metrics.emit_timer("call.success.latency.us", latency, tagkv={ "to": "data.olap.qap",
                                                                           "method": lookup['stage'],
                                                                           "from_cluster": "default",
                                                                           "to_cluster": "default"},
                               prefix=CONF.get('metrics_namespace_prefix'))
            metrics.emit_counter("call.success.throughput", 1, tagkv={"to": "data.olap.qap",
                                                                      "method": lookup['stage'],
                                                                      "from_cluster": "default",
                                                                      "to_cluster": "default"},
                                 prefix=CONF.get('metrics_namespace_prefix'))
            if 'error' in res:
                return (False, res['error'] )
            else:
                return (True, None)
        else:
            metrics.emit_timer("call.error.latency.us", latency, tagkv={ "to": "data.olap.qap",
                                                                         "method": lookup['stage'],
                                                                         "from_cluster": "default",
                                                                         "to_cluster": "default"},
                               prefix=CONF.get('metrics_namespace_prefix'))
            metrics.emit_counter("call.error.throughput", 1, tagkv={"to": "data.olap.qap",
                                                                    "method": lookup['stage'],
                                                                    "from_cluster": "default",
                                                                    "to_cluster": "default"},
                                 prefix=CONF.get('metrics_namespace_prefix'))

            return (False, 'HTTP Request Error!')

    def parse_hql(self, query, user='tiger', engine='hive'):
        lookup = {'username': user,
                  'engine': engine,
                  'format': 'json',
                  'stage': 'parse',
                  'query': query
        }

        return self._process_hql(lookup)

    def validate_hql(self, query, user='tiger', engine='hive'):
        lookup = {'username': user,
                  'engine': engine,
                  'format': 'json',
                  'stage': 'validate',
                  'query': query
        }

        return self._process_hql(lookup)

    def explain_hql(self, query, user='tiger', engine='hive'):
        lookup = {'username': user,
                  'engine': engine,
                  'format': 'json',
                  'stage': 'explain',
                  'query': query
        }

        return self._process_hql(lookup)

    def analysis_hql_cost(self, query, user='tiger', engine='hive'):
        lookup = {'username': user,
                  'engine': engine,
                  'format': 'json',
                  'stage': 'validate',
                  'query': query
        }
        response = self._get_response(lookup)
        if response:
            res = ujson.loads(response)
            cost_rows = res.get('plan_cost_rows', 0)
            cost_cpu = res.get('plan_cost_cpu', 0)
            cost_io = res.get('plan_cost_io', 0)
            cost_memory = res.get('plan_cost_memory', 0)
            return (cost_rows, cost_cpu, cost_io, cost_memory)

    def predict_hql_cost(self, query, user='tiger', engine='hive'):
        lookup = {'username': user,
                  'engine': engine,
                  'format': 'json',
                  'stage': 'validate',
                  'query': query
        }
        response = self._get_response(lookup)
        if response:
            res = ujson.loads(response)
            cost_rows = res.get('plan_cost_rows', 0)
            cost_cpu = res.get('plan_cost_cpu', 0)
            cost_io = res.get('plan_cost_io', 0)
            cost_memory = res.get('plan_cost_memory', 0)
            hive_cost_predict = res.get('plan_hive_cost_predict', 0)
            spark_cost_predict = res.get('plan_spark_cost_predict', 0)
            return {'plan_cost_rows': cost_rows,
                    'plan_cost_cpu': cost_cpu,
                    'plan_cost_io': cost_io,
                    'plan_cost_memory': cost_memory,
                    'plan_hive_cost_predict': hive_cost_predict,
                    'plan_spark_cost_predict': spark_cost_predict
            }

if __name__ == "__main__":
    client = QAPClient(cluster="PROD")
    sql = "select app_name, activation_channel, channel_account_subclass,os,delay,device_id,is_spam_now from test.dws_spam_device where dt = '20170214' and activation_channel ='weixin_video_cjhongbao' limit 3000000"
    print 'parse sql: %s' % sql
    print client.parse_hql(sql)

    print '=' * 10

    print 'validate sql: %s' % sql
    print client.validate_hql(sql)

    print '=' * 10

    print 'explain sql: %s' % sql
    print client.explain_hql(sql)

    print '=' * 10
    print 'analysis sql: %s' % sql
    print client.analysis_hql_cost(sql)

    print '=' * 10
    print 'predict sql: %s' % sql
    print client.predict_hql_cost(sql)
