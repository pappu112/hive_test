# coding: utf-8

import os
import re
import getpass
import logging
import operator
import socket
import random
import time
import requests
from datetime import datetime
from decimal import Decimal
from collections import OrderedDict
from itertools import imap

import raven

from pyutil.program.conf2 import Conf
from pyutil.program import metrics2 as metrics
from pyutil.consul.bridge import translate_one

from .TCLIService import TCLIService
from .TCLIService.ttypes import (TProtocolVersion, TStatusCode, TOpenSessionReq, TCloseSessionReq, TExecuteStatementReq,
                                 TFetchResultsReq, TGetResultSetMetadataReq, TFetchOrientation, TGetOperationStatusReq,
                                 TOperationState, TCancelOperationReq, TCloseOperationReq, TRowSet, TTypeId, TGetLogReq,
                                 TGetInfoReq, TGetInfoType)
from .python_util import parse_timestamp
from .thrift_util import get_client
from .cursor import HiveServer2Cursor
from .error import HiveServer2Exception
from thrift.transport.TTransport import TTransportException
from .exceptions import StructuredException

_TTypeId_to_TColumnValue_getters = {
    'BOOLEAN': operator.attrgetter('boolVal'),
    'TINYINT': operator.attrgetter('byteVal'),
    'SMALLINT': operator.attrgetter('i16Val'),
    'INT': operator.attrgetter('i32Val'),
    'BIGINT': operator.attrgetter('i64Val'),
    'TIMESTAMP': operator.attrgetter('stringVal'),
    'DATE': operator.attrgetter('stringVal'),
    'FLOAT': operator.attrgetter('doubleVal'),
    'DOUBLE': operator.attrgetter('doubleVal'),
    'STRING': operator.attrgetter('stringVal'),
    'DECIMAL': operator.attrgetter('stringVal'),
    'BINARY': operator.attrgetter('binaryVal'),
    'VARCHAR': operator.attrgetter('stringVal'),
    'CHAR': operator.attrgetter('stringVal'),
    'MAP': operator.attrgetter('stringVal'),
    'ARRAY': operator.attrgetter('stringVal'),
    'STRUCT': operator.attrgetter('stringVal'),
    'UNION': operator.attrgetter('stringVal'),
    'NULL': operator.attrgetter('stringVal'),
    'INTERVALYEARMONTH': operator.attrgetter('stringVal'),
    'INTERVALDAYTIME': operator.attrgetter('stringVal')
}

PRE_COLUMNAR_PROTOCOLS = {
    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V1,
    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V2,
    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V3,
    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V4,
    TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V5,
}

LOG = logging.getLogger('hiveserver2')

DEFAULT_TIMEOUT = 90
DEFAULT_QUERYCACHE_ROWS = 5000
DEFAULT_QUERY_TIMEOUT = 0
DEFAULT_CONNECT_RETRIES = 5
DEFAULT_BUFFER_SIZE = 32

CLUSTER2SERVICE = {
    "default": "data.olap.hiveserver2-default.service.lf",
    "haruna_default": "data.olap.hiveserver2-default.service.lf",
    "haruna_nearline": "data.olap.hiveserver2-default.service.lf",
    "haruna_report": "data.olap.hiveserver2-default.service.lf",
    "haruna_noauth": "data.olap.hiveserver2-default.service.lf",
    "haruna_qe": "data.olap.hiveserver2-qe.service.lf",
    "haruna_test": "hiveserver2-test.service.lf",
    "haruna_branch2": "hiveserver2-branch2.service.lf",
    "haruna_spark": "data.olap.hiveserver2_spark_root_data_etl.service.lf",
    "haruna_priest": "hiveserver2-priest.service.lf",
    "haruna_watcher": "hiveserver2-watcher.service.lf",
    "haruna_tob": "data.olap.hiveserver2-tob.service.lf",
    "haruna_ad": "data.olap.hiveserver2-ad.service.lf",
    "haruna_i18n_ad": "data.olap.hiveserver2-i18n-ad.service.maliva",
    "i18n_default": "data.olap.hiveserver2-i18n-default.service.maliva",
    "i18n_qe": "data.olap.hiveserver2-i18n.service.maliva",
    "i18n_qe_ali": "data.olap.hiveserver2-i18n_ali.service.maliva",
    "i18n_priest": "data.olap.hiveserver2-i18n-default.service.maliva"
}

CONF = Conf(os.path.join(os.path.dirname(__file__), 'hiveserver2.conf'))
def init_metrics():
    metrics.init(CONF)
    metrics.define_timer("call.success.latency.us", "us")
    metrics.define_timer("call.error.latency.us", "us")
    metrics.define_counter("call.success.throughput", "req")
    metrics.define_counter("call.error.throughput", "req")
    metrics.define_tagkv("to", ["data.olap.hs2"])
    metrics.define_tagkv("method", ["OpenSession",
                                    "CloseSession",
                                    "GetInfo",
                                    "ExecuteStatement",
                                    "GetTypeInfo",
                                    "GetCatalogs",
                                    "GetSchemas",
                                    "GetTables",
                                    "GetTableTypes",
                                    "GetColumns",
                                    "GetFunctions",
                                    "GetOperationStatus",
                                    "CancelOperation",
                                    "CloseOperation",
                                    "GetResultSetMetadata",
                                    "FetchResults",
                                    "GetDelegationToken",
                                    "CancelDelegationToken",
                                    "RenewDelegationToken",
                                    "GetLog"])
    metrics.define_tagkv("from_cluster", ["default"])
    metrics.define_tagkv("to_cluster", ["default",
                                        "haruna_default",
                                        "haruna_noauth",
                                        "haruna_nearline",
                                        "haruna_report",
                                        "haruna_qe",
                                        "haruna_test",
                                        "haruna_branch2",
                                        "haruna_spark",
                                        "haruna_priest",
                                        "haruna_watcher",
                                        "haruna_tob",
                                        "haruna_ad",
                                        "haruna_i18n_ad",
                                        "i18n_default",
                                        "i18n_qe",
                                        "i18n_qe_ali",
                                        "i18n_priest",
                                        ])


init_metrics()

def detect_socket_failure(host, port):
    try:
        sc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sc.connect((str(host), int(port)))
        sc.close()
        return True
    except Exception as ex:
        LOG.warning('fail to detect socket for host:%s and port: %s' % (host, port))
        return False

def connect(service_name, host='', port=None, cluster='default', consul_service='hiveserver2', timeout=DEFAULT_TIMEOUT, default_db=None, username=None, password=None,
            impersonation_enabled=False, use_sasl=True, use_ldap=False, use_kerberos=False, principal=None,
            querycache_rows=DEFAULT_QUERYCACHE_ROWS, query_timeout=DEFAULT_QUERY_TIMEOUT, connect_retries=DEFAULT_CONNECT_RETRIES, buffer_size=DEFAULT_BUFFER_SIZE):
    if not host:
        for i in xrange(5):
            try:
                translate_one(CLUSTER2SERVICE.get(cluster, consul_service))
            except Exception:
                raise
        server_list = translate_one(CLUSTER2SERVICE.get(cluster, consul_service))
        if not server_list or len(server_list) == 0:
            raise HiveServer2Exception("no server found for {}".format(cluster))
        # failure detection
        retries = connect_retries
        success = False

        while not success and retries > 0:
            server = random.choice(server_list)
            host, port = server[0], server[1]
            if detect_socket_failure(host, port):
                try:
                    hive_client = HiveServer2Client(service_name, host, int(port), timeout, default_db, username,
                                                    password, impersonation_enabled, use_sasl, use_ldap, use_kerberos, principal,
                                                    querycache_rows, query_timeout, cluster=cluster, buffer_size=buffer_size)
                    success = True
                except StructuredException:
                    LOG.warning('fail to create thrift connection with HiveServer2 host and port: %s:%s' % (host, port))

            retries -= 1
    LOG.info('HiveServer2 host and port: %s:%s' % (host, port))
    if not host or not port:
        raise HiveServer2Exception("invalid {} server: {}:{}".format(service_name, host, port))

    if 'hive_client' not in dir():
        try:
            hive_client = HiveServer2Client(service_name, host, int(port), timeout, default_db, username,
                                            password, impersonation_enabled, use_sasl, use_ldap, use_kerberos,
                                            principal, querycache_rows, query_timeout, cluster=cluster)
        except StructuredException:
            raise HiveServer2Exception("server is not available")
    return hive_client

def connect_to_hive(host='', port=None, cluster='default', timeout=DEFAULT_TIMEOUT, default_db=None, username=None, password=None,
                    impersonation_enabled=False, use_sasl=True, use_ldap=False, use_kerberos=False, principal=None,
                    querycache_rows=DEFAULT_QUERYCACHE_ROWS, query_timeout=DEFAULT_QUERY_TIMEOUT):
    return connect('hive', host, port, cluster, timeout, default_db, username, password,
                   impersonation_enabled, use_sasl, use_ldap, use_kerberos, principal,
                   querycache_rows, query_timeout)


def connect_to_impala(host='', port=None, cluster='default', timeout=DEFAULT_TIMEOUT, default_db=None, username=None, password=None,
                     impersonation_enabled=False, use_sasl=True, use_ldap=False, use_kerberos=False, principal=None,
                     querycache_rows=DEFAULT_QUERYCACHE_ROWS, query_timeout=DEFAULT_QUERY_TIMEOUT):
    return connect('impala', host, port, cluster, timeout, default_db, username, password,
                   impersonation_enabled, use_sasl, use_ldap, use_kerberos, principal,
                   querycache_rows, query_timeout)


class HiveServer2Client(object):
    HS2_SERVICES = ('hive', 'impala')
    HS2_MECHANISMS = {'KERBEROS': 'GSSAPI', 'NONE': 'PLAIN', 'NOSASL': 'NOSASL', 'LDAP': 'PLAIN'}

    def __init__(self, service_name, host, port, timeout=45, default_db=None, username=None, password=None,
                 impersonation_enabled=False, use_sasl=True, use_ldap=False, use_kerberos=False, principal=None,
                 querycache_rows=5000, query_timeout=0, cluster="haruna_default", buffer_size=DEFAULT_BUFFER_SIZE):
        if service_name not in HiveServer2Client.HS2_SERVICES:
            raise Exception('{} service not supported. Valid are {}.'.format(service_name, HiveServer2Client.HS2_SERVICES))
        self.service_name = service_name
        self.hs2_protocol_version = {
            'hive': TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6,
            'impala': TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6
        }[service_name]

        self.host = host
        self.port = port
        self.timeout = timeout
        self.default_db = default_db
        if username is None:
            username = getpass.getuser()
        self.username = username
        self.password = password
        self.impersonation_enabled = impersonation_enabled
        self.use_sasl = use_sasl
        self.use_ldap = use_ldap
        self.use_kerberos = use_kerberos
        self.principal = principal
        self.querycache_rows = querycache_rows
        self.query_timeout = query_timeout
        self.cluster = cluster
        self.buffer_size = buffer_size

        self._session = None

        use_sasl, mechanism, kerberos_principal_short_name = self.get_security()
        self._client = get_client(TCLIService.Client,
                                  self.host,
                                  self.port,
                                  service_name=service_name,
                                  kerberos_principal=kerberos_principal_short_name,
                                  use_sasl=use_sasl,
                                  mechanism=mechanism,
                                  username=username,
                                  password=password,
                                  timeout_seconds=timeout,
                                  use_ssl=False,
                                  )
        self.open_session()
        self._server_info = None

    @property
    def server_info(self):
        if not self._server_info:
            self._server_info = self.get_info()
        return self._server_info

    @property
    def session(self):
        return self._session

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        try:
            self.close_session()
        except Exception:
            LOG.warning('fail to close session before exit')

    def get_security(self):
        principal = self.principal

        if principal:
            kerberos_principal_short_name = principal.split('/', 1)[0]
        else:
            kerberos_principal_short_name = None

        if self.service_name == 'impala':
            if self.use_ldap:
                use_sasl = True
                mechanism = HiveServer2Client.HS2_MECHANISMS['NONE']
            else:
                use_sasl = False
                mechanism = HiveServer2Client.HS2_MECHANISMS['KERBEROS']
        else:
            if self.use_sasl:
                if self.use_kerberos:
                    mechanism = HiveServer2Client.HS2_MECHANISMS['KERBEROS']
                elif self.use_ldap:
                    mechanism = HiveServer2Client.HS2_MECHANISMS['LDAP']
                else:
                    mechanism = HiveServer2Client.HS2_MECHANISMS['NONE']
                use_sasl = True
            else:
                use_sasl = False
                mechanism = HiveServer2Client.HS2_MECHANISMS['NOSASL']

        return use_sasl, mechanism, kerberos_principal_short_name

    def cursor(self, operation_handle=None, operation_string=None):
        cursor = HiveServer2Cursor(self, None, operation_handle, operation_string, buffer_size=self.buffer_size)
        if self.default_db:
            cursor.execute("USE {}".format(self.default_db))
        return cursor

    def open_session(self):
        kwargs = {
            'client_protocol': self.hs2_protocol_version,
            'username': self.username,
            'configuration': {},
        }
        if self.service_name == 'hive':
            kwargs['configuration'].update({'hive.server2.proxy.user': self.username})
        elif self.service_name == 'impala':
            if self.impersonation_enabled:
                kwargs['configuration'].update({'impala.doas.user': self.username})
        req = TOpenSessionReq(**kwargs)
        start = time.time()
        try:
            res = self._client.OpenSession(req)
            end = time.time()
            metrics.emit_timer("call.success.latency.us", (end - start) * 1000000, tagkv={"to": "data.olap.hs2",
                                                                                          "method": "OpenSession",
                                                                                          "from_cluster": "default",
                                                                                          "to_cluster": self.cluster},
                               prefix=CONF.get('metrics_namespace_prefix'))
            metrics.emit_counter("call.success.throughput", 1, tagkv={"to": "data.olap.hs2",
                                                                      "method": "OpenSession",
                                                                      "from_cluster": "default",
                                                                      "to_cluster": self.cluster},
                                 prefix=CONF.get('metrics_namespace_prefix'))

            if res.status is not None and res.status.statusCode not in (TStatusCode.SUCCESS_STATUS,):
                if hasattr(res.status, 'errorMessage') and res.status.errorMessage:
                    message = res.status.errorMessage
                else:
                    message = ''
                raise HiveServer2Exception(Exception('Bad status for request %s:\n%s' % (req, res)), message=message)
            self._session = res.sessionHandle
        except Exception:
            end = time.time()
            metrics.emit_timer("call.error.latency.us", (end - start) * 1000000, tagkv={"to": "data.olap.hs2",
                                                                                        "method": "OpenSession",
                                                                                        "from_cluster": "default",
                                                                                        "to_cluster": self.cluster},
                               prefix=CONF.get('metrics_namespace_prefix'))
            metrics.emit_counter("call.error.throughput", 1, tagkv={"to": "data.olap.hs2",
                                                                    "method": "OpenSession",
                                                                    "from_cluster": "default",
                                                                    "to_cluster": self.cluster},
                                 prefix=CONF.get('metrics_namespace_prefix'))
            raise

    def close_session(self):
        if self._session:
            req = TCloseSessionReq(sessionHandle=self._session)
            start = time.time()
            try:
                self._client.CloseSession(req)
                end = time.time()
                metrics.emit_timer("call.success.latency.us", (end - start) * 1000000, tagkv={"to": "data.olap.hs2",
                                                                                              "method": "CloseSession",
                                                                                              "from_cluster": "default",
                                                                                              "to_cluster": self.cluster},
                                   prefix=CONF.get('metrics_namespace_prefix'))
                metrics.emit_counter("call.success.throughput", 1, tagkv={"to": "data.olap.hs2",
                                                                          "method": "CloseSession",
                                                                          "from_cluster": "default",
                                                                          "to_cluster": self.cluster},
                                     prefix=CONF.get('metrics_namespace_prefix'))

            except Exception:
                end = time.time()
                metrics.emit_timer("call.error.latency.us", (end - start) * 1000000, tagkv={"to": "data.olap.hs2",
                                                                                            "method": "CloseSession",
                                                                                            "from_cluster": "default",
                                                                                            "to_cluster": self.cluster},
                                   prefix=CONF.get('metrics_namespace_prefix'))
                metrics.emit_counter("call.error.throughput", 1, tagkv={"to": "data.olap.hs2",
                                                                        "method": "CloseSession",
                                                                        "from_cluster": "default",
                                                                        "to_cluster": self.cluster},
                                     prefix=CONF.get('metrics_namespace_prefix'))
                raise

    def close(self):
        self.close_session()

    def call(self, obj, attr, req, status=TStatusCode.SUCCESS_STATUS):
        if self._session is None:
            self.open_session()

        if hasattr(req, 'sessionHandle') and req.sessionHandle is None:
            req.sessionHandle = self._session
        start = time.time()
        try:
            res = getattr(obj, attr)(req)
            end = time.time()
            metrics.emit_timer("call.success.latency.us", (end - start) * 1000000, tagkv={"to": "data.olap.hs2",
                                                                                          "method": attr,
                                                                                          "from_cluster": "default",
                                                                                          "to_cluster": self.cluster},
                               prefix=CONF.get('metrics_namespace_prefix'))
            metrics.emit_counter("call.success.throughput", 1, tagkv={"to": "data.olap.hs2",
                                                                      "method": attr,
                                                                      "from_cluster": "default",
                                                                      "to_cluster": self.cluster},
                                 prefix=CONF.get('metrics_namespace_prefix'))
        except Exception:
            end = time.time()
            metrics.emit_timer("call.error.latency.us", (end - start) * 1000000, tagkv={"to": "data.olap.hs2",
                                                                                        "method": attr,
                                                                                        "from_cluster": "default",
                                                                                        "to_cluster": self.cluster},
                               prefix=CONF.get('metrics_namespace_prefix'))
            metrics.emit_counter("call.error.throughput", 1, tagkv={"to": "data.olap.hs2",
                                                                    "method": attr,
                                                                    "from_cluster": "default",
                                                                    "to_cluster": self.cluster},
                                 prefix=CONF.get('metrics_namespace_prefix'))
            raise

        if res.status.statusCode == TStatusCode.INVALID_HANDLE_STATUS or (
                res.status.statusCode == TStatusCode.ERROR_STATUS and \
                re.search('Invalid SessionHandle|Invalid session|Client session expired', res.status.errorMessage or '', re.I)):
            LOG.info('Retrying with a new session because for %s of %s' % (self.username, res))

            self.open_session()
            req.sessionHandle = self._session

            # Get back the name of the function to call
            res = getattr(obj, attr)(req)

        if status is not None and res.status.statusCode not in (
            TStatusCode.SUCCESS_STATUS, TStatusCode.SUCCESS_WITH_INFO_STATUS, TStatusCode.STILL_EXECUTING_STATUS):
            if hasattr(res.status, 'errorMessage') and res.status.errorMessage:
                message = res.status.errorMessage
            else:
                message = ''
            raise HiveServer2Exception(Exception('Bad status for request %s:\n%s' % (req, res)), message=message)
        else:
            return res

    def execute_statement(self, statement, configuration=None, max_rows=1000):
        if not configuration:
            configuration = {}
        if self.service_name == 'impala' and self.query_timeout > 0:
            configuration['QUERY_TIMEOUT_S'] = str(self.query_timeout)

        req = TExecuteStatementReq(statement=statement.encode('utf-8'), confOverlay=configuration)
        res = self.call(self._client, 'ExecuteStatement', req)
        return self.fetch_result(res.operationHandle, max_rows=max_rows), res.operationHandle

    def execute_async_statement(self, statement, configuration=None):
        if not configuration:
            configuration = {}
        if self.service_name == 'impala':
            if self.query_timeout > 0:
                configuration['QUERY_TIMEOUT_S'] = str(self.query_timeout)
            if self.querycache_rows > 0:
                configuration['impala.resultset.cache.size'] = str(self.querycache_rows)

        req = TExecuteStatementReq(statement=statement.encode('utf-8'), confOverlay=configuration, runAsync=True)
        res = self.call(self._client, 'ExecuteStatement', req)
        return res.operationHandle

    def fetch_result(self, operation_handle, orientation=TFetchOrientation.FETCH_NEXT, schema=None, max_rows=1000):
        if not operation_handle.hasResultSet:
            return [], None

        fetch_req = TFetchResultsReq(operationHandle=operation_handle, orientation=orientation, maxRows=max_rows)
        resp = self.call(self._client, 'FetchResults', fetch_req)

        if schema is None:
            schema = self.get_result_schema(operation_handle)

        rows = []
        if self.hs2_protocol_version == TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6:
            tcols = [_TTypeId_to_TColumnValue_getters[schema[i]['type']](col)
                     for (i, col) in enumerate(resp.results.columns)]
            num_cols = len(tcols)
            num_rows = len(tcols[0].values)
            for i in xrange(num_rows):
                row = []
                for j in xrange(num_cols):
                    type_ = schema[j]['type']
                    values = tcols[j].values
                    nulls = tcols[j].nulls
                    if nulls == '' or nulls == '\x00' or nulls =='\x00\x00':
                    #if nulls == '' or re.match('^(\x00)+$', nulls): # HS2 has just \x00 or '', Impala can have \x00\x00...
                        is_null = False
                    else:
                        # i / 8 is the byte, i % 8 is position in the byte; get the int
                        # repr and pull out the bit at the corresponding pos
                        # HS2 can have just \x00\x01 instead of \x00\x01\x00...
                        idx = i / 8
                        if idx >= len(nulls):
                            is_null = False
                        else:
                            is_null = ord(nulls[idx]) & (1 << (i % 8))
                    if is_null:
                        value = None
                    elif type_ == 'TIMESTAMP':
                        value = parse_timestamp(values[i])
                    elif type_ == 'DECIMAL' and values[i]:
                        value = Decimal(values[i])
                    elif type_ in ('MAP', 'ARRAY', 'STRUCT', 'UNION') and values[i]:
                        def to_utf8(obj):
                            if isinstance(obj, dict):
                                rs = {}
                                for k, v in obj.iteritems():
                                    if isinstance(k, unicode):
                                        k = k.encode('utf-8')
                                    rs[k] = to_utf8(v)
                                return rs
                            elif isinstance(obj, list):
                                return [to_utf8(item) for item in obj]
                            elif isinstance(obj, unicode):
                                return obj.encode('utf-8')
                            return obj
                        value = to_utf8(values[i])
                    else:
                        value = values[i]
                    row.append(value)
                rows.append(tuple(row))
        elif self.hs2_protocol_version in PRE_COLUMNAR_PROTOCOLS:
            for row in resp.results.rows:
                row_data = []
                for i, col in enumerate(row.colVals):
                    type_ = schema[i]['type']
                    value = _TTypeId_to_TColumnValue_getters[type_](col).value
                    if type_ == 'TIMESTAMP':
                        value = parse_timestamp(value)
                    elif type_ == 'DECIMAL' and value:
                        value = Decimal(value)
                    row_data.append(value)
                rows.append(tuple(row_data))
        else:
            version_name = TProtocolVersion._VALUES_TO_NAMES[self.hs2_protocol_version]
            raise HiveServer2Exception(Exception("Got HiveServer2 version {}. Expected V1 - V6".format(version_name)))

        return rows, schema

    def fetch_log(self, operation_handle, orientation=TFetchOrientation.FETCH_NEXT, max_rows=1000):
        req = TFetchResultsReq(operationHandle=operation_handle, orientation=orientation, maxRows=max_rows, fetchType=1)
        res = self.call(self._client, 'FetchResults', req)

        if self.hs2_protocol_version >= TProtocolVersion.HIVE_CLI_SERVICE_PROTOCOL_V6:
            lines = res.results.columns[0].stringVal.values
        else:
            lines = imap(lambda r: r.colVals[0].stringVal.value, res.results.rows)
        return '\n'.join(lines)

    def get_state(self, operation_handle):
        req = TGetOperationStatusReq(operationHandle=operation_handle)
        resp = self.call(self._client, 'GetOperationStatus', req)
        return TOperationState._VALUES_TO_NAMES[resp.operationState]

    def get_operation_status(self, operation_handle):
        req = TGetOperationStatusReq(operationHandle=operation_handle)
        return self.call(self._client, 'GetOperationStatus', req)

    def cancel_operation(self, operation_handle):
        req = TCancelOperationReq(operationHandle=operation_handle)
        self.call(self._client, 'CancelOperation', req)

    def close_operation(self, operation_handle):
        req = TCloseOperationReq(operationHandle=operation_handle)
        self.call(self._client, 'CloseOperation', req)

    def get_result_schema(self, operation_handle):
        if operation_handle.hasResultSet:
            meta_req = TGetResultSetMetadataReq(operationHandle=operation_handle)
            resp = self.call(self._client, 'GetResultSetMetadata', meta_req)
        else:
            resp = None

        schema = []
        if resp:
            for column in resp.schema.columns:
                schema.append(self._get_column_desc(column))
        return schema

    def _get_column_desc(self, column):
        name = column.columnName
        for ttype in column.typeDesc.types:
            if ttype.primitiveEntry is not None:
                entry = column.typeDesc.types[0].primitiveEntry
                type_ = TTypeId._VALUES_TO_NAMES[entry.type].split('_')[0]
                if type_ == 'DECIMAL':
                    qualifiers = entry.typeQualifiers.qualifiers
                    precision = qualifiers['precision'].i32Value
                    scale = qualifiers['scale'].i32Value
                    return {'name': name, 'type': type_, 'precision': precision, 'scale': scale}
                else:
                    return {'name': name, 'type': type_}
            elif ttype.mapEntry is not None:
                return {'name': name, 'type': ttype.mapEntry}
            elif ttype.unionEntry is not None:
                return {'name': name, 'type': ttype.unionEntry}
            elif ttype.arrayEntry is not None:
                return {'name': name, 'type': ttype.arrayEntry}
            elif ttype.structEntry is not None:
                return {'name': name, 'type': ttype.structEntry}
            elif ttype.userDefinedTypeEntry is not None:
                return {'name': name, 'type': ttype.userDefinedTypeEntry}

    def get_info(self):
        info = {}
        req = TGetInfoReq(infoType=TGetInfoType.CLI_DBMS_VER)
        res = self.call(self._client, 'GetInfo', req)
        info['dbms_ver'] = res.infoValue.stringValue
        return info

    def get_log(self, operation_handle, start_over=True):
        if self.service_name == 'impala' or self.server_info['dbms_ver'] < '0.14':
            req = TGetLogReq(operationHandle=operation_handle)
            res = self.call(self._client, 'GetLog', req)
            return res.log
        else:
            if start_over:
                orientation = TFetchOrientation.FETCH_FIRST
            else:
                orientation = TFetchOrientation.FETCH_NEXT
            return self.fetch_log(operation_handle, orientation=orientation)

    def get_partitions(self, table_name, database_name=None, max_parts=None, with_meta=False):
        if database_name:
            qualified_table_name = "{}.{}".format(database_name, table_name)
        else:
            qualified_table_name = table_name
        (rows, schema), operation_handle = self.execute_statement("SHOW PARTITIONS {}".format(qualified_table_name))
        self.close_operation(operation_handle)
        if self.service_name == 'impala':
            column_names = [column['name'] for column in schema[:-6]]
            partitions = [OrderedDict(zip(column_names, row[:-5])) for row in rows[:-1]]
        else:
            partitions = []
            for row in rows:
                partition = OrderedDict([column.split('=') for column in row[0].split('/')])
                partitions.append(partition)
        if max_parts:
            partitions = partitions[-max_parts:]

        if with_meta:
            from .hdfs import get_hdfs_client, parse_hdfs_uri, get_path_meta
            location = self.get_table_location(table_name, database_name)
            if location:
                fs_name, table_path = parse_hdfs_uri(location)
                hdfs_client = get_hdfs_client(fs_name)
                results = []
                paths = []
                for partition in partitions:
                    spec = '/'.join(["{}={}".format(k, v) for k, v in sorted(partition.iteritems())])
                    paths.append(os.path.join(table_path, spec))
                metas = get_path_meta(hdfs_client, paths)
                return zip(partitions, metas)
            return [(partition, {}) for partition in partitions]
        else:
            return partitions

    def get_table_schema(self, table_name, database_name='default'):
        rows = self._get_table_extended_describe(table_name, database_name)
        columns = []
        for row in rows:
            col_name, col_type, comment = row
            if col_name in ('', '# Partition Information', 'Detailed Table Information'):
                break
            columns.append({'name': col_name, 'type': col_type, 'comment': comment})
        return columns

    def get_table_location(self, table_name, database_name='default'):
        rows = self._get_table_extended_describe(table_name, database_name)
        for row in rows:
            col_name, describe = row[:2]
            if col_name == 'Detailed Table Information':
                match = re.search('location:([^,]+)', describe)
                if match is not None:
                    match = match.group(1)
                    return match
        return ''

    def get_table_createtime(self, table_name, database_name='default'):
        if self.service_name == "impala":
            return 0
        rows = self._get_table_extended_describe(table_name, database_name)
        pat = re.compile("createTime:(\d+)")
        for row in rows:
            match = re.findall(pat, str(row))
            if match:
                return match[0]
        return 0

    def _get_table_extended_describe(self, table_name, database_name):
        if database_name:
            table_name = "{}.{}".format(database_name, table_name)
        if self.service_name == 'impala':
            query = "DESCRIBE {}".format(table_name)
        else:
            query = "DESCRIBE EXTENDED {}".format(table_name)
        (rows, schema), operation_handle = self.execute_statement(query)
        self.close_operation(operation_handle)
        return rows
