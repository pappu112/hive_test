#!/bin/env python
# -*- encoding: utf-8 -*-

import os
import json
import sys
p = (os.path.dirname((os.path.abspath(__file__))))
if p not in sys.path:
    sys.path.append(p)
p = (os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if p not in sys.path:
    sys.path.append(p)
from hive_metastore2.ThriftHiveMetastore import Iface
from pyutil.consul.bridge import translate_one
from token_user_reader import USER_NO_AUTH
from token_user_reader import get_user_name_from_token
import requests
import random


# so we can do with different cluster toutiao(china),or i18n
env_dist = os.environ # environ是在os.py中定义的一个dict environ = {}

is_public_service = False
if env_dist.has_key('PUBLIC_SERVICE'):
    if env_dist['PUBLIC_SERVICE'] == 'true':
        is_public_service = True
    else:
        is_public_service = False

use_token_GDPR = True
if env_dist.has_key('USE_TOKEN_GDPR'):
    if env_dist['USE_TOKEN_GDPR'] == 'false':
        use_token_GDPR = False
    else:
        use_token_GDPR = True

debug_user_name=None
if env_dist.has_key('DEBUG_USER_NAME'):
    debug_user_name = env_dist['DEBUG_USER_NAME']

EE_WHITELIST = ["tiger", "root"]
Region = {"cn": "china","va": "i18n"}
GeminiPsm = {
    "cn": "data.olap.gemini-server.service.lf",
    "va": "data.olap.gemini-server-i18n.service.maliva"
}
GEMINI_URL_USER='/api/query/{}/verifyUserPri/'
GEMINI_URL_PSM='/api/query/{}/verifyPsmPri/'
HTTP_TIMEOUT=20
HTTP_RETRY=2

TAKE_EFFECT_REGION=['i18n']



def get_host_port_from_psm(psm=GeminiPsm['cn']):
    server_list = translate_one(psm)
    if not server_list or len(server_list) == 0:
        raise Exception("no server found for {}".format(psm))
    server = random.choice(server_list)
    return server[0], server[1]

def get_real_user_or_psm_name():
    ## get the real name from token
    if debug_user_name is None:
        return get_user_name_from_token()
    else:
        return debug_user_name


def __is_psm_name(user_name):
    if user_name is not None and user_name.startswith("psm_"):
        return True
    else:
        return False


def check_hive_privilege(database="", table="", type='SELECT',region=Region['cn'], username = 'not_set'):
    if is_public_service:
        return
    if region not in TAKE_EFFECT_REGION:
        return
    if username in EE_WHITELIST:
        return
    if not use_token_GDPR:
        return
    check_sentry_privilege(username, database, table, type, region)
    return


def check_sentry_privilege(username, database, table, type, region):
    print 'check sentry privilege ', username, database, table, type, region
    is_psm = __is_psm_name(username)
    if is_psm:
        data = {
            "psmname": username,
            "priType": type,
            "database": database,
            "table": table
        }
        URL = GEMINI_URL_PSM
    else:
        data = {
            "username": username,
            "priType": type,
            "database": database,
            "table": table
        }
        URL = GEMINI_URL_USER

    if region == Region['cn']:
        psm = GeminiPsm['cn']
    else:
        psm = GeminiPsm['va']
    retry = 0
    result = None
    while retry < HTTP_RETRY:
        try:
            host, port = get_host_port_from_psm(psm)
            headers = {'Content-type': 'application/json'}
            r = requests.post(
                url="http://" + host + ":" + str(port) + URL.format(region),
                data=json.dumps(data),
                headers=headers,
                timeout=HTTP_TIMEOUT)
            result = r.json()
            break
        except requests.exceptions.Timeout:
            print "WARN: timeout retry:" + str(retry + 1) + "/" + str(HTTP_RETRY)
        except Exception, err:
            print "WARN:" + err.message
        retry = retry + 1
    if result is None:
        return
    if result.has_key("result"):
        data = result["result"]
        if data.has_key("code") and data.has_key("message") and data["code"] == 1:
            if data['message'] == 'true':
                return
            else:
                raise Exception("username {} do not has {} privilege for {}.{}".format(username, type, database, table))


class HiveThriftPrivilegeClient(Iface):
    def __init__(self, thriftClient, region='cn', token="not_set", user="not_set"):
        self.thriftClient = thriftClient
        self.region = region
        self.token = token
        self.user = user
        token_pass = self.set_token()
        print "set token ", token_pass
        if token_pass == False:
            raise Exception("invalidate token, contact hive administrator")

    def set_token(self):
        return self.thriftClient.set_token(self.token)

    def create_database(self, database):
        raise Exception("Create database in client is not allowed, please contact to hive administrator;")

    def get_database(self, name):
        check_hive_privilege(database=name, type='SELECT',region=self.region, username=self.user)
        return self.thriftClient.get_database(name)

    def drop_database(self, name, deleteData, cascade):
        raise Exception("Drop database in client is not allowed, please contact to hive administrator;")

    def get_databases(self, pattern):
        return self.thriftClient.get_databases(pattern)

    def get_all_databases(self, ):
        return self.thriftClient.get_all_databases()

    def alter_database(self, dbname, db):
        check_hive_privilege(database=dbname, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.alter_database(dbname, db)

    def get_type(self, name):
        return self.thriftClient.get_type(name)

    def create_type(self, type):
        return self.thriftClient.create_type(type)

    def drop_type(self, type):
        return self.thriftClient.drop_type(type)

    def get_type_all(self, name):
        return self.thriftClient.get_type_all(name)

    def get_fields(self, db_name, table_name):
        check_hive_privilege(database=db_name, table=table_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_fields(db_name, table_name)

    def get_schema(self, db_name, table_name):
        check_hive_privilege(database=db_name, table=table_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_schema(db_name, table_name)

    def create_table(self, tbl):
        pass

    def create_table_with_environment_context(self, tbl, environment_context):
      """
      Parameters:
       - tbl
       - environment_context
      """
      pass

    def drop_table(self, dbname, name, deleteData):
        check_hive_privilege(database=dbname, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.drop_table(dbname, name, deleteData)

    def drop_table_with_environment_context(self, dbname, name, deleteData, environment_context):
        check_hive_privilege(database=dbname, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.drop_table_with_environment_context(dbname, name, deleteData,environment_context)

    def get_tables(self, db_name, pattern):
        check_hive_privilege(database=db_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_tables(db_name, pattern)

    def get_all_tables(self, db_name):
        check_hive_privilege(database=db_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_all_tables(db_name)

    def get_table(self, dbname, tbl_name):
        check_hive_privilege(database=dbname, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_table(dbname, tbl_name)

    def get_table_objects_by_name(self, dbname, tbl_names):
        check_hive_privilege(database=dbname, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_table_objects_by_name(dbname, tbl_names)

    def get_table_names_by_filter(self, dbname, filter, max_tables):
        check_hive_privilege(database=dbname, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_table_names_by_filter(dbname, filter, max_tables)

    def alter_table(self, dbname, tbl_name, new_tbl):
        check_hive_privilege(database=dbname, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.alter_table(dbname, tbl_name, new_tbl)

    def alter_table_with_environment_context(self, dbname, tbl_name, new_tbl, environment_context):
        check_hive_privilege(database=dbname, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.alter_table_with_environment_context(dbname, tbl_name, new_tbl, environment_context)

    def add_partition(self, new_part):
        table_name = new_part.tableName
        db_name = new_part.dbName
        check_hive_privilege(database=db_name, table=table_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.add_partition(new_part)

    def add_partition_with_environment_context(self, new_part, environment_context):
        table_name = new_part.tableName
        db_name = new_part.dbName
        check_hive_privilege(database=db_name, table=table_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.add_partition_with_environment_context(new_part, environment_context)

    def add_partitions(self, new_parts):
        for new_part in new_parts:
            table_name = new_part.tableName
            db_name = new_part.dbName
            check_hive_privilege(database=db_name, table=table_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.add_partitions(new_parts)

    def append_partition(self, db_name, tbl_name, part_vals):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.append_partition(db_name, tbl_name, part_vals)

    def add_partitions_req(self, request):
        return self.thriftClient.add_partitions_req(request)

    def append_partition_with_environment_context(self, db_name, tbl_name, part_vals, environment_context):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.append_partition_with_environment_context(db_name, tbl_name, part_vals, environment_context)

    def append_partition_by_name(self, db_name, tbl_name, part_name):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.append_partition_by_name(db_name, tbl_name, part_name)

    def append_partition_by_name_with_environment_context(self, db_name, tbl_name, part_name, environment_context):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.append_partition_by_name_with_environment_context(db_name, tbl_name, part_name,
                                                                                   environment_context)

    def drop_partition(self, db_name, tbl_name, part_vals, deleteData):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.drop_partition(db_name, tbl_name, part_vals, deleteData)

    def drop_partition_with_environment_context(self, db_name, tbl_name, part_vals, deleteData, environment_context):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.drop_partition_with_environment_context(db_name, tbl_name, part_vals, deleteData,
                                                                         environment_context)

    def drop_partition_by_name(self, db_name, tbl_name, part_name, deleteData):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.drop_partition_by_name(db_name, tbl_name, part_name, deleteData)

    def drop_partition_by_name_with_environment_context(self, db_name, tbl_name, part_name, deleteData,
                                                        environment_context):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.drop_partition_by_name_with_environment_context(db_name, tbl_name, part_name,
                                                                                 deleteData, environment_context)

    def drop_partitions_req(self, req):
        return self.thriftClient.drop_partitions_req(req)

    def get_partition(self, db_name, tbl_name, part_vals):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_partition(db_name, tbl_name, part_vals)

    def exchange_partition(self, partitionSpecs, source_db, source_table_name, dest_db, dest_table_name):
        check_hive_privilege(database=source_db, table=source_table_name, type='ALL', region=self.region, username=self.user)
        check_hive_privilege(database=dest_db, table=dest_table_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.exchange_partition(partitionSpecs, source_db, source_table_name, dest_db, dest_table_name)

    def get_partition_with_auth(self, db_name, tbl_name, part_vals, user_name, group_names):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.get_partition_with_auth(db_name, tbl_name, part_vals, user_name, group_names)

    def get_partition_by_name(self, db_name, tbl_name, part_name):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_partition_by_name(db_name, tbl_name, part_name)

    def get_partitions(self, db_name, tbl_name, max_parts):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_partitions(db_name, tbl_name, max_parts)

    def get_partitions_with_auth(self, db_name, tbl_name, max_parts, user_name, group_names):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_partitions_with_auth(db_name, tbl_name, max_parts, user_name, group_names)

    def get_partition_names(self, db_name, tbl_name, max_parts):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_partition_names(db_name, tbl_name, max_parts)

    def get_partitions_ps(self, db_name, tbl_name, part_vals, max_parts):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_partitions_ps(db_name, tbl_name, part_vals, max_parts)

    def get_partitions_ps_with_auth(self, db_name, tbl_name, part_vals, max_parts, user_name, group_names):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_partitions_ps_with_auth(db_name, tbl_name, part_vals, max_parts, user_name, group_names)

    def get_partition_names_ps(self, db_name, tbl_name, part_vals, max_parts):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_partition_names_ps(db_name, tbl_name, part_vals, max_parts)

    def get_partitions_by_filter(self, db_name, tbl_name, filter, max_parts):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_partitions_by_filter(db_name, tbl_name, filter, max_parts)

    def get_partitions_by_expr(self, req):
        #todo：？？
        return self.thriftClient.get_partitions_by_expr(req)

    def get_partitions_by_names(self, db_name, tbl_name, names):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_partitions_by_names(db_name, tbl_name, names)

    def alter_partition(self, db_name, tbl_name, new_part):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.alter_partition(db_name, tbl_name, new_part)

    def alter_partitions(self, db_name, tbl_name, new_parts):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.alter_partitions(db_name, tbl_name, new_parts)

    def alter_partition_with_environment_context(self, db_name, tbl_name, new_part, environment_context):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.alter_partition_with_environment_context(db_name, tbl_name, new_part, environment_context)

    def rename_partition(self, db_name, tbl_name, part_vals, new_part):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.rename_partition(db_name, tbl_name, part_vals, new_part)

    def partition_name_has_valid_characters(self, part_vals, throw_exception):
        return self.thriftClient.partition_name_has_valid_characters(part_vals, throw_exception)

    def get_config_value(self, name, defaultValue):
        return self.thriftClient.get_config_value(name, defaultValue)

    def partition_name_to_vals(self, part_name):
        return self.thriftClient.partition_name_to_vals(part_name)

    def partition_name_to_spec(self, part_name):
        return self.thriftClient.partition_name_to_spec(part_name)

    def markPartitionForEvent(self, db_name, tbl_name, part_vals, eventType):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        self.thriftClient.markPartitionForEvent(db_name, tbl_name, part_vals, eventType)

    def isPartitionMarkedForEvent(self, db_name, tbl_name, part_vals, eventType):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.isPartitionMarkedForEvent(db_name, tbl_name, part_vals, eventType)

    def add_index(self, new_index, index_table):
        return self.thriftClient.add_index(new_index, index_table)

    def alter_index(self, dbname, base_tbl_name, idx_name, new_idx):
        check_hive_privilege(database=dbname, table=base_tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.alter_index(dbname, base_tbl_name, idx_name, new_idx)

    def drop_index_by_name(self, db_name, tbl_name, index_name, deleteData):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.drop_index_by_name(db_name, tbl_name, index_name, deleteData)

    def get_index_by_name(self, db_name, tbl_name, index_name):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_index_by_name(db_name, tbl_name, index_name)

    def get_indexes(self, db_name, tbl_name, max_indexes):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_indexes(db_name, tbl_name, max_indexes)

    def get_index_names(self, db_name, tbl_name, max_indexes):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_index_names(db_name, tbl_name, max_indexes)

    def update_table_column_statistics(self, stats_obj):
        #todo：
        return self.thriftClient.update_table_column_statistics(stats_obj)

    def update_partition_column_statistics(self, stats_obj):
        # todo：
        return self.thriftClient.update_partition_column_statistics(stats_obj)

    def get_table_column_statistics(self, db_name, tbl_name, col_name):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_table_column_statistics(db_name, tbl_name, col_name)

    def get_partition_column_statistics(self, db_name, tbl_name, part_name, col_name):
        check_hive_privilege(database=db_name, table=tbl_name, type='SELECT', region=self.region, username=self.user)
        return self.thriftClient.get_partition_column_statistics(db_name, tbl_name, part_name, col_name)

    def get_table_statistics_req(self, request):
        # todo：
        return self.thriftClient.get_table_statistics_req(request)

    def get_partitions_statistics_req(self, request):
        # todo：
        return self.thriftClient.get_partitions_statistics_req(request)

    def delete_partition_column_statistics(self, db_name, tbl_name, part_name, col_name):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.delete_partition_column_statistics(db_name, tbl_name, part_name, col_name)

    def delete_table_column_statistics(self, db_name, tbl_name, col_name):
        check_hive_privilege(database=db_name, table=tbl_name, type='ALL', region=self.region, username=self.user)
        return self.thriftClient.delete_table_column_statistics(db_name, tbl_name, col_name)

    def create_function(self, func):
        return self.thriftClient.create_function(func)

    def drop_function(self, dbName, funcName):
        return self.thriftClient.drop_function(dbName, funcName)

    def alter_function(self, dbName, funcName, newFunc):
        return self.thriftClient.alter_function(dbName, funcName, newFunc)

    def get_functions(self, dbName, pattern):
        return self.thriftClient.get_functions(dbName, pattern)

    def get_function(self, dbName, funcName):
        return self.thriftClient.get_function(dbName, funcName)

    def create_role(self, role):
        return self.thriftClient.create_role(role)

    def drop_role(self, role_name):
        return self.thriftClient.drop_role(role_name)

    def get_role_names(self, ):
        return self.thriftClient.get_role_names()

    def grant_role(self, role_name, principal_name, principal_type, grantor, grantorType, grant_option):
        return self.thriftClient.grant_role(role_name, principal_name, principal_type, grantor, grantorType, grant_option)

    def revoke_role(self, role_name, principal_name, principal_type):
        return self.thriftClient.revoke_role(role_name, principal_name, principal_type)

    def list_roles(self, principal_name, principal_type):
        return self.thriftClient.list_roles(principal_name, principal_type)

    def get_principals_in_role(self, request):
        return self.thriftClient.get_principals_in_role(request)

    def get_role_grants_for_principal(self, request):
        return self.thriftClient.get_role_grants_for_principal(request)

    def get_privilege_set(self, hiveObject, user_name, group_names):
        return self.thriftClient.get_privilege_set(hiveObject, user_name, group_names)

    def list_privileges(self, principal_name, principal_type, hiveObject):
        return self.thriftClient.list_privileges(principal_name, principal_type, hiveObject)

    def grant_privileges(self, privileges):
        return self.thriftClient.grant_privileges(privileges)

    def revoke_privileges(self, privileges):
        return self.thriftClient.revoke_privileges(privileges)

    def set_ugi(self, user_name, group_names):
        return self.thriftClient.set_ugi(user_name, group_names)

    def get_delegation_token(self, token_owner, renewer_kerberos_principal_name):
        return self.thriftClient.get_delegation_token(token_owner, renewer_kerberos_principal_name)

    def renew_delegation_token(self, token_str_form):
        return self.thriftClient.renew_delegation_token(token_str_form)

    def cancel_delegation_token(self, token_str_form):
        return self.thriftClient.cancel_delegation_token(token_str_form)

    def get_open_txns(self, ):
        return self.thriftClient.get_open_txns()

    def get_open_txns_info(self, ):
        return self.thriftClient.get_open_txns_info()

    def open_txns(self, rqst):
        return self.thriftClient.open_txns(rqst)

    def abort_txn(self, rqst):
        return self.thriftClient.abort_txn(rqst)

    def commit_txn(self, rqst):
        return self.thriftClient.commit_txn(rqst)

    def lock(self, rqst):
        return self.thriftClient.lock(rqst)

    def check_lock(self, rqst):
        return self.thriftClient.check_lock(rqst)

    def unlock(self, rqst):
        return self.thriftClient.unlock(rqst)

    def show_locks(self, rqst):
        return self.thriftClient.show_locks(rqst)

    def heartbeat(self, ids):
        return self.thriftClient.heartbeat(ids)

    def heartbeat_txn_range(self, txns):
        return self.thriftClient.heartbeat_txn_range(txns)

    def compact(self, rqst):
        return self.thriftClient.compact(rqst)

    def show_compact(self, rqst):
        return self.thriftClient.show_compact(rqst)
