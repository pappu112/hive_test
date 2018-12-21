#coding: utf-8

import sys
import os
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from pyutil.hiveserver2 import connect, connect_to_hive, connect_to_impala

def connect_example():
    impala = connect('impala', cluster='default')
    impala.close()

    impala = connect_to_impala(cluster='nearline')
    impala.close()

    hive = connect('hive', cluster='default', timeout=60, username='tiger')
    hive.close()

    hive = connect_to_hive(cluster='haruna_default', timeout=60, username='tiger')
    hive.close()


def sync_query_example():
#with connect('hive', cluster='haruna_noauth', username='tiger') as client:
#with connect('hive', cluster='default', username='tiger') as client:
    with connect('impala', cluster='main') as client:
        with client.cursor() as cursor:
            #cursor.execute("select * from impression_stats_daily where `date`='20150711' limit 10")
            sql = "select count(*) from impression_stats_daily where `date`='20150711'"
            #sql = "insert overwrite directory '/user/chengzhangmin/hive/dau/feed/20150704' SELECT user_uid, user_uid_type, platform_type  FROM impression_stats_daily WHERE impr_time > 0 AND user_access <> 10 AND impr_source ='f' AND user_uid_type <> 13 AND impr_from = '' AND date >= '20150704' AND date <= '20150704' AND traffic_type = 'app'"
            #cursor.execute(sql, configuration={'mapred.job.queue.name': 'offline.data'})
            cursor.execute(sql)
            print 'done'
            print cursor.get_log()
            print cursor.fetchall()
            print cursor.description


def async_query_example():
    #with connect('impala', cluster='default') as client:
    with connect('hive', cluster='haruna_default', username='tiger') as client:
        with client.cursor() as cursor:
            cursor.execute("insert overwrite directory 'hdfs://kingkong/user/wangye/tmp/test123' select user_uid_type, user_uid from impression_stats_daily where `date`='20150711' limit 10", async=True, configuration={'mapred.job.queue.name': 'offline.data'})
            #cursor.execute("insert overwrite directory '/user/wangye/tmp/test123' select count(*) from impression_stats_daily where `date`='20150711' limit 10", async=True, configuration={'mapred.job.queue.name': 'nearline.stat'})
            #sql = "insert overwrite directory '/push/data/tmp/20150704' SELECT user_uid, user_uid_type, platform_type  FROM impression_stats_daily WHERE impr_time > 0 AND user_access <> 10 AND impr_source ='f' AND user_uid_type <> 13 AND impr_from = '' AND date >= '20150704' AND date <= '20150704' AND traffic_type = 'app'"
#cursor.execute(sql, async=True, configuration={'mapred.job.queue.name': 'offline.data'})
            while not cursor.is_finished():
                print 'waiting'
                log = cursor.get_log(start_over=False)
                if log:
                    print log
                time.sleep(1)
            log = cursor.get_log(start_over=False)
            if log:
                print log
            print cursor.fetchall()
            print cursor.description


def get_partitions_example():
    #with connect('impala', cluster='default') as client:
    with connect('hive', cluster='haruna_default', username='tiger') as client:
        with client.cursor() as cursor:
            #print cursor.get_partitions('dlu', database_name='app_data')
            #print cursor.get_partitions('impression_stats_daily')
            #print cursor.get_partitions('impression_stats_daily', max_parts=10, with_meta=True)
#            print cursor.get_partitions('parted_user_external_property')
            ts = client.get_table_createtime('ios_device_ext_dict', 'app_data')
            from datetime import datetime
            print datetime.fromtimestamp(int(ts)).strftime("%Y-%m-%d %H:%M:%S")



def udf_example():
    with connect('hive', cluster='noauth', username='tiger') as client:
        with client.cursor() as cursor:
            cursor.execute("add file hdfs://kingkong/user/tiger/udf/split_last_field.py")
            cursor.execute("""select transform(ut, uid, user_connects)
                              using 'python split_last_field.py --unique'
                              as (ut int, uid bigint, user_connect int)
                              from user_external_property_daily
                              where `date`='20150711' and user_connects != ''
                              limit 10
                           """, configuration={'mapred.job.queue.name': 'offline.data'})
            print cursor.fetchall()

if __name__ == '__main__':
    #connect_example()
#sync_query_example()
    #async_query_example()
    get_partitions_example()
    #get_table_schema_example()
    #udf_example()
