import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from hiveserver2.metastore import HiveThriftContext, HiveMetastoreClient
from hiveserver2 import connect

psm_i18n = 'data.olap.hms_py_i18n.service.maliva.byted.org'
psm_default = 'data.olap.catalogservice.service.lf.byted.org'

# test HiveThriftContext
# test psm
# test do not cache exception

def test_metastore(psm=''):
    num = 10
    database = 'aim'
    table = 'input_data'
    with HiveThriftContext(metastore_psm=psm) as client:

        # get partition
        print "test get_partitions"
        res = client.get_partitions(database, table, num)
        print res, "\n"

        # get database
        print "test get_database"
        res = client.get_database(database)
        print res, "\n"

        print "test get_all_databases"
        # get all databases
        res = client.get_all_databases()
        print res, "\n"

        print "test get_schema"
        # get schema
        res = client.get_schema(database, table)
        print res, "\n"

        print "test get_tables"
        # get tables
        res = client.get_tables(database, table)
        print res, "\n"

        # get all tables
        print "test get_all_tables"
        res = client.get_all_tables(database)
        print res, "\n"

        print "test get_partition_names"
        # get_partition_names
        res = client.get_partition_names(database, table, num)
        print res, "\n"
def test_metastore2(psm=''):
    num = 10
    client = HiveMetastoreClient(metastore_psm=psm)
    try:
        res = client.get_table_partitions("event_log_hourly", "origin_log", num)
        print res
    except Exception as e:
        print "get partition", e


def test_hiveserver2(cluster='i18n_default'):
    sql = "show tables"
    with connect('hive', cluster=cluster, username='ouchengeng') as client:
        with client.cursor() as cursor:
            cursor.execute(sql, configuration={'mapreduce.job.name': 'ouchengeng', 'mapreduce.job.priority': 'NORMAL',
                                               'yarn.cluster.name': 'default',
                                               'mapreduce.job.queuename': 'root.data.etl'})
            print cursor.fetchall()



if __name__ == '__main__':
    # metastore
    print "test i18n"
    test_metastore(psm_i18n)

    print "\n test"
    test_metastore(psm_default)