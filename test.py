HIVE_PSM_CN = "data.olap.catalogservice"
HIVE_PSM_VA = "data.olap.hms_py_i18n.service.maliva.byted.org"
HIVE_PSM_SG = "data.olap.hms_sg"

DATABASE_NAME = 'aim'
TABLE_NAME = 'input_data'
with HiveThriftContext(metastore_psm=psm) as client:
    try:
         parts = client.get_partition_by_name(DATABASE_NAME, TABLE_NAME,
                                                 "date={}".format(day))
            return parts
        except Exception:
            traceback.print_stack()
            print "failed to get_hive_partition_location {} {} {}".format(day, hour, app)
            return None