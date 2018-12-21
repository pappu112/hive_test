#coding: utf-8

from datetime import datetime
from bytesnake import hdfs_client
from .python_util import sizeof_fmt

def get_hdfs_client(fs_name):
    return hdfs_client.get_hdfs_client()

def parse_hdfs_uri(uri):
    if uri.startswith('hdfs://'):
        uri = uri[7:]
        split = uri.split('/', 1)
        fs = split[0]
        path = '/' + split[1]
    else:
        fs = ''
        path = uri
    return fs, path

def get_path_meta(client, paths):
    if not isinstance(paths, (tuple, list)):
        paths = [paths]
    result = []
    for item in client.count(paths):
        path = item['path']
        stat = client.stat([path])
        modification_time = datetime.fromtimestamp(stat['modification_time']/1000)
        result.append({'size': sizeof_fmt(item['length']), 'files_count': item['fileCount'], 'modification_time': modification_time})
    return result
