#coding: utf-8

class HiveServer2Exception(Exception):
    def __init__(self, e, message=''):
        super(HiveServer2Exception, self).__init__(e)
        self.message = message
