#coding: utf-8
import sys
import time
import re
import logging

from pyutil.program import metrics2 as metrics

from .error import HiveServer2Exception
from .TCLIService.ttypes import TOperationState

LOG = logging.getLogger('hiveserver2')

DEFAULT_BUFFER_SIZE = 32

MAX_RESULTS_SIZE = 128 * 1024 * 1024

class HiveServer2Cursor(object):
    def __init__(self, client, sentry_client, operation_handle=None, operation_string=None, buffer_size=DEFAULT_BUFFER_SIZE):
        self.client = client

        self._last_operation_handle = operation_handle
        self._last_operation_string = operation_string
        self._last_operation_active = False if operation_handle is None else True
        self._buffer = []

        self._sentry_client = sentry_client
        self._service_name = self.client.service_name
        self._server = "{}_{}".format(self.client.host.replace('.', '-'), self.client.port)

        self._description = None
        self._need_record_state = False
        self.buffer_size = buffer_size

    @property
    def query_string(self):
        return self._last_operation_string

    @property
    def operation_handle(self):
        return self._last_operation_handle

    @property
    def description(self):
        return self._description

    @property
    def has_result_set(self):
        return (self._last_operation_handle is not None and
                self._last_operation_handle.hasResultSet)

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc_value, _traceback):
        try:
            self.close()
        except Exception:
            LOG.warning('fail to close operation before exit')

    def execute(self, statement, configuration=None, timeout=None, async=False):
        if async:
            self.async_execute(statement, configuration)
        else:
            self.sync_execute(statement, configuration, timeout)

    def direct_execute(self, statement, configuration=None, timeout=None):
        statement = self._preprocess_statement(statement)
        self._reset_state()
        self._last_operation_string = statement
        execute_start = time.time()
        try:
            (rows, schema), operation_handle = self.client.execute_statement(statement, configuration)
            self._buffer.extend(rows)
            self._description = schema
            self._last_operation_handle = operation_handle
            self._last_operation_active = True

            self._wait_to_finish(execute_start, timeout)
        except Exception:
            raise

    def sync_execute(self, statement, configuration=None, timeout=None):
        statement = self._preprocess_statement(statement)
        self._reset_state()
        self._last_operation_string = statement
        try:
            operation_handle = self.client.execute_async_statement(statement, configuration)
            self._last_operation_handle = operation_handle
            self._last_operation_active = True
            execute_start = time.time()

            self._wait_to_finish(execute_start, timeout)
        except Exception:
            raise

    def async_execute(self, statement, configuration=None):
        statement = self._preprocess_statement(statement)
        self._reset_state()
        self._last_operation_string = statement
        try:
            operation_handle = self.client.execute_async_statement(statement, configuration)
            self._last_operation_handle = operation_handle
            self._last_operation_active = True
            self._need_record_state = True
        except Exception:
            raise

    def cancel(self):
        if self._last_operation_active:
            try:
                self.client.cancel_operation(self._last_operation_handle)
            except Exception:
                raise
            self._reset_state()

    def close(self):
        if self._last_operation_active:
            self._reset_state()

    def get_state(self):
        if self._last_operation_handle:
            try:
                operation_state = self.client.get_state(self._last_operation_handle)
            except Exception:
                raise
            self._record_async_state(operation_state)
            return operation_state

    def get_operation_status(self):
        if self._last_operation_handle:
            try:
                operation_status = self.client.get_operation_status(self._last_operation_handle)
            except Exception:
                raise
            return operation_status

    def is_finished(self):
        operation_status = self.get_operation_status()
        operation_state = TOperationState._VALUES_TO_NAMES[operation_status.operationState]
        return operation_state in {'ERROR_STATE', 'FINISHED_STATE', 'CANCELED_STATE',
                                   'CLOSED_STATE', 'UKNOWN_STATE'} \
                                   and not re.search('Invalid OperationHandle', \
                                                     operation_status.errorMessage or '', re.I)


    def is_running(self):
        operation_state = self.get_state()
        return operation_state == 'RUNNING_STATE'

    def is_success(self):
        operation_state = self.get_state()
        return operation_state in {'FINISHED_STATE', 'CLOSED_STATE'}

    def is_failure(self):
        operation_state = self.get_state()
        return operation_state in {'ERROR_STATE', 'CANCELED_STATE', 'UKNOWN_STATE'}

    def fetchone(self):
        try:
            return self.next()
        except StopIteration:
            return None

    def fetchsize(self, max_size=None):
        """
        fetch results which data size ls less than `max_size`(default value is 128 MB).
        """
        if max_size is None:
            max_size = MAX_RESULTS_SIZE
        local_buffer = []
        while sys.getsizeof(local_buffer) < max_size:
            try:
                local_buffer.append(self.next())
            except StopIteration:
                break
        return local_buffer

    def fetchmany(self, size=None):
        if size is None:
            size = self.buffer_size
        local_buffer = []
        i = 0
        while i < size:
            try:
                local_buffer.append(self.next())
                i += 1
            except StopIteration:
                break
        return local_buffer

    def fetchall(self):
        try:
            return list(self)
        except StopIteration:
            return []

    def __iter__(self):
        return self

    def next(self):
        if len(self._buffer) > 0:
            return self._buffer.pop(0)
        elif self._last_operation_active:
            rows, schema = self.client.fetch_result(self._last_operation_handle, schema=self._description, max_rows=self.buffer_size)
            self._description = schema
            self._buffer.extend(rows)
            if len(self._buffer) == 0:
                raise StopIteration
            return self._buffer.pop(0)
        else:
            raise StopIteration

    def explain(self):
        pass

    def ping(self):
        pass

    def get_log(self, start_over=True):
        if self._last_operation_handle:
            try:
                log = self.client.get_log(self._last_operation_handle, start_over)
            except Exception:
                raise
            return log
        return ''

    def get_profile(self):
        pass

    def get_summary(self):
        pass

    def get_databases(self):
        pass

    def database_exists(self):
        pass

    def get_tables(self, database_name=None):
        pass

    def table_exists(self, table_name, database_name=None):
        pass

    def get_functions(self, database_name=None):
        pass

    def get_partitions(self, table_name, database_name=None, max_parts=None, with_meta=False):
        self._last_operation_string = 'RPC_GET_PARTITIONS'
        try:
            rs = self.client.get_partitions(table_name, database_name, max_parts, with_meta)
        except Exception:
            raise
        return rs

    def get_table_schema(self, table_name, database_name=None):
        self._last_operation_string = 'RPC_GET_TABLE_SCHEMA'
        try:
            rs = self.client.get_table_schema(table_name, database_name)
        except Exception:
            raise
        return rs

    def _preprocess_statement(self, statement):
        statement = statement.strip().strip(';')
        return statement

    def _reset_state(self):
        self._buffer = []
        self._description = None
        if self._last_operation_active:
            self._last_operation_active = False
            self.client.close_operation(self._last_operation_handle)
        self._last_operation_handle = None
        self._last_operation_active = None
        self._need_record_state = False

    def _wait_to_finish(self, execute_start, timeout):
        loop_start = time.time()
        while True:
            operation_state = self.client.get_state(self._last_operation_handle)
            if operation_state == 'ERROR_STATE':
                print >>sys.stderr, self.get_log()
                raise HiveServer2Exception(Exception('Operation is in ERROR_STATE'))
            if operation_state in {'FINISHED_STATE', 'CANCELED_STATE',
                                   'CLOSED_STATE', 'UKNOWN_STATE'}:
                break

            if timeout is not None and time.time() - execute_start >= timeout:
                self.cancel()
                raise HiveServer2Exception(Exception('Execute timeout controlled by client'))

            time.sleep(self._get_sleep_interval(loop_start))

    def _get_sleep_interval(self, start_time):
        elapsed = time.time() - start_time
        if elapsed < 60.0:
            return 5
        return 10

    def _record_async_state(self, operation_state):
        if self._need_record_state:
            if operation_state in {'ERROR_STATE', 'CANCELED_STATE', 'UKNOWN_STATE'}:
                metric = 'execute.fail'
            elif operation_state in {'FINISHED_STATE', 'CLOSED_STATE'}:
                metric = 'execute.success'
            else:
                metric = None
            if metric:
                self._need_record_state = False
