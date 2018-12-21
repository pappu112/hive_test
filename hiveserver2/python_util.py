#coding: utf-8

import select
import socket
import re
from datetime import datetime

__all__ = ['CaseInsensitiveDict', 'create_synchronous_io_multiplexer']

class SynchronousIOMultiplexer(object):
  def read(self, rd):
    raise NotImplementedError

  def write(self, rd):
    raise NotImplementedError

  def error(self, rd):
    raise NotImplementedError


class SelectSynchronousIOMultiplexer(SynchronousIOMultiplexer):
  def __init__(self, timeout=0):
    self.timeout = 0

  def read(self, fds):
    rlist, wlist, xlist = select.select(fds, [], [], self.timeout)
    return rlist


class PollSynchronousIOMultiplexer(SynchronousIOMultiplexer):
  def __init__(self, timeout=0):
    self.timeout = 0

  def read(self, fds):
    poll_obj = select.poll()
    for fd in fds:
      poll_obj.register(fd, select.POLLIN)
    event_list = poll_obj.poll(self.timeout)
    return [fd_event_tuple[0] for fd_event_tuple in event_list]


def create_synchronous_io_multiplexer(timeout=0):
  try:
      from select import poll
      return PollSynchronousIOMultiplexer(timeout)
  except ImportError:
      pass
  return SelectSynchronousIOMultiplexer(timeout)


def sizeof_fmt(num, suffix='B'):
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f%s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f%s%s" % (num, 'Yi', suffix)


_TIMESTAMP_PATTERN = re.compile(r'(\d+-\d+-\d+ \d+:\d+:\d+(\.\d{,6})?)')

def parse_timestamp(value):
    if value:
        match = _TIMESTAMP_PATTERN.match(value)
        if match:
            if match.group(2):
                format = '%Y-%m-%d %H:%M:%S.%f'
                # use the pattern to truncate the value
                value = match.group()
            else:
                format = '%Y-%m-%d %H:%M:%S'
            value = datetime.strptime(value, format)
        else:
            raise Exception(
                'Cannot convert "{}" into a datetime'.format(value))
    else:
        value = None
    return value

