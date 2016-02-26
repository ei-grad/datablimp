# encoding: utf-8

from datetime import datetime
import re

from datablimp import E


apache_combined = re.compile(
    '(?P<clientip>[\d\.]+) (?P<ident>\S+) (?P<auth>\S+) '
    '\[(?P<timestamp>.*?)\] '
    '"(?P<method>\S+) (?P<path>.*) HTTP/(?P<httpversion>\d\.\d)" '
    '(?P<status_code>\d+|-) (?P<bytes>\d+|-)'
    '( "(?P<referrer>.*?)")?'
    '( "(?P<agent>.*)")?'
    '( "(?P<kv>([a-z_]+)="(.*)"))*'
)


class ApacheCombined(E.Base):

    datefmt = '%d/%b/%Y:%H:%M:%S %z'

    def extract(self, line):
        m = apache_combined.match(line)
        if m is None:
            # XXX: implement base exceptions for extractors
            raise E.FormatError("Not in apache combined format!")
        ret = m.groupdict()
        kv = ret.pop('kv')
        if kv is not None:
            # XXX: implement key=value format support
            pass
        ret['bytes'] = int(ret['bytes'])
        ret['status_code'] = int(ret['status_code'])
        ret['timestamp'] = datetime.strptime(
            ret['timestamp'],
            self.datefmt
        )
        return ret


class HTTPStats(E.Base):

    def __init__(self, class_thresholds=[0.05, 0.25, 0.5, 2.5],
                 percentiles=[50, 75, 90, 95, 99],
                 interval='1s'):
        self.class_thresholds = class_thresholds
        self.percentiles = percentiles

    def extract(self, value):
        pass
