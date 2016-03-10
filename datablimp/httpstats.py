# encoding: utf-8

from datetime import datetime
import re

from datablimp import E


# request_uri, http_version and status_code - field names used in RFC2616
apache_combined = re.compile(
    '(?P<clientip>[\d\.]+) (?P<ident>\S+) (?P<auth>\S+) '
    '\[(?P<timestamp>.*?)\] '
    '"(?P<method>\S+) (?P<request_uri>.*) HTTP/(?P<http_version>\d\.\d)" '
    '(?P<status_code>\d+|-) (?P<bytes>\d+|-)'
    '( "(?P<referrer>.*?)")?'
    '( "(?P<agent>.*)")?'
    '( (?P<kv>.*))?'
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

    def __init__(self,
                 class_thresholds=[0.05, 0.25, 0.5, 2.5],
                 class_labels=['immediate', 'fast', 'normal', 'slow',
                               'very_slow'],
                 percentiles=[50, 75, 90, 95, 99],
                 interval='1s'):
        self.class_thresholds = class_thresholds
        self.percentiles = percentiles
        self.interval = interval_to_seconds(interval)
        self.ret = None

    def extract(self, value):
        ret = self.ret
        timestamp = self.round_to_interval(value['timestamp'])
        if timestamp != ret['timestamp']:
            self.flush()
            self.ret = ret = {
                'timestamp': timestamp,
                'interval': self.interval
            }
        ret['count_total'] += 1
        if 'request_time' in value:
            # XXX: implement class/percentiles
            pass
        self.ret['code_%d' % value['status_code']] += 1

    def flush(self):
        if self.ret is not None:
            ret, self.ret = self.ret, None
            self.emit(ret)
