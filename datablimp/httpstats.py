# encoding: utf-8

import re

from datablimp import E


apache_combined = re.compile(
    '(?P<clientip>[\d\.]+) (?P<ident>\S+) (?P<auth>\S+) '
    '\[(?P<timestamp>.*?)\] '
    '"(?P<verb>\S+) (.*) HTTP/(?P<httpversion>\d\.\d)" '
    '(?P<response>\d+|-) (?P<bytes>\d+|-)'
    '( "(?P<referrer>.*?)")?'
    '( "(?P<agent>.*)")?'
    '( "(?P<kv>([a-z_]+)="(.*)"))*'
)


class ApacheCombined(E.Base):
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
        return ret


class HTTPStats(E.Base):
    def extract(self, value):
        pass
