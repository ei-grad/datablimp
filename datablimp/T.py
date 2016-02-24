from datetime import datetime
import pytz

from datablimp.base import Base


class Base(Base):
    def process(self, data):
        self.transform(data)
        return data


class ParseTimestamp(Base):

    def __init__(self, source='timestamp', target='timestamp', timezone='UTC'):
        super().__init__()
        self.source = source
        self.target = target
        self.timezone = pytz.timezone(timezone)

    def transform(self, doc):
        doc[self.target] = self.timezone.localize(datetime.utcfromtimestamp(doc[self.source]))
