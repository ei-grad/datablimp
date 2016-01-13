Datablimp
=========

[![Circle CI](https://circleci.com/gh/ei-grad/datablimp/tree/devel.svg?style=svg)](https://circleci.com/gh/ei-grad/datablimp/tree/devel)

Datablimp is planned to be an ETL framework with bolder approach.

* **Extensible:**

```python
import json
from time import time

from datablimp import E, T, L


# Extractor objects are very simple and intelligent - the only thing you have to
# implement is the `extract()` method, which yields the processed entity:

class GzipFile(E.File):
    def extract(self, filename):
        yield gzip.GzipFile(filename)


# Extractor objects are meant to be chained:

class SplitLines(E.Base):
    def extract(self, fp):
        for line in fp:
            yield line


class JSON(E.Base):

    def extract(self, data):

        # If the entity is extracted into new document, it should be wrapped
        # into E.Doc, which maintain the ```meta``` and ```data``` attributes
        # of document
        # (doc.__getitem__/__setitem__ are proxied to the doc.data,
        # and doc.meta is used to store internal info)

        yield E.Doc(json.loads(data))



# Transform objects just transform documents inplace:

class Greeter(T.Base):
    def transform(self, doc):
        doc['greeting'] = 'Hello, %s!' % doc[self.args[0]]


# Loaders just ... load! (I hope you got it).
# They also could be chained, as they work like a `tee` in pipeline.

class PrettyPrint(L.Base):
    def load(self, doc):
        print(json.dumps(doc, indent=4))


```


* **Expressive:**

```python
pipeline = GzipFile('users_login_time.gz') | \
    SplitLines() | \
    JSON() | \
    Greeter('login') | \
    PrettyPrint() | \
    L.PostgreSQL('postgresql:///greets').Table('greets')
```

* **Powerful:**

```python
from datablimp import D  # dispatch
from datablimp import F  # filter
from datablimp import S  # serialize

# Some builtin transforms:
event_transform = T.AddField({'_type': 'greet'}) | \
    T.Rename({'greeting': 'message'}) | \
    T.ParseTimestamp('time', target='dt', time_zone='US/Pacific')


class Enrich(T.Base):
    def transform(self, doc):
        if self.args[0] == 'greet': 
            doc['greeting_received'] = 'greeting' in doc


# Some load handlers:
aerospike = L.Aerospike({'hosts': [('127.0.0.1', 3000)]})
postgres = L.PostgreSQL('postgresql:///datablimp_import')
elastic = L.Elasticsearch('http://es2.ei-grad.ru')

type_dispatcher = D.ByKeyField(
    key_field='_type',
    output_factory=lambda key: D.Fanout(
        Enrich(key) | \
            D.Fanout(
                F.eq({'login': 'ei-grad'}) | es.Index('my-greets'),
                elastic.Index(datetime_pattern={
                    'format': '{}-%Y.%m.%d'.format(key),
                    'field': 'dt'
                }),
                aerospike.Namespace('datablimp').Set(datetime_pattern={
                    'format': '{}-%Y.%m.%d'.format(key),
                    'field': 'dt'
                }).AutoUUIDKey(),
            ),
        postgres.Table(key, auto_create=True, auto_alter=True),
    )
)

postgres_source = E.PostgreSQL('postgresql:///greets').Table('greets')

# serialize pipeline timings and statuses from E.Doc.meta
debug_logger = S.DebugSerializer() | L.File("datablimp.debug.log")

pipeline = postgres_source | event_transform | type_dispatcher | debug_logger
```


* Made with **perfomance** in mind:

```python
if __name__ == "__main__":
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(pipeline)
```


* And more:

```python
from datablimp import W  # watchers
from datablimp import P  # periodic
from datablimp import B  # buffers

# Watch files in directory, files matching glob pattern, or just a file:
D.Cat(
    W.Directory("data"),
    W.Glob("data/*"),
    W.File("data.gz"),
)

# Run run nested pipeline hourly or in intervals specified by crontab format:
D.Cat(
    P.Hourly(E.HTTP('http://example.com/feed')),
    P.Daily(E.HTTP('http://example.com/feed')),
    P.Crontab('*/5 * * * *', E.HTTP('http://example.com/feed'))
) | S.DebugSerializer() | L.Stdout()

# If some data should be bufferred:
E.AMQP().Queue('fast-rabbit') | \
    B.DiskBuffer() | \
    E.JSON() | \
    L.Elasticsearch().Index('slow-elastic')
```

What can you do with it?
