import logging

from bytewax.outputs import DynamicSink, StatelessSinkPartition

from redisvl.index import SearchIndex
from redis import DataError
from itertools import chain

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class _RedisVectorSink(StatelessSinkPartition):
    def __init__(self, index, collection_name):
        self.index = index
        self._collection_name = collection_name

    def write_batch(self, documents):
        try:
            self.index.load(list(chain.from_iterable(documents)))
        except DataError as e:
            logger.warning(f'''could not upload {documents}
                           because of {e}''')


class RedisVectorOutput(DynamicSink):
    """Redis Vector Output

    Workers are the unit of parallelism.

    Can support at-least-once processing. Messages from the resume
    epoch will be duplicated right after resume.

    Expects a batch of items to write to decrease cost of roundtrip.
    Use the batch operator before hand to collect items.
    """

    def __init__(self, collection_name, schema, overwrite, host="localhost", port=6379):
        self.collection_name = collection_name
        self.index = SearchIndex.from_dict(schema)
        self.index.connect(f"redis://{host}:{port}")
        if not self.index.exists():
            self.index.create(overwrite=overwrite)

    def build(self, worker_index, worker_count):
        return _RedisVectorSink(self.index, self.collection_name)
