from typing import Optional
import requests
from datetime import datetime, timedelta, timezone

from bytewax.inputs import PartitionedInput, StatefulSource

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
    

class _HNSource(StatefulSource):
    def __init__(
        self, interval, align_to, init_id, resume_state, now_getter=lambda: datetime.now(timezone.utc)
    ):
        self._interval = interval
        if resume_state:
            self.starting_id = resume_state
        elif init_id:
            self.starting_id = init_id
        else:
            self.starting_id = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json").json()

        now = now_getter()
        if align_to is not None:
            # Hell yeah timedelta implements remainder.
            since_last_awake = (now - align_to) % interval
            if since_last_awake > timedelta(seconds=0):
                until_next_awake = interval - since_last_awake
            else:
                # If now is exactly on the align_to mark (remainder is
                # 0), don't wait a whole interval; activate
                # immediately.
                until_next_awake = timedelta(seconds=0)
            self._next_awake = now + until_next_awake
        else:
            self._next_awake = now

    def next_batch(self):
        self._next_awake += self._interval
        self.current_id = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json").json()
        batch = list(range(self.starting_id, self.current_id))
        self.starting_id = self.current_id

        return batch

    def next_awake(self):
        return self._next_awake

    def snapshot(self):
        return self.current_id


class HNInput(PartitionedInput):
    """Calls a Hacker News API at a regular interval.

    There is no parallelism; only one worker will poll this source.

    This is best for low-throughput polling on the order of seconds to
    hours.

    If you need a high-throughput source, avoid this. Instead create a
    source using one of the other `Source` subclasses where you can
    have increased paralellism, batching, and finer control over
    timing.

    """

    def __init__(self, interval: timedelta, align_to: Optional[datetime] = None, init_id: Optional[int] = None):
        """Init.

        Args:
            interval:
                The interval between calling `next_item`.
            align_to:
                Align awake times to the given datetime. Defaults to
                now.
            init_id:
                item id to start from

        """
        self._interval = interval
        self._align_to = align_to
        self._init_id = init_id

    def list_parts(self):
        """Assumes the source has a single partition."""
        return ["singleton"]

    def build_part(self, _for_part, _resume_state):
        """See ABC docstring."""
        return _HNSource(self._interval, self._align_to, self._init_id, _resume_state)
