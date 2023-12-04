from dataclasses import dataclass
import requests
from datetime import datetime, timedelta, timezone
from typing import Callable, List, Optional, Tuple, TypeVar

from bytewax.inputs import FixedPartitionedSource, StatefulSourcePartition

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class _HNSourcePartition(StatefulSourcePartition[List[int], Optional[int]]):
    def __init__(
        self,
        now: datetime,
        interval: timedelta, 
        align_to: Optional[datetime], 
        init_id: Optional[int],
        resume_state: Optional[int]
    ):
        self._interval = interval
        if resume_state:
            self.starting_id = resume_state
        elif init_id:
            self.starting_id = init_id
        else:
            self.starting_id = requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json").json()

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

    def next_batch(self, _sched: datetime) -> List[int]:
        self._next_awake += self._interval
        self.current_id = int(requests.get("https://hacker-news.firebaseio.com/v0/maxitem.json").json())
        batch = list(range(self.starting_id, self.current_id))
        self.starting_id = self.current_id

        return batch

    def next_awake(self) -> Optional[datetime]:
        return self._next_awake

    def snapshot(self) -> Optional[int]:
        return self.current_id


class HNSource(FixedPartitionedSource):
    """Calls a Hacker News API at a regular interval.

    There is no parallelism; only one worker will poll this source.

    This is best for low-throughput polling on the order of seconds to
    hours.

    If you need a high-throughput source, avoid this. Instead create a
    source using one of the other `Source` subclasses where you can
    have increased paralellism, batching, and finer control over
    timing.

    """

    def __init__(
            self, 
            interval: timedelta, 
            align_to: Optional[datetime] = None, 
            init_id: Optional[int] = None):
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

    def list_parts(self) -> List[str]:
        """Assumes the source has a single partition."""
        return ["singleton"]

    def build_part(self, _now: datetime, part_key: str, resume_state: Optional[int]) -> _HNSourcePartition:
        """Called for each partition, limited to one part to avoid duplication"""
        return _HNSourcePartition(_now, self._interval, self._align_to, self._init_id, resume_state)
