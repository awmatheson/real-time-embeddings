"""Fetch the home page of hackernews every 15 mins and extract all the links from there."""
import requests
from datetime import datetime, timedelta
import time
from typing import Any, Optional

from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow

from hackernews_connector import HNInput
from redis_connector import RedisVectorOutput
from utils import safe_request, parse_html, hf_document_embed

from transformers import AutoTokenizer, AutoModel
import torch

tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
model = AutoModel.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

VECTOR_DIMENSIONS = 512
SCHEMA = {
    "index": {
        "name": "story_index",
        "prefix": "v1",
    },
    "fields": {
        "tag": [{"name": "key_id"}],
        "text": [{"name": "by"}],
        "numeric": [{"name": "descendants"}],
        "numeric": [{"name": "id"}],
        "numeric": [{"name": "score"}],
        "numeric": [{"name": "time"}],
        "text": [{"name": "title"}],
        "text": [{"name": "type"}],
        "text": [{"name": "url"}],
        "text": [{"name": "text"}],
        "vector": [{
                "name": "doc_embedding",
                "dims": VECTOR_DIMENSIONS,
                "distance_metric": "cosine",
                "algorithm": "flat",
                "datatype": "FLOAT32"}
        ]
    },
}
    
def download_metadata(hn_id):
    # Given an hacker news id returned from the api, fetch metadata
    req = requests.get(
        f"https://hacker-news.firebaseio.com/v0/item/{hn_id}.json"
    )
    if not req.json():
        logger.warning(f"error getting payload from item {hn_id} trying again")
        time.sleep(0.5)
        return download_metadata(hn_id)
    return req.json()

def download_html(metadata):
    try:
        html = safe_request(metadata["url"])
        return {**metadata, "content":html}
    except KeyError:
        logger.error(f"No url content for {metadata}")
        return None


def run_hn_flow(init_item=None, polling_interval=15): 
    flow = Dataflow()
    flow.input("in", HNInput(timedelta(seconds=polling_interval), None, init_item)) # skip the align_to argument
    # If you run this dataflow with multiple workers, downloads in
    # the next `map` will be parallelized thanks to .redistribute()
    flow.redistribute()
    flow.map(download_metadata)
    flow.inspect(logger.info)
    flow.filter(lambda document: document["type"]=="story")
    flow.map(download_html)
    flow.map(lambda document: parse_html(document, tokenizer))
    flow.filter(lambda x: x != None)
    flow.map(lambda document: hf_document_embed(document, tokenizer, model, torch, length=VECTOR_DIMENSIONS))
    flow.inspect(logger.info)
    flow.output("std-out", RedisVectorOutput("hn_stories", SCHEMA, overwrite=True))
    return flow
