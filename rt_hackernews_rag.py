"""Fetch the home page of hackernews every 15 mins and extract all the links from there."""
import requests
from datetime import datetime, timedelta
import time
from typing import Any, Optional

import bytewax.operators as op
from bytewax.dataflow import Dataflow

from hackernews_connector import HNSource
# from redis_connector import RedisVectorOutput
from utils import safe_request, parse_html, hf_document_embed, prep_text

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
    metadata = req.json()
    try:
        if "[delayed]" in metadata["text"]:
            logger.warning(f"text delayed for item {hn_id} trying again")
            time.sleep(0.5)
            return download_metadata(hn_id)
    except KeyError:
        logger.warning(f"item {metadata} has no key 'text'")
    return metadata

def download_html(metadata):
    try:
        html = safe_request(metadata["url"])
        return {**metadata, "content":html}
    except KeyError:
        logger.error(f"No url content for {metadata}")
        return None


def run_hn_flow(init_item=None, polling_interval=15): 
    flow = Dataflow("hn_stream")
    inp = op.input("in", flow, HNSource(timedelta(seconds=polling_interval), None, init_item)) # skip the align_to argument
    # If you run this dataflow with multiple workers, downloads in
    # the next `map` will be parallelized thanks to .redistribute()
    inp = op.redistribute("scaling", inp)
    enriched = op.map("enrich", inp, download_metadata)
    filtered = op.filter("filter_junk", enriched, lambda document: document["type"] in ["story", "comment"])
    stories, comments = op.branch("split_comments", filtered, lambda document: document["type"]=="story")
    op.inspect_debug("stories1", stories)
    op.inspect_debug("comments1", comments)
    stories = op.map("filter", stories, lambda document: "dead" in list(document.keys()))
    stories = op.map("fetch_html", stories, download_html)
    stories = op.map("parse_docs", stories, lambda document: parse_html(document, tokenizer))
    stories = op.filter("remove_failed", stories, lambda x: x != None)
    comments = op.map("clean_text", comments, lambda document: prep_text(document, tokenizer))
    
    stories = op.map("story_embeddings", stories, lambda document: hf_document_embed(document, tokenizer, model, torch, length=VECTOR_DIMENSIONS))
    comments = op.map("comment_embeddings", comments, lambda document: hf_document_embed(document, tokenizer, model, torch, length=VECTOR_DIMENSIONS))
    op.inspect_debug("stories2", stories)
    op.inspect_debug("comments2", comments)
    # flow.output("std-out", RedisVectorOutput("hn_stories", SCHEMA, overwrite=True))
    return flow
