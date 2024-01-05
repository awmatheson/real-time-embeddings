import requests
from datetime import timedelta
import time
from typing import Any, Optional
import logging

import bytewax.operators as op
from bytewax.dataflow import Dataflow

from hackernews_connector import HNSource
from redis_connector import RedisVectorOutput
from utils import safe_request, parse_html, hf_document_embed, prep_text
from schemas import VECTOR_DIMENSIONS, STORY_SCHEMA, COMMENT_SCHEMA

from transformers import AutoTokenizer, AutoModel
import torch

tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
model = AutoModel.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def download_metadata(hn_id, current_attempt=0, max_attempt=10):
    # Given an hacker news id returned from the api, fetch metadata
    if current_attempt > max_attempt:
        return None
    current_attempt += 1
    req = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{hn_id}.json")
    if not req.json():
        logger.warning(f"error getting payload from item {hn_id} trying again")
        time.sleep(0.5)
        return download_metadata(hn_id, current_attempt)
    metadata = req.json()
    try:
        if "[delayed]" in metadata["text"]:
            logger.warning(f"text delayed for item {hn_id} trying again")
            time.sleep(0.5)
            return download_metadata(hn_id, current_attempt)
    except KeyError:
        if metadata["type"] != "story":
            logger.warning(f"item {metadata} has no key 'text'")
        if metadata.get("deleted"):
            logger.warning(f"item {metadata} was deleted, skipping it")
            return None
    if metadata["type"] in ["story", "comment"]:
        return metadata
    else:
        return None


def download_html(metadata):
    try:
        html = safe_request(metadata["url"])
        return {**metadata, "content": html}
    except KeyError:
        logger.error(f"No url content for {metadata}")
        return None


def recurse_tree(metadata, og_metadata=None) -> any:
    if not og_metadata:
        og_metadata = metadata
    try:
        parent_id = metadata["parent"]
        parent_metadata = download_metadata(parent_id)
        return recurse_tree(parent_metadata[1], og_metadata)
    except KeyError:
        return {**og_metadata, "root_id": metadata["id"]}


def run_hn_flow(init_item=None, polling_interval=15):
    flow = Dataflow("hn_stream")
    inp = op.input(
        "in", flow, HNSource(timedelta(seconds=polling_interval), None, init_item)
    )
    inp = op.redistribute("scaling", inp)
    enriched = op.filter_map("enrich", inp, download_metadata)
    branch_out = op.branch(
        "split_comments", enriched, lambda document: document["type"] == "story"
    )

    # Stories
    stories = branch_out.trues
    stories = op.filter_map("fetch_html", stories, download_html)
    stories = op.filter_map(
        "parse_docs", stories, lambda content: parse_html(content, tokenizer)
    )
    stories = op.map(
        "story_embeddings",
        stories,
        lambda document: hf_document_embed(
            document, tokenizer, model, torch, length=VECTOR_DIMENSIONS
        ),
    )
    # Comments
    comments = branch_out.falses
    comments = op.map("get_story_id", comments, recurse_tree)
    comments = op.map(
        "clean_text", comments, lambda document: prep_text(document, tokenizer)
    )
    comments = op.map(
        "comment_embeddings",
        comments,
        lambda document: hf_document_embed(
            document, tokenizer, model, torch, length=VECTOR_DIMENSIONS
        ),
    )
    op.inspect_debug("stories2", stories)
    op.inspect_debug("comments2", comments)
    op.output(
        "stories_out",
        stories,
        RedisVectorOutput("hn_stories", STORY_SCHEMA, overwrite=True),
    )
    op.output(
        "comments_out",
        comments,
        RedisVectorOutput("hn_comments", COMMENT_SCHEMA, overwrite=True),
    )
    return flow
