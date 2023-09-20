"""Fetch the home page of hackernews every 15 mins and extract all the links from there."""
from datetime import timedelta
import requests

from bytewax.connectors.periodic import SimplePollingInput
from bytewax.connectors.stdio import StdOutput
from bytewax.dataflow import Dataflow

from webpage import safe_request

from unstructured.partition.html import partition_html
from unstructured.cleaners.core import (
    clean,
    replace_unicode_quotes,
    clean_non_ascii_chars,
)
from unstructured.staging.huggingface import chunk_by_attention_window
from unstructured.staging.huggingface import stage_for_transformers

from transformers import AutoTokenizer, AutoModel
import torch

tokenizer = AutoTokenizer.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")
model = AutoModel.from_pretrained("sentence-transformers/all-MiniLM-L6-v2")


class HNInput(SimplePollingInput):
    def next_item(self):
        # Extract the first 10 item ids from newstories api.
        # You can then use the id to fetch metadata about
        # an hackernews item
        return requests.get(
            "https://hacker-news.firebaseio.com/v0/newstories.json"
        ).json()[:10]


def download_metadata(hn_id):
    # Given an hacker news id returned from the api, fetch metadata
    return requests.get(
        f"https://hacker-news.firebaseio.com/v0/item/{hn_id}.json"
    ).json()


def download_content(metadata):
    print(metadata)
    content = safe_request(metadata["url"])
    # only return a dict with metadata plus the 
    # content of the page if we could get it
    if content:
        return {**metadata, "content": content}
    else: 
        print(f"Couldn't return content from {metadata['url']}")
     

def parse_html(metadata_content, tokenizer):
    try:
        print(f"parsing content from: {metadata_content['url']} - {metadata_content['content'][:200]}")
    except TypeError:
        print(f"moving on... can't parse: {metadata_content}")
        return None
    text = []
    article_elements = partition_html(text=metadata_content["content"])
    article_content = clean_non_ascii_chars(
        replace_unicode_quotes(
            clean(
                " ".join(
                    [
                        str(x) if x.to_dict()["type"] == "NarrativeText" else ""
                        for x in article_elements
                    ]
                )
            )
        )
    )
    text += chunk_by_attention_window(article_content, tokenizer)
    return {**metadata_content, "text": text}

def hf_document_embed(webpage, tokenizer, model, length=512):
    """
    Create an embedding from the provided document
    """
    embeddings = []
    for chunk in webpage["text"]:
        inputs = tokenizer(chunk, padding=True, truncation=True, return_tensors="pt", max_length=length)
        with torch.no_grad():
            embed = model(**inputs).last_hidden_state[:, 0].cpu().detach().numpy()
        embeddings.append(embed.flatten().tolist())
    return {**webpage, "embeddings": embeddings}

flow = Dataflow()
flow.input("in", HNInput(timedelta(minutes=15)))
flow.flat_map(lambda x: x)
# flow.inspect(print)
# If you run this dataflow with multiple workers, downloads in
# the next `map` will be parallelized thanks to .redistribute()
flow.redistribute()
flow.map(download_metadata)
flow.filter(lambda x: "url" in x.keys())
# flow.inspect(print)
flow.map(download_content)
flow.map(lambda x: parse_html(x, tokenizer))
flow.map(lambda x: hf_document_embed(x, tokenizer, model, length=512))
flow.output("out", StdOutput())
# flow.output(RedisVectorStore())