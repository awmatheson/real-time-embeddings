VECTOR_DIMENSIONS = 512
STORY_SCHEMA = {
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
        "vector": [
            {
                "name": "doc_embedding",
                "dims": VECTOR_DIMENSIONS,
                "distance_metric": "cosine",
                "algorithm": "flat",
                "datatype": "FLOAT32",
            }
        ],
    },
}

COMMENT_SCHEMA = {
    "index": {
        "name": "comment_index",
        "prefix": "v1",
    },
    "fields": {
        "tag": [{"name": "key_id"}],
        "text": [{"name": "by"}],
        "numeric": [{"name": "id"}],
        "numeric": [{"name": "parent"}],
        "numeric": [{"name": "time"}],
        "text": [{"name": "type"}],
        "text": [{"name": "root_id"}],
        "text": [{"name": "text"}],
        "vector": [
            {
                "name": "doc_embedding",
                "dims": VECTOR_DIMENSIONS,
                "distance_metric": "cosine",
                "algorithm": "flat",
                "datatype": "FLOAT32",
            }
        ],
    },
}
