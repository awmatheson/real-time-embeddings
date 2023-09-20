# Retrieval Augmented Generation

TODO: Intro to RAG and importance of real-time

## Hackernews RAG pipeline

In this demo we will use Bytewax as the basis to create a pipeline that will retrieve new hackernews posts, parallelize the parsing and creation of embeddings to be fed into Redis, our vector database.

-- ![image of pipeline]

## Setting up Redis

## Setting up our environment

```
pip install -r requirements.txt
```

## Run it

```
python -m bytewax.run rt_hackernews_rag:flow -p 3
```
