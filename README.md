# Retrieval Augmented Generation

RAG is a method to insert context into the response loop of a generative AI application. 

## Hackernews RAG pipeline

In this demo we will use Bytewax as the basis to create a pipeline that will retrieve new hackernews posts, parallelize the parsing and creation of embeddings to be fed into Redis, our vector database.

## Setting up Redis

docker run -d --name redis-stack -p 6379:6379 -p 8001:8001 redis/redis-stack:latest

## Setting up our environment

```bash
pip install -r requirements.txt
```

## Run it

```bash
python -m bytewax.run "rt_hackernews_rag:run_hn_flow()"
```

If you are starting with an older item id, you should increase the parallelism and implement a way to batch output with the `batch` operator.

## Custom Connectors

There are two custom connectors in this repository. `redis_connector.py` is vector sink for our embeddings and `hackernews_connector.py` is the hackernews api connector.

## Dataflow

The dataflow has 5 parts. 
Input - stream hackernews updates 
Preprocess - retrieve updates and filter for stories
Retrieve Content - download the html and the parse it into useable text
Vectorize - Create an embedding or list of embeddings for text
Output - write the vectors to Redis and create a new index


