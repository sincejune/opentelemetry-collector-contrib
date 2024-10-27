import json

from elasticsearch import Elasticsearch
from fastapi import FastAPI, HTTPException
from starlette.responses import PlainTextResponse

es = Elasticsearch(["http://localhost:9200"])

app = FastAPI()


# Perform the search query
@app.get("/query")
async def query():
    query_body = {
        "query": {
            "term": {
                "isplanquery": "no"
            }
        },
        "aggs": {
            "group_by_query_hash": {
                "terms": {
                    "field": "query_hash",
                    "size": 10
                }
            }
        }
    }
    response = es.search(index="metrics-sqlserverreceiver-default", body=query_body)

    # Print the aggregation results
    print("Aggregation Results:")
    for bucket in response["aggregations"]["group_by_query_hash"]["buckets"]:
        print(bucket)
        print(f"query_hash: {bucket['key']}, Doc Count: {bucket['doc_count']}")

    # Print the document hits
    print("Document Hits:")
    for hit in response["hits"]["hits"]:
        print(hit["_source"])
    return [x['key'] for x in response["aggregations"]["group_by_query_hash"]["buckets"]]


@app.get("/query/{query_hash}", response_class=PlainTextResponse)
async def query(query_hash: str):
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"isplanquery": "no"}},
                    {"term": {"query_hash": query_hash}}
                ]
            }
        },
        "size": 1
    }
    try:
        response = es.search(index="metrics-sqlserverreceiver-default", body=query_body)
        if response["hits"]["hits"]:
            return response["hits"]["hits"][0]["_source"]["statement"]
        else:
            return {"message": "No documents found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/queryplan/{query_hash}", response_class=PlainTextResponse)
async def query(query_hash: str):
    query_body = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"isplanquery": "yes"}},
                    {"term": {"query_hash": query_hash}}
                ]
            }
        },
        "size": 1
    }
    try:
        response = es.search(index="metrics-sqlserverreceiver-default", body=query_body)
        if response["hits"]["hits"]:
            return response["hits"]["hits"][0]["_source"]["query_plan"]
        else:
            return {"message": "No documents found"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
