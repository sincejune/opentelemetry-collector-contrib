import os

from elasticsearch import Elasticsearch
from fastapi import FastAPI, HTTPException
from starlette.responses import PlainTextResponse

ES_HOSTS = os.getenv("ES_HOSTS", "http://localhost:9200")
es = Elasticsearch([ES_HOSTS])

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
                    "size": 50
                }
            }
        }
    }
    response = es.search(index="metrics-sqlserverreceiver-default", body=query_body)

    # Print the aggregation results
    data = {}
    for bucket in response["aggregations"]["group_by_query_hash"]["buckets"]:
        query_hash = bucket['key']
        if query_hash == "" or query_hash is None:
            continue
        q2 = {
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
        response2 = es.search(index="metrics-sqlserverreceiver-default", body=q2)
        statement = response2["hits"]["hits"][0]["_source"]["statement"]
        data[query_hash] = statement

    return data

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



@app.get("/queryplan")
async def queryplan():
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
                    "size": 50
                }
            }
        }
    }
    response = es.search(index="metrics-sqlserverreceiver-default", body=query_body)

    # Print the aggregation results
    data = {}
    for bucket in response["aggregations"]["group_by_query_hash"]["buckets"]:
        query_hash = bucket['key']
        if query_hash == "" or query_hash is None:
            continue
        q2 = {
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
        response2 = es.search(index="metrics-sqlserverreceiver-default", body=q2)
        statement = response2["hits"]["hits"][0]["_source"]["statement"]

        q3 = {
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

        response3 = es.search(index="metrics-sqlserverreceiver-default", body=q3)
        query_plan = response3["hits"]["hits"][0]["_source"]["query_plan"]
        data[query_hash] = {"statement": statement, "query_plan": query_plan}

    return data


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


if __name__ == '__main__':
    import uvicorn

    uvicorn.run(app, host="127.0.0.1")
