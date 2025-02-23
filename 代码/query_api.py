
from fastapi import FastAPI, HTTPException, Query
from elasticsearch import Elasticsearch
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Web数据查询API")

# 允许跨域请求
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

class SearchResult(BaseModel):
    url: str
    content: str
    timestamp: datetime
    score: float

class SearchResponse(BaseModel):
    total: int
    took_ms: int
    results: List[SearchResult]

class ErrorResponse(BaseModel):
    detail: str

# 继承原有ESClient并扩展搜索功能
class QueryESClient:
    def __init__(self):
        self.elastic = Elasticsearch(
            hosts=["http://kafka:9200"],
            verify_certs=False,
            ssl_show_warn=False
        )
        self.index_alias = "web_data_current"  # 使用固定别名

    def search(self, query: str,
              page: int = 1,
              size: int = 10,
              start_time: Optional[datetime] = None,
              end_time: Optional[datetime] = None):
        """
        执行Elasticsearch搜索
        """
        # 构建查询DSL
        must_clauses = [{
            "multi_match": {
                "query": query,
                "fields": ["content^3", "url^2"],  # 内容权重更高
                "fuzziness": "AUTO"
            }
        }]

        # 时间范围过滤
        filter_clauses = []
        if start_time or end_time:
            time_range = {}
            if start_time:
                time_range["gte"] = start_time.isoformat()
            if end_time:
                time_range["lte"] = end_time.isoformat()
            filter_clauses.append({"range": {"timestamp": time_range}})

        body = {
            "query": {
                "bool": {
                    "must": must_clauses,
                    "filter": filter_clauses
                }
            },
            "highlight": {
                "fields": {
                    "content": {"number_of_fragments": 3}
                }
            },
            "from": (page - 1) * size,
            "size": size
        }

        try:
            response = self.elastic.search(
                index=self.index_alias,
                body=body
            )
            return self._format_response(response)
        except Exception as e:
            raise RuntimeError(f"搜索失败: {str(e)}")

    def _format_response(self, es_response):
        """格式化ES响应"""
        return {
            "total": es_response["hits"]["total"]["value"],
            "took_ms": es_response["took"],
            "results": [
                {
                    "url": hit["_source"]["url"],
                    "content": hit["_source"]["content"],
                    "timestamp": hit["_source"]["timestamp"],
                    "score": hit["_score"],
                    # 如果需要高亮显示可以添加：
                    # "highlight": hit.get("highlight", {})
                }
                for hit in es_response["hits"]["hits"]
            ]
        }

# 依赖注入客户端实例
es_client = QueryESClient()

@app.get("/search",
         response_model=SearchResponse,
         responses={500: {"model": ErrorResponse}},
         tags=["Search"],
         summary="全文搜索",
         description="根据关键词搜索已索引的网页内容")
async def search(
        q: str = Query(
            ...,
            min_length=2,
            examples={
                "normal": {
                    "summary": "典型搜索示例",
                    "value": "人工智能"
                },
                "empty": {
                    "summary": "空查询示例",
                    "value": ""
                }
            }
        ),
    page: int = Query(1, ge=1),
    size: int = Query(10, ge=1, le=100),
    start_time: Optional[datetime] = None,
    end_time: Optional[datetime] = None
):
    try:
        result = es_client.search(
            query=q,
            page=page,
            size=size,
            start_time=start_time,
            end_time=end_time
        )
        return result
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@app.get("/_health", tags=["Monitoring"])
def health_check():
    return {"status": "ok", "timestamp": datetime.now()}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
