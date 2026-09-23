"""请求体大小上限（纯 ASGI 中间件，在路由和鉴权之前生效）。

FastAPI 在执行鉴权依赖之前就会读完并解析 JSON 请求体：没有上限时，不带 key 的请求也能让副本缓冲任意大的请求体。
上传文件的接口（PUT /v1/leases/{id}/files）先鉴权、再由路由按 max_upload_bytes 流式计数，这里放行。
"""

import re

from starlette.datastructures import Headers
from starlette.exceptions import HTTPException
from starlette.responses import JSONResponse
from starlette.types import ASGIApp, Message, Receive, Scope, Send

_UPLOAD_PATH = re.compile(r"^/v1/leases/[^/]+/files$")


class BodyTooLarge(HTTPException):
    """流式读取时超出上限。必须是 HTTPException：从 receive 抛出的其他异常会被 FastAPI 转成 400。"""

    def __init__(self, limit: int):
        super().__init__(status_code=413, detail=f"request body exceeds {limit} bytes")


def too_large_response(detail: str) -> JSONResponse:
    return JSONResponse(status_code=413, content={"error": "PayloadTooLarge", "detail": detail})


class BodyLimitMiddleware:
    def __init__(self, app: ASGIApp, max_body_bytes: int):
        self.app = app
        self.limit = max_body_bytes

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http" or (scope["method"] == "PUT" and _UPLOAD_PATH.match(scope["path"])):
            await self.app(scope, receive, send)
            return
        declared = Headers(scope=scope).get("content-length")
        if declared is not None and declared.isdigit() and int(declared) > self.limit:
            # 声明的长度已超限：不读请求体，直接拒绝
            await too_large_response(f"request body exceeds {self.limit} bytes")(scope, receive, send)
            return
        received = 0

        async def limited_receive() -> Message:
            nonlocal received
            message = await receive()
            if message["type"] == "http.request":
                received += len(message.get("body", b""))
                if received > self.limit:
                    raise BodyTooLarge(self.limit)
            return message

        await self.app(scope, limited_receive, send)
