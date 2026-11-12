"""Compress page assets and JSON while preserving downloads and HTTP validators."""
from flask import Flask, Response, request
from flask_compress import Compress


def register_response_compression(app: Flask) -> None:
    app.config.update(
        COMPRESS_REGISTER=False,
        COMPRESS_ALGORITHM=["gzip"],
        COMPRESS_MIN_SIZE=1024,
        COMPRESS_LEVEL=6,
        COMPRESS_STREAMS=False,
        COMPRESS_MIMETYPES=[
            "text/html", "text/css", "text/javascript", "application/javascript",
            "application/json", "image/svg+xml",
        ],
    )
    compressor = Compress(app)

    @app.after_request
    def compress_response(response: Response) -> Response:
        # Downloads/ranges retain their original bytes and length. Only buffered
        # text and root-level frontend assets are eligible for materialization.
        if (response.status_code != 200 or "Range" in request.headers
                or "Content-Range" in response.headers
                or "attachment" in response.headers.get("Content-Disposition", "").lower()
                or "no-transform" in response.headers.get("Cache-Control", "").lower()):
            return response
        if response.mimetype not in compressor.compress_mimetypes_set:
            return response
        if response.direct_passthrough:
            if "/" in request.path.lstrip("/"):
                return response
            response.direct_passthrough = False
            response.set_data(response.get_data())
        return compressor.after_request(response)
