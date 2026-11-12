"""HTTP content negotiation must preserve values, validators and file bytes."""
import gzip
import json
from flask import Flask, jsonify, send_file
import pytest

from backend.http_compression import register_response_compression


@pytest.fixture
def client(tmp_path):
    app = Flask(__name__)
    register_response_compression(app)
    asset = tmp_path / "asset.js"
    asset.write_text("// isolated fixture\n" * 200)

    @app.get("/asset.js")
    def static_asset():
        response = send_file(asset)
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
        return response

    @app.get("/api/fixture")
    def api_fixture():
        return jsonify(missing=None, zero=0, text="synthetic transport fixture" * 200)

    @app.get("/download")
    def download():
        return send_file(asset, as_attachment=True)

    return app.test_client()


def test_json_negotiation_preserves_null_and_zero(client):
    plain = client.get("/api/fixture", headers={"Accept-Encoding": "identity"})
    compressed = client.get("/api/fixture", headers={"Accept-Encoding": "gzip"})
    assert compressed.headers["Content-Encoding"] == "gzip"
    assert "Accept-Encoding" in compressed.headers["Vary"]
    assert gzip.decompress(compressed.data) == plain.data
    assert len(compressed.data) < len(plain.data) / 2
    values = json.loads(gzip.decompress(compressed.data))
    assert values["missing"] is None and values["zero"] == 0
    refused = client.get("/api/fixture", headers={"Accept-Encoding": "gzip;q=0, identity"})
    assert "Content-Encoding" not in refused.headers
    assert refused.data == plain.data


def test_asset_encoding_has_own_validator_and_conditional_head(client):
    plain = client.get("/asset.js")
    compressed = client.get("/asset.js", headers={"Accept-Encoding": "gzip"})
    assert gzip.decompress(compressed.data) == plain.data
    assert compressed.headers["ETag"] != plain.headers["ETag"]
    headers = {"Accept-Encoding": "gzip", "If-None-Match": compressed.headers["ETag"]}
    conditional = client.get("/asset.js", headers=headers)
    assert conditional.status_code == 304 and conditional.data == b""
    assert conditional.headers["ETag"] == compressed.headers["ETag"]
    head = client.head("/asset.js", headers={"Accept-Encoding": "gzip"})
    assert head.data == b"" and head.headers["Content-Length"] == compressed.headers["Content-Length"]
    assert head.headers["ETag"] == compressed.headers["ETag"]
    assert head.headers["Cache-Control"] == plain.headers["Cache-Control"]


def test_download_and_range_preserve_original_bytes(client):
    plain = client.get("/asset.js")
    download = client.get("/download", headers={"Accept-Encoding": "gzip"})
    assert "Content-Encoding" not in download.headers and download.data == plain.data
    ranged = client.get("/asset.js", headers={"Accept-Encoding": "gzip", "Range": "bytes=0-15"})
    assert ranged.status_code == 206 and ranged.data == plain.data[:16]
    assert "Content-Encoding" not in ranged.headers
