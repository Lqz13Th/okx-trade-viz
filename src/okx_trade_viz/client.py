from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timedelta, timezone
from typing import Any, Mapping

from .config import Settings


PRIVATE_GET_PATHS = frozenset(
    {
        "/api/v5/account/bills",
        "/api/v5/account/bills-archive",
        "/api/v5/account/config",
        "/api/v5/account/positions",
        "/api/v5/trade/fills",
        "/api/v5/trade/fills-history",
        "/api/v5/trade/order",
        "/api/v5/trade/order-algo",
        "/api/v5/trade/orders-algo-history",
        "/api/v5/trade/orders-algo-pending",
        "/api/v5/trade/orders-history",
        "/api/v5/trade/orders-history-archive",
        "/api/v5/trade/orders-pending",
    }
)
PUBLIC_GET_PATHS = frozenset(
    {
        "/api/v5/market/history-candles",
        "/api/v5/public/instruments",
        "/api/v5/public/time",
    }
)
RETRYABLE_OKX_CODES = frozenset({"50004", "50011", "50013", "50040"})


class _NoRedirectHandler(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # noqa: ANN001, ANN201
        return None


class OKXAPIError(RuntimeError):
    def __init__(self, code: str, message: str, http_status: int | None = None):
        self.code = code
        self.http_status = http_status
        super().__init__(f"OKX error {code}: {message}")


class ReadOnlyOKXClient:
    """A deliberately GET-only OKX V5 client with an endpoint allowlist."""

    def __init__(self, settings: Settings, timeout: float = 20.0):
        self.settings = settings
        self.timeout = timeout
        self._clock_offset_ms = 0
        self._opener = urllib.request.build_opener(_NoRedirectHandler())

    @staticmethod
    def _query_string(params: Mapping[str, Any] | None) -> str:
        if not params:
            return ""
        clean = [(key, str(value)) for key, value in params.items() if value is not None]
        return urllib.parse.urlencode(clean)

    def _timestamp(self) -> str:
        now = datetime.now(timezone.utc) + timedelta(milliseconds=self._clock_offset_ms)
        return now.isoformat(timespec="milliseconds").replace("+00:00", "Z")

    def _headers(self, request_path: str, authenticated: bool) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "User-Agent": "okx-trade-viz/0.1",
        }
        if not authenticated:
            return headers

        timestamp = self._timestamp()
        payload = f"{timestamp}GET{request_path}"
        signature = base64.b64encode(
            hmac.new(
                self.settings.api_secret.encode(),
                payload.encode(),
                hashlib.sha256,
            ).digest()
        ).decode()
        headers.update(
            {
                "OK-ACCESS-KEY": self.settings.api_key,
                "OK-ACCESS-SIGN": signature,
                "OK-ACCESS-TIMESTAMP": timestamp,
                "OK-ACCESS-PASSPHRASE": self.settings.passphrase,
            }
        )
        if self.settings.demo:
            headers["x-simulated-trading"] = "1"
        return headers

    def get(
        self,
        path: str,
        params: Mapping[str, Any] | None = None,
        *,
        authenticated: bool = True,
        retries: int = 3,
    ) -> list[dict[str, Any]]:
        allowed = PRIVATE_GET_PATHS if authenticated else PUBLIC_GET_PATHS
        if path not in allowed:
            raise ValueError(f"只读接口不在白名单中：{path}")

        query = self._query_string(params)
        request_path = path + (f"?{query}" if query else "")

        for attempt in range(retries):
            # Rebuild headers on every attempt so a retry never reuses an old
            # timestamp or signature.
            request = urllib.request.Request(
                self.settings.base_url + request_path,
                headers=self._headers(request_path, authenticated),
                method="GET",
            )
            try:
                with self._opener.open(request, timeout=self.timeout) as response:
                    payload = json.loads(response.read().decode("utf-8"))
            except urllib.error.HTTPError as exc:
                try:
                    payload = json.loads(exc.read().decode("utf-8"))
                except (json.JSONDecodeError, UnicodeDecodeError):
                    payload = {}
                code = str(payload.get("code", exc.code))
                message = str(payload.get("msg", "HTTP request failed"))
                if exc.code in {429, 500, 502, 503, 504} and attempt + 1 < retries:
                    time.sleep(0.5 * (2**attempt))
                    continue
                raise OKXAPIError(code, message, exc.code) from None
            except (urllib.error.URLError, TimeoutError, OSError) as exc:
                if attempt + 1 < retries:
                    time.sleep(0.5 * (2**attempt))
                    continue
                raise ConnectionError(f"无法连接 OKX：{type(exc).__name__}") from None
            except json.JSONDecodeError:
                raise OKXAPIError("INVALID_JSON", "OKX 返回了无法解析的响应") from None

            code = str(payload.get("code", ""))
            if code != "0":
                if code == "50102" and authenticated and attempt + 1 < retries:
                    self.sync_clock()
                    continue
                if code in RETRYABLE_OKX_CODES and attempt + 1 < retries:
                    time.sleep(0.5 * (2**attempt))
                    continue
                raise OKXAPIError(code or "UNKNOWN", str(payload.get("msg", "request failed")))
            rows = payload.get("data", [])
            if not isinstance(rows, list):
                raise OKXAPIError("INVALID_DATA", "OKX data 字段不是数组")
            return [row for row in rows if isinstance(row, dict)]

        raise AssertionError("unreachable")

    def sync_clock(self) -> int:
        rows = self.get("/api/v5/public/time", authenticated=False)
        if not rows or not str(rows[0].get("ts", "")).isdigit():
            raise OKXAPIError("INVALID_TIME", "OKX 时间接口缺少 ts")
        server_ms = int(rows[0]["ts"])
        local_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
        self._clock_offset_ms = server_ms - local_ms
        return self._clock_offset_ms
