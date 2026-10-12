from __future__ import annotations

import stat
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse


DEFAULT_BASE_URL = "https://openapi.okx.com"
ALLOWED_REST_HOSTS = frozenset({"openapi.okx.com"})


class ConfigurationError(ValueError):
    """Raised when the local secret configuration is unsafe or incomplete."""


def _load_dotenv(path: Path) -> dict[str, str]:
    values: dict[str, str] = {}
    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        if line.startswith("export "):
            line = line[7:].lstrip()
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        values[key] = value
    return values


def _validate_official_base_url(base_url: str) -> str:
    parsed = urlparse(base_url)
    hostname = (parsed.hostname or "").lower()
    if parsed.scheme != "https":
        raise ConfigurationError("OKX_BASE_URL 必须使用 HTTPS")
    if parsed.username or parsed.password or parsed.port:
        raise ConfigurationError("OKX_BASE_URL 不得包含用户信息或自定义端口")
    if hostname not in ALLOWED_REST_HOSTS:
        raise ConfigurationError("拒绝把 OKX 凭据发送到未批准的主机；当前只允许 openapi.okx.com")
    if parsed.path not in {"", "/"} or parsed.query or parsed.fragment:
        raise ConfigurationError("OKX_BASE_URL 只能包含协议和主机名")
    return base_url.rstrip("/")


@dataclass(frozen=True)
class Settings:
    api_key: str
    api_secret: str
    passphrase: str
    demo: bool
    base_url: str = DEFAULT_BASE_URL

    @classmethod
    def from_env_file(cls, path: Path) -> "Settings":
        path = path.expanduser().resolve()
        if not path.is_file():
            raise ConfigurationError(f"找不到凭据文件：{path}")

        mode = stat.S_IMODE(path.stat().st_mode)
        if mode & 0o077:
            raise ConfigurationError(f"凭据文件权限过宽（{mode:o}）；请先执行 chmod 600 {path}")

        values = _load_dotenv(path)
        required = {
            "OKX_API_KEY": values.get("OKX_API_KEY", ""),
            "OKX_API_SECRET": values.get("OKX_API_SECRET", ""),
            "OKX_API_PASSPHRASE": values.get("OKX_API_PASSPHRASE", ""),
        }
        missing = [name for name, value in required.items() if not value]
        if missing:
            raise ConfigurationError("缺少变量：" + ", ".join(missing))

        demo_value = values.get("OKX_DEMO")
        if demo_value is None:
            raise ConfigurationError("必须显式设置 OKX_DEMO=0（实盘）或 1（模拟盘）")
        normalized_demo = demo_value.strip().lower()
        if normalized_demo in {"1", "true", "yes", "demo"}:
            demo = True
        elif normalized_demo in {"0", "false", "no", "live"}:
            demo = False
        else:
            raise ConfigurationError("OKX_DEMO 只能是 0/1、false/true、no/yes 或 live/demo")
        return cls(
            api_key=required["OKX_API_KEY"],
            api_secret=required["OKX_API_SECRET"],
            passphrase=required["OKX_API_PASSPHRASE"],
            demo=demo,
            base_url=_validate_official_base_url(values.get("OKX_BASE_URL", DEFAULT_BASE_URL)),
        )
