from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
import time
from copy import deepcopy
from typing import Any, Callable


def app_base_dir() -> str:
    if getattr(sys, "frozen", False):
        return os.path.dirname(os.path.abspath(sys.executable))
    return os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def default_config_path() -> str:
    return os.path.join(app_base_dir(), "config.json")


CONFIG_PATH = default_config_path()
_LAST_CONFIG_ERROR: str = ""

DEFAULT_CONFIG: dict[str, Any] = {
    "alert_spread": 1.0,
    "sound_path": "",
    "main_top_n": 100,
    "detail_top_n": 50,
    "proxy": "",
    "websocket_proxy": "auto",
    "request_retries": 3,
    "retry_delay_sec": 1.0,
    "history_db_path": "spread_history.sqlite3",
}


def _merged_config(data: dict[str, Any]) -> dict[str, Any]:
    merged = deepcopy(DEFAULT_CONFIG)
    values = dict(data)
    if "top_n" in values and "detail_top_n" not in values:
        values["detail_top_n"] = values["top_n"]
    merged.update(values)
    return merged


def get_config_path() -> str:
    return CONFIG_PATH


def get_config_error() -> str:
    return _LAST_CONFIG_ERROR


def _set_config_error(message: str) -> None:
    global _LAST_CONFIG_ERROR
    _LAST_CONFIG_ERROR = message
    logging.error(message)
    print(message)


def _clear_config_error() -> None:
    global _LAST_CONFIG_ERROR
    _LAST_CONFIG_ERROR = ""


def _read_config_file() -> tuple[dict[str, Any], bool]:
    if not os.path.exists(CONFIG_PATH):
        _clear_config_error()
        return {}, True
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
    except Exception as exc:
        _set_config_error(f"Config load error: {CONFIG_PATH}: {exc}")
        return {}, False
    if not isinstance(data, dict):
        _set_config_error(f"Config load error: {CONFIG_PATH}: root value must be a JSON object")
        return {}, False
    _clear_config_error()
    return data, True


def ensure_config() -> dict[str, Any]:
    if not os.path.exists(CONFIG_PATH):
        save_config(DEFAULT_CONFIG)
        return deepcopy(DEFAULT_CONFIG)
    data, ok = _read_config_file()
    cfg = _merged_config(data)
    if ok:
        save_config(cfg)
    return cfg


def load_config() -> dict[str, Any]:
    data, _ok = _read_config_file()
    return _merged_config(data)


def save_config(data: dict[str, Any]) -> None:
    try:
        existing, ok = _read_config_file()
        if not ok:
            existing = {}
        existing.update(data)
        cfg = _merged_config(existing)
        with open(CONFIG_PATH, "w", encoding="utf-8") as f:
            json.dump(cfg, f, ensure_ascii=False, indent=2)
    except Exception as exc:
        print(f"Config save error: {exc}")


def get_int_config(key: str, default: int) -> int:
    value = load_config().get(key, default)
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return default


def get_float_config(key: str, default: float) -> float:
    value = load_config().get(key, default)
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def project_path(path: str) -> str:
    if os.path.isabs(path):
        return path
    return os.path.join(app_base_dir(), path)


def get_history_db_path() -> str:
    value = str(load_config().get("history_db_path", DEFAULT_CONFIG["history_db_path"])).strip()
    return project_path(value or DEFAULT_CONFIG["history_db_path"])


def get_proxy_url(kind: str = "rest") -> str:
    cfg = load_config()
    if kind == "websocket":
        ws_value = str(cfg.get("websocket_proxy", "auto")).strip()
        if ws_value.lower() in {"", "none", "off", "direct", "no"}:
            return ""
        if ws_value.lower() != "auto":
            return ws_value
    value = cfg.get("proxy", "")
    return str(value).strip() if value else ""


def masked_proxy_url(proxy: str | None = None, kind: str = "rest") -> str:
    proxy = get_proxy_url(kind) if proxy is None else proxy
    if not proxy:
        return "нет"
    return re.sub(r"://([^:@/\s]+):([^@/\s]+)@", "://***:***@", proxy)


def mask_sensitive_text(text: str) -> str:
    for kind in ("rest", "websocket"):
        proxy = get_proxy_url(kind)
        if proxy:
            text = text.replace(proxy, masked_proxy_url(proxy))
    return re.sub(r"://([^:@/\s]+):([^@/\s]+)@", "://***:***@", text)


def proxy_mode_label(kind: str = "rest") -> str:
    proxy = get_proxy_url(kind)
    if not proxy:
        return f"{kind}=direct"
    scheme = proxy.split("://", 1)[0] if "://" in proxy else "unknown"
    return f"{kind}={scheme} ({masked_proxy_url(proxy)})"


def apply_proxy_options(config: dict[str, Any], kind: str = "rest") -> dict[str, Any]:
    proxy = get_proxy_url(kind)
    if not proxy:
        return config

    cfg = dict(config)
    lower = proxy.lower()
    if lower.startswith(("socks://", "socks4://", "socks5://")):
        if kind == "websocket":
            cfg["wsSocksProxy"] = proxy
        else:
            cfg["socksProxy"] = proxy
    elif lower.startswith("https://"):
        if kind == "websocket":
            cfg["wssProxy"] = proxy
        else:
            cfg["httpsProxy"] = proxy
    else:
        if kind == "websocket":
            cfg["wsProxy"] = proxy
        else:
            cfg["httpProxy"] = proxy
    return cfg


def ccxt_config(
    options: dict[str, Any] | None = None,
    timeout: int | None = None,
    proxy_kind: str = "rest",
) -> dict[str, Any]:
    cfg: dict[str, Any] = {"enableRateLimit": True}
    if timeout is not None:
        cfg["timeout"] = timeout
    if options is not None:
        cfg["options"] = options
    return apply_proxy_options(cfg, proxy_kind)


def human_error(exc: Exception) -> str:
    name = type(exc).__name__
    text = mask_sensitive_text(str(exc))
    lower = text.lower()

    if name == "IncompleteReadError" or "0 bytes read on a total of 2 expected bytes" in lower:
        return (
            "соединение оборвалось во время TLS/прокси handshake "
            "(часто неверная схема proxy, плохой порт или прокси не поддерживает CONNECT/WSS)"
        )
    if "400" in lower and "bad request" in lower and "url='http" in lower:
        return "HTTP-прокси вернул 400 на WebSocket/CONNECT; REST может работать, но WSS через этот прокси не поддерживается"
    if "decryption_failed_or_bad_record_mac" in lower or "bad record mac" in lower:
        return "TLS-соединение повреждено/разорвано прокси; часто признак нестабильного или неподходящего прокси"
    if "����" in text:
        return "соединение было принудительно закрыто удалённой стороной или прокси"
    if "unexpected socks version" in lower:
        return "порт не похож на SOCKS-прокси; попробуйте схему http:// для этого host:port"
    if "to use socks proxy" in lower and "aiohttp_socks" in lower:
        return "для SOCKS-прокси не установлена зависимость aiohttp-socks"
    if name in {"TimeoutError", "RequestTimeout"} or "timed out" in lower or "timeout" in lower:
        return "таймаут сетевого запроса"
    if name in {"ProxyError", "InvalidProxySettings"}:
        return f"ошибка настроек/работы прокси: {text}"
    if text:
        return f"{name}: {text}"
    return name


def format_network_error(source: str, stage: str, exc: Exception, proxy_kind: str = "rest") -> str:
    return f"{source} [{stage}] {proxy_mode_label(proxy_kind)}: {human_error(exc)}"


async def async_retry(coro_factory: Callable[[], Any], what: str):
    retries = max(1, get_int_config("request_retries", DEFAULT_CONFIG["request_retries"]))
    delay = max(0.0, get_float_config("retry_delay_sec", DEFAULT_CONFIG["retry_delay_sec"]))
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            return await coro_factory()
        except Exception as exc:
            last_exc = exc
            if attempt + 1 < retries:
                await asyncio.sleep(delay * (attempt + 1))
    assert last_exc is not None
    raise RuntimeError(f"{what}: {last_exc}") from last_exc


def sync_retry(call_factory: Callable[[], Any], what: str):
    retries = max(1, get_int_config("request_retries", DEFAULT_CONFIG["request_retries"]))
    delay = max(0.0, get_float_config("retry_delay_sec", DEFAULT_CONFIG["retry_delay_sec"]))
    last_exc: Exception | None = None
    for attempt in range(retries):
        try:
            return call_factory()
        except Exception as exc:
            last_exc = exc
            if attempt + 1 < retries:
                time.sleep(delay * (attempt + 1))
    assert last_exc is not None
    raise RuntimeError(f"{what}: {last_exc}") from last_exc
