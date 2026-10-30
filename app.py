import os
import re
import threading
import time

import requests
import urllib3
from flask import Flask, jsonify, request, send_from_directory

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

app = Flask(__name__)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

HOME = "https://mis.twse.com.tw/stock/index.jsp"
API = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36"
)

CACHE_SECONDS = 3      # 同一組代碼 3 秒內不重複打證交所
MAX_CODES = 30
CODE_RE = re.compile(r"^[0-9A-Za-z]{1,8}$")

http = requests.Session()
http.headers.update(
    {"User-Agent": UA, "Referer": HOME, "Accept-Language": "zh-TW,zh;q=0.9"}
)
verify_tls = True
session_at = 0.0
lock = threading.Lock()
cache = {}


def _get(url, **kw):
    """GET；若證交所憑證在這個環境驗證失敗，退而不驗證（只是公開報價）。"""
    global verify_tls
    try:
        return http.get(url, timeout=10, verify=verify_tls, **kw)
    except requests.exceptions.SSLError:
        verify_tls = False
        return http.get(url, timeout=10, verify=False, **kw)


def ensure_session(force=False):
    """證交所要先開首頁拿 cookie 才給 API 資料，約每 10 分鐘更新一次。"""
    global session_at
    if force or time.time() - session_at > 600:
        _get(HOME)
        session_at = time.time()


def fetch_raw(codes):
    # 不知道是上市還是上櫃，兩邊都問，沒有的那邊會回空
    ex_ch = "|".join(f"{m}_{c}.tw" for c in codes for m in ("tse", "otc"))
    params = {
        "ex_ch": ex_ch,
        "json": "1",
        "delay": "0",
        "_": int(time.time() * 1000),
    }
    r = _get(API, params=params)
    r.raise_for_status()
    return r.json().get("msgArray", [])


def num(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def levels(prices, qtys):
    ps = (prices or "").split("_")
    qs = (qtys or "").split("_")
    out = []
    for i, p in enumerate(ps):
        pv = num(p)
        if pv is None:
            continue
        qv = num(qs[i]) if i < len(qs) else None
        out.append({"p": pv, "q": int(qv) if qv is not None else None})
    return out


def normalize(x):
    price = num(x.get("z"))
    prev = num(x.get("y"))
    chg = pct = None
    if price is not None and prev:
        chg = round(price - prev, 2)
        pct = round((price - prev) / prev * 100, 2)
    vol = num(x.get("v"))
    ex = x.get("ex")
    return {
        "code": x.get("c"),
        "name": x.get("n"),
        "market": "上市" if ex == "tse" else "上櫃" if ex == "otc" else ex,
        "price": price,
        "prev": prev,
        "change": chg,
        "pct": pct,
        "open": num(x.get("o")),
        "high": num(x.get("h")),
        "low": num(x.get("l")),
        "up": num(x.get("u")),
        "down": num(x.get("w")),
        "vol": int(vol) if vol is not None else None,  # 單位：張
        "time": x.get("t"),
        "date": x.get("d"),
        "bids": levels(x.get("b"), x.get("g")),
        "asks": levels(x.get("a"), x.get("f")),
    }


def get_quotes(codes):
    key = ",".join(sorted(codes))
    with lock:  # 一次只讓一個請求去問證交所，其餘直接吃快取
        now = time.time()
        hit = cache.get(key)
        if hit and now - hit[0] < CACHE_SECONDS:
            return hit[1]
        try:
            ensure_session()
            try:
                raw = fetch_raw(codes)
            except Exception:
                ensure_session(force=True)
                raw = fetch_raw(codes)
        except Exception:
            if hit and now - hit[0] < 60:
                return hit[1]
            raise
        items = {}
        for x in raw:
            if x.get("n"):  # 查無此代碼的項目沒有名稱
                it = normalize(x)
                items[it["code"].lower()] = it
        result = list(items.values())
        cache[key] = (time.time(), result)
        if len(cache) > 50:
            cache.pop(min(cache, key=lambda k: cache[k][0]))
        return result


def reply(payload, status=200):
    resp = jsonify(payload)
    resp.status_code = status
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.get("/")
def index():
    return send_from_directory(BASE_DIR, "index.html")


@app.get("/healthz")
def healthz():
    return "ok"


@app.get("/api/quotes")
def api_quotes():
    codes = []
    for c in request.args.get("codes", "").split(","):
        c = c.strip()
        if not c:
            continue
        if not CODE_RE.match(c):
            return reply({"ok": False, "error": f"代碼格式不對：{c}"}, 400)
        c = "t00" if c.lower() == "t00" else c.upper()  # t00 = 加權指數
        if c not in codes:
            codes.append(c)
    if not codes:
        return reply({"ok": True, "items": [], "missing": []})
    if len(codes) > MAX_CODES:
        return reply({"ok": False, "error": f"一次最多 {MAX_CODES} 檔"}, 400)
    try:
        items = get_quotes(codes)
    except Exception as e:
        return reply({"ok": False, "error": f"證交所連線失敗（{type(e).__name__}）"}, 502)
    got = {it["code"].lower() for it in items}
    missing = [c for c in codes if c.lower() not in got]
    return reply({"ok": True, "items": items, "missing": missing})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
