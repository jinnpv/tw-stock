import csv
import io
import json
import os
import re
import threading
import time
from datetime import datetime, timedelta, timezone

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

CACHE_SECONDS = 3       # 同一檔 3 秒內不重複問證交所
CHUNK = 50              # 每次最多問 50 檔
MAX_CODES = 800
MAX_POLL = 150          # 背景最多同時記錄幾檔走勢
CODE_RE = re.compile(r"^[0-9A-Za-z]{1,8}$")
TZ = timezone(timedelta(hours=8))

http = requests.Session()
http.headers.update(
    {"User-Agent": UA, "Referer": HOME, "Accept-Language": "zh-TW,zh;q=0.9"}
)
verify_tls = True
session_at = 0.0

fetch_lock = threading.Lock()
meta_lock = threading.Lock()
hist_lock = threading.Lock()
watch_lock = threading.Lock()

quote_cache = {}   # code(lower) -> (ts, item | None)
history = {}       # code(lower) -> {"date": "YYYYMMDD", "pts": {minute_idx: price}}
watch = {}         # code -> (last_seen_ts, pinned)
meta = {}          # code(upper) -> {"n": 名稱, "ind": 產業別, "ex": "tse" | "otc"}
meta_state = {"ok": False, "at": 0.0}
ind_cache = {"at": 0.0, "data": []}

IND_NAMES = {
    "01": "水泥工業", "02": "食品工業", "03": "塑膠工業", "04": "紡織纖維",
    "05": "電機機械", "06": "電器電纜", "08": "玻璃陶瓷", "09": "造紙工業",
    "10": "鋼鐵工業", "11": "橡膠工業", "12": "汽車工業", "14": "建材營造",
    "15": "航運業", "16": "觀光餐旅", "17": "金融保險", "18": "貿易百貨",
    "19": "綜合", "20": "其他", "21": "化學工業", "22": "生技醫療業",
    "23": "油電燃氣業", "24": "半導體業", "25": "電腦及週邊設備業",
    "26": "光電業", "27": "通信網路業", "28": "電子零組件業",
    "29": "電子通路業", "30": "資訊服務業", "31": "其他電子業",
    "32": "文化創意業", "33": "農業科技業", "34": "電子商務",
    "35": "綠能環保", "36": "數位雲端", "37": "運動休閒", "38": "居家生活",
}


# ---------------------------------------------------------------- 網路
def _get(url, **kw):
    """GET；若憑證在這個環境驗證失敗，退而不驗證（都是公開資料）。"""
    global verify_tls
    try:
        return http.get(url, timeout=12, verify=verify_tls, **kw)
    except requests.exceptions.SSLError:
        verify_tls = False
        return http.get(url, timeout=12, verify=False, **kw)


def ensure_session(force=False):
    global session_at
    if force or time.time() - session_at > 600:
        _get(HOME)
        session_at = time.time()


def tw_now():
    return datetime.now(TZ)


def market_open():
    n = tw_now()
    if n.weekday() >= 5:
        return False
    m = n.hour * 60 + n.minute
    return 535 <= m <= 811  # 08:55–13:31


# ---------------------------------------------------------------- 公司基本資料 / 產業別
def _pick(d, *names):
    for k in names:
        v = d.get(k)
        if v not in (None, ""):
            return str(v).strip()
    return ""


def _rows(url):
    r = _get(url)
    r.raise_for_status()
    if url.endswith(".csv"):
        text = r.content.decode("utf-8-sig", errors="replace")
        return list(csv.DictReader(io.StringIO(text)))
    return r.json()


def load_meta():
    sources = [
        ("tse", ["https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
                 "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"]),
        ("otc", ["https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O",
                 "https://mopsfin.twse.com.tw/opendata/t187ap03_O.csv"]),
    ]
    new = {}
    for ex, urls in sources:
        for u in urls:
            try:
                n = 0
                for d in _rows(u):
                    code = _pick(d, "公司代號", "SecuritiesCompanyCode", "Code")
                    if not code or not CODE_RE.match(code):
                        continue
                    name = _pick(d, "公司簡稱", "CompanyAbbreviation", "公司名稱", "CompanyName")
                    ind = _pick(d, "產業別", "SecuritiesIndustryCode", "Industry")
                    new[code.upper()] = {"n": name, "ind": ind, "ex": ex}
                    n += 1
                if n:
                    break
            except Exception:
                continue
    if new:
        with meta_lock:
            meta.clear()
            meta.update(new)
        meta_state.update(ok=True, at=time.time())
        ind_cache["at"] = 0.0


def meta_loop():
    while True:
        try:
            load_meta()
        except Exception:
            pass
        time.sleep(43200 if meta_state["ok"] else 60)


def build_industries():
    if ind_cache["data"] and time.time() - ind_cache["at"] < 600:
        return ind_cache["data"]
    with meta_lock:
        items = list(meta.items())
    buckets = {}
    for code, m in items:
        raw = (m.get("ind") or "").strip()
        if raw.isdigit():
            k = raw.zfill(2)
            name = IND_NAMES.get(k, raw)
        else:
            k, name = "99", (raw or "其他")
        b = buckets.setdefault(name, {"k": k, "codes": []})
        b["codes"].append(code)
    out = []
    for name, b in sorted(buckets.items(), key=lambda kv: (kv[1]["k"], kv[0])):
        if name in ("80", "管理股票"):
            continue
        out.append({"id": "i:" + name, "name": name, "codes": sorted(b["codes"])})
    ind_cache.update(at=time.time(), data=out)
    return out


def load_concepts():
    try:
        with open(os.path.join(BASE_DIR, "groups.json"), encoding="utf-8") as f:
            data = json.load(f)
    except Exception:
        return []
    out = []
    for g in data.get("groups", []):
        name = str(g.get("name", "")).strip()
        codes = []
        for c in g.get("codes", []):
            c = str(c).strip().upper()
            if CODE_RE.match(c) and c not in codes:
                codes.append(c)
        if name and codes:
            out.append({"id": "c:" + name, "name": name, "codes": codes})
    return out


# ---------------------------------------------------------------- 報價
def channels(code):
    if code.lower() == "t00":
        return ["tse_t00.tw"]
    m = meta.get(code.upper())
    if m:
        return [f"{m['ex']}_{code.upper()}.tw"]
    return [f"tse_{code}.tw", f"otc_{code}.tw"]  # 不知道市場就兩邊都問


def fetch_raw(codes):
    ex_ch = "|".join(ch for c in codes for ch in channels(c))
    params = {"ex_ch": ex_ch, "json": "1", "delay": "0", "_": int(time.time() * 1000)}
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
    tv = num(x.get("tv"))
    ex = x.get("ex")
    bids = levels(x.get("b"), x.get("g"))
    asks = levels(x.get("a"), x.get("f"))
    side = None  # 單量顏色：外盤(紅) / 內盤(綠)，由最新成交價與買賣一檔推測
    if price is not None and bids and asks:
        if price >= asks[0]["p"]:
            side = "out"
        elif price <= bids[0]["p"]:
            side = "in"
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
        "vol": int(vol) if vol is not None else None,     # 總量（張）
        "lastq": int(tv) if tv is not None else None,     # 單量（張）
        "side": side,
        "time": x.get("t"),
        "date": x.get("d"),
        "bids": bids,
        "asks": asks,
    }


def record_history(it):
    p, t, d = it.get("price"), it.get("time"), it.get("date")
    if p is None or not t or not d:
        return
    try:
        hh, mm, _ = t.split(":")
        idx = int(hh) * 60 + int(mm) - 540
    except Exception:
        return
    if idx < 0:
        return
    idx = min(idx, 270)
    code = it["code"].lower()
    with hist_lock:
        h = history.get(code)
        if not h or h["date"] != d:
            h = history[code] = {"date": d, "pts": {}}
        h["pts"][idx] = p


def series_for(code):
    with hist_lock:
        h = history.get(code.lower())
        if not h or not h["pts"]:
            return None
        pts = dict(h["pts"])
        date = h["date"]
    out, cur = [], None
    for i in range(max(pts) + 1):
        if i in pts:
            cur = pts[i]
        out.append(cur)
    return {"d": date, "p": out}


def fresh(code, now):
    ent = quote_cache.get(code.lower())
    return bool(ent) and now - ent[0] < CACHE_SECONDS


def get_quotes(codes):
    now = time.time()
    need = [c for c in codes if not fresh(c, now)]
    last_err = None
    if need:
        with fetch_lock:  # 一次只讓一個請求去問證交所，其餘吃快取
            now = time.time()
            need = [c for c in need if not fresh(c, now)]
            for i in range(0, len(need), CHUNK):
                part = need[i:i + CHUNK]
                try:
                    ensure_session()
                    try:
                        raw = fetch_raw(part)
                    except Exception:
                        ensure_session(force=True)
                        raw = fetch_raw(part)
                except Exception as e:
                    last_err = e
                    break
                got = {}
                for x in raw:
                    if x.get("n"):  # 查無此代碼的項目沒有名稱
                        it = normalize(x)
                        got[it["code"].lower()] = it
                ts = time.time()
                for c in part:
                    it = got.get(c.lower())
                    quote_cache[c.lower()] = (ts, it)
                    if it:
                        record_history(it)
                if i + CHUNK < len(need):
                    time.sleep(0.3)
    out = []
    for c in codes:
        ent = quote_cache.get(c.lower())
        if ent and ent[1]:
            out.append(ent[1])
    if last_err and not out:
        raise last_err
    return out


# ---------------------------------------------------------------- 背景記錄走勢
def touch(codes, pinned=False):
    now = time.time()
    with watch_lock:
        for c in codes:
            old = watch.get(c)
            watch[c] = (now, pinned or bool(old and old[1]))


def poll_loop():
    while True:
        time.sleep(5)
        try:
            if not market_open():
                continue
            now = time.time()
            with watch_lock:
                for c, v in list(watch.items()):
                    if now - v[0] > 86400:
                        del watch[c]
                items = [(c, v) for c, v in watch.items() if v[1] or now - v[0] < 1800]
                items.sort(key=lambda kv: -kv[1][0])
                codes = [c for c, _ in items[:MAX_POLL]]
            if codes:
                get_quotes(codes)
        except Exception:
            pass


_started = False


def start_background():
    global _started
    if _started:
        return
    _started = True
    threading.Thread(target=meta_loop, daemon=True).start()
    threading.Thread(target=poll_loop, daemon=True).start()


start_background()


# ---------------------------------------------------------------- 路由
def reply(payload, status=200):
    resp = jsonify(payload)
    resp.status_code = status
    resp.headers["Cache-Control"] = "no-store"
    return resp


def parse_codes(raw_list):
    codes = []
    for c in raw_list:
        c = str(c).strip()
        if not c:
            continue
        if not CODE_RE.match(c):
            raise ValueError(f"代碼格式不對：{c}")
        c = "t00" if c.lower() == "t00" else c.upper()  # t00 = 加權指數
        if c not in codes:
            codes.append(c)
    return codes


@app.get("/")
def index():
    return send_from_directory(BASE_DIR, "index.html")


@app.get("/healthz")
def healthz():
    return "ok"


@app.route("/api/quotes", methods=["GET", "POST"])
def api_quotes():
    try:
        if request.method == "POST":
            body = request.get_json(silent=True) or {}
            codes = parse_codes(body.get("codes", []))
            pin = bool(body.get("pin"))
        else:
            codes = parse_codes(request.args.get("codes", "").split(","))
            pin = request.args.get("pin") == "1"
    except ValueError as e:
        return reply({"ok": False, "error": str(e)}, 400)
    if not codes:
        return reply({"ok": True, "items": [], "missing": []})
    if len(codes) > MAX_CODES:
        return reply({"ok": False, "error": f"一次最多 {MAX_CODES} 檔"}, 400)
    touch(codes, pin)
    try:
        items = get_quotes(codes)
    except Exception as e:
        return reply({"ok": False, "error": f"證交所連線失敗（{type(e).__name__}）"}, 502)
    got = {it["code"].lower() for it in items}
    missing = [c for c in codes if c.lower() not in got]
    return reply({"ok": True, "items": items, "missing": missing})


@app.get("/api/series")
def api_series():
    try:
        codes = parse_codes(request.args.get("codes", "").split(","))
    except ValueError as e:
        return reply({"ok": False, "error": str(e)}, 400)
    out = {}
    for c in codes[:100]:
        s = series_for(c)
        if s:
            out[c] = s
    return reply({"ok": True, "series": out})


@app.get("/api/groups")
def api_groups():
    return reply({
        "ok": True,
        "ready": meta_state["ok"],
        "concept": load_concepts(),
        "industry": build_industries(),
    })


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))
