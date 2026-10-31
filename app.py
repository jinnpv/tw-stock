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
CODE_RE = re.compile(r"^[0-9A-Za-z]{1,8}$")
TZ = timezone(timedelta(hours=8))

http = requests.Session()
http.headers.update(
    {"User-Agent": UA, "Referer": HOME, "Accept-Language": "zh-TW,zh;q=0.9"}
)
verify_tls = True
session_at = 0.0

meta_http = requests.Session()
meta_http.headers.update({"User-Agent": UA, "Accept-Language": "zh-TW,zh;q=0.9"})
meta_verify = True

fetch_lock = threading.Lock()
meta_lock = threading.Lock()

quote_cache = {}   # code(lower) -> (ts, item | None)
meta = {}          # code(upper) -> {"n","ind","indname","ex","shares","cap"}
leaders = {}       # 產業名稱 -> 龍頭代號
meta_state = {"ok": False, "at": 0.0}
ind_cache = {"at": 0.0, "data": []}
diag = {"meta_status": "尚未開始", "meta_err": "", "quote_err": "", "quote_ms": None,
        "prices": 0, "leaders": 0}

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


# ---------------------------------------------------------------- 報價用網路
def _get(url, **kw):
    """GET；若憑證在這個環境驗證失敗，退而不驗證（都是公開資料）。"""
    global verify_tls
    try:
        return http.get(url, timeout=(3, 6), verify=verify_tls, **kw)
    except requests.exceptions.SSLError:
        verify_tls = False
        return http.get(url, timeout=(3, 6), verify=False, **kw)


def ensure_session(force=False):
    global session_at
    if force or time.time() - session_at > 600:
        _get(HOME)
        session_at = time.time()


def tw_now():
    return datetime.now(TZ)


IND_KEY = {v: k for k, v in IND_NAMES.items()}


# ---------------------------------------------------------------- 公司資料 / 產業別 / 龍頭
def load_companies_file():
    """讀 GitHub Actions 每天產生的 companies.json（不用連證交所）。"""
    path = os.path.join(BASE_DIR, "companies.json")
    try:
        with open(path, encoding="utf-8") as f:
            data = json.load(f)
        comp = data.get("companies") or {}
        if len(comp) < 500:
            return False
        new = {}
        for code, v in comp.items():
            name, ind, ex, shares = v
            new[code.upper()] = {"n": name, "ind": ind, "indname": ind, "ex": ex, "shares": shares}
        found = dict(data.get("leaders") or {})
    except Exception as e:
        diag["meta_status"] = f"companies.json 讀取失敗：{type(e).__name__}"
        return False
    with meta_lock:
        meta.clear()
        meta.update(new)
        leaders.clear()
        leaders.update(found)
    diag["leaders"] = len(found)
    meta_state.update(ok=True, at=time.time())
    ind_cache["at"] = 0.0
    diag["meta_status"] = f"已載入 companies.json（{data.get('updated', '?')}）：{len(new)} 檔公司、{len(found)} 個產業龍頭"
    return True


def _pick(d, *names):
    for k in names:
        v = d.get(k)
        if v not in (None, ""):
            return str(v).strip()
    return ""


def _pick_like(d, *parts):
    for k, v in d.items():
        if v not in (None, "") and any(p in str(k) for p in parts):
            return str(v).strip()
    return ""


def _to_num(v):
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def _meta_fetch(url, limit=30):
    """下載整份檔案；整體最多 limit 秒，連名稱解析卡住也會被放棄。"""
    global meta_verify
    box = {}

    def work():
        global meta_verify
        try:
            try:
                r = meta_http.get(url, timeout=(4, 8), stream=True, verify=meta_verify)
            except requests.exceptions.SSLError:
                meta_verify = False
                r = meta_http.get(url, timeout=(4, 8), stream=True, verify=False)
            r.raise_for_status()
            deadline = time.time() + limit
            chunks = []
            for ch in r.iter_content(65536):
                chunks.append(ch)
                if time.time() > deadline:
                    r.close()
                    raise TimeoutError("下載太久")
            box["data"] = b"".join(chunks)
        except Exception as e:
            box["err"] = e

    t = threading.Thread(target=work, daemon=True)
    t.start()
    t.join(limit + 5)
    if t.is_alive():
        raise TimeoutError("連線卡住")
    if "err" in box:
        raise box["err"]
    return box["data"]


def _meta_rows(url):
    raw = _meta_fetch(url)
    text = raw.decode("utf-8-sig", errors="replace")
    if url.endswith(".csv"):
        return list(csv.DictReader(io.StringIO(text)))
    return json.loads(text)


def _try_sources(label, urls, errs):
    for u in urls:
        diag["meta_status"] = f"下載 {label}：{u.split('/')[-1]}"
        try:
            rows = _meta_rows(u)
            if rows:
                return rows
            errs.append(f"{u} 沒有資料")
        except Exception as e:
            errs.append(f"{u} {type(e).__name__}: {str(e)[:80]}")
    return []


def load_meta():
    errs = []
    new = {}

    for ex, label, urls in [
        ("tse", "上市公司", ["https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
                            "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"]),
        ("otc", "上櫃公司", ["https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O",
                            "https://mopsfin.twse.com.tw/opendata/t187ap03_O.csv"]),
    ]:
        for d in _try_sources(label, urls, errs):
            code = _pick(d, "公司代號", "SecuritiesCompanyCode", "Code")
            if not code or not CODE_RE.match(code):
                continue
            ind = _pick(d, "產業別", "SecuritiesIndustryCode", "Industry")
            if ind.isdigit():
                indname = IND_NAMES.get(ind.zfill(2), ind)
            else:
                indname = ind
            new[code.upper()] = {
                "n": _pick(d, "公司簡稱", "CompanyAbbreviation", "公司名稱", "CompanyName"),
                "ind": ind,
                "indname": indname,
                "ex": ex,
                "shares": _to_num(_pick_like(d, "已發行普通股", "IssueShares", "IssuedShares")),
            }

    # 收盤價（用來算市值，決定龍頭）
    prices = {}
    for label, urls in [
        ("上市收盤價", ["https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"]),
        ("上櫃收盤價", ["https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"]),
    ]:
        for d in _try_sources(label, urls, errs):
            code = _pick(d, "Code", "SecuritiesCompanyCode", "代號")
            px = _to_num(_pick(d, "ClosingPrice", "Close", "收盤價", "收盤"))
            if code and px:
                prices[code.upper()] = px

    diag["meta_err"] = " | ".join(errs)
    diag["prices"] = len(prices)
    if new:
        best = {}
        for code, m in new.items():
            m["cap"] = m["shares"] * prices[code] if m["shares"] and code in prices else None
            if not m["indname"] or m["indname"] in ("80", "管理股票"):
                continue
            b = best.setdefault(m["indname"], [])
            b.append((code, m))
        found = {}
        for name, items in best.items():
            with_cap = [(m["cap"], c) for c, m in items if m["cap"]]
            if with_cap:
                found[name] = max(with_cap)[1]
            else:
                with_sh = [(m["shares"], c) for c, m in items if m["shares"]]
                if with_sh:
                    found[name] = max(with_sh)[1]
        with meta_lock:
            meta.clear()
            meta.update(new)
            leaders.clear()
            leaders.update(found)
        diag["leaders"] = len(found)
        meta_state.update(ok=True, at=time.time())
        ind_cache["at"] = 0.0
        diag["meta_status"] = f"完成：{len(new)} 檔公司、{len(prices)} 筆收盤價、{len(found)} 個產業龍頭"
    else:
        diag["meta_status"] = "失敗：沒有取得公司資料"


def meta_loop():
    while True:
        try:
            if not load_companies_file():
                load_meta()
        except Exception as e:
            diag["meta_status"] = f"失敗：{type(e).__name__}: {str(e)[:100]}"
        time.sleep(43200 if meta_state["ok"] else 90)


def build_industries():
    if ind_cache["data"] and time.time() - ind_cache["at"] < 600:
        return ind_cache["data"]
    with meta_lock:
        items = list(meta.items())
    buckets = {}
    for code, m in items:
        raw = (m.get("ind") or "").strip()
        name = m.get("indname") or "其他"
        k = raw.zfill(2) if raw.isdigit() else IND_KEY.get(name, "99")
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
        "vol": int(vol) if vol is not None else None,  # 總量（張）
        "time": x.get("t"),
        "date": x.get("d"),
        "bids": levels(x.get("b"), x.get("g")),
        "asks": levels(x.get("a"), x.get("f")),
    }


def decorate(it):
    """補上產業別與是否為產業龍頭。"""
    out = dict(it)
    code = (it.get("code") or "").upper()
    m = meta.get(code)
    if m and m.get("indname"):
        out["industry"] = m["indname"]
        out["leader"] = leaders.get(m["indname"]) == code
    elif code == "T00":
        out["industry"] = "加權指數"
    elif code.startswith("00"):
        out["industry"] = "ETF"
    return out


def fresh(code, now):
    ent = quote_cache.get(code.lower())
    return bool(ent) and now - ent[0] < CACHE_SECONDS


def _refresh(need):
    """向證交所更新 need 這幾檔；整體最多約 12 秒。回傳錯誤（沒有就是 None）。"""
    started = time.time()
    need = [c for c in need if not fresh(c, started)]
    last_err = None
    for i in range(0, len(need), CHUNK):
        if time.time() - started > 12:
            return TimeoutError("證交所回應太慢")
        part = need[i:i + CHUNK]
        try:
            try:
                ensure_session()
            except Exception:
                pass
            try:
                raw = fetch_raw(part)
            except Exception:
                if time.time() - started > 8:
                    raise
                try:
                    ensure_session(force=True)
                except Exception:
                    pass
                raw = fetch_raw(part)
        except Exception as e:
            diag["quote_err"] = f"{type(e).__name__}: {str(e)[:120]}"
            return e
        got = {}
        for x in raw:
            if x.get("n"):  # 查無此代碼的項目沒有名稱
                it = normalize(x)
                got[it["code"].lower()] = it
        ts = time.time()
        for c in part:
            quote_cache[c.lower()] = (ts, got.get(c.lower()))
        diag["quote_err"] = ""
        diag["quote_ms"] = int((time.time() - started) * 1000)
        if i + CHUNK < len(need):
            time.sleep(0.3)
    return last_err


def get_quotes(codes):
    now = time.time()
    need = [c for c in codes if not fresh(c, now)]
    last_err = None
    if need:
        # 最多等 6 秒搶鎖；搶不到就先用舊資料，避免把伺服器的工作執行緒全部卡住
        if fetch_lock.acquire(timeout=6):
            try:
                last_err = _refresh(need)
            finally:
                fetch_lock.release()
        else:
            last_err = TimeoutError("伺服器忙碌")
    out = []
    for c in codes:
        ent = quote_cache.get(c.lower())
        if ent and ent[1]:
            out.append(decorate(ent[1]))
    if last_err and not out:
        raise last_err
    return out


_started = False


def start_background():
    global _started
    if _started:
        return
    _started = True
    load_companies_file()  # 先同步讀檔，第一個請求進來時就有產業資料
    threading.Thread(target=meta_loop, daemon=True).start()


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
        else:
            codes = parse_codes(request.args.get("codes", "").split(","))
    except ValueError as e:
        return reply({"ok": False, "error": str(e)}, 400)
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


@app.get("/api/diag")
def api_diag():
    return reply({
        "server_time": tw_now().strftime("%Y-%m-%d %H:%M:%S"),
        "meta_ok": meta_state["ok"],
        "meta_status": diag["meta_status"],
        "meta_count": len(meta),
        "price_count": diag["prices"],
        "leader_count": diag["leaders"],
        "meta_err": diag["meta_err"],
        "quote_err": diag["quote_err"],
        "last_fetch_ms": diag["quote_ms"],
        "cached_quotes": len(quote_cache),
    })


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
