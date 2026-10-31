#!/usr/bin/env python3
"""下載上市櫃公司清單（產業別、發行股數）與最近收盤價，算出各產業龍頭，存成 companies.json。
由 GitHub Actions 每天執行；也可以在自己的電腦上直接跑：python build_companies.py"""
import csv
import io
import json
import re
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
IND = {
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
CODE_RE = re.compile(r"^[0-9A-Za-z]{1,8}$")
errors = []


def get(url, tries=3, timeout=60):
    last = None
    for i in range(tries):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
            with urllib.request.urlopen(req, timeout=timeout) as r:
                return r.read()
        except Exception as e:  # noqa: BLE001
            last = e
            time.sleep(2 * (i + 1))
    raise last


def pick(d, *names):
    for k in names:
        v = d.get(k)
        if v not in (None, ""):
            return str(v).strip()
    return ""


def pick_like(d, *parts):
    for k, v in d.items():
        if v not in (None, "") and any(p in str(k) for p in parts):
            return str(v).strip()
    return ""


def num(v):
    try:
        return float(str(v).replace(",", ""))
    except (TypeError, ValueError):
        return None


def rows(urls, label):
    for u in urls:
        try:
            text = get(u).decode("utf-8-sig", errors="replace")
            data = list(csv.DictReader(io.StringIO(text))) if u.endswith(".csv") else json.loads(text)
            if data:
                print(f"OK  {label}: {len(data)} 筆  <- {u}")
                return data
            errors.append(f"{label}: {u} 沒有資料")
        except Exception as e:  # noqa: BLE001
            errors.append(f"{label}: {u} {type(e).__name__}: {str(e)[:100]}")
            print(f"ERR {label}: {u} {type(e).__name__}: {str(e)[:100]}")
    return []


def isin_rows(mode, ex, label):
    """備援：國際證券辨識號碼公告頁（只有產業別，沒有發行股數）。"""
    url = f"https://isin.twse.com.tw/isin/C_public.jsp?strMode={mode}"
    out = {}
    try:
        html = get(url).decode("cp950", errors="replace")
    except Exception as e:  # noqa: BLE001
        errors.append(f"{label}: {url} {type(e).__name__}: {str(e)[:100]}")
        return out
    in_stock = False
    for tr in re.findall(r"<tr>(.*?)</tr>", html, flags=re.S | re.I):
        cells = [re.sub(r"<.*?>", "", c).strip() for c in re.findall(r"<td[^>]*>(.*?)</td>", tr, flags=re.S | re.I)]
        if len(cells) == 1:
            in_stock = cells[0].replace("\u3000", "").strip() == "股票"
            continue
        if not in_stock or len(cells) < 5:
            continue
        m = re.match(r"^([0-9A-Za-z]{1,8})[\u3000 ]+(.+)$", cells[0])
        if m and cells[4]:
            out[m.group(1).upper()] = [m.group(2).strip(), cells[4], ex, None]
    print(f"{'OK ' if out else 'ERR'} {label}(備援): {len(out)} 筆")
    return out


def main():
    companies = {}
    for ex, label, urls in [
        ("tse", "上市公司", ["https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
                            "https://mopsfin.twse.com.tw/opendata/t187ap03_L.csv"]),
        ("otc", "上櫃公司", ["https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O",
                            "https://mopsfin.twse.com.tw/opendata/t187ap03_O.csv"]),
    ]:
        got = 0
        for d in rows(urls, label):
            code = pick(d, "公司代號", "SecuritiesCompanyCode", "Code")
            if not code or not CODE_RE.match(code):
                continue
            ind = pick(d, "產業別", "SecuritiesIndustryCode", "Industry")
            indname = IND.get(ind.zfill(2), ind) if ind.isdigit() else ind
            companies[code.upper()] = [
                pick(d, "公司簡稱", "CompanyAbbreviation", "公司名稱", "CompanyName"),
                indname, ex, num(pick_like(d, "已發行普通股", "IssueShares", "IssuedShares")),
            ]
            got += 1
        if not got:  # 備援
            companies.update(isin_rows(2 if ex == "tse" else 4, ex, label))

    prices = {}
    for label, urls in [
        ("上市收盤價", ["https://openapi.twse.com.tw/v1/exchangeReport/STOCK_DAY_ALL"]),
        ("上櫃收盤價", ["https://www.tpex.org.tw/openapi/v1/tpex_mainboard_daily_close_quotes"]),
    ]:
        for d in rows(urls, label):
            code = pick(d, "Code", "SecuritiesCompanyCode")
            px = num(pick(d, "ClosingPrice", "Close"))
            if code and px:
                prices[code.upper()] = px

    if len(companies) < 500:
        print("失敗：公司資料太少（%d 筆），不更新。" % len(companies))
        print("\n".join(errors))
        sys.exit(1)

    by = {}
    for code, (name, ind, ex, shares) in companies.items():
        if ind and ind not in ("80", "管理股票"):
            by.setdefault(ind, []).append(code)
    leaders = {}
    for ind, codes in by.items():
        cap = [(companies[c][3] * prices[c], c) for c in codes if companies[c][3] and c in prices]
        if cap:
            leaders[ind] = max(cap)[1]
        else:
            sh = [(companies[c][3], c) for c in codes if companies[c][3]]
            if sh:
                leaders[ind] = max(sh)[1]

    today = datetime.now(timezone(timedelta(hours=8))).strftime("%Y-%m-%d")
    with open("companies.json", "w", encoding="utf-8") as f:
        json.dump({"updated": today, "companies": companies, "leaders": leaders},
                  f, ensure_ascii=False, separators=(",", ":"))
    print(f"完成：{len(companies)} 檔公司、{len(prices)} 筆收盤價、{len(leaders)} 個產業有龍頭")
    if errors:
        print("部分來源失敗：\n" + "\n".join(errors))


if __name__ == "__main__":
    main()
