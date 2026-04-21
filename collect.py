"""
collect.py — Brand Intelligence Pipeline
Run: python collect.py
Requires: python -m pip install anthropic httpx sendgrid python-dotenv
"""

import os
import json
import logging
import datetime
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

load_dotenv()

import httpx
import anthropic
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

BRANDS = [
    {"name":"Acushnet","ticker":"GOLF","public":True,"category":"golf","search_terms":["Acushnet","FootJoy","Titleist apparel"]},
    {"name":"Topgolf Callaway Brands","ticker":"MODG","public":True,"category":"golf","search_terms":["Callaway Golf","Travis Mathew","Topgolf Callaway"]},
    {"name":"Dick's Sporting Goods","ticker":"DKS","public":True,"category":"golf","search_terms":["Dick's Sporting Goods golf","Golf Galaxy"]},
    {"name":"Nike","ticker":"NKE","public":True,"category":"golf","search_terms":["Nike golf apparel","Nike golf fashion"]},
    {"name":"Greyson","ticker":None,"public":False,"category":"golf","search_terms":["Greyson Clothiers","Greyson golf"]},
    {"name":"Castore","ticker":None,"public":False,"category":"golf","search_terms":["Castore golf","Castore apparel"]},
    {"name":"Dunning Golf","ticker":None,"public":False,"category":"golf","search_terms":["Dunning Golf"]},
    {"name":"Holderness and Bourne","ticker":None,"public":False,"category":"golf","search_terms":["Holderness and Bourne"]},
    {"name":"Galvin Green","ticker":None,"public":False,"category":"golf","search_terms":["Galvin Green"]},
    {"name":"J. Lindeberg","ticker":None,"public":False,"category":"golf","search_terms":["J Lindeberg golf"]},
    {"name":"KJUS","ticker":None,"public":False,"category":"golf","search_terms":["KJUS golf"]},
    {"name":"Brunello Cucinelli","ticker":"BCUCY","public":True,"category":"luxury","search_terms":["Brunello Cucinelli"]},
    {"name":"Hugo Boss","ticker":"BOSS","public":True,"category":"luxury","search_terms":["Hugo Boss","BOSS fashion"]},
    {"name":"Ralph Lauren","ticker":"RL","public":True,"category":"luxury","search_terms":["Ralph Lauren","Polo Golf"]},
    {"name":"Paul Smith","ticker":None,"public":False,"category":"luxury","search_terms":["Paul Smith fashion"]},
]

BRAND_KEYWORDS = [
    "acushnet","footjoy","titleist","callaway","travis mathew","topgolf",
    "dick's sporting goods","golf galaxy","nike golf","greyson","castore",
    "dunning golf","holderness","galvin green","lindeberg","kjus",
    "brunello cucinelli","hugo boss","ralph lauren","polo golf","paul smith",
    "golf apparel","golf fashion","luxury apparel","luxury fashion",
    "luxury brand","golf wear","golf clothing","sportswear","athleisure",
]

RETAIL_DIVE_FEEDS = [
    "https://www.retaildive.com/feeds/news/",
    "https://www.retaildive.com/feeds/apparel/",
]

SEEKING_ALPHA_FEEDS = [
    {"ticker": "GOLF", "name": "Acushnet",                "url": "https://seekingalpha.com/symbol/GOLF/feed.xml",  "category": "golf"},
    {"ticker": "MODG", "name": "Topgolf Callaway Brands", "url": "https://seekingalpha.com/symbol/MODG/feed.xml",  "category": "golf"},
    {"ticker": "DKS",  "name": "Dick's Sporting Goods",   "url": "https://seekingalpha.com/symbol/DKS/feed.xml",   "category": "golf"},
    {"ticker": "NKE",  "name": "Nike",                    "url": "https://seekingalpha.com/symbol/NKE/feed.xml",   "category": "golf"},
    {"ticker": "BCUCY","name": "Brunello Cucinelli",       "url": "https://seekingalpha.com/symbol/BCUCY/feed.xml", "category": "luxury"},
    {"ticker": "BOSS", "name": "Hugo Boss",                "url": "https://seekingalpha.com/symbol/BOSS/feed.xml",  "category": "luxury"},
    {"ticker": "RL",   "name": "Ralph Lauren",             "url": "https://seekingalpha.com/symbol/RL/feed.xml",    "category": "luxury"},
]

def parse_rss_feed(feed_url, source_name, lookback_days=2):
    articles = []
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; brand-intel-bot/1.0)",
        "Accept": "application/rss+xml, application/xml, text/xml, */*",
    }
    try:
        resp = httpx.get(feed_url, headers=headers, timeout=20, follow_redirects=True)
        resp.raise_for_status()
        root = ET.fromstring(resp.content)
        ns = {"atom": "http://www.w3.org/2005/Atom"}
        items = root.findall(".//item") or root.findall(".//atom:entry", ns)
        log.info("  %s '%s': %d items found", source_name, feed_url, len(items))
        cutoff = datetime.date.today() - datetime.timedelta(days=lookback_days)
        for item in items:
            title = (getattr(item.find("title"), "text", "") or getattr(item.find("atom:title", ns), "text", "") or "").strip()
            link = (getattr(item.find("link"), "text", "") or (item.find("atom:link", ns).get("href") if item.find("atom:link", ns) is not None else "") or "").strip()
            description = (getattr(item.find("description"), "text", "") or getattr(item.find("atom:summary", ns), "text", "") or getattr(item.find("atom:content", ns), "text", "") or "").strip()
            pub_date = (getattr(item.find("pubDate"), "text", "") or getattr(item.find("atom:published", ns), "text", "") or datetime.date.today().isoformat()).strip()
            try:
                from email.utils import parsedate_to_datetime
                pub_date = parsedate_to_datetime(pub_date).date().isoformat()
            except Exception:
                pub_date = pub_date[:10]
            try:
                if datetime.date.fromisoformat(pub_date) < cutoff:
                    continue
            except Exception:
                pass
            if not link or not title:
                continue
            articles.append({"title": title, "url": link, "description": description, "content": description, "publishedAt": pub_date, "source": {"name": source_name}})
    except Exception as e:
        log.warning("  RSS feed error for %s: %s", feed_url, e)
    return articles

def fetch_retail_dive():
    articles = []
    for feed_url in RETAIL_DIVE_FEEDS:
        articles.extend(parse_rss_feed(feed_url, "Retail Dive"))
    filtered = [a for a in articles if any(kw in (a.get("title","") + " " + a.get("description","")).lower() for kw in BRAND_KEYWORDS)]
    seen, unique = set(), []
    for a in filtered:
        if a["url"] not in seen:
            seen.add(a["url"])
            unique.append(a)
    log.info("  Retail Dive relevant articles: %d", len(unique))
    return unique

def match_brand(article):
    content_lower = (article.get("title","") + " " + article.get("description","")).lower()
    for brand in BRANDS:
        if brand["name"].lower() in content_lower:
            return brand["name"]
        for term in brand.get("search_terms", []):
            if term.lower() in content_lower:
                return brand["name"]
    return "Industry"

def fetch_seeking_alpha():
    results = []
    seen = set()
    for feed in SEEKING_ALPHA_FEEDS:
        articles = parse_rss_feed(feed["url"], "Seeking Alpha", lookback_days=2)
        for a in articles:
            if a["url"] not in seen:
                seen.add(a["url"])
                results.append({**a, "brand_name": feed["name"], "ticker": feed["ticker"], "category": feed["category"]})
    log.info("  Seeking Alpha total articles: %d", len(results))
    return results

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SB_HEADERS = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"}

def sb_upsert(table, row):
    url = f"{SUPABASE_URL}/rest/v1/{table}"
    try:
        resp = httpx.post(url, headers=SB_HEADERS, json=row, timeout=15)
        if resp.status_code not in (200, 201):
            log.warning("  Supabase error %s: %s", resp.status_code, resp.text[:200])
        else:
            log.info("  Saved to Supabase: %s", row.get("title","")[:60])
    except Exception as e:
        log.warning("  Supabase error: %s", e)

claude_client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

def get_latest_model():
    for model in ["claude-sonnet-4-5", "claude-3-7-sonnet-20250219", "claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022"]:
        try:
            claude_client.messages.create(model=model, max_tokens=10, messages=[{"role":"user","content":"hi"}])
            log.info("Using Claude model: %s", model)
            return model
        except Exception:
            continue
    raise RuntimeError("No Claude model available — check your API key")

def fetch_news(brand):
    api_key = os.environ.get("NEWS_API_KEY", "")
    from_date = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    articles = []
    for term in brand["search_terms"]:
        try:
            resp = httpx.get("https://newsapi.org/v2/everything", params={"q": term, "from": from_date, "sortBy": "relevancy", "language": "en", "pageSize": 5, "apiKey": api_key}, timeout=15)
            resp.raise_for_status()
            fetched = resp.json().get("articles", [])
            articles.extend(fetched)
            log.info("  NewsAPI '%s': %d articles", term, len(fetched))
        except Exception as e:
            log.warning("  NewsAPI error for '%s': %s", term, e)
    seen, unique = set(), []
    for a in articles:
        url = a.get("url", "")
        if url and url not in seen:
            seen.add(url)
            unique.append(a)
    return unique

def fetch_earnings(brand):
    if not brand.get("public") or not brand.get("ticker"):
        return []
    today = datetime.date.today()
    start = (today - datetime.timedelta(days=2)).isoformat()
    ticker = brand["ticker"]
    try:
        resp = httpx.get(f"https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22&forms=8-K&dateRange=custom&startdt={start}&enddt={today.isoformat()}", timeout=15, headers={"User-Agent": "brand-intel contact@example.com"})
        resp.raise_for_status()
        hits = resp.json().get("hits", {}).get("hits", [])
        results = []
        for hit in hits[:3]:
            src = hit.get("_source", {})
            results.append({"title": f"{brand['name']} 8-K Filing", "url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={ticker}&type=8-K", "text_snippet": src.get("file_date", ""), "filed_at": src.get("file_date", today.isoformat()), "source_type": "earnings_transcript"})
        return results
    except Exception as e:
        log.warning("  EDGAR error for %s: %s", ticker, e)
        return []

NEWS_PROMPT = """You are a fashion and golf apparel analyst. Summarise the article below.
Respond with ONLY a valid JSON object — no markdown, no backticks, no explanation before or after.
Use exactly these keys:
{"summary":"2-3 sentence summary","sentiment":"bullish or neutral or bearish","sentiment_score":0.0,"key_themes":["tag1","tag2"],"strategic_commentary":"strategy or launch mentions, or empty string"}"""

EARNINGS_PROMPT = """You are a fashion and golf apparel analyst. Analyse the earnings filing below.
Respond with ONLY a valid JSON object — no markdown, no backticks, no explanation before or after.
Use exactly these keys:
{"summary":"3-4 sentence summary","sentiment":"bullish or neutral or bearish","sentiment_score":0.0,"revenue_commentary":"revenue figures","strategic_themes":["tag1","tag2"],"management_tone":"confident or cautious or defensive or mixed","key_risks":"risks mentioned","key_opportunities":"opportunities mentioned"}"""

MODEL = None

def summarise(content, source_type):
    global MODEL
    if MODEL is None:
        MODEL = get_latest_model()
    system = EARNINGS_PROMPT if source_type == "earnings_transcript" else NEWS_PROMPT
    try:
        msg = claude_client.messages.create(model=MODEL, max_tokens=800, system=system, messages=[{"role": "user", "content": content[:12000]}])
        raw = ""
        for block in msg.content:
            if hasattr(block, "text"):
                raw += block.text
        raw = raw.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()
        if not raw:
            return {}
        return json.loads(raw)
    except Exception as e:
        log.warning("  Claude error: %s", e)
        return {}

def save(brand_name, ticker, category, item, analysis, source_type):
    row = {"brand_name": brand_name, "ticker": ticker, "category": category, "source_type": source_type, "title": str(item.get("title", ""))[:500], "url": str(item.get("url", ""))[:1000], "published_at": item.get("publishedAt") or item.get("filed_at"), "source_name": item.get("source", {}).get("name", "") if isinstance(item.get("source"), dict) else "", "summary": analysis.get("summary", ""), "sentiment": analysis.get("sentiment", "neutral"), "sentiment_score": analysis.get("sentiment_score", 0.0), "key_themes": analysis.get("key_themes", analysis.get("strategic_themes", [])), "strategic_commentary": analysis.get("strategic_commentary", ""), "revenue_commentary": analysis.get("revenue_commentary", ""), "management_tone": analysis.get("management_tone", ""), "key_risks": analysis.get("key_risks", ""), "key_opportunities": analysis.get("key_opportunities", ""), "processed_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
    sb_upsert("articles", row)

def send_email(results):
    if not results:
        log.info("No results to email.")
        return
    today = datetime.date.today().strftime("%B %d, %Y")
    earnings = [r for r in results if r.get("source_type") == "earnings_transcript"]
    news = [r for r in results if r.get("source_type") != "earnings_transcript"]
    def badge(s):
        colors = {"bullish":"#3B6D11","bearish":"#A32D2D","neutral":"#5F5E5A"}
        bgs = {"bullish":"#EAF3DE","bearish":"#FCEBEB","neutral":"#F1EFE8"}
        return f'<span style="background:{bgs.get(s,"#F1EFE8")};color:{colors.get(s,"#5F5E5A")};padding:2px 8px;border-radius:4px;font-size:12px;">{s}</span>'
    def block(r):
        themes = ", ".join(r.get("key_themes") or [])
        source = r.get("source_name","") or ""
        return f"""<div style="border:1px solid #e5e5e5;border-radius:8px;padding:16px;margin-bottom:12px;"><div style="display:flex;justify-content:space-between;gap:8px;"><a href="{r.get('url','')}" style="font-size:14px;font-weight:500;color:#1a1a1a;text-decoration:none;">{r.get('title','')}</a>{badge(r.get('sentiment','neutral'))}</div><div style="font-size:12px;color:#888;margin:6px 0;">{r.get('brand_name','')} · {source or 'SEC EDGAR'} · {str(r.get('publishedAt') or r.get('published_at',''))[:10]}</div><p style="font-size:13px;color:#444;line-height:1.6;margin:8px 0 0;">{r.get('summary','')}</p>{"<div style='font-size:11px;color:#aaa;margin-top:6px;'>"+themes+"</div>" if themes else ""}</div>"""
    earnings_html = "".join(block(r) for r in earnings) or "<p style='color:#888;'>No earnings calls today.</p>"
    news_html = "".join(block(r) for r in news[:30]) or "<p style='color:#888;'>No news articles today.</p>"
    html = f"""<html><body style="font-family:sans-serif;max-width:680px;margin:0 auto;padding:24px;"><h1 style="font-size:20px;font-weight:500;">Brand Intelligence Digest</h1><p style="color:#888;font-size:13px;">{today} · {len(results)} items</p><hr style="border:none;border-top:1px solid #e5e5e5;margin:20px 0;"><h2 style="font-size:14px;text-transform:uppercase;color:#555;margin-bottom:12px;">Earnings Calls</h2>{earnings_html}<h2 style="font-size:14px;text-transform:uppercase;color:#555;margin:24px 0 12px;">News</h2>{news_html}<p style="font-size:11px;color:#bbb;margin-top:32px;border-top:1px solid #e5e5e5;padding-top:16px;">AI-generated summaries. Not investment advice.</p></body></html>"""
    to_emails = [e.strip() for e in os.environ.get("DIGEST_TO_EMAILS","").split(",")]
    message = Mail(from_email=os.environ.get("DIGEST_FROM_EMAIL",""), to_emails=to_emails, subject=f"Brand Intelligence Digest — {today}", html_content=html)
    try:
        sg = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY",""))
        resp = sg.send(message)
        log.info("Email sent. Status: %s", resp.status_code)
    except Exception as e:
        log.error("SendGrid error: %s", e)

def run():
    log.info("=== Brand Intelligence Pipeline starting ===")
    all_results = []
    seen_urls = set()

    def process_and_save(brand_name, ticker, category, article, source_type):
        url = article.get("url","")
        if not url or url in seen_urls:
            return
        seen_urls.add(url)
        content = f"{article.get('title','')}\n\n{article.get('description','')}\n\n{article.get('content','')}"
        analysis = summarise(content, source_type)
        if analysis:
            save(brand_name, ticker, category, article, analysis, source_type)
            all_results.append({**article, **analysis, "brand_name": brand_name, "source_type": source_type})

    for brand in BRANDS:
        log.info("Processing: %s", brand["name"])
        for article in fetch_news(brand):
            process_and_save(brand["name"], brand.get("ticker"), brand["category"], article, "news")
        if brand.get("public"):
            for filing in fetch_earnings(brand):
                process_and_save(brand["name"], brand.get("ticker"), brand["category"], filing, "earnings_transcript")

    log.info("Fetching Retail Dive RSS...")
    for article in fetch_retail_dive():
        brand_name = match_brand(article)
        matched = next((b for b in BRANDS if b["name"] == brand_name), None)
        category = matched["category"] if matched else "general"
        ticker = matched.get("ticker") if matched else None
        process_and_save(brand_name, ticker, category, article, "news")

    log.info("Fetching Seeking Alpha RSS...")
    for article in fetch_seeking_alpha():
        process_and_save(article["brand_name"], article["ticker"], article["category"], article, "news")

    log.info("Pipeline complete. %d items processed.", len(all_results))
    send_email(all_results)
    log.info("=== Done ===")

if __name__ == "__main__":
    run()
