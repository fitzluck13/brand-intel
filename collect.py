"""
collect.py — Brand Intelligence Pipeline
Run: python collect.py
Requires: python -m pip install anthropic httpx sendgrid python-dotenv
"""

import os
import json
import logging
import datetime
from dotenv import load_dotenv

load_dotenv()

import httpx
import anthropic
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)

# ── Brand list ────────────────────────────────────────────────────────────────

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

# ── Supabase via direct HTTP ──────────────────────────────────────────────────

SUPABASE_URL = os.environ.get("SUPABASE_URL", "").rstrip("/")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_KEY", "")
SB_HEADERS = {
    "apikey": SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type": "application/json",
    "Prefer": "resolution=merge-duplicates",
}

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

# ── Claude client ─────────────────────────────────────────────────────────────

claude_client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

def get_latest_model():
    """Return the best available Claude model."""
    # Try models in order of preference
    for model in ["claude-sonnet-4-5", "claude-3-7-sonnet-20250219", "claude-3-5-sonnet-20241022", "claude-3-5-haiku-20241022"]:
        try:
            test = claude_client.messages.create(
                model=model, max_tokens=10,
                messages=[{"role":"user","content":"hi"}]
            )
            log.info("Using Claude model: %s", model)
            return model
        except Exception:
            continue
    raise RuntimeError("No Claude model available — check your API key")

# ── News fetching ─────────────────────────────────────────────────────────────

def fetch_news(brand):
    api_key = os.environ.get("NEWS_API_KEY", "")
    from_date = (datetime.date.today() - datetime.timedelta(days=1)).isoformat()
    articles = []
    for term in brand["search_terms"]:
        try:
            resp = httpx.get(
                "https://newsapi.org/v2/everything",
                params={"q": term, "from": from_date, "sortBy": "relevancy", "language": "en", "pageSize": 5, "apiKey": api_key},
                timeout=15,
            )
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

# ── Earnings fetching (SEC EDGAR) ─────────────────────────────────────────────

def fetch_earnings(brand):
    if not brand.get("public") or not brand.get("ticker"):
        return []
    today = datetime.date.today()
    start = (today - datetime.timedelta(days=2)).isoformat()
    ticker = brand["ticker"]
    try:
        resp = httpx.get(
            f"https://efts.sec.gov/LATEST/search-index?q=%22{ticker}%22&forms=8-K&dateRange=custom&startdt={start}&enddt={today.isoformat()}",
            timeout=15,
            headers={"User-Agent": "brand-intel contact@example.com"},
        )
        resp.raise_for_status()
        hits = resp.json().get("hits", {}).get("hits", [])
        results = []
        for hit in hits[:3]:
            src = hit.get("_source", {})
            results.append({
                "title": f"{brand['name']} 8-K Filing",
                "url": f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={ticker}&type=8-K",
                "text_snippet": src.get("file_date", ""),
                "filed_at": src.get("file_date", today.isoformat()),
                "source_type": "earnings_transcript",
            })
        return results
    except Exception as e:
        log.warning("  EDGAR error for %s: %s", ticker, e)
        return []

# ── Claude summarisation ──────────────────────────────────────────────────────

NEWS_PROMPT = """You are a fashion and golf apparel analyst. Summarise the article below.
Respond with ONLY a valid JSON object — no markdown, no backticks, no explanation before or after.
Use exactly these keys:
{"summary":"2-3 sentence summary","sentiment":"bullish or neutral or bearish","sentiment_score":0.0,"key_themes":["tag1","tag2"],"strategic_commentary":"strategy or launch mentions, or empty string"}"""

EARNINGS_PROMPT = """You are a fashion and golf apparel analyst. Analyse the earnings filing below.
Respond with ONLY a valid JSON object — no markdown, no backticks, no explanation before or after.
Use exactly these keys:
{"summary":"3-4 sentence summary","sentiment":"bullish or neutral or bearish","sentiment_score":0.0,"revenue_commentary":"revenue figures","strategic_themes":["tag1","tag2"],"management_tone":"confident or cautious or defensive or mixed","key_risks":"risks mentioned","key_opportunities":"opportunities mentioned"}"""

MODEL = None  # set on first run

def summarise(content, source_type):
    global MODEL
    if MODEL is None:
        MODEL = get_latest_model()

    system = EARNINGS_PROMPT if source_type == "earnings_transcript" else NEWS_PROMPT
    try:
        msg = claude_client.messages.create(
            model=MODEL,
            max_tokens=800,
            system=system,
            messages=[{"role": "user", "content": content[:12000]}],
        )
        # Extract text from response
        raw = ""
        for block in msg.content:
            if hasattr(block, "text"):
                raw += block.text
        raw = raw.strip()

        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        raw = raw.strip()

        if not raw:
            log.warning("  Claude returned empty response")
            return {}

        return json.loads(raw)
    except json.JSONDecodeError as e:
        log.warning("  Claude JSON parse error: %s | raw: %.100s", e, raw)
        return {}
    except Exception as e:
        log.warning("  Claude error: %s", e)
        return {}

# ── Save to Supabase ──────────────────────────────────────────────────────────

def save(brand, item, analysis, source_type):
    row = {
        "brand_name": brand["name"],
        "ticker": brand.get("ticker"),
        "category": brand["category"],
        "source_type": source_type,
        "title": str(item.get("title", ""))[:500],
        "url": str(item.get("url", ""))[:1000],
        "published_at": item.get("publishedAt") or item.get("filed_at"),
        "source_name": item.get("source", {}).get("name", "") if isinstance(item.get("source"), dict) else "",
        "summary": analysis.get("summary", ""),
        "sentiment": analysis.get("sentiment", "neutral"),
        "sentiment_score": analysis.get("sentiment_score", 0.0),
        "key_themes": analysis.get("key_themes", analysis.get("strategic_themes", [])),
        "strategic_commentary": analysis.get("strategic_commentary", ""),
        "revenue_commentary": analysis.get("revenue_commentary", ""),
        "management_tone": analysis.get("management_tone", ""),
        "key_risks": analysis.get("key_risks", ""),
        "key_opportunities": analysis.get("key_opportunities", ""),
        "processed_at": datetime.datetime.utcnow().isoformat(),
    }
    sb_upsert("articles", row)

# ── Email digest ──────────────────────────────────────────────────────────────

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
        return f"""<div style="border:1px solid #e5e5e5;border-radius:8px;padding:16px;margin-bottom:12px;">
          <div style="display:flex;justify-content:space-between;gap:8px;">
            <a href="{r.get('url','')}" style="font-size:14px;font-weight:500;color:#1a1a1a;text-decoration:none;">{r.get('title','')}</a>
            {badge(r.get('sentiment','neutral'))}
          </div>
          <div style="font-size:12px;color:#888;margin:6px 0;">{r.get('brand_name','')} · {r.get('source_name','') or 'SEC EDGAR'} · {str(r.get('published_at',''))[:10]}</div>
          <p style="font-size:13px;color:#444;line-height:1.6;margin:8px 0 0;">{r.get('summary','')}</p>
          {"<div style='font-size:11px;color:#aaa;margin-top:6px;'>"+themes+"</div>" if themes else ""}
        </div>"""

    earnings_html = "".join(block(r) for r in earnings) or "<p style='color:#888;'>No earnings calls today.</p>"
    news_html = "".join(block(r) for r in news[:20]) or "<p style='color:#888;'>No news articles today.</p>"

    html = f"""<html><body style="font-family:sans-serif;max-width:680px;margin:0 auto;padding:24px;">
      <h1 style="font-size:20px;font-weight:500;">Brand Intelligence Digest</h1>
      <p style="color:#888;font-size:13px;">{today} · {len(results)} items</p>
      <hr style="border:none;border-top:1px solid #e5e5e5;margin:20px 0;">
      <h2 style="font-size:14px;text-transform:uppercase;letter-spacing:.05em;color:#555;margin-bottom:12px;">Earnings Calls</h2>
      {earnings_html}
      <h2 style="font-size:14px;text-transform:uppercase;letter-spacing:.05em;color:#555;margin:24px 0 12px;">News</h2>
      {news_html}
      <p style="font-size:11px;color:#bbb;margin-top:32px;border-top:1px solid #e5e5e5;padding-top:16px;">AI-generated summaries. Not investment advice.</p>
    </body></html>"""

    to_emails = [e.strip() for e in os.environ.get("DIGEST_TO_EMAILS","").split(",")]
    message = Mail(
        from_email=os.environ.get("DIGEST_FROM_EMAIL",""),
        to_emails=to_emails,
        subject=f"Brand Intelligence Digest — {today}",
        html_content=html,
    )
    try:
        sg = SendGridAPIClient(os.environ.get("SENDGRID_API_KEY",""))
        resp = sg.send(message)
        log.info("Email sent. Status: %s", resp.status_code)
    except Exception as e:
        log.error("SendGrid error: %s", e)

# ── Main ──────────────────────────────────────────────────────────────────────

def run():
    log.info("=== Brand Intelligence Pipeline starting ===")
    all_results = []

    for brand in BRANDS:
        log.info("Processing: %s", brand["name"])

        for article in fetch_news(brand):
            content = f"{article.get('title','')}\n\n{article.get('description','')}\n\n{article.get('content','')}"
            analysis = summarise(content, "news")
            if analysis:
                save(brand, article, analysis, "news")
                all_results.append({**article, **analysis, "brand_name": brand["name"], "source_type": "news"})

        if brand.get("public"):
            for filing in fetch_earnings(brand):
                analysis = summarise(filing.get("text_snippet", filing.get("title", "")), "earnings_transcript")
                if analysis:
                    save(brand, filing, analysis, "earnings_transcript")
                    all_results.append({**filing, **analysis, "brand_name": brand["name"]})

    log.info("Pipeline complete. %d items processed.", len(all_results))
    send_email(all_results)
    log.info("=== Done ===")

if __name__ == "__main__":
    run()
