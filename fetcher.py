"""
الراصد — محرك الجلب v3.0
=========================
- RSS محسّن مع User-Agent حقيقي + retry
- تلغرام عبر t.me/s/channel scraping
- كلمات مفتاحية ديناميكية من Supabase
- يخزن كل الأخبار (ليس فقط العراق)
- ترجمة تلقائية
- منع التكرار بـ url_hash

pip install feedparser httpx supabase python-dotenv
"""

import os, re, hashlib, logging, time
from datetime import datetime, timezone, timedelta

import feedparser
import httpx
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

BROWSER_UA = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36"
FEED_UA    = "Mozilla/5.0 (compatible; RasidBot/3.0; +https://awwawdsa.github.io/iraq-intel)"

_KEYWORDS = []
FALLBACK_KW = [
    "العراق","عراقي","بغداد","البصرة","الموصل","أربيل","النجف","كركوك","الحشد الشعبي","فصائل","السوداني","كردستان",
    "iraq","iraqi","baghdad","basra","mosul","erbil","kirkuk","pmf","peshmerga","kurdistan","al-sudani","iran iraq",
    "سوريا","دمشق","حلب","الأسد","هيئة تحرير الشام",
    "syria","damascus","aleppo","hts","hayat tahrir al-sham",
    "تركيا","أردوغان","أنقرة","turkey","turkish","erdogan","ankara","pkk",
    "إيران","طهران","الحرس الثوري","iran","tehran","irgc","khamenei",
    "الشرق الأوسط","middle east","خليج","gulf",
]

def load_keywords():
    global _KEYWORDS
    try:
        res = supabase.table("keywords").select("word").eq("is_active", True).execute()
        if res.data:
            words = [r["word"].strip() for r in res.data if r.get("word","").strip()]
            if words:
                _KEYWORDS = words
                log.info(f"✓ كلمات من DB: {len(words)}")
                return
    except Exception as e:
        log.warning(f"keywords: {e}")
    _KEYWORDS = FALLBACK_KW.copy()
    log.info(f"✓ كلمات احتياطية: {len(_KEYWORDS)}")

def get_kws():
    return _KEYWORDS or FALLBACK_KW

def match_kws(text):
    t = text.lower()
    found = [k for k in get_kws() if k.lower() in t]
    return bool(found), found[:20]

CATEGORY_KW = {
    "security":  ["attack","strike","bomb","explosion","killed","wounded","arrest","isis","داعش","هجوم","انفجار","قتيل","اعتقال"],
    "politics":  ["parliament","election","government","minister","party","vote","برلمان","انتخابات","حكومة","وزير","رئيس"],
    "diplomacy": ["ambassador","embassy","treaty","agreement","sanctions","diplomatic","سفير","اتفاقية","دبلوماسي","عقوبات"],
    "economy":   ["oil","budget","economy","trade","investment","gdp","نفط","موازنة","اقتصاد","تجارة","استثمار"],
    "military":  ["military","army","forces","operation","airstrike","drone","missile","عسكري","جيش","قوات","عملية","صاروخ"],
    "energy":    ["oil field","gas","pipeline","opec","barrel","energy","حقل","غاز","أوبك","طاقة"],
    "kurdistan": ["kurdistan","peshmerga","barzani","erbil","كردستان","بيشمركة","بارزاني"],
}

def detect_cat(text):
    t = text.lower()
    scores = {c: sum(1 for k in kws if k in t) for c, kws in CATEGORY_KW.items()}
    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "other"

def score_imp(text, kw_count, title=""):
    s = min(kw_count * 10, 40)
    urgent = ["عاجل","breaking","urgent","killed","explosion","انفجار","قتل","attack","airstrike","مجزرة"]
    if any(u in text.lower() or u in title.lower() for u in urgent):
        s += 30
    return min(s, 100)

def translate(text, lang="auto"):
    if not text or lang == "ar": return text
    try:
        r = httpx.get("https://translate.googleapis.com/translate_a/single",
            params={"client":"gtx","sl":lang if lang!="multi" else "auto","tl":"ar","dt":"t","q":text[:2000]},
            headers={"User-Agent": BROWSER_UA}, timeout=15)
        d = r.json()
        return "".join(t[0] for t in d[0] if t[0]) if d and d[0] else text
    except:
        return text

def clean(html):
    return re.sub(r"<[^>]+>", " ", html or "").replace("&nbsp;"," ").replace("&amp;","&").replace("&quot;",'"').strip()

def url_hash(url, title):
    return hashlib.md5(f"{url}:{title[:60]}".encode()).hexdigest()

def exists(url, title):
    if not url: return False
    try:
        h = url_hash(url, title)
        # Try url_hash first (faster)
        try:
            r = supabase.table("articles").select("id").eq("url_hash", h).limit(1).execute()
            if r.data: return True
        except:
            pass
        # Fallback: check by original_url
        r2 = supabase.table("articles").select("id").eq("original_url", url).limit(1).execute()
        return bool(r2.data)
    except:
        return False

def parse_date(entry):
    for f in ["published_parsed","updated_parsed","created_parsed"]:
        t = entry.get(f)
        if t:
            try: return datetime(*t[:6], tzinfo=timezone.utc).isoformat()
            except: pass
    return datetime.now(timezone.utc).isoformat()

def get_body(entry):
    for f in ["content","summary","description"]:
        v = entry.get(f)
        if v:
            if isinstance(v, list) and v: return clean(v[0].get("value",""))
            if isinstance(v, str): return clean(v)
    return ""

def insert_article(source, title, body, url, pub_date, lang, kws):
    category   = detect_cat(f"{title} {body}")
    importance = score_imp(f"{title} {body}", len(kws), title)
    title_ar   = title if lang == "ar" else translate(title, lang)
    body_ar    = (body if lang == "ar" else translate(body[:1500], lang)) if body else ""
    iraq_kws   = ["العراق","iraq","iraqi","baghdad","بغداد","الحشد","pmf"]
    mentions   = any(k.lower() in f"{title} {body}".lower() for k in iraq_kws)

    row = {
        "source_id": source["id"],
        "title_original": title,
        "title_ar": title_ar,
        "body_original": body[:8000],
        "body_ar": body_ar,
        "language": lang,
        "original_url": url,
        "url_hash": url_hash(url, title),
        "mentions_iraq": mentions,
        "iraq_keywords": kws,
        "iraq_relevance_score": min(len(kws)*10, 100),
        "category": category,
        "importance_score": importance,
        "published_at": pub_date,
        "fetched_at": datetime.now(timezone.utc).isoformat(),
    }
    try:
        supabase.table("articles").insert(row).execute()
        return True
    except Exception as e:
        err_str = str(e).lower()
        if "duplicate" in err_str or "unique" in err_str:
            return False
        if "url_hash" in err_str or "column" in err_str:
            # Try without url_hash if column doesn't exist
            row2 = {k:v for k,v in row.items() if k != "url_hash"}
            try:
                supabase.table("articles").insert(row2).execute()
                return True
            except Exception as e2:
                if "duplicate" not in str(e2).lower():
                    log.debug(f"insert2: {e2}")
                return False
        log.debug(f"insert: {e}")
        return False

# ══ RSS ══
def fetch_rss(source):
    url  = source.get("rss_url","")
    lang = source.get("language","en")
    if not url: return 0

    feed = None
    uas = [FEED_UA, BROWSER_UA, "FeedFetcher-Google", "python-feedparser/6.0"]

    for ua in uas:
        try:
            r = httpx.get(url, headers={"User-Agent":ua,"Accept":"application/xml,text/xml,*/*"}, timeout=20, follow_redirects=True)
            if r.status_code == 200:
                feed = feedparser.parse(r.text)
                if feed.entries: break
            elif r.status_code in [403,429,503]:
                time.sleep(3)
        except Exception as e:
            log.debug(f"  rss attempt: {e}")
    
    if not feed or not feed.entries:
        # محاولة أخيرة بـ feedparser مباشرة
        try:
            feed = feedparser.parse(url)
        except: pass

    if not feed or not feed.entries:
        log.warning(f"  ✗ {source['name']}: لا توجد مداخل RSS")
        return 0

    log.info(f"  ✓ {source['name']}: {len(feed.entries)} مدخل")
    cutoff = datetime.now(timezone.utc) - timedelta(days=7)
    added = 0

    for entry in feed.entries[:30]:
        title = clean(entry.get("title","")).strip()
        if not title or len(title) < 5: continue
        art_url = entry.get("link","") or entry.get("url","")
        if exists(art_url, title): continue

        body = get_body(entry)
        full = f"{title} {body}"

        _, kws = match_kws(full)

        pub = parse_date(entry)
        try:
            dt = datetime.fromisoformat(pub.replace("Z","+00:00"))
            if dt < cutoff: continue
        except: pass

        if insert_article(source, title, body, art_url, pub, lang, kws):
            added += 1
            log.info(f"  + [{score_imp(full,len(kws),title):3d}] {title[:65]}")

    return added

# ══ تلغرام ══
def fetch_telegram(source):
    url  = source.get("rss_url","")
    lang = source.get("language","ar")

    # تحويل الرابط
    if "t.me/" in url:
        parts = url.replace("https://","").replace("http://","").split("t.me/")
        channel = parts[-1].split("/")[0].strip("@/ ")
    else:
        channel = url.strip("@/ ")
    
    tg_url = f"https://t.me/s/{channel}"
    log.info(f"  تلغرام: {tg_url}")

    try:
        r = httpx.get(tg_url, headers={
            "User-Agent": BROWSER_UA,
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "ar,en;q=0.8",
        }, timeout=25, follow_redirects=True)
        
        if r.status_code != 200:
            log.warning(f"  تلغرام HTTP {r.status_code}")
            return 0
    except Exception as e:
        log.warning(f"  تلغرام: {e}")
        return 0

    html = r.text

    # استخراج الرسائل
    # نمط 1: div class tgme_widget_message_text
    msgs = re.findall(r'class="tgme_widget_message_text[^"]*"[^>]*>(.*?)</div>', html, re.DOTALL)
    
    # نمط 2: إذا لم يجد
    if not msgs:
        msgs = re.findall(r'<div class="js-message_text[^>]*>(.*?)</div>', html, re.DOTALL)

    # نمط 3: أي نص داخل message wrapper
    if not msgs:
        msgs = re.findall(r'tgme_widget_message_wrap[^>]*>.*?<div[^>]*>(.*?)</div>', html, re.DOTALL)

    # استخراج التواريخ
    dates = re.findall(r'<time[^>]*datetime="([^"]+)"', html)
    # استخراج روابط الرسائل
    links = re.findall(rf'href="(https://t\.me/{channel}/\d+)"', html)

    log.info(f"  تلغرام: {len(msgs)} رسالة، {len(dates)} تاريخ")

    added = 0
    for i, msg_html in enumerate(msgs[:25]):
        text = clean(msg_html).strip()
        if len(text) < 15: continue

        art_url = links[i] if i < len(links) else f"{tg_url}#{i}"
        pub = dates[i] if i < len(dates) else datetime.now(timezone.utc).isoformat()

        if exists(art_url, text): continue

        _, kws = match_kws(text)
        title = text[:120].replace("\n"," ").strip()

        if insert_article(source, title, text, art_url, pub, lang, kws):
            added += 1
            log.info(f"  + [TG] {title[:60]}")

    return added

# ══ معالجة مصدر ══
def process_source(source):
    typ = source.get("type","newspaper")
    url = source.get("rss_url","") or ""

    if not url:
        log.info(f"  [{source.get('name','?')}] لا رابط")
        return 0

    if typ == "telegram" or "t.me" in url:
        return fetch_telegram(source)
    
    if typ == "youtube" or "youtube.com" in url:
        log.info(f"  [{source.get('name','?')}] يوتيوب — يحتاج YouTube API")
        return 0

    return fetch_rss(source)

# ══ RUN ══
def run():
    log.info("=" * 50)
    log.info(f"الراصد v3.0 | {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    log.info("=" * 50)

    load_keywords()

    try:
        res = supabase.table("sources").select("*").eq("is_active", True).execute()
        sources = res.data or []
    except Exception as e:
        log.error(f"sources: {e}"); return

    log.info(f"مصادر نشطة: {len(sources)}")
    if not sources:
        log.warning("أضف مصادر من صفحة الإضافة أولاً")
        return

    total = 0
    for s in sources:
        try:
            n = process_source(s)
            total += n
        except Exception as e:
            log.error(f"  ✗ {s.get('name','?')}: {e}")
        time.sleep(1.5)

    log.info("=" * 50)
    log.info(f"✓ انتهى | مضاف: {total}")
    log.info("=" * 50)

if __name__ == "__main__":
    run()
