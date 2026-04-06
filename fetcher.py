"""
Iraq Intel Platform — RSS Fetcher & Iraq Filter
================================================
يسحب المقالات من 26 مصدر، يفلتر ذكر العراق،
ويكتب النتائج في Supabase.

التشغيل:
    pip install feedparser httpx supabase python-dotenv
    python fetcher.py

أو عبر cron كل 30 دقيقة:
    */30 * * * * /usr/bin/python3 /path/to/fetcher.py
"""

import os
import re
import json
import hashlib
import logging
from datetime import datetime, timezone
from typing import Optional

import feedparser
import httpx
from supabase import create_client, Client
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# الإعداد
# ─────────────────────────────────────────────
load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_KEY"]  # service_role key

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ─────────────────────────────────────────────
# ترجمة تلقائية بـ Google Translate (مجاني)
# ─────────────────────────────────────────────
def translate_to_arabic(text: str, src_lang: str = 'auto') -> str:
    """ترجمة النص للعربية باستخدام Google Translate غير الرسمي"""
    if not text or src_lang == 'ar':
        return text
    try:
        url = 'https://translate.googleapis.com/translate_a/single'
        params = {
            'client': 'gtx',
            'sl': src_lang if src_lang != 'multi' else 'auto',
            'tl': 'ar',
            'dt': 't',
            'q': text[:2000]  # حد الترجمة
        }
        with httpx.Client(timeout=15) as client:
            resp = client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()
            if data and data[0]:
                translated = ''.join([t[0] for t in data[0] if t[0]])
                return translated
    except Exception as e:
        log.debug(f"ترجمة فاشلة: {e}")
    return text




# ─────────────────────────────────────────────
# كلمات مفتاحية — فلتر العراق
# ─────────────────────────────────────────────
# ─────────────────────────────────────────────
# الكلمات المفتاحية — تُحمّل من Supabase ديناميكياً
# ─────────────────────────────────────────────
_DYNAMIC_KEYWORDS: list[str] = []  # تُملأ عند بدء التشغيل

# كلمات احتياطية إذا فشل جلب Supabase
FALLBACK_KEYWORDS = [
    # العراق
    "العراق","عراقي","بغداد","البصرة","الموصل","أربيل","النجف","كركوك",
    "الحشد الشعبي","فصائل","السوداني","البرلمان العراقي","كردستان",
    "iraq","iraqi","baghdad","basra","mosul","erbil","kirkuk",
    "pmf","peshmerga","kurdistan","al-sudani","iran iraq",
    # سوريا
    "سوريا","سوري","دمشق","حلب","إدلب","درعا","الأسد","هيئة تحرير الشام",
    "syria","syrian","damascus","aleppo","idlib","hts","hayat tahrir",
    # تركيا
    "تركيا","أردوغان","أنقرة","العملية التركية",
    "turkey","turkish","erdogan","ankara","pkk",
    # إيران
    "إيران","إيراني","طهران","الحرس الثوري","خامنئي",
    "iran","iranian","tehran","irgc","khamenei",
    # المنطقة
    "الشرق الأوسط","middle east","خليج","gulf",
]

def load_keywords_from_db() -> list[str]:
    """جلب الكلمات المفتاحية من Supabase"""
    global _DYNAMIC_KEYWORDS
    try:
        res = supabase.table("keywords").select("word").eq("is_active", True).execute()
        if res.data:
            words = [r["word"].strip().lower() for r in res.data if r.get("word")]
            _DYNAMIC_KEYWORDS = words
            log.info(f"✓ جُلبت {len(words)} كلمة مفتاحية من قاعدة البيانات")
            return words
    except Exception as e:
        log.warning(f"تعذّر جلب الكلمات من DB، استخدام الاحتياطية: {e}")
    _DYNAMIC_KEYWORDS = [k.lower() for k in FALLBACK_KEYWORDS]
    return _DYNAMIC_KEYWORDS

def get_keywords() -> list[str]:
    return _DYNAMIC_KEYWORDS if _DYNAMIC_KEYWORDS else [k.lower() for k in FALLBACK_KEYWORDS]

# تصنيف تلقائي بناءً على كلمات مفتاحية
CATEGORY_KEYWORDS = {
    "security": [
        "attack", "strike", "bomb", "explosion", "killed", "wounded",
        "arrest", "isis", "daesh", "militia", "armed", "weapon",
        "هجوم", "انفجار", "قتيل", "اعتقال", "داعش", "مسلحين",
    ],
    "politics": [
        "parliament", "election", "government", "minister", "prime minister",
        "political", "party", "vote", "coalition", "برلمان", "انتخابات",
        "حكومة", "وزير", "رئيس الوزراء", "ائتلاف", "حزب",
    ],
    "diplomacy": [
        "ambassador", "embassy", "visit", "treaty", "agreement", "sanctions",
        "diplomatic", "foreign minister", "سفير", "سفارة", "اتفاقية",
        "وزير الخارجية", "علاقات دولية", "عقوبات",
    ],
    "economy": [
        "oil", "budget", "gdp", "economy", "trade", "investment",
        "inflation", "currency", "نفط", "موازنة", "اقتصاد", "تجارة",
        "استثمار", "تضخم",
    ],
    "military": [
        "military", "army", "forces", "troops", "operation", "airstrike",
        "drone", "missile", "عسكري", "جيش", "قوات", "عملية", "طائرة مسيرة",
        "صاروخ", "قصف جوي",
    ],
    "energy": [
        "oil field", "gas", "pipeline", "opec", "barrel", "energy",
        "حقل نفط", "غاز", "خط أنابيب", "أوبك", "برميل", "طاقة",
    ],
    "kurdistan": [
        "kurdistan", "peshmerga", "barzani", "erbil", "sulaymaniyah",
        "كردستان", "بيشمركة", "بارزاني", "أربيل", "السليمانية",
    ],
}


# ─────────────────────────────────────────────
# دوال المعالجة
# ─────────────────────────────────────────────

def detect_keywords(text: str, lang: str = "en") -> tuple[bool, list[str]]:
    """
    يكشف الكلمات المفتاحية — يعمل مع أي موضوع وليس العراق فقط
    """
    text_lower = text.lower()
    found = []
    for kw in get_keywords():
        if kw.lower() in text_lower and kw.lower() not in found:
            found.append(kw)
    return len(found) > 0, found[:20]

def detect_iraq(text: str, lang: str = "en") -> tuple[bool, list[str]]:
    """wrapper للتوافق"""
    return detect_keywords(text, lang)
def detect_category(text: str) -> str:
    """تصنيف المقالة تلقائياً"""
    text_lower = text.lower()
    scores = {cat: 0 for cat in CATEGORY_KEYWORDS}

    for cat, keywords in CATEGORY_KEYWORDS.items():
        for kw in keywords:
            if kw.lower() in text_lower:
                scores[cat] += 1

    best = max(scores, key=scores.get)
    return best if scores[best] > 0 else "other"


def score_importance(entry: dict, iraq_keywords: list) -> int:
    """
    يحسب درجة أهمية المقالة (0-100)
    المعايير: عدد كلمات العراق + طول المقالة + الأولوية
    """
    score = 0

    # كثافة ذكر العراق
    score += min(len(iraq_keywords) * 8, 40)

    # وجود عنوان قوي
    title = entry.get("title", "")
    if any(kw.lower() in title.lower()
           for kw_list in IRAQ_KEYWORDS.values()
           for kw in kw_list):
        score += 20

    # طول المحتوى
    summary = entry.get("summary", "")
    if len(summary) > 500:
        score += 10
    if len(summary) > 1500:
        score += 10

    # وجود تاريخ حديث
    published = entry.get("published_parsed")
    if published:
        score += 10

    # وجود رابط أصلي
    if entry.get("link"):
        score += 10

    return min(score, 100)


def url_hash(url: str) -> str:
    """معرف فريد للرابط لتجنب التكرار"""
    return hashlib.md5(url.encode()).hexdigest()


def parse_date(entry) -> Optional[str]:
    """تحليل التاريخ من feedparser"""
    if entry.get("published_parsed"):
        try:
            dt = datetime(*entry.published_parsed[:6], tzinfo=timezone.utc)
            return dt.isoformat()
        except Exception:
            pass
    return None


def extract_body(entry) -> str:
    """استخراج النص الكامل من المقالة"""
    # feedparser يضع المحتوى الكامل في content[0].value إن وجد
    if entry.get("content"):
        return entry.content[0].get("value", "")
    return entry.get("summary", "")


def article_exists(url: str) -> bool:
    """تحقق إذا كان الرابط موجود مسبقاً"""
    try:
        res = supabase.table("articles")\
            .select("id")\
            .eq("original_url", url)\
            .limit(1)\
            .execute()
        return len(res.data) > 0
    except Exception:
        return False


# ─────────────────────────────────────────────
# المعالج الرئيسي
# ─────────────────────────────────────────────

def process_source(source: dict) -> int:
    """
    يعالج مصدراً واحداً ويرجع عدد المقالات المضافة.
    """
    rss_url = source.get("rss_url")
    if not rss_url:
        log.debug(f"[{source['slug']}] لا يوجد RSS — تخطّي")
        return 0

    log.info(f"[{source['slug']}] جلب: {rss_url}")

    try:
        feed = feedparser.parse(rss_url)
    except Exception as e:
        log.error(f"[{source['slug']}] خطأ في الجلب: {e}")
        return 0

    if feed.bozo and not feed.entries:
        log.warning(f"[{source['slug']}] تغذية معطوبة أو فارغة")
        return 0

    added = 0

    for entry in feed.entries:
        url = entry.get("link", "")
        if not url:
            continue

        # تجنب التكرار
        if article_exists(url):
            continue

        title = entry.get("title", "").strip()
        body  = extract_body(entry)
        full_text = f"{title} {body}"

        lang = source.get("language", "en")
        mentions_iraq, iraq_keywords = detect_iraq(full_text, lang)

        importance = 0
        category   = "other"

        # حساب الأهمية والتصنيف لكل الأخبار المطابقة
        if mentions_iraq:
            importance = score_importance(entry, iraq_keywords)
            category   = detect_category(full_text)
        else:
            importance = min(len(iraq_keywords) * 15, 70)
            category   = detect_category(full_text)

        # ترجمة العنوان والنص إذا لم يكن عربياً
        title_ar = title if lang == 'ar' else translate_to_arabic(title, lang)
        body_ar  = body[:2000] if lang == 'ar' else translate_to_arabic(body[:2000], lang)

        row = {
            "source_id":          source["id"],
            "title_original":     title,
            "title_ar":           title_ar,
            "body_original":      body[:10000],
            "body_ar":            body_ar,
            "language":           lang,
            "original_url":       url,
            "mentions_iraq":      mentions_iraq,
            "iraq_keywords":      iraq_keywords,
            "iraq_relevance_score": min(len(iraq_keywords) * 10, 100),
            "category":           category,
            "importance_score":   importance,
            "published_at":       parse_date(entry),
            "fetched_at":         datetime.now(timezone.utc).isoformat(),
        }

        try:
            supabase.table("articles").insert(row).execute()
            added += 1
            kw_count = len(iraq_keywords)
            log.info(f"  ✓ [{importance:3d}] [{kw_count}kw] {title[:70]}")
        except Exception as e:
            log.error(f"  ✗ خطأ في الإدراج: {e}")

    return added


def run():
    """الدالة الرئيسية"""
    log.info("=" * 60)
    log.info("Iraq Intel Fetcher — بدء الجلب")
    log.info("=" * 60)

    # جلب الكلمات المفتاحية أولاً
    load_keywords_from_db()

    # جلب المصادر النشطة من Supabase
    try:
        res = supabase.table("sources")\
            .select("*")\
            .eq("is_active", True)\
            .not_.is_("rss_url", "null")\
            .execute()
        sources = res.data
    except Exception as e:
        log.error(f"فشل جلب المصادر: {e}")
        return

    log.info(f"مصادر نشطة: {len(sources)}")

    total_added   = 0
    total_iraq    = 0

    for source in sources:
        added = process_source(source)
        total_added += added

    # إحصائية سريعة من قاعدة البيانات
    try:
        res = supabase.table("articles")\
            .select("id", count="exact")\
            .eq("mentions_iraq", True)\
            .gte("fetched_at",
                 datetime.now(timezone.utc).replace(
                     hour=0, minute=0, second=0
                 ).isoformat())\
            .execute()
        total_iraq = res.count or 0
    except Exception:
        pass

    log.info("=" * 60)
    log.info(f"✓ تمت الدورة — مضاف: {total_added} | عراقي اليوم: {total_iraq}")
    log.info("=" * 60)


if __name__ == "__main__":
    run()
