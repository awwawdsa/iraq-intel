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
IRAQ_KEYWORDS = {
    # العربية
    "ar": [
        "العراق", "عراقي", "عراقية", "عراقيين",
        "بغداد", "البصرة", "الموصل", "أربيل", "النجف", "كربلاء",
        "الكوفة", "الرمادي", "تكريت", "الفلوجة", "كركوك",
        "السليمانية", "دهوك", "الأنبار", "ذي قار", "ميسان",
        "الحشد الشعبي", "الحشد", "فصائل", "الكاظمي", "السوداني",
        "البرلمان العراقي", "مجلس النواب",
        "اقليم كردستان", "البيشمركة", "مسعود بارزاني",
        "كتائب حزب الله", "الحرس الثوري في العراق",
        "سامراء", "الديوانية", "واسط", "صلاح الدين",
    ],
    # الإنجليزية
    "en": [
        "iraq", "iraqi", "baghdad", "basra", "mosul", "erbil", "irbil",
        "najaf", "karbala", "kirkuk", "sulaymaniyah", "dohuk",
        "anbar", "tikrit", "fallujah", "ramadi", "samarra",
        "kurdistan", "peshmerga", "pmf", "popular mobilization",
        "hashd", "hashd al-shaabi", "kataib hezbollah",
        "iraqi parliament", "council of representatives",
        "al-sudani", "sudani", "barzani", "masoud barzani",
        "shia militias iraq", "iran iraq", "us forces iraq",
        "isis iraq", "isil iraq", "daesh iraq",
    ],
    # الفارسية
    "fa": [
        "عراق", "بغداد", "بصره", "موصل", "اربیل",
        "حشد شعبی", "کردستان عراق", "پیشمرگه",
    ],
    # التركية
    "tr": [
        "irak", "bağdat", "basra", "musul", "erbil",
        "kürdistan", "peşmerge", "haşdi şabi",
        "kuzey irak", "irak kürtleri",
    ],
}

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

def detect_iraq(text: str, lang: str = "en") -> tuple[bool, list[str]]:
    """
    يكشف إذا كان النص يذكر العراق.
    يرجع (True/False, قائمة الكلمات المفتاحية التي وجدها)
    """
    text_lower = text.lower()
    found = []

    # دائماً نفحص الإنجليزية والعربية بغض النظر عن لغة المصدر
    langs_to_check = ["en", "ar"]
    if lang in IRAQ_KEYWORDS and lang not in langs_to_check:
        langs_to_check.append(lang)

    for check_lang in langs_to_check:
        for kw in IRAQ_KEYWORDS.get(check_lang, []):
            if kw.lower() in text_lower:
                found.append(kw)

    return bool(found), list(set(found))


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

        if mentions_iraq:
            importance = score_importance(entry, iraq_keywords)
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
            status = "🇮🇶" if mentions_iraq else "  "
            log.info(f"  {status} [{importance:3d}] {title[:70]}")
        except Exception as e:
            log.error(f"  ✗ خطأ في الإدراج: {e}")

    return added


def run():
    """الدالة الرئيسية"""
    log.info("=" * 60)
    log.info("Iraq Intel Fetcher — بدء الجلب")
    log.info("=" * 60)

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
