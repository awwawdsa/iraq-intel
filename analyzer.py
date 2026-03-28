"""
Iraq Intel Platform — Analyzer
================================
يأخذ المقالات غير المحللة من Supabase،
يرسلها لـ Claude API، ويخزن التحليل.

التشغيل:
    ANTHROPIC_API_KEY=sk-... python analyzer.py
"""

import os
import json
import logging
from datetime import datetime, timezone

import httpx
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

SUPABASE_URL  = os.environ["SUPABASE_URL"]
SUPABASE_KEY  = os.environ["SUPABASE_KEY"]
ANTHROPIC_KEY = os.environ["ANTHROPIC_API_KEY"]
MODEL         = "claude-sonnet-4-20250514"

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)


# ─────────────────────────────────────────────
# Prompt التحليل
# ─────────────────────────────────────────────

SYSTEM_PROMPT = """أنت محلل استخباراتي إعلامي متخصص بالشأن العراقي.
مهمتك تحليل مقالات إخبارية وتقارير بحثية وإنتاج تحليل منظم بالعربية.
كن دقيقاً وموضوعياً. لا تضف معلومات من خارج النص.
أجب دائماً بـ JSON فقط بدون أي نص إضافي أو backticks."""

ANALYSIS_PROMPT = """حلل المقالة التالية وأعطني JSON بهذا الهيكل بالضبط:

{{
  "summary_ar": "ملخص دقيق بالعربية في 3-5 جمل",
  "key_claims": "أبرز ادعاء أو معلومة في المقالة",
  "actors": "الأطراف الرئيسية المذكورة (أشخاص، حكومات، منظمات)",
  "related_topics": ["موضوع1", "موضوع2", "موضوع3"],
  "stance": "neutral | pro_iraq | anti_iraq | pro_iran | pro_us | pro_turkey | unclear",
  "significance": "low | medium | high | critical",
  "sentiment": "positive | negative | neutral | mixed",
  "event_context": "وصف موجز للحدث إن وجد، وإلا null"
}}

العنوان: {title}
المصدر: {source_name} ({source_region})
اللغة الأصلية: {language}

النص:
{body}
"""


# ─────────────────────────────────────────────
# الاتصال بـ Claude API
# ─────────────────────────────────────────────

def analyze_article(article: dict, source: dict) -> dict | None:
    """يرسل المقالة لـ Claude ويرجع التحليل كـ dict"""

    title  = article.get("title_ar") or article.get("title_original", "")
    body   = article.get("body_ar")  or article.get("body_original",  "")

    # تقليص النص إذا كان طويلاً جداً (حد السياق)
    body = body[:4000] if body else ""

    if not body and not title:
        return None

    prompt = ANALYSIS_PROMPT.format(
        title       = title[:300],
        source_name = source.get("name", ""),
        source_region = source.get("region", ""),
        language    = article.get("language", "en"),
        body        = body,
    )

    try:
        with httpx.Client(timeout=60) as client:
            resp = client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key":         ANTHROPIC_KEY,
                    "anthropic-version": "2023-06-01",
                    "content-type":      "application/json",
                },
                json={
                    "model":      MODEL,
                    "max_tokens": 800,
                    "system":     SYSTEM_PROMPT,
                    "messages": [
                        {"role": "user", "content": prompt}
                    ],
                }
            )
            resp.raise_for_status()
            data = resp.json()

        raw_text = data["content"][0]["text"].strip()

        # تنظيف إذا أضاف Claude backticks رغم التعليمات
        if raw_text.startswith("```"):
            raw_text = raw_text.split("```")[1]
            if raw_text.startswith("json"):
                raw_text = raw_text[4:]

        return json.loads(raw_text)

    except httpx.HTTPStatusError as e:
        log.error(f"HTTP error: {e.response.status_code} — {e.response.text[:200]}")
    except json.JSONDecodeError as e:
        log.error(f"JSON parse error: {e}")
    except Exception as e:
        log.error(f"Unexpected error: {e}")

    return None


# ─────────────────────────────────────────────
# المعالجة الدُفعية
# ─────────────────────────────────────────────

def run(batch_size: int = 20):
    """
    يعالج دُفعة من المقالات غير المحللة.
    batch_size: عدد المقالات لكل تشغيل (للتحكم بالتكلفة)
    """
    log.info("=" * 55)
    log.info("Iraq Intel Analyzer — بدء التحليل")
    log.info("=" * 55)

    # مقالات عراقية لم تُحلَّل بعد (LEFT JOIN على analyses)
    try:
        # Supabase لا يدعم LEFT JOIN مباشرة في client،
        # نستخدم RPC أو نجلب IDs المحللة أولاً
        analyzed_res = supabase.table("analyses")\
            .select("article_id")\
            .execute()
        analyzed_ids = {r["article_id"] for r in analyzed_res.data}

        articles_res = supabase.table("articles")\
            .select("*, sources(id, name, region)")\
            .eq("mentions_iraq", True)\
            .order("importance_score", desc=True)\
            .limit(batch_size + len(analyzed_ids))\
            .execute()

        articles = [
            a for a in articles_res.data
            if a["id"] not in analyzed_ids
        ][:batch_size]

    except Exception as e:
        log.error(f"فشل جلب المقالات: {e}")
        return

    if not articles:
        log.info("لا توجد مقالات جديدة للتحليل")
        return

    log.info(f"مقالات للتحليل: {len(articles)}")

    success = 0
    failed  = 0

    for article in articles:
        source = article.get("sources") or {}
        title  = (article.get("title_ar") or article.get("title_original", ""))[:60]

        log.info(f"  → {title}...")

        result = analyze_article(article, source)

        if not result:
            failed += 1
            continue

        # التحقق من صحة الحقول المطلوبة
        valid_stances     = {"neutral","pro_iraq","anti_iraq","pro_iran","pro_us","pro_turkey","unclear"}
        valid_significance= {"low","medium","high","critical"}
        valid_sentiment   = {"positive","negative","neutral","mixed"}

        row = {
            "article_id":     article["id"],
            "summary_ar":     result.get("summary_ar"),
            "key_claims":     result.get("key_claims"),
            "actors":         result.get("actors"),
            "related_topics": result.get("related_topics", []),
            "stance":         result.get("stance") if result.get("stance") in valid_stances else "unclear",
            "significance":   result.get("significance") if result.get("significance") in valid_significance else "medium",
            "sentiment":      result.get("sentiment") if result.get("sentiment") in valid_sentiment else "neutral",
            "event_context":  result.get("event_context"),
            "model_used":     MODEL,
            "analyzed_at":    datetime.now(timezone.utc).isoformat(),
        }

        try:
            supabase.table("analyses").insert(row).execute()

            # تحديث importance_score في articles بناءً على significance
            sig_score = {"low": 20, "medium": 50, "high": 75, "critical": 95}
            new_score = sig_score.get(row["significance"], 50)

            supabase.table("articles")\
                .update({"importance_score": new_score, "processed_at": row["analyzed_at"]})\
                .eq("id", article["id"])\
                .execute()

            log.info(f"    ✓ [{row['significance'].upper()}] {row['stance']}")
            success += 1

        except Exception as e:
            log.error(f"    ✗ خطأ في الحفظ: {e}")
            failed += 1

    log.info("=" * 55)
    log.info(f"✓ انتهى — نجاح: {success} | فشل: {failed}")
    log.info("=" * 55)


if __name__ == "__main__":
    import sys
    batch = int(sys.argv[1]) if len(sys.argv) > 1 else 20
    run(batch_size=batch)
