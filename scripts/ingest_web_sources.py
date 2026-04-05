"""
Ingest DSSY web sources into RAG document_chunks table.
Fetches official Goa government URLs, extracts text, and stores as
searchable chunks in Neon PostgreSQL + pgvector.

Run manually:   python -m scripts.ingest_web_sources
Or auto-runs at app startup via main.py lifespan.
"""
import asyncio
import logging
import re
import httpx

logger = logging.getLogger(__name__)

# ── Official DSSY / Goa Government URLs to scrape ──────────────────────────
# These are static government pages — content rarely changes.
WEB_SOURCES = [
    {
        "url": "https://socialwelfare.goa.gov.in/dayanand-social-security-scheme-dsss/",
        "name": "DSSY_Official_Page",
        "desc": "Official DSSY scheme page from Directorate of Social Welfare, Government of Goa",
    },
    {
        "url": "https://socialwelfare.goa.gov.in/schemes/",
        "name": "Goa_SW_All_Schemes",
        "desc": "All schemes administered by Directorate of Social Welfare, Goa",
    },
    {
        "url": "https://socialwelfare.goa.gov.in/",
        "name": "Goa_SW_Homepage",
        "desc": "Directorate of Social Welfare, Government of Goa homepage",
    },
    {
        "url": "https://socialwelfare.goa.gov.in/list-of-beneficiaries/",
        "name": "DSSY_Beneficiary_List_Page",
        "desc": "DSSY beneficiary list page from Directorate of Social Welfare",
    },
    {
        "url": "https://scpwd.goa.gov.in/statesectorschemes/",
        "name": "Goa_Disability_Schemes",
        "desc": "State sector disability schemes including DSSY from State Commissioner for PwD, Goa",
    },
    {
        "url": "https://www.myscheme.gov.in/schemes/ddssy",
        "name": "DDSSY_MyScheme",
        "desc": "DDSSY (Deen Dayal Swasthya Seva Yojana) on myScheme - different from DSSY",
    },
    {
        "url": "https://goaonline.gov.in/appln/uil/deptservices?__DocId=IFT&__ServiceId=IFT47",
        "name": "Goa_Online_DSSY_Service",
        "desc": "DSSY application service on Goa Online portal",
    },
]

# Timeout and retry settings
FETCH_TIMEOUT = 30
MAX_RETRIES = 2


def _html_to_text(html: str) -> str:
    """Extract readable text from HTML without BeautifulSoup."""
    # Remove script and style blocks
    text = re.sub(r'<script[^>]*>.*?</script>', '', html, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<style[^>]*>.*?</style>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<nav[^>]*>.*?</nav>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<footer[^>]*>.*?</footer>', '', text, flags=re.DOTALL | re.IGNORECASE)
    text = re.sub(r'<header[^>]*>.*?</header>', '', text, flags=re.DOTALL | re.IGNORECASE)
    # Convert common tags to readable format
    text = re.sub(r'<br\s*/?>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</?p[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</?div[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'</?li[^>]*>', '\n- ', text, flags=re.IGNORECASE)
    text = re.sub(r'<h[1-6][^>]*>', '\n## ', text, flags=re.IGNORECASE)
    text = re.sub(r'</h[1-6]>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<tr[^>]*>', '\n', text, flags=re.IGNORECASE)
    text = re.sub(r'<t[dh][^>]*>', ' | ', text, flags=re.IGNORECASE)
    # Strip all remaining tags
    text = re.sub(r'<[^>]+>', '', text)
    # Decode common HTML entities
    text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
    text = text.replace('&nbsp;', ' ').replace('&quot;', '"').replace('&#39;', "'")
    text = re.sub(r'&#\d+;', '', text)
    text = re.sub(r'&\w+;', '', text)
    # Clean up whitespace
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'\n\s*\n', '\n\n', text)
    text = re.sub(r'\n{3,}', '\n\n', text)
    return text.strip()


async def fetch_url(url: str) -> str | None:
    """Fetch a URL and return extracted text, or None on failure."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) NAG-DSSY-RAG/3.0",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    }
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            async with httpx.AsyncClient(timeout=FETCH_TIMEOUT, follow_redirects=True) as client:
                resp = await client.get(url, headers=headers)
                if resp.status_code == 200:
                    content_type = resp.headers.get("content-type", "")
                    if "text/html" in content_type or "application/xhtml" in content_type:
                        text = _html_to_text(resp.text)
                        if len(text) > 100:
                            logger.info(f"Fetched {url}: {len(text)} chars")
                            return text
                        else:
                            logger.warning(f"URL {url} returned too little text ({len(text)} chars)")
                            return None
                    elif "application/pdf" in content_type:
                        logger.info(f"Skipping PDF: {url}")
                        return None
                    else:
                        logger.warning(f"Unexpected content-type from {url}: {content_type}")
                        return None
                else:
                    logger.warning(f"HTTP {resp.status_code} from {url} (attempt {attempt})")
        except Exception as e:
            logger.warning(f"Fetch failed for {url} (attempt {attempt}): {e}")
        if attempt < MAX_RETRIES:
            await asyncio.sleep(2)
    return None


async def fetch_all_sources() -> list[dict]:
    """Fetch all web sources and return list of {name, text, url, desc}."""
    results = []
    for src in WEB_SOURCES:
        text = await fetch_url(src["url"])
        if text:
            # Prepend source metadata to the content
            header = (
                f"[SOURCE: {src['desc']}]\n"
                f"[URL: {src['url']}]\n\n"
            )
            results.append({
                "name": src["name"],
                "text": header + text,
                "url": src["url"],
                "desc": src["desc"],
            })
    return results


async def ingest_web_sources(db):
    """
    Fetch all DSSY web sources and ingest into document_chunks.
    Skips sources already ingested (by doc_name).
    """
    from backend.services.rag_service import is_ingested, ingest

    sources = await fetch_all_sources()
    ingested_count = 0

    for src in sources:
        doc_name = f"WEB_{src['name']}"
        if await is_ingested(db, doc_name):
            logger.info(f"Already ingested: {doc_name}")
            continue
        try:
            await ingest(
                db, doc_name, src["text"],
                {"source": src["url"], "type": "web_scrape", "desc": src["desc"]}
            )
            ingested_count += 1
            logger.info(f"Ingested web source: {doc_name} ({len(src['text'])} chars)")
        except Exception as e:
            logger.error(f"Failed to ingest {doc_name}: {e}")

    logger.info(f"Web source ingestion complete: {ingested_count} new sources ingested")
    return ingested_count


# ── Manual run ──────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys, os
    sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
    from dotenv import load_dotenv
    load_dotenv()

    from backend.database import neon_session_context
    from backend.services.rag_service import setup

    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    async def main():
        async with neon_session_context() as db:
            await setup(db)
            count = await ingest_web_sources(db)
            print(f"\nDone. Ingested {count} web sources.")

    asyncio.run(main())
