"""
crawl_portal tool
-----------------
Playwright-based headless browser crawling for state Medicaid portals.
Handles SharePoint, JS-rendered pages, and dynamic content.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any
from urllib.parse import unquote, urljoin, urlparse

from playwright.sync_api import Browser, BrowserContext, Page, sync_playwright
from strands import tool

logger = logging.getLogger(__name__)

DOWNLOADABLE_EXTENSIONS: dict[str, str] = {
    ".pdf": "pdf",
    ".xls": "xls",
    ".xlsx": "xlsx",
    ".csv": "csv",
    ".zip": "zip",
}

# Keywords that indicate a link leads to fee-schedule content (follow deeper)
FEE_SCHEDULE_NAV_KEYWORDS: list[str] = [
    "fee schedule", "fee-schedule", "feeschedule",
    "rates", "reimbursement", "rate schedule",
    "provider", "billing", "payment",
    "physician", "dental", "pharmacy", "dme",
    "outpatient", "inpatient", "laboratory",
    "behavioral", "home health", "vision",
    "medicaid", "hcpcs", "cpt",
    "schedule of maximum allowance",
    "max allowable", "allowable fee",
    "shared documents", "documents",
]

# Keywords that signal irrelevant navigation links (skip)
SKIP_KEYWORDS: list[str] = [
    "login", "sign in", "sign out", "logout",
    "contact us", "privacy policy", "terms of use",
    "accessibility", "sitemap", "help", "faq",
    "careers", "about us", "news", "press",
    "social media", "facebook", "twitter",
    "javascript:", "mailto:", "#",
]


# ── Helpers ───────────────────────────────────────────────────────────────────


def _detect_file_type(url: str) -> str | None:
    """Return file-type string if URL points to a downloadable file, else None."""
    clean = unquote(url.lower()).split("?")[0].split("#")[0]
    for ext, ftype in DOWNLOADABLE_EXTENSIONS.items():
        if clean.endswith(ext):
            return ftype
    return None


def _is_same_domain(base_url: str, target_url: str) -> bool:
    """True when *target_url* shares the same (or sub-) domain as *base_url*."""
    base = urlparse(base_url).netloc.replace("www.", "")
    target = urlparse(target_url).netloc.replace("www.", "")
    return (
        base == target
        or target.endswith(f".{base}")
        or base.endswith(f".{target}")
    )


def _is_relevant_nav_link(link_text: str, href: str) -> bool:
    """Heuristic: should this navigation link be followed for deeper crawling?"""
    combined = f"{link_text} {href}".lower()
    if any(skip in combined for skip in SKIP_KEYWORDS):
        return False
    return any(kw in combined for kw in FEE_SCHEDULE_NAV_KEYWORDS)


def _detect_portal_type(page: Page, url: str) -> str:
    """Guess portal technology from page content / URL patterns."""
    content = page.content().lower()
    if "sharepoint" in content or "/_layouts/" in url or "sp." in urlparse(url).netloc:
        return "sharepoint"
    if "drupal" in content:
        return "drupal"
    if "wordpress" in content or "wp-content" in content:
        return "wordpress"
    return "custom"


def _extract_page_content(page: Page, base_url: str) -> dict[str, Any]:
    """
    Extract downloadable dataset links and relevant navigation links
    from the current page.
    """
    datasets: list[dict] = []
    nav_links: list[str] = []

    try:
        page.wait_for_load_state("networkidle", timeout=15_000)
    except Exception:
        pass  # best-effort; some pages never reach 'networkidle'

    anchors = page.query_selector_all("a[href]")

    for anchor in anchors:
        try:
            href = anchor.get_attribute("href")
            if not href:
                continue

            full_url = urljoin(page.url, href.strip())

            if any(full_url.startswith(p) for p in ("javascript:", "mailto:")):
                continue

            link_text = (anchor.inner_text() or "").strip()
            if not link_text:
                link_text = anchor.get_attribute("title") or ""

            # Surrounding context
            context = ""
            parent_section = ""
            try:
                context = page.evaluate(
                    """(el) => {
                        let p = el.closest('tr') || el.closest('li')
                              || el.closest('div') || el.parentElement;
                        return p ? p.innerText.substring(0, 500) : '';
                    }""",
                    anchor,
                )
                parent_section = page.evaluate(
                    """(el) => {
                        let s = el.closest('table') || el.closest('ul')
                              || el.closest('section');
                        let h = s ? s.querySelector('th, h1, h2, h3, h4, caption') : null;
                        return h ? h.innerText.substring(0, 200) : '';
                    }""",
                    anchor,
                )
            except Exception:
                pass

            # Try to detect a date in the same row
            last_modified = None
            try:
                last_modified = page.evaluate(
                    r"""(el) => {
                        let row = el.closest('tr');
                        if (!row) return null;
                        for (let cell of row.querySelectorAll('td')) {
                            let t = cell.innerText;
                            if (/\d{1,2}[\/\-]\d{1,2}[\/\-]\d{2,4}/.test(t)) return t.trim();
                            if (/\d{4}[\/\-]\d{1,2}[\/\-]\d{1,2}/.test(t)) return t.trim();
                            if (/(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)/i.test(t)) return t.trim();
                        }
                        return null;
                    }""",
                    anchor,
                )
            except Exception:
                pass

            # Extract file extension
            file_ext = href.lower().split('.')[-1] if '.' in href else ''
            
            # ONLY keep flat files: Excel and CSV
            if file_ext not in ['xlsx', 'xls', 'csv']:
                continue  # Skip PDF, ZIP, and all other file types

            file_type = _detect_file_type(full_url)
            if file_type:
                datasets.append({
                    "url": full_url,
                    "title": link_text or unquote(full_url.split("/")[-1]),
                    "file_type": file_type,
                    "page_source_url": page.url,
                    "context_text": (context or "")[:500],
                    "parent_section": (parent_section or "")[:200],
                    "last_modified": last_modified,
                })
            elif _is_same_domain(base_url, full_url) and _is_relevant_nav_link(link_text, full_url):
                nav_links.append(full_url)

        except Exception as exc:
            logger.debug("Error processing anchor: %s", exc)

    # SharePoint: also check for iframes that may contain document libraries
    try:
        for iframe in page.query_selector_all("iframe"):
            src = iframe.get_attribute("src")
            if src and _is_same_domain(base_url, urljoin(page.url, src)):
                nav_links.append(urljoin(page.url, src))
    except Exception:
        pass

    return {"datasets": datasets, "nav_links": list(set(nav_links))}


# ── Strands tool ──────────────────────────────────────────────────────────────


@tool
def crawl_portal(portal_url: str, max_depth: int = 1) -> dict[str, Any]:
    """
    Crawl a state Medicaid portal using Playwright headless browser to discover
    all downloadable Medicaid fee schedule dataset links.

    This tool navigates through the portal pages, handles JavaScript-rendered
    content (including SharePoint portals), and extracts downloadable file links
    (PDF, Excel, CSV, ZIP) along with their metadata such as surrounding context,
    section headers, and last modified dates.

    The crawl is bounded by max_depth to prevent infinite traversal. Only links
    that appear relevant to Medicaid fee schedules are followed for deeper crawling.

    Args:
        portal_url: The root URL of the state Medicaid fee schedule portal.
                    Example: 'https://extranet-sp.dhss.alaska.gov/hcs/medicaidalaska/Provider/Sites/FeeSchedule.html'
        max_depth: Maximum number of link-hops from the root URL to follow. Default 3.

    Returns:
        A dictionary containing:
        - datasets: List of discovered downloadable files with metadata
        - crawled_pages: List of all page URLs visited
        - portal_type: Detected portal technology (sharepoint, drupal, custom)
        - errors: Any errors encountered during crawling
        - total_discovered: Count of unique downloadable files found
    """
    # ── Defense-in-depth URL validation (tool-level) ────────────────────
    parsed_url = urlparse(portal_url)
    if parsed_url.scheme not in ("http", "https") or not parsed_url.netloc:
        return {
            "datasets": [],
            "crawled_pages": [],
            "portal_type": "unknown",
            "errors": [f"Invalid portal URL: '{portal_url}' — must be a valid http(s) URL with a hostname."],
            "total_discovered": 0,
        }

    all_datasets: list[dict] = []
    crawled_pages: list[str] = []
    errors: list[str] = []
    portal_type = "unknown"

    visited: set[str] = set()
    queue: list[tuple[str, int]] = [(portal_url, 0)]

    # Track AgentCore browser session for cleanup
    _agentcore_browser_client = None

    with sync_playwright() as pw:
        try:
            if os.environ.get("DOCKER_CONTAINER"):
                # ── AgentCore Browser (production) ─────────────────────
                # Use the managed cloud browser via CDP instead of a
                # local Chromium binary.  See:
                # https://docs.aws.amazon.com/bedrock-agentcore/latest/devguide/browser-quickstart-playwright.html
                from bedrock_agentcore.tools.browser_client import BrowserClient  # noqa: E402

                region = os.environ.get("AWS_REGION", "us-east-1")
                _agentcore_browser_client = BrowserClient(region)
                _agentcore_browser_client.start(
                    viewport={"width": 1920, "height": 1080},
                )
                ws_url, ws_headers = _agentcore_browser_client.generate_ws_headers()

                logger.info("Connecting to AgentCore Browser via CDP …")
                browser: Browser = pw.chromium.connect_over_cdp(
                    ws_url,
                    headers=ws_headers,
                )
            else:
                # ── Local Chromium (development / testing) ─────────────
                logger.info("Launching local Chromium (dev mode) …")
                browser = pw.chromium.launch(
                    headless=True,
                    args=["--no-sandbox", "--disable-setuid-sandbox"],
                )
        except Exception as exc:
            if _agentcore_browser_client:
                try:
                    _agentcore_browser_client.stop()
                except Exception:
                    pass
            return {
                "datasets": [],
                "crawled_pages": [],
                "portal_type": "unknown",
                "errors": [f"Browser launch failed: {exc}"],
                "total_discovered": 0,
            }

        context: BrowserContext = browser.new_context(
            user_agent=(
                "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
            ),
            viewport={"width": 1920, "height": 1080},
            accept_downloads=False,
            ignore_https_errors=True,
        )
        context.set_default_timeout(30_000)
        page = context.new_page()

        while queue:
            current_url, depth = queue.pop(0)
            normalised = current_url.split("#")[0].rstrip("/")
            if normalised in visited or depth > max_depth:
                continue
            visited.add(normalised)

            try:
                logger.info("[Depth %d/%d] Crawling: %s",
                            depth, max_depth, current_url)
                response = page.goto(
                    current_url, wait_until="domcontentloaded", timeout=30_000)

                if response and response.status >= 400:
                    errors.append(f"HTTP {response.status} for {current_url}")
                    continue

                # Extra wait for JS-heavy / SharePoint pages
                page.wait_for_timeout(3_000)

                # Expand collapsed sections (SharePoint-specific)
                try:
                    expanders = page.query_selector_all(
                        "[aria-expanded='false'], .ms-navheader, .expand-collapse"
                    )
                    for exp in expanders[:10]:
                        exp.click()
                        page.wait_for_timeout(500)
                except Exception:
                    pass

                if depth == 0:
                    portal_type = _detect_portal_type(page, current_url)
                    logger.info("Detected portal type: %s", portal_type)

                crawled_pages.append(current_url)

                result = _extract_page_content(page, portal_url)
                all_datasets.extend(result["datasets"])

                if depth < max_depth:
                    for nav_link in result["nav_links"]:
                        nav_norm = nav_link.split("#")[0].rstrip("/")
                        if nav_norm not in visited:
                            queue.append((nav_link, depth + 1))

            except Exception as exc:
                msg = f"Error crawling {current_url}: {exc}"
                logger.error(msg)
                errors.append(msg)

        browser.close()

        # Clean up AgentCore Browser session
        if _agentcore_browser_client:
            try:
                _agentcore_browser_client.stop()
                logger.info("AgentCore Browser session stopped.")
            except Exception as exc:
                logger.warning("Failed to stop AgentCore Browser session: %s", exc)

    # De-duplicate by URL
    seen: set[str] = set()
    unique: list[dict] = []
    for ds in all_datasets:
        key = ds["url"].split("#")[0].rstrip("/")
        if key not in seen:
            seen.add(key)
            unique.append(ds)

    logger.info("Crawl complete: %d unique datasets from %d pages",
                len(unique), len(crawled_pages))

    return {
        "datasets": unique,
        "crawled_pages": crawled_pages,
        "portal_type": portal_type,
        "errors": errors,
        "total_discovered": len(unique),
    }
