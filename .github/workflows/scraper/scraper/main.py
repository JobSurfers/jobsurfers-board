import os
import re
import asyncio
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from supabase import create_client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── NORMALIZACIÓN ──────────────────────────────────────────────────────────────

def guess_category(title):
    t = title.lower()
    if any(x in t for x in ["engineer","developer","devops","backend","frontend","fullstack","data","ml","qa","cloud","software","python","java","react","node"]): return "tech"
    if any(x in t for x in ["sales","account executive","business development","bdr","sdr"]): return "sales"
    if any(x in t for x in ["marketing","growth","seo","content","brand","social media"]): return "marketing"
    if any(x in t for x in ["design","ux","ui","product designer","graphic"]): return "design"
    if any(x in t for x in ["finance","accounting","controller","cfo","treasury"]): return "finance"
    if any(x in t for x in ["hr","recruiter","talent","people","human resources"]): return "hr"
    if any(x in t for x in ["analyst","data analyst","bi","business intelligence"]): return "data"
    return "ops"

def guess_seniority(title):
    t = title.lower()
    if any(x in t for x in ["intern","internship","pasante"]): return "intern"
    if any(x in t for x in ["junior","jr","entry"]): return "junior"
    if any(x in t for x in ["senior","sr","staff","principal"]): return "senior"
    if any(x in t for x in ["lead","tech lead","team lead"]): return "lead"
    if any(x in t for x in ["director","vp","head of","chief"]): return "director"
    return "mid"

def guess_work_type(location):
    l = (location or "").lower()
    if any(x in l for x in ["remote","latam","anywhere","worldwide","global"]): return "remote"
    if "hybrid" in l: return "hybrid"
    return "onsite"

def guess_employment_type(s):
    s = (s or "").lower()
    if "part" in s: return "parttime"
    if "contract" in s or "freelance" in s: return "contract"
    return "fulltime"

def guess_country(location):
    l = (location or "").lower()
    if "argentina" in l or "buenos aires" in l: return "argentina"
    if "brazil" in l or "brasil" in l or "são paulo" in l: return "brazil"
    if "mexico" in l or "méxico" in l: return "mexico"
    if "colombia" in l or "bogotá" in l: return "colombia"
    if "chile" in l or "santiago" in l: return "chile"
    if "uruguay" in l or "montevideo" in l: return "uruguay"
    return "latam"
    # ── WORKABLE ───────────────────────────────────────────────────────────────────

def scrape_workable(slug, source_name, source_url):
    print(f"  Workable: {slug}")
    jobs = []
    try:
        url = f"https://apply.workable.com/api/v1/widget/accounts/{slug}"
        r = requests.get(url, timeout=15)
        data = r.json()
        for j in data.get("jobs", []):
            title = j.get("title","")
            location = j.get("location", {})
            loc_str = location.get("city","") + ", " + location.get("country","") if location else ""
            jobs.append({
                "title": title,
                "location": loc_str,
                "category": guess_category(title),
                "seniority": guess_seniority(title),
                "work_type": guess_work_type(loc_str),
                "employment_type": guess_employment_type(j.get("employment_type","")),
                "source_name": source_name,
                "source_id": slug,
                "source_ats": "workable",
                "apply_url": j.get("url",""),
                "source_url": source_url,
            })
        print(f"    → {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── GREENHOUSE ─────────────────────────────────────────────────────────────────

def scrape_greenhouse(slug, source_name, source_url):
    print(f"  Greenhouse: {slug}")
    jobs = []
    try:
        url = f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"
        r = requests.get(url, timeout=15)
        data = r.json()
        for j in data.get("jobs", []):
            title = j.get("title","")
            loc_str = j.get("location",{}).get("name","")
            jobs.append({
                "title": title,
                "location": loc_str,
                "category": guess_category(title),
                "seniority": guess_seniority(title),
                "work_type": guess_work_type(loc_str),
                "employment_type": "fulltime",
                "source_name": source_name,
                "source_id": slug,
                "source_ats": "greenhouse",
                "apply_url": j.get("absolute_url",""),
                "source_url": source_url,
            })
        print(f"    → {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── ASHBY ──────────────────────────────────────────────────────────────────────

def scrape_ashby(slug, source_name, source_url):
    print(f"  Ashby: {slug}")
    jobs = []
    try:
        url = f"https://api.ashbyhq.com/posting-api/job-board/{slug}"
        r = requests.get(url, timeout=15)
        data = r.json()
        for j in data.get("jobPostings", []):
            title = j.get("title","")
            loc_str = j.get("location","")
            jobs.append({
                "title": title,
                "location": loc_str,
                "category": guess_category(title),
                "seniority": guess_seniority(title),
                "work_type": guess_work_type(loc_str),
                "employment_type": guess_employment_type(j.get("employmentType","")),
                "source_name": source_name,
                "source_id": slug,
                "source_ats": "ashby",
                "apply_url": j.get("jobUrl",""),
                "source_url": source_url,
            })
        print(f"    → {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs
    # ── BEAUTIFULSOUP ──────────────────────────────────────────────────────────────

def scrape_static(url, source_name, source_id, selectors):
    print(f"  Static: {source_name}")
    jobs = []
    try:
        headers = {"User-Agent": "Mozilla/5.0"}
        r = requests.get(url, headers=headers, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        for sel in selectors:
            items = soup.select(sel)
            for item in items:
                title = item.get_text(strip=True)
                link = item.get("href","") or (item.find("a") or {}).get("href","")
                if not title: continue
                jobs.append({
                    "title": title,
                    "location": "LATAM",
                    "category": guess_category(title),
                    "seniority": guess_seniority(title),
                    "work_type": "remote",
                    "employment_type": "fulltime",
                    "source_name": source_name,
                    "source_id": source_id,
                    "source_ats": "static",
                    "apply_url": link if link.startswith("http") else url,
                    "source_url": url,
                })
        print(f"    → {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── PLAYWRIGHT ─────────────────────────────────────────────────────────────────

async def scrape_playwright(url, source_name, source_id, selector, link_selector=None):
    print(f"  Playwright: {source_name}")
    jobs = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            await page.goto(url, timeout=30000)
            await page.wait_for_selector(selector, timeout=15000)
            items = await page.query_selector_all(selector)
            for item in items:
                title = await item.inner_text()
                title = title.strip()
                if not title: continue
                link = url
                if link_selector:
                    el = await item.query_selector(link_selector)
                    if el:
                        link = await el.get_attribute("href") or url
                        if not link.startswith("http"):
                            from urllib.parse import urljoin
                            link = urljoin(url, link)
                jobs.append({
                    "title": title,
                    "location": "LATAM",
                    "category": guess_category(title),
                    "seniority": guess_seniority(title),
                    "work_type": "remote",
                    "employment_type": "fulltime",
                    "source_name": source_name,
                    "source_id": source_id,
                    "source_ats": "playwright",
                    "apply_url": link,
                    "source_url": url,
                })
            await browser.close()
        print(f"    → {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs
    # ── MAIN ───────────────────────────────────────────────────────────────────────

async def main():
    all_jobs = []

    print("\n== APIs públicas ==")

    # Workable
    all_jobs += scrape_workable("careersactivatetalent", "Activate Talent", "https://apply.workable.com/careersactivatetalent/")
    all_jobs += scrape_workable("hiresur", "HireSur", "https://apply.workable.com/hiresur/")
    all_jobs += scrape_workable("remote-talent-latam", "Remote Talent LATAM", "https://apply.workable.com/remote-talent-latam/")

    # Greenhouse
    all_jobs += scrape_greenhouse("nearsure", "Nearsure", "https://www.nearsure.com/job-opportunities")
    all_jobs += scrape_greenhouse("andela", "Andela", "https://talent.andela.com/")

    # Ashby
    all_jobs += scrape_ashby("silver", "Silver.dev", "https://silver.dev")

    print("\n== HTML estático ==")

    all_jobs += scrape_static(
        "https://www.lupahire.com/open-roles", "Lupa Hire", "lupa",
        ["h3.job-title", "h3", ".job-title"]
    )
    all_jobs += scrape_static(
        "https://talent.latinolegends.com/jobs", "Latino Legends", "latinolegends",
        [".job-item a", ".position a", "h2 a"]
    )
    all_jobs += scrape_static(
        "https://jobs.hirewithnear.com/jobs/", "Near", "near",
        [".job-title", "h3 a", ".position-title"]
    )

    print("\n== Playwright (JS) ==")

    all_jobs += await scrape_playwright(
        "https://www.athyna.com/for-talent", "Athyna", "athyna",
        ".job-card", "a"
    )
    all_jobs += await scrape_playwright(
        "https://recruitcrm.io/jobs/TLNT_Group_jobs", "TLNT", "tlnt",
        ".job-list-item", "a"
    )
    all_jobs += await scrape_playwright(
        "https://jobs.simera.io/", "Simera", "simera",
        ".job-card", "a"
    )
    all_jobs += await scrape_playwright(
        "https://pitcheers.com/jobs-search-result/", "Pitcheers", "pitcheers",
        ".job-item", "a"
    )
    all_jobs += await scrape_playwright(
        "https://www.theflock.com/en/talent/our-openings", "The Flock", "theflock",
        ".opening-item", "a"
    )

    print(f"\n== Total recolectado: {len(all_jobs)} jobs ==")

    # Guardar en Supabase
    if all_jobs:
        print("Guardando en Supabase...")
        try:
            # Desactivar todos los anteriores
            supabase.table("jobs").update({"is_active": False}).eq("is_active", True).execute()
            # Insertar los nuevos
            supabase.table("jobs").insert(all_jobs).execute()
            print(f"✅ {len(all_jobs)} jobs guardados correctamente")
        except Exception as e:
            print(f"ERROR guardando en Supabase: {e}")
    else:
        print("⚠️ No se encontraron jobs")

if __name__ == "__main__":
    asyncio.run(main())