import os
import asyncio
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── CATEGORIZACIÓN ─────────────────────────────────────────────────────────────
def guess_category(title):
    t = title.lower()
    if any(x in t for x in ["engineer","developer","devops","backend","frontend","front-end","back-end","fullstack","full stack","full-stack","software","qa","cloud","python","java","react","node","ios","android","mobile","sre","architect","data scientist","machine learning","ml ","ai ","infrastructure","platform","embedded","firmware","golang","ruby","php","scala","kotlin","swift","typescript","elixir"]): return "tech"
    if any(x in t for x in ["sales","account executive","business development","bdr","sdr","revenue","closing","quota","comercial","ventas","ejecutivo de cuenta","account manager","sales manager","sales rep"]): return "sales"
    if any(x in t for x in ["marketing","growth","seo","sem","content","brand","social media","copywriter","demand generation","performance","media buyer","paid","email market","crm market","comunicacion","publicidad","community manager","digital marketing"]): return "marketing"
    if any(x in t for x in ["design","designer","ux","ui ","user experience","user interface","graphic","visual","product designer","illustrat","figma","creative","motion"]): return "design"
    if any(x in t for x in ["finance","accounting","controller","cfo","treasury","bookkeeper","payroll","contabilidad","finanzas","tesoreria","cuentas por","financial analyst"]): return "finance"
    if any(x in t for x in ["hr","recruiter","talent acquisition","people ops","human resources","recursos humanos","rrhh","onboarding","hrbp","people partner"]): return "hr"
    if any(x in t for x in ["data analyst","data engineer","analytics","business intelligence","bi ","tableau","looker","power bi","sql","etl","data warehouse","scientist","databricks","dbt "]): return "data"
    if any(x in t for x in ["product manager","product owner","pm ","scrum master","agile coach","project manager","program manager","delivery manager","technical program"]): return "ops"
    if any(x in t for x in ["customer success","customer support","customer service","helpdesk","soporte","cx ","cs manager","client success","account manager","client manager"]): return "ops"
    return "ops"

def guess_seniority(title):
    t = title.lower()
    if any(x in t for x in ["intern","internship","pasante","trainee","practicante"]): return "intern"
    if any(x in t for x in ["junior","jr.","jr ","entry level","associate ","level 1","nivel 1","ssr ","semi senior","semi-senior","semisenior"]): return "junior"
    if any(x in t for x in ["senior","sr.","sr ","staff","principal","expert","especialista senior"]): return "senior"
    if any(x in t for x in ["lead","tech lead","team lead","lider","engineering lead","staff engineer"]): return "lead"
    if any(x in t for x in ["director","vp ","vice president","head of","chief","cto","cpo","coo","manager","gerente"]): return "director"
    return "mid"

def guess_work_type(text):
    t = (text or "").lower()
    if any(x in t for x in ["remote","remoto","latam","anywhere","worldwide","global","work from home","wfh","fully remote","100% remote","distributed","latin america","america latina"]): return "remote"
    if "hybrid" in t or "hibrido" in t or "híbrido" in t: return "hybrid"
    return "onsite"

def guess_employment_type(s):
    s = (s or "").lower()
    if "part" in s: return "parttime"
    if "contract" in s or "freelance" in s: return "contract"
    return "fulltime"

def is_valid_job_title(title):
    if not title or len(title) < 5 or len(title) > 150: return False
    t = title.lower().strip()
    skip_phrases = [
        "cookie","privacy policy","terms of service","faq","about us","contact us",
        "home","login","sign up","sign in","subscribe","newsletter","follow us",
        "read more","learn more","click here","submit","apply now","view all",
        "see all","load more","show more","©","copyright","all rights reserved",
        "powered by","built with","get started","schedule a call","book a demo",
        "our services","our solutions","our team","our mission","our values",
        "case studies","testimonials","how it works","why choose","what we do",
        "town life","it's all here","stay in the loop","explore more","community",
        "help us find","tell us more","share your info","get updates","why are you",
        "this website uses cookies","analytics technologies","head hunter de especialidad",
        "una extension","el match","flexible y ajustado","resultado de mayor",
        "evitamos sesgos","impulsamos","oportunidades para todos","solution",
        "jose b. gomez","maría","pedro","juan","carlos","ana ","luis ","maria ",
        "find your perfect","we'd like to get","please share","stay up on",
        "explore more of","why are you looking",
    ]
    for phrase in skip_phrases:
        if phrase in t: return False
    job_keywords = [
        "engineer","developer","analyst","manager","designer","coordinator",
        "specialist","consultant","director","lead","head of","officer","associate",
        "executive","representative","strategist","architect","scientist","researcher",
        "recruiter","advisor","support","success","operations","marketing","sales",
        "product","project","program","data","cloud","devops","backend","frontend",
        "fullstack","full stack","mobile","qa","ux","ui","seo","content","brand",
        "finance","accounting","hr","talent","people","customer","account","business",
        "software","platform","infrastructure","security","network","systems",
        "ingeniero","desarrollador","analista","gerente","diseñador","coordinador",
        "especialista","consultor","lider","ejecutivo","reclutador","asesor",
        "soporte","operaciones","ventas","producto","proyecto","contador",
        "recursos humanos","atención al cliente","servicio al cliente",
    ]
    return any(w in t for w in job_keywords)

# ── WORKABLE API ───────────────────────────────────────────────────────────────
def scrape_workable(slug, source_name, source_url):
    print(f"  Workable: {slug}")
    jobs = []
    try:
        r = requests.get(f"https://apply.workable.com/api/v1/widget/accounts/{slug}", timeout=15)
        data = r.json()
        for j in data.get("jobs", []):
            title = j.get("title","")
            if not title: continue
            location = j.get("location", {})
            loc_str = ", ".join(filter(None,[location.get("city",""), location.get("country","")])) if location else ""
            remote = j.get("remote", False)
            wt = "remote" if remote else guess_work_type(loc_str)
            jobs.append({
                "title": title,
                "location": loc_str or "LATAM",
                "category": guess_category(title),
                "seniority": guess_seniority(title),
                "work_type": wt,
                "employment_type": guess_employment_type(j.get("employment_type","")),
                "source_name": source_name,
                "source_id": slug,
                "source_ats": "workable",
                "apply_url": j.get("url",""),
                "source_url": source_url
            })
        print(f"    -> {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── GREENHOUSE API ─────────────────────────────────────────────────────────────
def scrape_greenhouse(slug, source_name, source_url):
    print(f"  Greenhouse: {slug}")
    jobs = []
    try:
        r = requests.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs", timeout=15)
        data = r.json()
        for j in data.get("jobs", []):
            title = j.get("title","")
            if not title: continue
            loc_str = j.get("location",{}).get("name","")
            wt = "remote" if any(x in loc_str.lower() for x in ["remote","remoto","anywhere","latam","latin america"]) else guess_work_type(loc_str)
            jobs.append({
                "title": title,
                "location": loc_str or "LATAM",
                "category": guess_category(title),
                "seniority": guess_seniority(title),
                "work_type": wt,
                "employment_type": "fulltime",
                "source_name": source_name,
                "source_id": slug,
                "source_ats": "greenhouse",
                "apply_url": j.get("absolute_url",""),
                "source_url": source_url
            })
        print(f"    -> {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── ASHBY API ──────────────────────────────────────────────────────────────────
def scrape_ashby(slug, source_name, source_url):
    print(f"  Ashby: {slug}")
    jobs = []
    try:
        r = requests.get(f"https://api.ashbyhq.com/posting-api/job-board/{slug}", timeout=15)
        data = r.json()
        for j in data.get("jobPostings", []):
            title = j.get("title","")
            if not title: continue
            loc_str = j.get("location","")
            is_remote = j.get("isRemote", False)
            wt = "remote" if is_remote else guess_work_type(loc_str)
            jobs.append({
                "title": title,
                "location": loc_str or "LATAM",
                "category": guess_category(title),
                "seniority": guess_seniority(title),
                "work_type": wt,
                "employment_type": guess_employment_type(j.get("employmentType","")),
                "source_name": source_name,
                "source_id": slug,
                "source_ats": "ashby",
                "apply_url": j.get("jobUrl",""),
                "source_url": source_url
            })
        print(f"    -> {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── LEVER API ──────────────────────────────────────────────────────────────────
def scrape_lever(slug, source_name, source_url):
    print(f"  Lever: {slug}")
    jobs = []
    try:
        r = requests.get(f"https://api.lever.co/v0/postings/{slug}?mode=json", timeout=15)
        data = r.json()
        for j in data:
            title = j.get("text","")
            if not title: continue
            categories = j.get("categories",{})
            loc_str = categories.get("location","") or categories.get("allLocations","")
            commitment = categories.get("commitment","")
            wt = "remote" if any(x in (loc_str+commitment).lower() for x in ["remote","remoto","anywhere","latam"]) else guess_work_type(loc_str)
            jobs.append({
                "title": title,
                "location": loc_str or "LATAM",
                "category": guess_category(title),
                "seniority": guess_seniority(title),
                "work_type": wt,
                "employment_type": guess_employment_type(commitment),
                "source_name": source_name,
                "source_id": slug,
                "source_ats": "lever",
                "apply_url": j.get("hostedUrl",""),
                "source_url": source_url
            })
        print(f"    -> {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── APPLYTOJOB API ─────────────────────────────────────────────────────────────
def scrape_applytojob(slug, source_name, source_url):
    print(f"  ApplyToJob: {slug}")
    jobs = []
    try:
        r = requests.get(
            f"https://{slug}.applytojob.com/apply",
            headers={"User-Agent":"Mozilla/5.0","Accept":"text/html"},
            timeout=15
        )
        soup = BeautifulSoup(r.text, "lxml")
        for item in soup.select("a.job-title, .job-title a, h3 a[href*='/apply/'], a[href*='/apply/']"):
            title = item.get_text(strip=True)
            link = item.get("href","")
            if not is_valid_job_title(title): continue
            if link and not link.startswith("http"):
                link = f"https://{slug}.applytojob.com{link}"
            jobs.append({
                "title": title,
                "location": "LATAM",
                "category": guess_category(title),
                "seniority": guess_seniority(title),
                "work_type": "remote",
                "employment_type": "fulltime",
                "source_name": source_name,
                "source_id": slug,
                "source_ats": "applytojob",
                "apply_url": link or source_url,
                "source_url": source_url
            })
        print(f"    -> {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── ZOHO RECRUIT ───────────────────────────────────────────────────────────────
def scrape_zoho(url, source_name, source_id):
    print(f"  Zoho: {source_name}")
    jobs = []
    try:
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        for item in soup.select(".jobTitleLink, .job-name a, .position-title a, td.jobTitleLink a"):
            title = item.get_text(strip=True)
            link = item.get("href","")
            if not is_valid_job_title(title): continue
            jobs.append({
                "title": title,
                "location": "LATAM",
                "category": guess_category(title),
                "seniority": guess_seniority(title),
                "work_type": "remote",
                "employment_type": "fulltime",
                "source_name": source_name,
                "source_id": source_id,
                "source_ats": "zoho",
                "apply_url": link if link.startswith("http") else url,
                "source_url": url
            })
        print(f"    -> {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── PLAYWRIGHT ─────────────────────────────────────────────────────────────────
async def scrape_playwright(url, source_name, source_id, selector):
    print(f"  Playwright: {source_name}")
    jobs = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            await page.goto(url, timeout=30000)
            await page.wait_for_selector(selector, timeout=15000)
            for item in await page.query_selector_all(selector):
                title = (await item.inner_text()).strip()
                if not is_valid_job_title(title): continue
                # Intentar conseguir el link de apply
                apply_url = url
                link_el = await item.query_selector("a")
                if link_el:
                    href = await link_el.get_attribute("href")
                    if href:
                        apply_url = href if href.startswith("http") else url + href
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
                    "apply_url": apply_url,
                    "source_url": url
                })
            await browser.close()
        print(f"    -> {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── MAIN ───────────────────────────────────────────────────────────────────────
async def main():
    all_jobs = []

    print("\n== Workable API ==")
    all_jobs += scrape_workable("hiresur", "Sur", "https://apply.workable.com/hiresur/")
    all_jobs += scrape_workable("remote-talent-latam", "Remote Talent LATAM", "https://apply.workable.com/remote-talent-latam/")

    print("\n== Greenhouse API ==")
    all_jobs += scrape_greenhouse("nearsure", "Nearsure", "https://www.nearsure.com/job-opportunities")
    all_jobs += scrape_greenhouse("andela", "Andela", "https://talent.andela.com/")

    print("\n== Ashby API ==")
    all_jobs += scrape_ashby("silver", "Silver Dev", "https://silver.dev")

    print("\n== Lever API ==")
    all_jobs += scrape_lever("deel", "Deel", "https://jobs.lever.co/deel")
    all_jobs += scrape_lever("remote", "Remote.com", "https://jobs.lever.co/remote")

    print("\n== ApplyToJob ==")
    all_jobs += scrape_applytojob("devlane", "Devlane", "https://devlane.applytojob.com/apply")

    print("\n== Zoho Recruit ==")
    all_jobs += scrape_zoho("https://thehireboost.zohorecruit.com/jobs/Careers", "HireBoost", "hireboost")

    print("\n== Playwright (JS sites) ==")
    all_jobs += await scrape_playwright("https://www.athyna.com/for-talent#Open-Roles", "Athyna", "athyna", "[class*='job'], [class*='role'], [class*='opening'], [class*='position']")
    all_jobs += await scrape_playwright("https://recruitcrm.io/jobs/TLNT_Group_jobs", "TLNT", "tlnt", ".job-list-item, [class*='job-card']")
    all_jobs += await scrape_playwright("https://jobs.simera.io/", "Simera", "simera", "[class*='job-card'], [class*='JobCard']")
    all_jobs += await scrape_playwright("https://pitcheers.com/jobs-search-result/", "Pitcheers", "pitcheers", "[class*='job'], [class*='position']")
    all_jobs += await scrape_playwright("https://www.theflock.com/en/talent/our-openings", "The Flock", "theflock", "[class*='opening'], [class*='job'], [class*='position']")
    all_jobs += await scrape_playwright("https://torre.ai/", "Torre", "torre", "[class*='opportunity'], [class*='job-card']")
    all_jobs += await scrape_playwright("https://hirelatam.com/jobs/", "HireLatam", "hirelatam", "[class*='job'], [class*='position'], [class*='opening']")
    all_jobs += await scrape_playwright("https://jobs.worldteams.com/jobs", "Worldteams", "worldteams", "[class*='job'], [class*='position']")
    all_jobs += await scrape_playwright("https://www.lupahire.com/open-roles", "Lupa Hire", "lupa", "[class*='job'], [class*='role'], [class*='position']")
    all_jobs += await scrape_playwright("https://jobs.hirewithnear.com/jobs/", "Near", "near", "[class*='job-title'], [class*='position']")
    all_jobs += await scrape_playwright("https://www.howdylatam.com/oportunidades", "Howdy", "howdy", "[class*='job'], [class*='oportunidad'], [class*='position']")
    all_jobs += await scrape_playwright("https://kala-talent.com/opportunities/", "KalaTalent", "kalatalent", "[class*='job'], [class*='opportunity'], [class*='position']")
    all_jobs += await scrape_playwright("https://webstarted.com/careers", "Webstarted", "webstarted", "[class*='job'], [class*='position'], [class*='career']")
    all_jobs += await scrape_playwright("https://www.onstrider.com/jobs", "Strider", "strider", "[class*='job'], [class*='position'], [class*='role']")
    all_jobs += await scrape_playwright("https://work.withforward.com/", "Forward", "forward", "[class*='job'], [class*='role'], [class*='position'], h2 a, h3 a")

    # Filtro final — solo títulos válidos
    all_jobs = [j for j in all_jobs if is_valid_job_title(j.get("title",""))]

    print(f"\n== Total válidos: {len(all_jobs)} jobs ==")

    if all_jobs:
        print("Guardando en Supabase...")
        try:
            supabase.table("jobs").update({"is_active": False}).eq("is_active", True).execute()
            supabase.table("jobs").insert(all_jobs).execute()
            print(f"OK: {len(all_jobs)} jobs guardados")
        except Exception as e:
            print(f"ERROR Supabase: {e}")
    else:
        print("No se encontraron jobs validos")

if __name__ == "__main__":
    asyncio.run(main())