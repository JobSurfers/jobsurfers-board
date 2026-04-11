import os
import asyncio
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── HELPERS ────────────────────────────────────────────────────────────────────
def guess_category(title):
    t = title.lower()
    if any(x in t for x in ["engineer","developer","devops","backend","frontend","front-end","back-end","fullstack","full stack","full-stack","software","qa","cloud","python","java","react","node","ios","android","mobile","sre","architect","data scientist","machine learning","ml ","ai ","infrastructure","platform","embedded","firmware","golang","ruby","php","scala","kotlin","swift","typescript","elixir","wordpress","shopify"]): return "tech"
    if any(x in t for x in ["sales","account executive","business development","bdr","sdr","revenue","closing","quota","comercial","ventas","ejecutivo de cuenta","account manager","sales manager","sales rep","closer","setter"]): return "sales"
    if any(x in t for x in ["marketing","growth","seo","sem","content","brand","social media","copywriter","demand generation","performance","media buyer","paid","email market","crm market","comunicacion","publicidad","community manager","digital marketing","paid ads","facebook ads","google ads"]): return "marketing"
    if any(x in t for x in ["design","designer","ux","ui ","user experience","user interface","graphic","visual","product designer","illustrat","figma","creative","motion","video editor","editor"]): return "design"
    if any(x in t for x in ["finance","accounting","controller","cfo","treasury","bookkeeper","payroll","contabilidad","finanzas","tesoreria","cuentas por","financial analyst","accountant","bookkeeping"]): return "finance"
    if any(x in t for x in ["hr","recruiter","talent acquisition","people ops","human resources","recursos humanos","rrhh","onboarding","hrbp","people partner","staffing"]): return "hr"
    if any(x in t for x in ["data analyst","data engineer","analytics","business intelligence","bi ","tableau","looker","power bi","sql","etl","data warehouse","scientist","databricks","dbt "]): return "data"
    if any(x in t for x in ["product manager","product owner","pm ","scrum master","agile","project manager","program manager","delivery manager","technical program","project coordinator"]): return "ops"
    if any(x in t for x in ["customer success","customer support","customer service","helpdesk","soporte","cx ","cs manager","client success","account manager","virtual assistant","executive assistant","administrative"]): return "ops"
    return "ops"

def guess_seniority(title):
    t = title.lower()
    if any(x in t for x in ["intern","internship","pasante","trainee","practicante"]): return "intern"
    if any(x in t for x in ["junior","jr.","jr ","entry level","associate ","level 1","nivel 1","semi senior","semi-senior","semisenior","ssr "]): return "junior"
    if any(x in t for x in ["senior","sr.","sr ","staff","principal","expert"]): return "senior"
    if any(x in t for x in ["lead","tech lead","team lead","lider","engineering lead"]): return "lead"
    if any(x in t for x in ["director","vp ","vice president","head of","chief","cto","cpo","coo","manager","gerente"]): return "director"
    return "mid"

def guess_work_type(text):
    t = (text or "").lower()
    if any(x in t for x in ["remote","remoto","latam","anywhere","worldwide","global","work from home","wfh","fully remote","100% remote","distributed","latin america","america latina","anywhere in"]): return "remote"
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
    skip = [
        "cookie","privacy","terms","faq","about","contact","home","login","sign up",
        "subscribe","newsletter","follow","read more","learn more","click here",
        "submit","view all","see all","load more","©","copyright","powered by",
        "get started","schedule","book a demo","our services","our team","our mission",
        "case studies","testimonials","how it works","why choose","what we do",
        "jose b.","josé b.","pedro ","juan ","carlos ","ana g","luis g",
        "town life","it's all here","stay in the loop","explore more","community",
        "help us find","tell us more","share your info","get updates",
        "this website uses cookies","analytics technologies","head hunter de especialidad",
        "una extension","el match correcto","flexible y ajustado","resultado de mayor",
        "evitamos sesgos","impulsamos","oportunidades para todos",
        "find your perfect","we'd like to get","please share",
    ]
    for phrase in skip:
        if phrase in t: return False
    job_keywords = [
        "engineer","developer","analyst","manager","designer","coordinator","specialist",
        "consultant","director","lead","head of","officer","associate","executive",
        "representative","strategist","architect","scientist","researcher","recruiter",
        "advisor","support","success","operations","marketing","sales","product",
        "project","program","data","cloud","devops","backend","frontend","fullstack",
        "full stack","mobile","qa","ux","ui","seo","content","brand","finance",
        "accounting","hr","talent","people","customer","account","business","software",
        "platform","infrastructure","security","network","systems","editor","writer",
        "assistant","coordinator","bookkeeper","accountant","payroll","virtual",
        # español
        "ingeniero","desarrollador","analista","gerente","diseñador","coordinador",
        "especialista","consultor","lider","ejecutivo","reclutador","asesor",
        "soporte","operaciones","ventas","producto","proyecto","contador","asistente",
        "recursos humanos","atención","servicio al cliente","programador",
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
            jobs.append({"title":title,"location":loc_str or "LATAM","category":guess_category(title),"seniority":guess_seniority(title),"work_type":wt,"employment_type":guess_employment_type(j.get("employment_type","")),"source_name":source_name,"source_id":slug,"source_ats":"workable","apply_url":j.get("url",""),"source_url":source_url})
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
            jobs.append({"title":title,"location":loc_str or "LATAM","category":guess_category(title),"seniority":guess_seniority(title),"work_type":wt,"employment_type":"fulltime","source_name":source_name,"source_id":slug,"source_ats":"greenhouse","apply_url":j.get("absolute_url",""),"source_url":source_url})
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
            jobs.append({"title":title,"location":loc_str or "LATAM","category":guess_category(title),"seniority":guess_seniority(title),"work_type":wt,"employment_type":guess_employment_type(j.get("employmentType","")),"source_name":source_name,"source_id":slug,"source_ats":"ashby","apply_url":j.get("jobUrl",""),"source_url":source_url})
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
            jobs.append({"title":title,"location":loc_str or "LATAM","category":guess_category(title),"seniority":guess_seniority(title),"work_type":wt,"employment_type":guess_employment_type(commitment),"source_name":source_name,"source_id":slug,"source_ats":"lever","apply_url":j.get("hostedUrl",""),"source_url":source_url})
        print(f"    -> {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── APPLYTOJOB ─────────────────────────────────────────────────────────────────
def scrape_applytojob(slug, source_name, source_url):
    print(f"  ApplyToJob: {slug}")
    jobs = []
    try:
        r = requests.get(source_url, headers={"User-Agent":"Mozilla/5.0"}, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        for item in soup.select("a[href*='/apply/']"):
            title = item.get_text(strip=True)
            link = item.get("href","")
            if not is_valid_job_title(title): continue
            if link and not link.startswith("http"):
                link = f"https://{slug}.applytojob.com{link}"
            jobs.append({"title":title,"location":"LATAM","category":guess_category(title),"seniority":guess_seniority(title),"work_type":"remote","employment_type":"fulltime","source_name":source_name,"source_id":slug,"source_ats":"applytojob","apply_url":link or source_url,"source_url":source_url})
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
        for item in soup.select(".jobTitleLink, .job-name a, td.jobTitleLink a, .position-title a"):
            title = item.get_text(strip=True)
            link = item.get("href","")
            if not is_valid_job_title(title): continue
            jobs.append({"title":title,"location":"LATAM","category":guess_category(title),"seniority":guess_seniority(title),"work_type":"remote","employment_type":"fulltime","source_name":source_name,"source_id":source_id,"source_ats":"zoho","apply_url":link if link.startswith("http") else url,"source_url":url})
        print(f"    -> {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── RECRUITCRM ─────────────────────────────────────────────────────────────────
def scrape_recruitcrm(url, source_name, source_id):
    print(f"  RecruitCRM: {source_name}")
    jobs = []
    try:
        # RecruitCRM has a JSON API
        slug = url.split("/jobs/")[-1]
        api_url = f"https://recruitcrm.io/api/v1/jobs/{slug}"
        r = requests.get(api_url, headers={"User-Agent":"Mozilla/5.0","Accept":"application/json"}, timeout=15)
        if r.status_code == 200:
            data = r.json()
            items = data.get("data", data.get("jobs", []))
            for j in items:
                title = j.get("name","") or j.get("title","")
                if not is_valid_job_title(title): continue
                loc_str = j.get("city","") or j.get("location","")
                jobs.append({"title":title,"location":loc_str or "LATAM","category":guess_category(title),"seniority":guess_seniority(title),"work_type":guess_work_type(loc_str),"employment_type":"fulltime","source_name":source_name,"source_id":source_id,"source_ats":"recruitcrm","apply_url":j.get("apply_url","") or url,"source_url":url})
        if not jobs:
            # Fallback to HTML
            r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=15)
            soup = BeautifulSoup(r.text, "lxml")
            for item in soup.select(".job-list-item h3, .job-title, .position-title"):
                title = item.get_text(strip=True)
                if not is_valid_job_title(title): continue
                jobs.append({"title":title,"location":"LATAM","category":guess_category(title),"seniority":guess_seniority(title),"work_type":"remote","employment_type":"fulltime","source_name":source_name,"source_id":source_id,"source_ats":"recruitcrm","apply_url":url,"source_url":url})
        print(f"    -> {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── PLAYWRIGHT ─────────────────────────────────────────────────────────────────
async def scrape_playwright(url, source_name, source_id, selector, title_selector=None):
    print(f"  Playwright: {source_name}")
    jobs = []
    try:
        async with async_playwright() as p:
            browser = await p.chromium.launch()
            page = await browser.new_page()
            await page.goto(url, timeout=30000)
            await page.wait_for_selector(selector, timeout=15000)
            await asyncio.sleep(2)  # wait for JS to render
            for item in await page.query_selector_all(selector):
                # Get title from specific child or full text
                if title_selector:
                    el = await item.query_selector(title_selector)
                    title = (await el.inner_text()).strip() if el else (await item.inner_text()).strip()
                else:
                    title = (await item.inner_text()).strip()
                    # Take only first line if multiline
                    title = title.split("\n")[0].strip()
                if not is_valid_job_title(title): continue
                apply_url = url
                link_el = await item.query_selector("a")
                if link_el:
                    href = await link_el.get_attribute("href")
                    if href:
                        apply_url = href if href.startswith("http") else url.rstrip("/") + "/" + href.lstrip("/")
                jobs.append({"title":title,"location":"LATAM","category":guess_category(title),"seniority":guess_seniority(title),"work_type":"remote","employment_type":"fulltime","source_name":source_name,"source_id":source_id,"source_ats":"playwright","apply_url":apply_url,"source_url":url})
            await browser.close()
        print(f"    -> {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── MAIN ───────────────────────────────────────────────────────────────────────
async def main():
    all_jobs = []

    # ── WORKABLE API (confirmed working) ──────────────────────────────────────
    print("\n== Workable API ==")
    all_jobs += scrape_workable("careersactivatetalent", "Activate Talent", "https://apply.workable.com/careersactivatetalent/")
    all_jobs += scrape_workable("hiresur", "Sur", "https://apply.workable.com/hiresur/")
    all_jobs += scrape_workable("remote-talent-latam", "Remote Talent LATAM", "https://apply.workable.com/remote-talent-latam/")

    # ── GREENHOUSE API (confirmed working) ────────────────────────────────────
    print("\n== Greenhouse API ==")
    all_jobs += scrape_greenhouse("nearsure", "Nearsure", "https://www.nearsure.com/job-opportunities")
    all_jobs += scrape_greenhouse("andela", "Andela", "https://talent.andela.com/")

    # ── ASHBY API (confirmed working) ─────────────────────────────────────────
    print("\n== Ashby API ==")
    all_jobs += scrape_ashby("silver", "Silver Dev", "https://silver.dev")

    # ── LEVER API ─────────────────────────────────────────────────────────────
    print("\n== Lever API ==")
    all_jobs += scrape_lever("deel", "Deel", "https://jobs.lever.co/deel")
    all_jobs += scrape_lever("remote", "Remote.com", "https://jobs.lever.co/remote")

    # ── APPLYTOJOB ────────────────────────────────────────────────────────────
    print("\n== ApplyToJob ==")
    all_jobs += scrape_applytojob("devlane", "Devlane", "https://devlane.applytojob.com/apply")
    all_jobs += scrape_applytojob("hireatomic", "Hire Atomic", "https://jobs.hireatomic.com/jobs")

    # ── ZOHO RECRUIT ──────────────────────────────────────────────────────────
    print("\n== Zoho ==")
    all_jobs += scrape_zoho("https://thehireboost.zohorecruit.com/jobs/Careers", "HireBoost", "hireboost")

    # ── RECRUITCRM ────────────────────────────────────────────────────────────
    print("\n== RecruitCRM ==")
    all_jobs += scrape_recruitcrm("https://recruitcrm.io/jobs/TLNT_Group_jobs", "TLNT", "tlnt")

    # ── PLAYWRIGHT ────────────────────────────────────────────────────────────
    print("\n== Playwright ==")
    all_jobs += await scrape_playwright(
        "https://www.athyna.com/for-talent#Open-Roles", "Athyna", "athyna",
        "[class*='JobCard'], [class*='job-card'], [class*='JobItem'], [class*='job-item']"
    )
    all_jobs += await scrape_playwright(
        "https://jobs.simera.io/", "Simera", "simera",
        "[class*='job-card'], [class*='JobCard'], [class*='position']"
    )
    all_jobs += await scrape_playwright(
        "https://pitcheers.com/jobs-search-result/", "Pitcheers", "pitcheers",
        "[class*='job'], [class*='position'], [class*='opening']"
    )
    all_jobs += await scrape_playwright(
        "https://www.theflock.com/en/talent/our-openings", "The Flock", "theflock",
        "[class*='opening'], [class*='job'], [class*='position']"
    )
    all_jobs += await scrape_playwright(
        "https://hirelatam.com/jobs/", "HireLatam", "hirelatam",
        "[class*='job'], [class*='position'], [class*='opening']"
    )
    all_jobs += await scrape_playwright(
        "https://jobs.worldteams.com/jobs", "Worldteams", "worldteams",
        "[class*='job'], [class*='position']"
    )
    all_jobs += await scrape_playwright(
        "https://www.lupahire.com/open-roles", "Lupa Hire", "lupa",
        "[class*='job'], [class*='role'], [class*='position']"
    )
    all_jobs += await scrape_playwright(
        "https://jobs.hirewithnear.com/jobs/", "Near", "near",
        "[class*='job-title'], [class*='position'], [class*='job-card']"
    )
    all_jobs += await scrape_playwright(
        "https://www.howdy.com/", "Howdy", "howdy",
        "[class*='job'], [class*='role'], [class*='opening'], [class*='position']"
    )
    all_jobs += await scrape_playwright(
        "https://kala-talent.com/opportunities/", "KalaTalent", "kalatalent",
        "[class*='job'], [class*='opportunity'], [class*='position']"
    )
    all_jobs += await scrape_playwright(
        "https://webstarted.com/careers", "Webstarted", "webstarted",
        "[class*='job'], [class*='position'], [class*='career']"
    )
    all_jobs += await scrape_playwright(
        "https://www.onstrider.com/jobs", "Strider", "strider",
        "[class*='job'], [class*='position'], [class*='role']"
    )
    all_jobs += await scrape_playwright(
        "https://work.withforward.com/", "Forward", "forward",
        "[class*='job'], [class*='role'], [class*='position'], h2, h3"
    )
    all_jobs += await scrape_playwright(
        "https://www.projectgrowth.com/", "Project Growth", "projectgrowth",
        "[class*='job'], [class*='role'], [class*='position'], [class*='opening']"
    )
    all_jobs += await scrape_playwright(
        "https://www.virtuallatinos.com/", "Virtual Latinos", "virtuallatinos",
        "[class*='job'], [class*='role'], [class*='position']"
    )
    all_jobs += await scrape_playwright(
        "https://jobs.virtualwizards.io/", "Virtual Wizards", "virtualwizards",
        "[class*='job'], [class*='role'], [class*='position']"
    )
    all_jobs += await scrape_playwright(
        "https://www.wizards.lat/", "Wizards.lat", "wizardslat",
        "[class*='job'], [class*='role'], [class*='position'], [class*='opening']"
    )
    all_jobs += await scrape_playwright(
        "https://www.workbetternow.com/", "Work Better Now", "workbetternow",
        "[class*='job'], [class*='role'], [class*='position'], [class*='opening']"
    )
    all_jobs += await scrape_playwright(
        "https://talent.latinolegends.com/jobs", "Latino Legends", "latinolegends",
        "[class*='job'], [class*='position'], [class*='opening']"
    )
    all_jobs += await scrape_playwright(
        "https://vintti.com/", "Vintti", "vintti",
        "[class*='job'], [class*='role'], [class*='position'], [class*='career']"
    )

    # Final filter
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