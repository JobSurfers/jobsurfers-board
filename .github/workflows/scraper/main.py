import os
import asyncio
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

# ── CATEGORIZACIÓN MEJORADA ────────────────────────────────────────────────────
def guess_category(title):
    t = title.lower()
    if any(x in t for x in ["engineer","developer","devops","backend","front","fullstack","full stack","software","qa","cloud","python","java","react","node","ios","android","mobile","sre","architect","data scientist","machine learning","ml ","ai ","devops","infrastructure","platform","embedded","firmware"]): return "tech"
    if any(x in t for x in ["sales","account executive","business development","bdr","sdr","revenue","closing","quota","comercial","ventas","ejecutivo de cuenta"]): return "sales"
    if any(x in t for x in ["marketing","growth","seo","sem","content","brand","social media","copywriter","demand generation","performance","media buyer","paid","email market","crm market","comunicacion","publicidad"]): return "marketing"
    if any(x in t for x in ["design","designer","ux","ui ","user experience","user interface","graphic","visual","product designer","illustrat","figma","creative"]): return "design"
    if any(x in t for x in ["finance","accounting","controller","cfo","treasury","bookkeeper","payroll","contabilidad","finanzas","tesoreria","cuentas por"]): return "finance"
    if any(x in t for x in ["hr","recruiter","talent acquisition","people","human resources","recursos humanos","rrhh","onboarding","people ops","hrbp"]): return "hr"
    if any(x in t for x in ["data analyst","data engineer","analytics","business intelligence","bi ","tableau","looker","power bi","sql","etl","data warehouse","scientist"]): return "data"
    if any(x in t for x in ["product manager","product owner","pm ","scrum","agile","project manager","program manager","delivery","jira"]): return "ops"
    if any(x in t for x in ["customer success","customer support","customer service","support","helpdesk","atencion al cliente","soporte","cx ","cs ","account manager","client"]): return "ops"
    if any(x in t for x in ["operations","ops","logistics","supply chain","procurement","purchasing","warehouse"]): return "ops"
    return "ops"

def guess_seniority(title):
    t = title.lower()
    if any(x in t for x in ["intern","internship","pasante","trainee","practicante"]): return "intern"
    if any(x in t for x in ["junior","jr.","jr ","entry level","associate","i ","level 1","nivel 1"]): return "junior"
    if any(x in t for x in ["senior","sr.","sr ","staff","principal","expert","especialista senior","ssr","semi senior","semi-senior","semisenior"]): return "senior"
    if any(x in t for x in ["lead","tech lead","team lead","líder","lider tecnico","engineering lead"]): return "lead"
    if any(x in t for x in ["director","vp ","vice president","head of","chief","cto","cpo","coo","manager","gerente"]): return "director"
    return "mid"

def guess_work_type(text):
    t = (text or "").lower()
    if any(x in t for x in ["remote","remoto","latam","anywhere","worldwide","global","work from home","wfh","fully remote","100% remote","distributed","anywhere in"]): return "remote"
    if "hybrid" in t or "hibrido" in t or "híbrido" in t: return "hybrid"
    return "onsite"

def guess_employment_type(s):
    s = (s or "").lower()
    if "part" in s: return "parttime"
    if "contract" in s or "freelance" in s: return "contract"
    return "fulltime"

def is_valid_job_title(title):
    """Filtra textos que claramente NO son títulos de trabajo"""
    if not title or len(title) < 4 or len(title) > 150: return False
    t = title.lower().strip()
    # Excluir navegación, marketing, cookies y contenido web genérico
    skip = [
        "cookie","privacy","terms","faq","about","contact","home","login","sign",
        "subscribe","newsletter","follow","share","read more","learn more","click",
        "submit","send","apply now","view all","see all","load more","show more",
        "©","copyright","all rights reserved","powered by","built with",
        "solution","empresa","client","servicios","nosotros","quienes somos",
        "nuestro","nuestra","team","equipo","por que","¿por","como funciona",
        "casos de exito","beneficios","ventajas","caracteristicas","funcionalidades",
        "town life","it's all here","stay in the loop","explore more","community",
        "find your","help us find","tell us more","share your info","get updates",
        "why are you","head hunter de especialidad","una extension","el match",
        "flexible y ajustado","resultado de mayor","evitamos sesgos","impulsamos",
        "oportunidades para todos","this website uses cookies","analytics technologies",
    ]
    for s in skip:
        if s in t: return False
    # Debe tener al menos una palabra clave de trabajo
    job_words = [
        "engineer","developer","analyst","manager","designer","coordinator",
        "specialist","consultant","director","lead","head","officer","associate",
        "executive","representative","strategist","architect","scientist","researcher",
        "recruiter","advisor","support","success","operations","marketing","sales",
        "product","project","program","data","cloud","devops","backend","frontend",
        "fullstack","mobile","qa","ux","ui","seo","content","brand","finance",
        "accounting","hr","talent","people","customer","account","business",
        # español
        "ingeniero","desarrollador","analista","gerente","diseñador","coordinador",
        "especialista","consultor","director","lider","ejecutivo","representante",
        "reclutador","asesor","soporte","operaciones","ventas","producto","proyecto",
        "contador","recursos humanos","atención","cliente","negocio",
    ]
    return any(w in t for w in job_words)

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
        r = requests.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true", timeout=15)
        data = r.json()
        for j in data.get("jobs", []):
            title = j.get("title","")
            if not title: continue
            loc_str = j.get("location",{}).get("name","")
            # Greenhouse incluye metadata de remote en el contenido
            content = j.get("content","").lower()
            wt = "remote" if any(x in loc_str.lower() or x in content for x in ["remote","remoto","anywhere","latam"]) else guess_work_type(loc_str)
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

# ── ZOHO RECRUIT ───────────────────────────────────────────────────────────────
def scrape_zoho(url, source_name, source_id):
    print(f"  Zoho: {source_name}")
    jobs = []
    try:
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        for item in soup.select(".jobTitleLink, .job-title, h2 a, .position-title a"):
            title = item.get_text(strip=True)
            link = item.get("href","")
            if not is_valid_job_title(title): continue
            if link and not link.startswith("http"):
                link = url.split("/jobs")[0] + link
            jobs.append({"title":title,"location":"LATAM","category":guess_category(title),"seniority":guess_seniority(title),"work_type":"remote","employment_type":"fulltime","source_name":source_name,"source_id":source_id,"source_ats":"zoho","apply_url":link or url,"source_url":url})
        print(f"    -> {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── PLAYWRIGHT ─────────────────────────────────────────────────────────────────
async def scrape_playwright(url, source_name, source_id, selector, apply_selector=None):
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
                title = (await item.inner_text()).strip()
                if not is_valid_job_title(title): continue
                apply_url = url
                if apply_selector:
                    btn = await item.query_selector(apply_selector)
                    if btn:
                        apply_url = await btn.get_attribute("href") or url
                jobs.append({"title":title,"location":"LATAM","category":guess_category(title),"seniority":guess_seniority(title),"work_type":"remote","employment_type":"fulltime","source_name":source_name,"source_id":source_id,"source_ats":"playwright","apply_url":apply_url,"source_url":url})
            await browser.close()
        print(f"    -> {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

# ── MAIN ───────────────────────────────────────────────────────────────────────
async def main():
    all_jobs = []

    print("\n== Workable API ==")
    # Estas agencias tienen Workable — datos limpios directamente de la API
    all_jobs += scrape_workable("hiresur", "Sur", "https://apply.workable.com/hiresur/")
    all_jobs += scrape_workable("remote-talent-latam", "Remote Talent LATAM", "https://apply.workable.com/remote-talent-latam/")
    all_jobs += scrape_workable("nexton", "Nexton", "https://apply.workable.com/nexton/")

    print("\n== Greenhouse API ==")
    all_jobs += scrape_greenhouse("nearsure", "Nearsure", "https://www.nearsure.com/job-opportunities")
    all_jobs += scrape_greenhouse("andela", "Andela", "https://talent.andela.com/")

    print("\n== Ashby API ==")
    all_jobs += scrape_ashby("silver", "Silver Dev", "https://silver.dev")

    print("\n== Lever API ==")
    all_jobs += scrape_lever("deel", "Deel", "https://jobs.lever.co/deel")
    all_jobs += scrape_lever("remote", "Remote.com", "https://jobs.lever.co/remote")

    print("\n== Zoho Recruit ==")
    # HireBoost usa Zoho
    all_jobs += scrape_zoho("https://thehireboost.zohorecruit.com/jobs/Careers", "HireBoost", "hireboost")

    print("\n== Playwright (JS sites) ==")
    all_jobs += await scrape_playwright("https://www.athyna.com/for-talent#Open-Roles", "Athyna", "athyna", ".job-item, [class*='job-card'], [class*='JobCard'], [class*='opening']")
    all_jobs += await scrape_playwright("https://recruitcrm.io/jobs/TLNT_Group_jobs", "TLNT", "tlnt", ".job-list-item, .job-card, [class*='job']")
    all_jobs += await scrape_playwright("https://jobs.simera.io/", "Simera", "simera", ".job-card, [class*='job-card'], [class*='JobCard']")
    all_jobs += await scrape_playwright("https://pitcheers.com/jobs-search-result/", "Pitcheers", "pitcheers", ".job-item, [class*='job']")
    all_jobs += await scrape_playwright("https://www.theflock.com/en/talent/our-openings", "The Flock", "theflock", "[class*='opening'], [class*='job'], [class*='position']")
    all_jobs += await scrape_playwright("https://torre.ai/", "Torre", "torre", "[class*='opportunity'], [class*='job-card'], [class*='position']")
    all_jobs += await scrape_playwright("https://hirelatam.com/jobs/", "HireLatam", "hirelatam", "[class*='job'], [class*='position'], [class*='opening']")
    all_jobs += await scrape_playwright("https://jobs.worldteams.com/jobs", "Worldteams", "worldteams", "[class*='job'], [class*='position'], h2, h3")
    all_jobs += await scrape_playwright("https://www.lupahire.com/open-roles", "Lupa Hire", "lupa", "[class*='job'], [class*='role'], [class*='position'], h3")
    all_jobs += await scrape_playwright("https://jobs.hirewithnear.com/jobs/", "Near", "near", "[class*='job-title'], [class*='position'], h3")

    # Filtramos jobs sin título válido que puedan haber pasado
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