import os
import asyncio
import requests
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
from supabase import create_client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

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

def scrape_workable(slug, source_name, source_url):
    print(f"  Workable: {slug}")
    jobs = []
    try:
        r = requests.get(f"https://apply.workable.com/api/v1/widget/accounts/{slug}", timeout=15)
        data = r.json()
        for j in data.get("jobs", []):
            title = j.get("title","")
            location = j.get("location", {})
            loc_str = location.get("city","") + ", " + location.get("country","") if location else ""
            jobs.append({"title":title,"location":loc_str,"category":guess_category(title),"seniority":guess_seniority(title),"work_type":guess_work_type(loc_str),"employment_type":guess_employment_type(j.get("employment_type","")),"source_name":source_name,"source_id":slug,"source_ats":"workable","apply_url":j.get("url",""),"source_url":source_url})
        print(f"    → {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

def scrape_greenhouse(slug, source_name, source_url):
    print(f"  Greenhouse: {slug}")
    jobs = []
    try:
        r = requests.get(f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs", timeout=15)
        data = r.json()
        for j in data.get("jobs", []):
            title = j.get("title","")
            loc_str = j.get("location",{}).get("name","")
            jobs.append({"title":title,"location":loc_str,"category":guess_category(title),"seniority":guess_seniority(title),"work_type":guess_work_type(loc_str),"employment_type":"fulltime","source_name":source_name,"source_id":slug,"source_ats":"greenhouse","apply_url":j.get("absolute_url",""),"source_url":source_url})
        print(f"    → {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

def scrape_ashby(slug, source_name, source_url):
    print(f"  Ashby: {slug}")
    jobs = []
    try:
        r = requests.get(f"https://api.ashbyhq.com/posting-api/job-board/{slug}", timeout=15)
        data = r.json()
        for j in data.get("jobPostings", []):
            title = j.get("title","")
            loc_str = j.get("location","")
            jobs.append({"title":title,"location":loc_str,"category":guess_category(title),"seniority":guess_seniority(title),"work_type":guess_work_type(loc_str),"employment_type":guess_employment_type(j.get("employmentType","")),"source_name":source_name,"source_id":slug,"source_ats":"ashby","apply_url":j.get("jobUrl",""),"source_url":source_url})
        print(f"    → {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

def scrape_static(url, source_name, source_id, selectors):
    print(f"  Static: {source_name}")
    jobs = []
    try:
        r = requests.get(url, headers={"User-Agent":"Mozilla/5.0"}, timeout=15)
        soup = BeautifulSoup(r.text, "lxml")
        for sel in selectors:
            for item in soup.select(sel):
                title = item.get_text(strip=True)
                link = item.get("href","") or (item.find("a") or {}).get("href","")
                if not title: continue
                jobs.append({"title":title,"location":"LATAM","category":guess_category(title),"seniority":guess_seniority(title),"work_type":"remote","employment_type":"fulltime","source_name":source_name,"source_id":source_id,"source_ats":"static","apply_url":link if link.startswith("http") else url,"source_url":url})
        print(f"    → {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

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
                if title:
                    jobs.append({"title":title,"location":"LATAM","category":guess_category(title),"seniority":guess_seniority(title),"work_type":"remote","employment_type":"fulltime","source_name":source_name,"source_id":source_id,"source_ats":"playwright","apply_url":url,"source_url":url})
            await browser.close()
        print(f"    → {len(jobs)} jobs")
    except Exception as e:
        print(f"    ERROR: {e}")
    return jobs

async def main():
    all_jobs = []
    print("\n== APIs públicas ==")
    all_jobs += scrape_workable("careersactivatetalent","Activate Talent","https://apply.workable.com/careersactivatetalent/")
    all_jobs += scrape_workable("hiresur","HireSur","https://apply.workable.com/hiresur/")
    all_jobs += scrape_workable("remote-talent-latam","Remote Talent LATAM","https://apply.workable.com/remote-talent-latam/")
    all_jobs += scrape_greenhouse("nearsure","Nearsure","https://www.nearsure.com/job-opportunities")
    all_jobs += scrape_greenhouse("andela","Andela","https://talent.andela.com/")
    all_jobs += scrape_ashby("silver","Silver.dev","https://silver.dev")
    # Nuevas agencias — HTML estático
    all_jobs += scrape_static("https://work.withforward.com/", "Forward", "forward", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://www.worldteams.com/", "World Teams", "worldteams", [".job-title", "h3 a", ".position-title"])
    all_jobs += scrape_static("https://www.howdy.com/", "Howdy", "howdy", [".job-title", "h3", ".opening"])
    all_jobs += scrape_static("https://www.wearekadre.com/", "Kadre", "kadre", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://www.kalatalent.com/", "Kala Talent", "kala", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://www.nexton.com/", "Nexton", "nexton", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://www.webstarted.com/", "Webstarted", "webstarted", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://www.purrfecthire.com/", "Purrfect Hire", "purrfecthire", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://www.latamjobs.com/", "LATAM Jobs", "latamjobs", [".job-title", "h3 a", ".position"])
    all_jobs += scrape_static("https://latamcent.com/", "Latamcent", "latamcent", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://www.bajanearshore.com/", "Baja Nearshore", "bajanearshore", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://www.betterpros.com/", "BetterPros", "betterpros", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://brazoderecho.com/", "Brazo Derecho", "brazoderecho", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://www.delfoshr.com/", "Delfos HR", "delfoshr", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://www.devlane.com/", "Devlane", "devlane", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://www.prometeotalent.com/", "Prometeo Talent", "prometeo", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://www.retalent.com/", "Retalent", "retalent", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://www.upscale.lat", "Upscale", "upscale", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://www.getagents.co", "Get Agents", "getagents", [".job-title", "h3", ".position"])
    all_jobs += scrape_static("https://work.withforward.com/", "Forward", "forward", [".job-title","h3 a",".position"])
    all_jobs += scrape_static("https://www.howdy.com/", "Howdy", "howdy", [".job-title","h3",".opening"])
    all_jobs += scrape_static("https://www.wearekadre.com/", "Kadre", "kadre", [".job-title","h3",".position"])
    all_jobs += scrape_static("https://www.kalatalent.com/", "KalaTalent", "kalatalent", [".job-title","h3",".position"])
    all_jobs += scrape_static("https://www.nexton.com/", "Nexton", "nexton", [".job-title","h3",".position"])
    all_jobs += scrape_static("https://www.webstarted.com/", "Webstarted", "webstarted", [".job-title","h3",".position"])
    all_jobs += scrape_static("https://www.purrfecthire.com/", "Purrfect Hire", "purrfecthire", [".job-title","h3",".position"])
    all_jobs += scrape_static("https://www.latamjobs.com/", "Latam Jobs", "latamjobs", [".job-title","h3 a",".position"])
    all_jobs += scrape_static("https://latamcent.com/", "LatamCent", "latamcent", [".job-title","h3",".position"])
    all_jobs += scrape_static("https://www.bajanearshore.com/", "Baja Nearshore", "bajanearshore", [".job-title","h3",".position"])
    all_jobs += scrape_static("https://www.betterpros.com/", "Better Pros", "betterpros", [".job-title","h3",".position"])
    all_jobs += scrape_static("https://brazoderecho.com/", "Brazo Derecho", "brazoderecho", [".job-title","h3",".position"])
    all_jobs += scrape_static("https://www.delfoshr.com/", "Delfos HR", "delfoshr", [".job-title","h3",".position"])
    all_jobs += scrape_static("https://www.prometeotalent.com/", "Prometeo", "prometeo", [".job-title","h3",".position"])
    all_jobs += scrape_static("https://www.retalent.com/", "Retalent", "retalent", [".job-title","h3",".position"])
    all_jobs += scrape_static("https://www.upscale.lat", "Upscale", "upscale", [".job-title","h3",".position"])
    all_jobs += scrape_static("http://www.getagents.co", "Agent Careers", "getagents", [".job-title","h3",".position"])
    print("\n== HTML estático ==")
    all_jobs += scrape_static("https://www.lupahire.com/open-roles","Lupa Hire","lupa",["h3.job-title","h3",".job-title"])
    all_jobs += scrape_static("https://talent.latinolegends.com/jobs","Latino Legends","latinolegends",[".job-item a",".position a","h2 a"])
    all_jobs += scrape_static("https://jobs.hirewithnear.com/jobs/","Near","near",[".job-title","h3 a",".position-title"])
    # Nuevas agencias — Playwright
    all_jobs += await scrape_playwright("https://thehireboost.com/", "The Hire Boost", "hireboost", ".job-card")
    all_jobs += await scrape_playwright("https://www.hirelatam.com/", "Hire LATAM", "hirelatam", ".job-card")
    all_jobs += await scrape_playwright("https://torre.ai/", "Torre", "torre", ".opportunity-card")
    all_jobs += await scrape_playwright("https://www.weareseeders.com/", "Seeders", "seeders", ".job-item")
    all_jobs += await scrape_playwright("https://thehireboost.com/", "HireBoost", "hireboost", ".job-card")
    all_jobs += await scrape_playwright("https://www.hirelatam.com/", "HireLatam", "hirelatam", ".job-card")
    all_jobs += await scrape_playwright("https://www.worldteams.com/", "Worldteams", "worldteams", ".job-item")
    all_jobs += await scrape_playwright("https://torre.ai/", "Torre", "torre", ".opportunity-card")
    print("\n== Playwright ==")
    all_jobs += await scrape_playwright("https://www.athyna.com/for-talent","Athyna","athyna",".job-card")
    all_jobs += await scrape_playwright("https://recruitcrm.io/jobs/TLNT_Group_jobs","TLNT","tlnt",".job-list-item")
    all_jobs += await scrape_playwright("https://jobs.simera.io/","Simera","simera",".job-card")
    all_jobs += await scrape_playwright("https://pitcheers.com/jobs-search-result/","Pitcheers","pitcheers",".job-item")
    all_jobs += await scrape_playwright("https://www.theflock.com/en/talent/our-openings","The Flock","theflock",".opening-item")
    print(f"\n== Total: {len(all_jobs)} jobs ==")
    if all_jobs:
        print("Guardando en Supabase...")
        try:
            supabase.table("jobs").update({"is_active":False}).eq("is_active",True).execute()
            supabase.table("jobs").insert(all_jobs).execute()
            print(f"✅ {len(all_jobs)} jobs guardados")
        except Exception as e:
            print(f"ERROR: {e}")

if __name__ == "__main__":
    asyncio.run(main())