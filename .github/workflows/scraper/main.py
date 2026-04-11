<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Job Surfers · Find Your Dream Job</title>
<link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap" rel="stylesheet">
<style>
*, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Inter', sans-serif; background: #F4F6FA; color: #1a1a2e; font-size: 13px; -webkit-font-smoothing: antialiased; min-height: 100vh; }

.header { background: #1B2340; height: 56px; display: flex; align-items: center; padding: 0 24px; position: sticky; top: 0; z-index: 200; }
.header-logo { display: flex; align-items: center; gap: 10px; text-decoration: none; }
.header-logo-img { width: 30px; height: 30px; border-radius: 8px; overflow: hidden; }
.header-logo-img img { width: 100%; height: 100%; object-fit: cover; }
.header-logo-text { font-size: 15px; font-weight: 700; color: #fff; letter-spacing: -0.3px; }
.header-right { margin-left: auto; display: flex; align-items: center; gap: 12px; }
.header-search { display: flex; align-items: center; gap: 8px; background: rgba(255,255,255,0.08); border: 1px solid rgba(255,255,255,0.15); border-radius: 8px; padding: 0 14px; width: 280px; height: 36px; transition: all 0.15s; }
.header-search:focus-within { background: rgba(255,255,255,0.12); border-color: #3DBFBF; }
.header-search svg { color: rgba(255,255,255,0.4); flex-shrink: 0; }
.header-search input { background: transparent; border: none; outline: none; font-family: inherit; font-size: 13px; color: #fff; width: 100%; }
.header-search input::placeholder { color: rgba(255,255,255,0.35); }
.live-badge { display: flex; align-items: center; gap: 6px; background: rgba(61,191,191,0.15); border: 1px solid rgba(61,191,191,0.3); border-radius: 20px; padding: 4px 12px; font-size: 11px; font-weight: 600; color: #3DBFBF; white-space: nowrap; }
.live-dot { width: 6px; height: 6px; border-radius: 50%; background: #3DBFBF; animation: pulse 2s infinite; }
@keyframes pulse { 0%,100%{opacity:1;transform:scale(1)} 50%{opacity:0.5;transform:scale(0.8)} }

.body-wrap { display: flex; min-height: calc(100vh - 56px); }

.sidebar { width: 220px; flex-shrink: 0; background: #fff; border-right: 1px solid #E5E9F2; padding: 20px 0; position: sticky; top: 56px; height: calc(100vh - 56px); overflow-y: auto; }
.sidebar::-webkit-scrollbar { width: 4px; }
.sidebar::-webkit-scrollbar-thumb { background: #E5E9F2; border-radius: 4px; }
.sb-section { padding: 0 16px 16px; border-bottom: 1px solid #F0F2F8; margin-bottom: 4px; }
.sb-section:last-child { border-bottom: none; }
.sb-title { font-size: 10px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.1em; color: #9CA3AF; margin-bottom: 8px; padding-top: 16px; }
.sb-row { display: flex; align-items: center; justify-content: space-between; padding: 5px 8px; border-radius: 6px; cursor: pointer; user-select: none; transition: background 0.1s; }
.sb-row:hover { background: #F4F6FA; }
.sb-row.active { background: #EEF9F9; }
.sb-row.active .sb-label { color: #0D9488; font-weight: 600; }
.sb-check { width: 14px; height: 14px; border: 1.5px solid #D1D5DB; border-radius: 3px; flex-shrink: 0; display: flex; align-items: center; justify-content: center; margin-right: 8px; transition: all 0.12s; }
.sb-row.active .sb-check { background: #3DBFBF; border-color: #3DBFBF; }
.sb-tick { display: none; }
.sb-row.active .sb-tick { display: block; }
.sb-label { font-size: 12px; color: #374151; flex: 1; }
.sb-count { font-size: 11px; font-weight: 500; color: #9CA3AF; background: #F4F6FA; padding: 1px 7px; border-radius: 12px; }
.sb-row.active .sb-count { background: #D1F5F5; color: #0D9488; }

.main { flex: 1; min-width: 0; padding: 24px 28px; }
.page-header { margin-bottom: 20px; }
.page-title { font-size: 22px; font-weight: 700; letter-spacing: -0.5px; margin-bottom: 6px; }
.page-title span { color: #3DBFBF; }
.page-desc { font-size: 13px; color: #6B7280; }

.metrics { display: flex; gap: 12px; margin-bottom: 24px; }
.metric-card { background: #fff; border: 1px solid #E5E9F2; border-radius: 10px; padding: 14px 20px; flex: 1; max-width: 160px; }
.metric-num { font-size: 28px; font-weight: 700; letter-spacing: -1px; color: #1B2340; }
.metric-num.teal { color: #3DBFBF; }
.metric-label { font-size: 11px; color: #9CA3AF; margin-top: 2px; font-weight: 500; }

.section-header { display: flex; align-items: center; justify-content: space-between; margin-bottom: 14px; padding-bottom: 12px; border-bottom: 1px solid #E5E9F2; }
.section-label { font-size: 11px; font-weight: 700; text-transform: uppercase; letter-spacing: 0.08em; color: #9CA3AF; display: flex; align-items: center; gap: 8px; }
.section-label-dot { width: 8px; height: 8px; border-radius: 50%; background: #3DBFBF; }
.result-info { font-size: 12px; color: #9CA3AF; }
.result-info b { color: #1B2340; }

.jobs-grid { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; }

.job-card { background: #fff; border: 1.5px solid #E5E9F2; border-radius: 12px; padding: 18px; display: flex; flex-direction: column; gap: 10px; text-decoration: none; color: inherit; transition: border-color 0.15s, box-shadow 0.15s, transform 0.12s; cursor: pointer; position: relative; overflow: hidden; }
.job-card::before { content: ''; position: absolute; top: 0; left: 0; right: 0; height: 3px; background: transparent; transition: background 0.15s; }
.job-card:hover { border-color: #3DBFBF; box-shadow: 0 4px 20px rgba(61,191,191,0.12); transform: translateY(-2px); }
.job-card:hover::before { background: #3DBFBF; }

.jc-header { display: flex; align-items: flex-start; justify-content: space-between; gap: 10px; }
.jc-logo { width: 40px; height: 40px; border-radius: 10px; display: flex; align-items: center; justify-content: center; font-weight: 700; font-size: 13px; flex-shrink: 0; }
.c0{background:#E8F8F8;color:#0D9488} .c1{background:#FEF9EC;color:#D97706} .c2{background:#EEF0F7;color:#1B2340}
.c3{background:#FEF2F2;color:#DC2626} .c4{background:#F3F0FF;color:#7C3AED} .c5{background:#FFF0FB;color:#BE185D}
.c6{background:#EDFDF4;color:#166534} .c7{background:#FFF7ED;color:#C2410C}
.jc-time { font-size: 10px; color: #9CA3AF; white-space: nowrap; }
.jc-title { font-size: 14px; font-weight: 700; color: #1B2340; line-height: 1.3; }
.jc-agency { font-size: 12px; color: #6B7280; }
.jc-tags { display: flex; flex-wrap: wrap; gap: 5px; }
.tag { font-size: 10px; font-weight: 500; padding: 3px 8px; border-radius: 20px; }
.tag-remote { background: #E8F8F8; color: #0D9488; }
.tag-hybrid { background: #FEF9EC; color: #D97706; }
.tag-onsite { background: #F4F6FA; color: #6B7280; }
.tag-cat { background: #EEF0F7; color: #1B2340; }
.tag-seniority { background: #F3F0FF; color: #7C3AED; }
.jc-footer { display: flex; align-items: center; justify-content: space-between; padding-top: 8px; border-top: 1px solid #F0F2F8; margin-top: auto; }
.jc-source { font-size: 11px; color: #9CA3AF; }
.jc-apply { font-size: 11px; font-weight: 600; color: #fff; background: #1B2340; border: none; border-radius: 7px; padding: 6px 14px; cursor: pointer; text-decoration: none; transition: background 0.12s; font-family: inherit; white-space: nowrap; }
.jc-apply:hover { background: #3DBFBF; }

.skeleton-card { background: #fff; border: 1.5px solid #E5E9F2; border-radius: 12px; padding: 18px; animation: shimmer 1.5s ease-in-out infinite; }
@keyframes shimmer { 0%,100%{opacity:1}50%{opacity:0.5} }
.sk-row { display: flex; gap: 10px; margin-bottom: 12px; align-items: center; }
.sk-logo { width: 40px; height: 40px; background: #E5E9F2; border-radius: 10px; flex-shrink: 0; }
.sk-lines { flex: 1; }
.sk-line { height: 11px; background: #E5E9F2; border-radius: 4px; margin-bottom: 7px; }
.sk-line.w80{width:80%}.sk-line.w55{width:55%}.sk-line.w35{width:35%}

.empty-state { grid-column: 1/-1; text-align: center; padding: 64px 20px; }
.empty-state h3 { font-size: 16px; font-weight: 600; margin-bottom: 8px; color: #1B2340; }
.empty-state p { color: #6B7280; font-size: 13px; }
.error-box { grid-column: 1/-1; background: #FEF2F2; border: 1px solid #FECACA; border-radius: 12px; padding: 28px; text-align: center; }
.error-box h3 { color: #DC2626; font-size: 15px; font-weight: 600; margin-bottom: 8px; }
.error-box p { color: #6B7280; font-size: 13px; margin-bottom: 16px; }
.agency-grid { display: flex; flex-wrap: wrap; gap: 6px; justify-content: center; }
.agency-chip { font-size: 11px; font-weight: 600; color: #0D9488; border: 1px solid #AEECD8; border-radius: 20px; padding: 4px 12px; text-decoration: none; background: #E8F8F8; }

@media(max-width:1100px){ .jobs-grid { grid-template-columns: repeat(2,1fr); } }
@media(max-width:768px){ .body-wrap{flex-direction:column} .sidebar{width:100%;height:auto;position:static;border-right:none;border-bottom:1px solid #E5E9F2} .jobs-grid{grid-template-columns:1fr} .metrics{flex-wrap:wrap} .header-search{width:200px} }
</style>
</head>
<body>

<header class="header">
  <a class="header-logo" href="#">
    <div class="header-logo-img">
      <img src="https://iuhhcbvlgmlvflwzcdss.supabase.co/storage/v1/object/public/Job%20Surfers/Logo%20Job%20Surfers%20AI.webp" alt="Job Surfers">
    </div>
    <span class="header-logo-text">Job Surfers</span>
  </a>
  <div class="header-right">
    <div class="header-search">
      <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2"><circle cx="7" cy="7" r="5"/><path d="M11 11l3 3"/></svg>
      <input id="q-search" placeholder="Search by role, skill, country..." oninput="doFilter()">
    </div>
    <div class="live-badge"><span class="live-dot"></span><span id="nav-count">—</span> live roles</div>
  </div>
</header>

<div class="body-wrap">
  <aside class="sidebar">
    <div class="sb-section">
      <div class="sb-title">Category</div>
      <div class="sb-row" onclick="toggleFilter('cat','tech',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">Tech &amp; Eng</span><span class="sb-count" id="fc-tech">—</span></div>
      <div class="sb-row" onclick="toggleFilter('cat','sales',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">Sales</span><span class="sb-count" id="fc-sales">—</span></div>
      <div class="sb-row" onclick="toggleFilter('cat','marketing',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">Marketing</span><span class="sb-count" id="fc-marketing">—</span></div>
      <div class="sb-row" onclick="toggleFilter('cat','design',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">Design</span><span class="sb-count" id="fc-design">—</span></div>
      <div class="sb-row" onclick="toggleFilter('cat','finance',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">Finance</span><span class="sb-count" id="fc-finance">—</span></div>
      <div class="sb-row" onclick="toggleFilter('cat','hr',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">HR &amp; Talent</span><span class="sb-count" id="fc-hr">—</span></div>
      <div class="sb-row" onclick="toggleFilter('cat','data',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">Data</span><span class="sb-count" id="fc-data">—</span></div>
      <div class="sb-row" onclick="toggleFilter('cat','ops',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">Operations</span><span class="sb-count" id="fc-ops">—</span></div>
    </div>
    <div class="sb-section">
      <div class="sb-title">Seniority</div>
      <div class="sb-row" onclick="toggleFilter('sn','junior',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">Junior</span><span class="sb-count" id="fc-junior">—</span></div>
      <div class="sb-row" onclick="toggleFilter('sn','mid',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">Mid-level</span><span class="sb-count" id="fc-mid">—</span></div>
      <div class="sb-row" onclick="toggleFilter('sn','senior',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">Senior</span><span class="sb-count" id="fc-senior">—</span></div>
      <div class="sb-row" onclick="toggleFilter('sn','lead',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">Lead</span><span class="sb-count" id="fc-lead">—</span></div>
      <div class="sb-row" onclick="toggleFilter('sn','director',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">Director</span><span class="sb-count" id="fc-director">—</span></div>
    </div>
    <div class="sb-section">
      <div class="sb-title">Work Type</div>
      <div class="sb-row" onclick="toggleFilter('wt','remote',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">Remote</span><span class="sb-count" id="fc-remote">—</span></div>
      <div class="sb-row" onclick="toggleFilter('wt','hybrid',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">Hybrid</span><span class="sb-count" id="fc-hybrid">—</span></div>
      <div class="sb-row" onclick="toggleFilter('wt','onsite',this)"><div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div><span class="sb-label">On-site</span><span class="sb-count" id="fc-onsite">—</span></div>
    </div>
    <div class="sb-section">
      <div class="sb-title">Agency</div>
      <div id="agency-filters"></div>
    </div>
  </aside>

  <main class="main">
    <div class="page-header">
      <h1 class="page-title">Open Roles from <span>Latin America</span></h1>
      <p class="page-desc">Job Surfers aggregates the best opportunities from 40+ recruiting agencies in LATAM. Apply directly — no middleman.</p>
    </div>
    <div class="metrics">
      <div class="metric-card"><div class="metric-num teal" id="m-total">—</div><div class="metric-label">active roles</div></div>
      <div class="metric-card"><div class="metric-num" id="m-remote">—</div><div class="metric-label">remote roles</div></div>
      <div class="metric-card"><div class="metric-num">40+</div><div class="metric-label">agencies</div></div>
    </div>
    <div class="section-header">
      <div class="section-label"><span class="section-label-dot"></span>OPEN ROLES — LAST 4 WEEKS</div>
      <div class="result-info"><b id="result-count">—</b> results</div>
    </div>
    <div class="jobs-grid" id="jobs-grid">
      <div class="skeleton-card"><div class="sk-row"><div class="sk-logo"></div><div class="sk-lines"><div class="sk-line w80"></div><div class="sk-line w55"></div></div></div></div>
      <div class="skeleton-card"><div class="sk-row"><div class="sk-logo"></div><div class="sk-lines"><div class="sk-line w80"></div><div class="sk-line w55"></div></div></div></div>
      <div class="skeleton-card"><div class="sk-row"><div class="sk-logo"></div><div class="sk-lines"><div class="sk-line w80"></div><div class="sk-line w55"></div></div></div></div>
      <div class="skeleton-card"><div class="sk-row"><div class="sk-logo"></div><div class="sk-lines"><div class="sk-line w80"></div><div class="sk-line w55"></div></div></div></div>
      <div class="skeleton-card"><div class="sk-row"><div class="sk-logo"></div><div class="sk-lines"><div class="sk-line w80"></div><div class="sk-line w55"></div></div></div></div>
      <div class="skeleton-card"><div class="sk-row"><div class="sk-logo"></div><div class="sk-lines"><div class="sk-line w80"></div><div class="sk-line w55"></div></div></div></div>
    </div>
  </main>
</div>

<script>
const SB_URL = "https://iuhhcbvlgmlvflwzcdss.supabase.co";
const SB_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Iml1aGhjYnZsZ21sdmZsd3pjZHNzIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NzM2NzQ4NDksImV4cCI6MjA4OTI1MDg0OX0.2YSn_tK8Oz7mZxC3hGqG0y208a5JBEPqGTyhAG1Qt8w";

const PARTNERS = [
  {name:"Athyna",url:"https://www.athyna.com/"},
  {name:"Strider",url:"http://app.onstrider.com/r/jobsurfers"},
  {name:"The Flock",url:"https://www.theflock.com/en/talent/our-openings"},
  {name:"Sur",url:"https://apply.workable.com/hiresur/"},
  {name:"HireBoost",url:"https://thehireboost.com/"},
  {name:"HireLatam",url:"https://www.hirelatam.com/"},
  {name:"Worldteams",url:"https://www.worldteams.com/"},
  {name:"Howdy",url:"https://www.howdy.com/"},
  {name:"Kadre",url:"https://www.wearekadre.com/"},
  {name:"KalaTalent",url:"https://www.kalatalent.com/"},
  {name:"Latam Jobs",url:"https://www.latamjobs.com/"},
  {name:"LatamCent",url:"https://latamcent.com/"},
  {name:"Latino Legends",url:"https://talent.latinolegends.com/jobs"},
  {name:"Near",url:"https://jobs.hirewithnear.com/jobs/"},
  {name:"Nearsure",url:"https://www.nearsure.com/job-opportunities"},
  {name:"Pitcheers",url:"https://pitcheers.com/jobs-search-result/"},
  {name:"Project Growth",url:"https://www.projectgrowth.com/"},
  {name:"Simera",url:"https://jobs.simera.io/"},
  {name:"Virtual Wizards",url:"https://jobs.virtualwizards.io/"},
  {name:"Webstarted",url:"https://www.webstarted.com/"},
  {name:"Andela",url:"https://talent.andela.com/"},
  {name:"Silver Dev",url:"http://silver.dev"},
  {name:"Baja Nearshore",url:"https://www.bajanearshore.com/"},
  {name:"Devlane",url:"https://devlane.applytojob.com/apply"},
  {name:"Prometeo",url:"https://www.prometeotalent.com/"},
  {name:"Retalent",url:"https://www.retalent.com/"},
  {name:"Upscale",url:"https://www.upscale.lat"},
  {name:"Vintti",url:"https://careers.vintti.com/"},
  {name:"Virtual Latinos",url:"https://www.virtuallatinos.com/"},
  {name:"Wizards.lat",url:"https://www.wizards.lat/"},
  {name:"Work Better Now",url:"https://www.workbetternow.com/"},
  {name:"Hire Atomic",url:"https://jobs.hireatomic.com/jobs"},
  {name:"TLNT",url:"https://recruitcrm.io/jobs/TLNT_Group_jobs"},
  {name:"Deel",url:"https://jobs.lever.co/deel"},
];

// Valid job title keywords — filters out web sections and marketing text
const JOB_KEYWORDS = [
  "engineer","developer","analyst","manager","designer","coordinator","specialist",
  "consultant","director","lead","head of","officer","associate","executive",
  "representative","strategist","architect","scientist","researcher","recruiter",
  "advisor","support","success","operations","marketing","sales","product",
  "project","program","data","cloud","devops","backend","frontend","fullstack",
  "full stack","mobile","qa","ux","ui","seo","content","brand","finance",
  "accounting","hr","talent","people","customer","account","business","software",
  "platform","security","systems","editor","writer","assistant","bookkeeper",
  "accountant","payroll","virtual","ingeniero","desarrollador","analista",
  "gerente","diseñador","coordinador","especialista","ejecutivo","reclutador",
  "soporte","ventas","producto","proyecto","contador","asistente","programador",
];

function isValidJobTitle(title) {
  if (!title || title.length < 5 || title.length > 150) return false;
  const t = title.toLowerCase();
  const skip = [
    "cookie","privacy","faq","about","contact","sign up","subscribe","newsletter",
    "read more","learn more","©","copyright","powered by","get started","our services",
    "our team","our mission","case studies","how it works","why choose",
    "town life","it's all here","stay in the loop","explore more","help us find",
    "tell us more","this website uses","head hunter de especialidad","una extension",
    "el match","oportunidades para todos","evitamos sesgos","impulsamos",
    "resultado de mayor","flexible y ajustado","jose b.","josé b.",
  ];
  if (skip.some(s => t.includes(s))) return false;
  return JOB_KEYWORDS.some(w => t.includes(w));
}

const COLORS = ['c0','c1','c2','c3','c4','c5','c6','c7'];
let ALL = [];
let F = { cat:[], wt:[], sn:[], agency:[] };

function colorFor(n) { let h=0; for(const c of (n||'')) h=(h*31+c.charCodeAt(0))&0xffff; return COLORS[h%COLORS.length]; }
function inits(n) { return (n||'?').split(' ').slice(0,2).map(w=>w[0]||'').join('').toUpperCase()||'?'; }
function cap(s) { return s?s.charAt(0).toUpperCase()+s.slice(1):''; }
function ago(iso) {
  if(!iso) return '';
  const s=Math.floor((Date.now()-new Date(iso))/1000);
  if(s<3600) return Math.floor(s/60)+'m ago';
  if(s<86400) return Math.floor(s/3600)+'h ago';
  if(s<604800) return Math.floor(s/86400)+'d ago';
  return Math.floor(s/604800)+'w ago';
}

function toggleFilter(g,v,el) {
  const a=F[g], i=a.indexOf(v);
  if(i>-1){a.splice(i,1);el.classList.remove('active');}
  else{a.push(v);el.classList.add('active');}
  doFilter();
}

function doFilter() {
  const q = document.getElementById('q-search').value.trim().toLowerCase();
  const res = ALL.filter(j => {
    if(!isValidJobTitle(j.title)) return false;
    if(q && !(j.title||'').toLowerCase().includes(q) && !(j.source_name||'').toLowerCase().includes(q) && !(j.location||'').toLowerCase().includes(q)) return false;
    if(F.cat.length && !F.cat.includes(j.category)) return false;
    if(F.wt.length  && !F.wt.includes(j.work_type)) return false;
    if(F.sn.length  && !F.sn.includes(j.seniority)) return false;
    if(F.agency.length && !F.agency.includes(j.source_id)) return false;
    return true;
  });
  renderGrid(res);
}

function renderGrid(jobs) {
  const grid = document.getElementById('jobs-grid');
  document.getElementById('result-count').textContent = jobs.length;
  grid.innerHTML = '';
  if(!jobs.length) {
    const empty = document.createElement('div');
    empty.className = 'empty-state';
    empty.innerHTML = '<h3>No roles match your search</h3><p>Try different keywords or clear your filters.</p>';
    grid.appendChild(empty);
    return;
  }
  jobs.forEach(j => {
    const card = document.createElement('a');
    card.className = 'job-card';
    card.href = j.apply_url || '#';
    card.target = '_blank';
    card.rel = 'noopener noreferrer';

    const hdr = document.createElement('div');
    hdr.className = 'jc-header';
    const logo = document.createElement('div');
    logo.className = 'jc-logo ' + colorFor(j.source_name);
    logo.textContent = inits(j.source_name);
    const time = document.createElement('span');
    time.className = 'jc-time';
    time.textContent = ago(j.scraped_at);
    hdr.appendChild(logo); hdr.appendChild(time);
    card.appendChild(hdr);

    const title = document.createElement('div');
    title.className = 'jc-title';
    title.textContent = j.title || '';
    card.appendChild(title);

    const agency = document.createElement('div');
    agency.className = 'jc-agency';
    agency.textContent = (j.source_name||'') + (j.location ? ' · ' + j.location : '');
    card.appendChild(agency);

    const tags = document.createElement('div');
    tags.className = 'jc-tags';
    if(j.category){const t=document.createElement('span');t.className='tag tag-cat';t.textContent=cap(j.category);tags.appendChild(t);}
    if(j.work_type){const t=document.createElement('span');t.className='tag tag-'+j.work_type;t.textContent=cap(j.work_type);tags.appendChild(t);}
    if(j.seniority){const t=document.createElement('span');t.className='tag tag-seniority';t.textContent=cap(j.seniority);tags.appendChild(t);}
    card.appendChild(tags);

    const footer = document.createElement('div');
    footer.className = 'jc-footer';
    const src = document.createElement('span');
    src.className = 'jc-source';
    src.textContent = j.source_name || '';
    const btn = document.createElement('a');
    btn.className = 'jc-apply';
    btn.href = j.apply_url || '#';
    btn.target = '_blank';
    btn.rel = 'noopener noreferrer';
    btn.textContent = 'Apply →';
    btn.addEventListener('click', e => e.stopPropagation());
    footer.appendChild(src); footer.appendChild(btn);
    card.appendChild(footer);
    grid.appendChild(card);
  });
}

function updateCounts() {
  const validJobs = ALL.filter(j => isValidJobTitle(j.title));
  const c=(id,fn)=>{const el=document.getElementById(id);if(el)el.textContent=validJobs.filter(fn).length;};
  c('fc-tech',j=>j.category==='tech'); c('fc-sales',j=>j.category==='sales');
  c('fc-marketing',j=>j.category==='marketing'); c('fc-design',j=>j.category==='design');
  c('fc-finance',j=>j.category==='finance'); c('fc-hr',j=>j.category==='hr');
  c('fc-data',j=>j.category==='data'); c('fc-ops',j=>j.category==='ops');
  c('fc-remote',j=>j.work_type==='remote'); c('fc-hybrid',j=>j.work_type==='hybrid');
  c('fc-onsite',j=>j.work_type==='onsite');
  c('fc-junior',j=>j.seniority==='junior'); c('fc-mid',j=>j.seniority==='mid');
  c('fc-senior',j=>j.seniority==='senior'); c('fc-lead',j=>j.seniority==='lead');
  c('fc-director',j=>j.seniority==='director');
  document.getElementById('m-total').textContent = validJobs.length;
  document.getElementById('m-remote').textContent = validJobs.filter(j=>j.work_type==='remote').length;
  document.getElementById('nav-count').textContent = validJobs.length;
}

function buildAgencyFilters() {
  const validJobs = ALL.filter(j => isValidJobTitle(j.title));
  const sources = [...new Map(validJobs.map(j=>[j.source_id, j.source_name])).entries()]
    .filter(([id])=>id).sort((a,b)=>a[1].localeCompare(b[1]));
  const container = document.getElementById('agency-filters');
  container.innerHTML = '';
  sources.forEach(([sid, sname]) => {
    const count = validJobs.filter(j=>j.source_id===sid).length;
    const row = document.createElement('div');
    row.className = 'sb-row';
    row.onclick = () => toggleFilter('agency', sid, row);
    row.innerHTML = '<div class="sb-check"><svg class="sb-tick" width="9" height="9" viewBox="0 0 10 10" fill="none" stroke="#fff" stroke-width="2.5"><path d="M2 5l2.5 2.5L8 3"/></svg></div>'
      + '<span class="sb-label">' + sname + '</span>'
      + '<span class="sb-count">' + count + '</span>';
    container.appendChild(row);
  });
}

async function load() {
  try {
    const fourWeeksAgo = new Date(Date.now() - 28*24*60*60*1000).toISOString();
    const res = await fetch(
      SB_URL + '/rest/v1/jobs?is_active=eq.true&scraped_at=gte.' + fourWeeksAgo + '&order=scraped_at.desc&limit=2000',
      { headers: { apikey: SB_KEY, Authorization: 'Bearer ' + SB_KEY } }
    );
    if(!res.ok) throw new Error('HTTP ' + res.status);
    ALL = await res.json();
    updateCounts();
    buildAgencyFilters();
    doFilter();
  } catch(err) {
    console.error(err);
    const grid = document.getElementById('jobs-grid');
    grid.innerHTML = '';
    const box = document.createElement('div');
    box.className = 'error-box';
    box.innerHTML = '<h3>Could not load jobs</h3><p>Browse our partner agencies directly:</p>'
      + '<div class="agency-grid">' + PARTNERS.map(p=>'<a class="agency-chip" href="'+p.url+'" target="_blank" rel="noopener">'+p.name+'</a>').join('') + '</div>';
    grid.appendChild(box);
    document.getElementById('result-count').textContent = '0';
  }
}

load();
</script>
</body>
</html>