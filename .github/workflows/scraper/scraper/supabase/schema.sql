-- Tabla principal de jobs
CREATE TABLE IF NOT EXISTS jobs (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  title TEXT NOT NULL,
  location TEXT,
  category TEXT,
  seniority TEXT,
  work_type TEXT,
  employment_type TEXT,
  source_name TEXT,
  source_id TEXT,
  source_ats TEXT,
  apply_url TEXT,
  source_url TEXT,
  scraped_at TIMESTAMPTZ DEFAULT now(),
  is_active BOOLEAN DEFAULT true
);

-- Lectura pública, escritura solo con service_role
ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;

CREATE POLICY "Lectura pública" ON jobs
  FOR SELECT USING (true);

CREATE POLICY "Escritura service_role" ON jobs
  FOR ALL USING (auth.role() = 'service_role');
  