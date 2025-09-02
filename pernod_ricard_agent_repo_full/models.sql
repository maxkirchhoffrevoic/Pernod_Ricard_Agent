-- minimal schema
create extension if not exists "uuid-ossp";
create extension if not exists vector;

CREATE TABLE IF NOT EXISTS company (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  name text NOT NULL,
  domain text,
  notes jsonb,
  created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS source (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id uuid REFERENCES company(id),
  url text,
  title text,
  published_at timestamptz,
  language text,
  raw_text text,
  hash text UNIQUE,
  embedding vector(1536)
);

CREATE TABLE IF NOT EXISTS signal (
  id uuid PRIMARY KEY DEFAULT uuid_generate_v4(),
  company_id uuid REFERENCES company(id),
  type text,
  value jsonb,
  confidence numeric,
  source_ids uuid[],
  detected_at timestamptz DEFAULT now()
);
