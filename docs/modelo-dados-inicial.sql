-- Modelo relacional inicial proposto para fase seguinte (PostgreSQL)

create table app_user (
  id uuid primary key,
  email text unique not null,
  password_hash text not null,
  name text not null,
  role text not null default 'owner',
  created_at timestamptz not null default now()
);

create table upload_batch (
  id uuid primary key,
  file_name text not null,
  uploaded_by uuid not null references app_user(id),
  uploaded_at timestamptz not null default now(),
  row_count int not null default 0,
  status text not null default 'imported'
);

create table financial_entry_raw (
  id uuid primary key,
  upload_id uuid not null references upload_batch(id),
  row_number int,
  data_movimento date,
  descricao text,
  cliente_original text,
  projeto_original text,
  parceiro_original text,
  conta_original text,
  detalhe_original text,
  valor numeric(14,2),
  payload_json jsonb,
  created_at timestamptz not null default now()
);

create table financial_entry_working (
  id uuid primary key,
  raw_id uuid not null references financial_entry_raw(id),
  cliente_oficial text,
  projeto_oficial text,
  tipo_cadastro text,
  natureza text,
  centro_custo text,
  parceiro text,
  categoria text,
  detalhe_despesa text,
  conta_cartao text,
  forma_pagamento text,
  status text,
  rateio_json jsonb,
  updated_at timestamptz not null default now()
);

create table review_registry (
  id uuid primary key,
  nome_original text not null,
  nome_oficial text not null,
  tipo_sugerido text not null,
  tipo_final text not null,
  cliente_vinculado text,
  projeto_vinculado text,
  manter_alias boolean not null default true,
  observacao text,
  status_revisao text not null default 'pendente',
  created_at timestamptz not null default now()
);

create table analysis_issue (
  id uuid primary key,
  upload_id uuid not null references upload_batch(id),
  entry_id uuid,
  level text not null,
  code text not null,
  message text not null,
  status text not null default 'aberta',
  created_at timestamptz not null default now()
);

create table classification_rule (
  id uuid primary key,
  nome_original text not null,
  nome_oficial text not null,
  tipo_final text not null,
  natureza_padrao text,
  ativo boolean not null default true,
  created_at timestamptz not null default now()
);
