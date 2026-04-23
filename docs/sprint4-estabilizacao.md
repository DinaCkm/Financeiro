# Sprint 4 — Estabilização para uso real (PostgreSQL)

## 1) Modelo de banco proposto (PostgreSQL)

O fluxo oficial mantém a **aba principal CKM** como origem de importação e persiste entidades operacionais separadas:

- `users`: autenticação básica.
- `uploads`: metadados da importação (arquivo, data, volume).
- `entries`: lançamentos importados e já normalizados (inclui `dc`, `tipoOriginal`, `statusPlanilha`).
- `issues`: pendências/alertas de pré-análise.
- `review_registry`: cadastro revisável central.
- `saved_rules`: regras futuras (alias, vínculo projeto->cliente, updates).

Referência SQL detalhada: `docs/modelo-dados-inicial.sql`.

## 2) Plano de migração JSON -> PostgreSQL

1. **Preparar schema e seed**
   - Inicializar schema no Postgres.
   - Criar usuário inicial (`owner@ckm.local`).

2. **Exportar estado atual do JSON**
   - Ler `data/db.json` em snapshot único.

3. **Carga inicial no Postgres**
   - Inserir tabelas na ordem: `users` -> `uploads` -> `entries` -> `issues` -> `review_registry` -> `saved_rules`.

4. **Troca controlada do backend**
   - Ativar `DATABASE_URL` no ambiente.
   - Backend passa a usar storage Postgres automaticamente.

5. **Validação pós-migração**
   - Conferir contagem por entidade.
   - Conferir login, upload, pré-análise, cadastros e dashboard.

6. **Cutover definitivo**
   - Após migração validada, operação diária roda apenas com `DATABASE_URL`.

## 3) Impacto nas rotas e telas existentes

### Rotas/API
- **Sem mudança de contrato** de rota.
- Persistência passa a ser durável no Postgres quando `DATABASE_URL` estiver ativo.
- Endpoints mantidos:
  - `/api/upload`
  - `/api/review/*`
  - `/api/entries/*`

### Telas
- **Sem mudança de navegação** nesta etapa.
- Ganho principal: continuidade entre deploys/restarts (dados não se perdem).

## 4) Backlog técnico Sprint 4

### Núcleo de dados
- [x] Introduzir storage oficial PostgreSQL com schema/seed no startup.
- [x] Inicialização de schema/seed no startup.
- [x] Backfill automatizado de `data/db.json` para Postgres via script dedicado (`scripts/migrate_json_to_pg.js`).
- [ ] Índices e constraints adicionais para performance e integridade.

### Fluxo operacional diário
- [ ] Filtros avançados em `/pendencias` (código, bloqueante, período, status).
- [ ] Ações em lote no `/cadastros` (revisar em massa, aplicar regra por padrão).
- [ ] Encerramento operacional por dia (snapshot de fechamento).

### Dashboard gerencial
- [ ] Série temporal diária (7/30 dias) com baseline e tendência.
- [ ] Top clientes/projetos com filtros por período.
- [ ] Indicador de risco de caixa por janela.

### Confiabilidade
- [ ] Auditoria de ajustes manuais (`entries` e `review_registry`).
- [ ] Healthcheck com status de conexão Postgres.
- [ ] Plano de backup/restauração.
