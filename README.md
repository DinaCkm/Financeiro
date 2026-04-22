# Painel Financeiro Gerencial CKM (MVP - Sprint 1)

## Como executar
```bash
npm run dev
```
Acesse: `http://localhost:3000/login`

Usuário de teste:
- email: `owner@ckm.local`
- senha: `123456`

## Entregas da Sprint 1
- Autenticação básica.
- Upload de planilha em **CSV** (separador `;`).
- Persistência bruta em `data/db.json`.
- Pré-análise com pendências e alertas.
- Cadastro revisável para nomes importados.
- Edição manual via APIs de revisão/reclassificação.
- Dashboard inicial por cliente/projeto.

## Limitações conhecidas
- Importação de XLSX ainda não habilitada (planejada Sprint 2).
- Persistência atual em JSON local (modelo SQL já definido em `docs/modelo-dados-inicial.sql`).
