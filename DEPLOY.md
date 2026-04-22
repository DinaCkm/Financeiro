# Deploy no Railway

## Pré-requisitos

- Conta no [Railway](https://railway.app)
- Repositório conectado ao Railway via GitHub

## Configuração

O projeto já inclui os arquivos de configuração necessários:

| Arquivo | Finalidade |
|---|---|
| `railway.json` | Configuração principal do deploy (builder, start command, healthcheck) |
| `nixpacks.toml` | Define Node.js 20 como runtime |
| `.gitignore` | Exclui arquivos desnecessários do repositório |

## Variáveis de Ambiente

Nenhuma variável obrigatória para a Sprint 1. Recomendado configurar no Railway:

| Variável | Valor sugerido | Descrição |
|---|---|---|
| `PORT` | (automático pelo Railway) | Porta da aplicação |
| `NODE_ENV` | `production` | Ambiente de execução |

## Como fazer o deploy

### Opção 1 — Via GitHub (recomendado)

1. Acesse [railway.app](https://railway.app) e faça login.
2. Clique em **New Project** → **Deploy from GitHub repo**.
3. Selecione o repositório `DinaCkm/Financeiro`.
4. O Railway detectará automaticamente o `railway.json` e iniciará o build.
5. Após o deploy, acesse a URL gerada e faça login com:
   - **E-mail:** `owner@ckm.local`
   - **Senha:** `123456`

### Opção 2 — Via Railway CLI

```bash
npm install -g @railway/cli
railway login
railway link
railway up
```

## Observações importantes

- A Sprint 1 utiliza **persistência em arquivo JSON local** (`data/db.json`).
  - No Railway, o sistema de arquivos é **efêmero** — os dados são perdidos a cada redeploy.
  - Para persistência durável, a Sprint 2 prevê migração para **PostgreSQL** (Railway oferece plugin nativo).
- O healthcheck aponta para `/login` — certifique-se de que a rota está acessível após o start.

## Próximos passos (Sprint 2)

- Adicionar **PostgreSQL** via Railway Plugin.
- Migrar `server.js` para **Next.js + API Routes**.
- Substituir `data/db.json` por queries SQL com **Prisma ORM**.
