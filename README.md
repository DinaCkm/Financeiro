# Painel Financeiro Gerencial CKM

MVP em evolução com foco de **aderência à operação real CKM**.

## Como executar
```bash
npm run dev
```
Acesse: `http://localhost:3000/login`

Usuário de teste:
- email: `owner@ckm.local`
- senha: `123456`

## Sprint 3 (entrega atual)
- Pré-análise refinada com destaque claro para:
  - despesas sem projeto,
  - estrutura lançada como cliente,
  - mútuos incorretos,
  - novos cadastros,
  - conflitos de alias,
  - pendências bloqueantes.
- Cadastro revisável otimizado para operação diária (foco em pendentes, reclassificação rápida e ações diretas).
- Dashboard gerencial reforçado com:
  - saldo de hoje,
  - projeção 7/30 dias,
  - contas a pagar/receber,
  - resultado por cliente/projeto.
- Reaplicação consistente de regras salvas em novas importações (alias, vínculo projeto-cliente e regras de atualização).

## Colunas esperadas pela importação atual
A importação faz mapeamento por sinônimos de cabeçalho.

Campos canônicos:
- `data`
- `descricao`
- `cliente`
- `projeto`
- `parceiro`
- `conta`
- `detalhe`
- `valor`
- `centroCusto`
- `formaPagamento`

Sinônimos aceitos (exemplos):
- `descricao` -> descrição, historico, histórico
- `cliente` -> client, contratante
- `projeto` -> project, contrato, frente
- `parceiro` -> prestador, fornecedor, beneficiário
- `conta` -> cartão, conta/cartão
- `valor` -> amount, vlr, valor total
- `centroCusto` -> centro_custo, cc
- `dc` -> D/C, débito/crédito
- `tipoOriginal` -> tipo, tp-despesa
- `detDespesa` -> det-despesa
- `statusPlanilha` -> status

## Como testar com a planilha real da CKM
1. Entre em `/login` e autentique.
2. Acesse `/upload`.
3. Selecione a planilha real (`.xlsx`/`.xlsm`/`.csv`).
4. Faça a importação e valide o retorno JSON (`importedRows`, `alerts`, `blockingIssues`).
5. Acesse `/pendencias` para revisar os blocos prioritários de pré-análise.
6. Acesse `/cadastros` para consolidar aliases, vincular projeto->cliente e reclassificar rapidamente.
7. Faça novo upload da mesma base (ou base incremental) e confirme se regras anteriores foram reaplicadas.
8. Valide no resumo da pré-análise os alertas CKM específicos: `RECEITA_SEM_CLIENTE` e `LANCAMENTO_CANCELADO`.

## Limitações conhecidas
- Parser XLSX/XLSM lê a **primeira aba** apenas.
- Fórmulas complexas, células muito mescladas e múltiplos layouts na mesma aba podem exigir ajustes.
- Não há importação via `multipart/form-data` nesta fase (envio em base64 pelo frontend).
- Persistência permanece em `data/db.json` (migração SQL já mapeada em `docs/modelo-dados-inicial.sql`).
- Colunas bancárias de saldo por linha (ex.: `BB`, `ITAÚ`, `BRB`) ainda não são pivotadas para lançamentos individuais.
