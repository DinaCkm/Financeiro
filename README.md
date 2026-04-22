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

## Sprint 2 (entrega atual)
- Suporte de importação para **CSV, XLSX e XLSM**.
- Parser flexível de cabeçalhos para aproximar o layout da planilha operacional real.
- Fluxo centrado em **cadastro revisável** (consolidação de aliases, conversão de tipo, vínculo projeto->cliente e regras futuras).
- Regras salvas aplicadas automaticamente em novas importações.

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

## Limitações conhecidas
- Parser XLSX/XLSM lê a **primeira aba** apenas.
- Fórmulas complexas e planilhas com layout altamente mesclado podem exigir ajuste adicional.
- Não há importação via `multipart/form-data` nesta fase (envio em base64 pelo frontend).
- Persistência permanece em `data/db.json` (migração SQL já mapeada em `docs/modelo-dados-inicial.sql`).

## Próxima prioridade
Compatibilização incremental com a planilha operacional real da CKM (amostras reais, ajustes de mapeamento, validações de negócio e cobertura de casos específicos).
