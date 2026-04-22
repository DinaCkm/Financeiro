# Arquitetura Técnica Sugerida (MVP)

## Visão Geral
- **Aplicação web monolítica Node.js** para acelerar entrega da Sprint 1.
- **Camadas lógicas**: apresentação (HTML), aplicação (regras de pré-análise), persistência (JSON local no MVP; SQL já desenhado para migração).
- **Autenticação básica por sessão cookie**.

## Componentes
1. **Módulo de Upload**
   - Recebe CSV (base para evoluir para XLSX).
   - Armazena upload e lançamentos brutos.
2. **Módulo de Pré-análise**
   - Gera erros bloqueantes e alertas.
   - Aplica taxonomia inicial e natureza sugerida.
3. **Módulo de Cadastro Revisável**
   - Consolida nomes únicos importados.
   - Sugere tipo e permite ajuste manual.
4. **Módulo de Reclassificação de Lançamentos**
   - Permite edição de campos críticos.
5. **Dashboard Inicial**
   - Exibe resultado por cliente/projeto e indicadores de caixa.

## Evolução planejada pós-Sprint 1
- Migrar para Next.js + API Routes.
- Migrar persistência para PostgreSQL com Prisma.
- Adicionar fila assíncrona de processamento.
- Suportar XLSX nativamente com parser robusto.
