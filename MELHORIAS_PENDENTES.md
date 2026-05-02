# Melhorias Pendentes — CKM Financeiro

## Prioridade 1 — Após finalizar Cadastros Mestres

### 01. Busca por número do lançamento
- **Onde:** Card de pesquisa na aba Lançamentos
- **O que:** Adicionar campo de busca pelo `numLanc` (número sequencial do lançamento)
- **Como:** Input numérico no filtro da tela `/lancamentos` que filtra por `e.numLanc === parseInt(busca)`

### 02. Preenchimento automático de CPF/CNPJ no novo lançamento
- **Onde:** Formulário de novo lançamento
- **Regra:** Campo "Favorecido" deve ser um select/datalist vinculado ao cadastro de Clientes/Fornecedores
- **Comportamento:** Ao selecionar o favorecido, o campo CPF/CNPJ é preenchido automaticamente com os dados do cadastro
- **Obrigatoriedade:** O favorecido DEVE estar cadastrado antes de criar o lançamento (validação no front e no back)
- **Campos a preencher automaticamente:** cpfCnpj, tipo (cliente/fornecedor/prestador)
