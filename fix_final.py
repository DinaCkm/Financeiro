#!/usr/bin/env python3
"""
Correção definitiva e cirúrgica do server.js.

PROBLEMAS IDENTIFICADOS:
1. Rotas novas (/cadastros-mestres, /contratos, /contas, /conciliacao e suas APIs)
   não têm verificação de autenticação — dependem do guard global da linha 1433,
   mas esse guard usa `const user = currentUser(req, db)` e o `user` é uma variável
   local do bloco do handler. As rotas novas foram adicionadas FORA do escopo onde
   `user` está definido, então `user` é `undefined` nelas → redirect para /login.

2. /referencias faz `const db = loadDb()` DEPOIS de verificar `user` — isso é
   desnecessário pois `db` já foi carregado na linha 1393.

3. Sessões em memória (Map) — perdidas a cada redeploy.
   SOLUÇÃO: adicionar tabela `sessions` no PostgreSQL e persistir sessões lá.

SOLUÇÃO:
- Adicionar `const user = requireAuth(req, res, db); if (!user) return;` no início
  de cada rota nova de página (GET), e `if (!requireAuth(req, res, db)) return;`
  nas APIs novas (POST).
- Adicionar persistência de sessão no PostgreSQL via storage.js.
"""

with open('server.js', 'r', encoding='utf-8') as f:
    content = f.read()

# ============================================================
# CORREÇÃO 1: Adicionar requireAuth nas rotas novas de página (GET)
# Cada rota nova começa com `if (req.method === 'GET' && url.pathname === '/ROTA') {`
# e logo em seguida tem `let variaveis = ...` ou `try {`
# Precisamos inserir `const user = requireAuth(req, res, db); if (!user) return;`
# ANTES do `let` ou `try`.
# ============================================================

import re

# Rotas de página que precisam de auth adicionada
PAGE_ROUTES = [
    '/cadastros-mestres',
    '/contratos',
    '/contas',
    '/conciliacao',
]

# APIs que precisam de auth adicionada
API_ROUTES = [
    '/api/contratos',
    '/api/contas',
]

# Padrão para rotas de página: logo após o `if (req.method === 'GET' && url.pathname === '/ROTA') {`
# vem uma linha com `let ` ou `try {` ou `const pg`
# Vamos inserir a verificação de auth logo após o `{` de abertura da rota

changes = 0

for route in PAGE_ROUTES:
    # Encontrar o padrão: `if (req.method === 'GET' && url.pathname === '/ROTA') {\n    let`
    # ou `if (req.method === 'GET' && url.pathname === '/ROTA') {\n    try`
    pattern = rf"(if \(req\.method === 'GET' && url\.pathname === '{re.escape(route)}'\) \{{\n)(    (?:let |try \{{|const pg))"
    replacement = rf"\1    const user = requireAuth(req, res, db); if (!user) return;\n\2"
    new_content = re.sub(pattern, replacement, content)
    if new_content != content:
        print(f"  ✅ Auth adicionada em GET {route}")
        content = new_content
        changes += 1
    else:
        print(f"  ⚠️  Padrão não encontrado para GET {route} — verificando manualmente...")
        # Tentar padrão mais amplo
        pattern2 = rf"(if \(req\.method === 'GET' && url\.pathname === '{re.escape(route)}'\) \{{)"
        matches = list(re.finditer(pattern2, content))
        if matches:
            m = matches[0]
            print(f"     Encontrado na posição {m.start()} — inserindo auth...")
            # Encontrar a próxima linha após o `{`
            pos = m.end()
            # Inserir após o `{` e newline
            insert_text = "\n    const user = requireAuth(req, res, db); if (!user) return;"
            content = content[:pos] + insert_text + content[pos:]
            changes += 1
            print(f"  ✅ Auth adicionada em GET {route} (método 2)")
        else:
            print(f"  ❌ Rota GET {route} não encontrada no arquivo!")

for route in API_ROUTES:
    # Para APIs POST: inserir após `if (req.method === 'POST' && url.pathname === '/ROTA') {\n`
    pattern = rf"(if \(req\.method === 'POST' && url\.pathname === '{re.escape(route)}'\) \{{\n)(    const pg)"
    replacement = rf"\1    if (!requireAuth(req, res, db)) return;\n\2"
    new_content = re.sub(pattern, replacement, content)
    if new_content != content:
        print(f"  ✅ Auth adicionada em POST {route}")
        content = new_content
        changes += 1
    else:
        print(f"  ⚠️  Padrão não encontrado para POST {route}")

# API de baixa de conta
pattern_baixa = r"(if \(req\.method === 'POST' && url\.pathname\.startsWith\('/api/contas/'\) && url\.pathname\.endsWith\('/baixa'\)\) \{\n)(    const pg)"
replacement_baixa = r"\1    if (!requireAuth(req, res, db)) return;\n\2"
new_content = re.sub(pattern_baixa, replacement_baixa, content)
if new_content != content:
    print(f"  ✅ Auth adicionada em POST /api/contas/*/baixa")
    content = new_content
    changes += 1

# ============================================================
# CORREÇÃO 2: Remover `const db = loadDb()` duplicado na rota /referencias
# Na linha 2747 há um `const db = loadDb()` desnecessário — o db já foi carregado
# ============================================================
# Padrão: dentro da rota /referencias, após verificar user, tem `const db = loadDb();`
old_refs = "    if (!user) { res.writeHead(302, { Location: '/login' }); res.end(); return; }\n    const db = loadDb();\n    const refs = db.referencias"
new_refs = "    if (!user) { res.writeHead(302, { Location: '/login' }); res.end(); return; }\n    const refs = db.referencias"
if old_refs in content:
    content = content.replace(old_refs, new_refs)
    print(f"  ✅ db duplicado removido em /referencias")
    changes += 1
else:
    print(f"  ⚠️  db duplicado em /referencias não encontrado (pode já estar correto)")

# ============================================================
# VERIFICAÇÃO FINAL
# ============================================================
print(f"\n=== TOTAL DE CORREÇÕES: {changes} ===")

# Verificar se ainda há rotas novas sem auth
for route in PAGE_ROUTES + API_ROUTES:
    # Contar quantas vezes a rota aparece sem requireAuth nas 3 linhas seguintes
    lines = content.split('\n')
    for i, line in enumerate(lines):
        if f"url.pathname === '{route}'" in line:
            context = '\n'.join(lines[i:i+5])
            has_auth = 'requireAuth' in context
            print(f"  {'✅' if has_auth else '❌'} {route}: {'com auth' if has_auth else 'SEM AUTH!'}")

with open('server.js', 'w', encoding='utf-8') as f:
    f.write(content)

print("\nArquivo salvo.")
