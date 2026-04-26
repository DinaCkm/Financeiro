#!/usr/bin/env python3
"""
Correção cirúrgica do server.js:
1. Remover linhas duplicadas de `const db = loadDb(); const user = currentUser(req, db);`
   e os `if (!user)` seguintes nas novas rotas (linhas 3671+)
2. Substituir `checkAuth(req, res)` por verificação correta usando `user` já disponível
3. Garantir que checkAuth seja definida como função válida
"""

with open('server.js', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Padrões a remover (linhas duplicadas nas novas rotas após linha 3600)
REMOVE_PATTERNS = [
    'const db = loadDb(); const user = currentUser(req, db);',
    'if (!user) { res.writeHead(302, { Location: \'/login\' }); res.end(); return; }',
    'if (!user) { return json(res, 401, { error: \'Não autenticado\' }); }',
    'if (!user) return json(res, 401, { error: \'Não autenticado\' });',
]

new_lines = []
skip_next = False

for i, line in enumerate(lines):
    lineno = i + 1  # 1-indexed
    stripped = line.strip()
    
    # Só remover nas novas rotas (após linha 3600)
    if lineno > 3600:
        if any(p in stripped for p in REMOVE_PATTERNS):
            print(f"  REMOVENDO linha {lineno}: {stripped[:80]}")
            continue
    
    new_lines.append(line)

# Agora corrigir checkAuth: substituir por verificação usando `user` já disponível
# checkAuth é usado nas rotas /referencias, /historico e APIs de entries
# Nessas rotas, `user` já está definido pelo guard global (linha 1433)
# Então `checkAuth(req, res)` deve ser substituído por `!user` simples

result = []
for line in new_lines:
    if 'if (!checkAuth(req, res)) return;' in line:
        # Substituir por verificação usando user já disponível
        indent = len(line) - len(line.lstrip())
        new_line = ' ' * indent + 'if (!user) { res.writeHead(302, { Location: \'/login\' }); res.end(); return; }\n'
        print(f"  CORRIGINDO checkAuth: {line.strip()[:60]} -> {new_line.strip()[:60]}")
        result.append(new_line)
    else:
        result.append(line)

with open('server.js', 'w', encoding='utf-8') as f:
    f.writelines(result)

print(f"\nTotal de linhas: {len(result)}")
print(f"checkAuth restantes: {sum(1 for l in result if 'checkAuth' in l)}")
print(f"getSession restantes: {sum(1 for l in result if 'getSession' in l)}")
print(f"db duplicado restantes: {sum(1 for l in result if 'const db = loadDb(); const user = currentUser' in l)}")
