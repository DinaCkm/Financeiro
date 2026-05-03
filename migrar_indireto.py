import os, sys, json

# Ler DATABASE_URL das variáveis de ambiente ou do arquivo de configuração do Railway
DATABASE_URL = os.environ.get('DATABASE_URL', '')

if not DATABASE_URL:
    # Tentar ler do railway.toml ou de arquivos de configuração
    for fname in ['/home/ubuntu/Financeiro/.env', '/home/ubuntu/Financeiro/.env.local', '/home/ubuntu/.env']:
        try:
            with open(fname) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith('DATABASE_URL'):
                        DATABASE_URL = line.split('=', 1)[1].strip().strip('"').strip("'")
                        break
        except:
            pass
        if DATABASE_URL:
            break

if not DATABASE_URL:
    print("ERRO: DATABASE_URL não encontrada. Informe via variável de ambiente.")
    sys.exit(1)

import psycopg2

print(f"Conectando ao banco...")
conn = psycopg2.connect(DATABASE_URL)
conn.autocommit = False
cur = conn.cursor()

# 1. Verificar todos os valores distintos de natureza e classificacao
print("\n=== Valores distintos de natureza (campo real no banco) ===")
cur.execute("""
    SELECT data->>'natureza' as natureza, COUNT(*) as total
    FROM entries
    WHERE data->>'natureza' IS NOT NULL AND data->>'natureza' != ''
    GROUP BY data->>'natureza'
    ORDER BY total DESC
""")
rows = cur.fetchall()
for r in rows:
    print(f"  '{r[0]}': {r[1]} lançamentos")

print("\n=== Valores distintos de classificacao ===")
cur.execute("""
    SELECT data->>'classificacao' as classificacao, COUNT(*) as total
    FROM entries
    WHERE data->>'classificacao' IS NOT NULL AND data->>'classificacao' != ''
    GROUP BY data->>'classificacao'
    ORDER BY total DESC
""")
rows2 = cur.fetchall()
for r in rows2:
    print(f"  '{r[0]}': {r[1]} lançamentos")

# 2. Identificar variantes de "indireto"
print("\n=== Variantes de INDIRETO encontradas ===")
cur.execute("""
    SELECT data->>'natureza' as natureza, COUNT(*) as total
    FROM entries
    WHERE LOWER(data->>'natureza') LIKE '%indiret%'
    GROUP BY data->>'natureza'
    ORDER BY total DESC
""")
indiretos = cur.fetchall()
for r in indiretos:
    print(f"  '{r[0]}': {r[1]} lançamentos")

cur.execute("""
    SELECT data->>'classificacao' as classificacao, COUNT(*) as total
    FROM entries
    WHERE LOWER(data->>'classificacao') LIKE '%indiret%'
    GROUP BY data->>'classificacao'
    ORDER BY total DESC
""")
indiretos2 = cur.fetchall()
for r in indiretos2:
    print(f"  (classificacao) '{r[0]}': {r[1]} lançamentos")

# 3. Executar a migração
print("\n=== Executando migração: unificar para 'Custo Indireto' ===")

# Atualizar campo natureza
cur.execute("""
    UPDATE entries
    SET data = jsonb_set(
        jsonb_set(data, '{natureza}', '"Custo Indireto"'),
        '{classificacao}', '"Custo Indireto"'
    )
    WHERE LOWER(data->>'natureza') LIKE '%indiret%'
""")
n1 = cur.rowcount
print(f"  Atualizados por natureza: {n1} lançamentos")

# Atualizar campo classificacao (onde natureza não tem indireto mas classificacao tem)
cur.execute("""
    UPDATE entries
    SET data = jsonb_set(
        jsonb_set(data, '{classificacao}', '"Custo Indireto"'),
        '{natureza}', '"Custo Indireto"'
    )
    WHERE LOWER(data->>'classificacao') LIKE '%indiret%'
    AND NOT (LOWER(data->>'natureza') LIKE '%indiret%')
""")
n2 = cur.rowcount
print(f"  Atualizados por classificacao (sem natureza): {n2} lançamentos")

total = n1 + n2
print(f"\n  Total migrado: {total} lançamentos")

conn.commit()
print("\n✅ Migração concluída com sucesso!")

# Verificar resultado
print("\n=== Verificação pós-migração ===")
cur.execute("""
    SELECT data->>'natureza' as natureza, COUNT(*) as total
    FROM entries
    WHERE LOWER(data->>'natureza') LIKE '%indiret%'
    GROUP BY data->>'natureza'
    ORDER BY total DESC
""")
restantes = cur.fetchall()
if restantes:
    print("  Ainda existem variantes:")
    for r in restantes:
        print(f"    '{r[0]}': {r[1]}")
else:
    print("  ✅ Nenhuma variante restante — todos unificados como 'Custo Indireto'")

conn.close()
