import os, json
import psycopg2

DATABASE_URL = os.environ.get('DATABASE_URL', '')
if not DATABASE_URL:
    # tentar ler do .env
    try:
        with open('/home/ubuntu/Financeiro/.env') as f:
            for line in f:
                if line.startswith('DATABASE_URL'):
                    DATABASE_URL = line.split('=',1)[1].strip().strip('"').strip("'")
    except: pass

print("Conectando ao banco...")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

# Buscar lançamentos relacionados a Luciana
cur.execute("""
    SELECT id, data->>'dataISO' as data, data->>'valor' as valor, data->>'dc' as dc,
           data->>'cliente' as cliente, data->>'favorecido' as favorecido,
           data->>'parceiro' as parceiro, data->>'descritivo' as descritivo,
           data->>'centroCusto' as cc
    FROM entries
    WHERE LOWER(data::text) LIKE '%luciana%'
    ORDER BY data->>'dataISO' DESC
    LIMIT 20
""")
rows = cur.fetchall()
print(f"\n=== Lançamentos com 'luciana' ({len(rows)} encontrados) ===")
for r in rows:
    print(f"  Data: {r[1]} | Valor: {r[2]} | D/C: {r[3]} | Cliente: {r[4]} | Favorecido: {r[5]} | CC: {r[8]}")
    print(f"    Descritivo: {r[7]}")

# Buscar lançamentos com valor próximo a 1310
cur.execute("""
    SELECT id, data->>'dataISO' as data, data->>'valor' as valor, data->>'dc' as dc,
           data->>'cliente' as cliente, data->>'favorecido' as favorecido,
           data->>'descritivo' as descritivo
    FROM entries
    WHERE ABS((data->>'valor')::numeric) BETWEEN 1309 AND 1311
    ORDER BY data->>'dataISO' DESC
    LIMIT 20
""")
rows2 = cur.fetchall()
print(f"\n=== Lançamentos com valor ~R$1.310 ({len(rows2)} encontrados) ===")
for r in rows2:
    print(f"  Data: {r[1]} | Valor: {r[2]} | D/C: {r[3]} | Cliente: {r[4]} | Favorecido: {r[5]}")
    print(f"    Descritivo: {r[6]}")

conn.close()
