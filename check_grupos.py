import psycopg2, json

conn = psycopg2.connect(
    host='shortline.proxy.rlwy.net',
    port=49767,
    user='postgres',
    password='nYLqelflGRzsXFafMcvzfEtqlTvyLItr',
    dbname='railway'
)
cur = conn.cursor()

print("=== Grupos salvos nos lançamentos (campo grupoDespesa) ===")
cur.execute("""
    SELECT data->>'grupoDespesa' as grupo, COUNT(*) 
    FROM entries 
    WHERE data->>'grupoDespesa' IS NOT NULL AND data->>'grupoDespesa' != '' 
    GROUP BY 1 ORDER BY 2 DESC LIMIT 20
""")
for row in cur.fetchall():
    print(f"  {row[0]!r:50s} → {row[1]} lançamentos")

print("\n=== Tipos salvos nos lançamentos (campo tipoDespesa) ===")
cur.execute("""
    SELECT data->>'tipoDespesa' as tipo, COUNT(*) 
    FROM entries 
    WHERE data->>'tipoDespesa' IS NOT NULL AND data->>'tipoDespesa' != '' 
    GROUP BY 1 ORDER BY 2 DESC LIMIT 20
""")
for row in cur.fetchall():
    print(f"  {row[0]!r:50s} → {row[1]} lançamentos")

print("\n=== Grupos cadastrados na tabela grupos_despesa ===")
cur.execute("SELECT codigo, nome FROM grupos_despesa WHERE ativo=true ORDER BY nome")
for row in cur.fetchall():
    print(f"  codigo={row[0]!r:30s} nome={row[1]!r}")

print("\n=== Tipos cadastrados na tabela tipos_despesa (com grupo) ===")
cur.execute("""
    SELECT td.codigo, td.nome, gd.codigo as grupo_cod, gd.nome as grupo_nome 
    FROM tipos_despesa td 
    LEFT JOIN grupos_despesa gd ON gd.id=td.grupo_id 
    WHERE td.ativo=true 
    ORDER BY gd.nome NULLS LAST, td.nome
    LIMIT 30
""")
for row in cur.fetchall():
    print(f"  tipo_cod={row[0]!r:30s} tipo_nome={row[1]!r:30s} grupo_cod={row[2]!r:20s} grupo_nome={row[3]!r}")

conn.close()
