#!/usr/bin/env python3
"""
Script para atualizar projeto e natureza dos lançamentos via PostgreSQL direto.
Usa os numLanc extraídos do arquivo pasted_content_16.txt
"""
import os
import sys

# Dados extraídos do arquivo
lancamentos = [
    (8973,  "BANESE PSI01", "Despesa Direta"),
    (9328,  "BANESE PSI01", "Despesa Direta"),
    (9336,  "BANESE PSI01", "Receita Direta"),
    (9337,  "BANESE PSI01", "Despesa Direta"),
    (9393,  "BANESE PSI01", "Despesa Direta"),
    (9394,  "BANESE PSI01", "Despesa Direta"),
    (9403,  "BANESE PSI01", "Despesa Direta"),
    (9407,  "BANESE PSI01", "Despesa Direta"),
    (9415,  "BANESE PSI01", "Despesa Direta"),
    (9553,  "BANESE PSI01", "Despesa Direta"),
    (9554,  "BANESE PSI01", "Despesa Direta"),
    (9556,  "BANESE PSI01", "Despesa Direta"),
    (9608,  "BANESE PSI01", "Despesa Direta"),
    (9614,  "BANESE PSI01", "Receita Direta"),
    (9615,  "BANESE PSI01", "Despesa Direta"),
    (9660,  "BANESE PSI02", "Despesa Direta"),
    (9671,  "BANESE PSI02", "Despesa Direta"),
    (9677,  "BANESE PSI02", "Despesa Direta"),
    (9706,  "BANESE PSI02", "Despesa Direta"),
    (9708,  "BANESE PSI02", "Despesa Direta"),
    (9709,  "BANESE PSI02", "Receita Direta"),
    (9746,  "BANESE PSI02", "Despesa Direta"),
    (9776,  "BANESE PSI02", "Despesa Direta"),
    (9778,  "BANESE PSI02", "Despesa Direta"),
    (9816,  "BANESE PSI02", "Despesa Direta"),
    (9888,  "BANESE PSI02", "Despesa Direta"),
    (10053, "BANESE PSI02", "Despesa Direta"),
    (10055, "BANESE PSI02", "Despesa Direta"),
    (10061, "BANESE PSI02", "Despesa Direta"),
    (10063, "BANESE PSI02", "Despesa Direta"),
    (10066, "BANESE PSI02", "Despesa Direta"),
    (10145, "BANESE PSI02", "Despesa Direta"),
    (10173, "BANESE PSI02", "Despesa Direta"),
    (10216, "BANESE PSI02", "Despesa Direta"),
    (10315, "BANESE PSI02", "Despesa Direta"),
    (10348, "BANESE PSI02", "Despesa Direta"),
    (10350, "BANESE PSI02", "Despesa Direta"),
    (10459, "BANESE PSI02", "Despesa Direta"),
    (10477, "BANESE PSI02", "Despesa Direta"),
    (10626, "BANESE PSI02", "Despesa Direta"),
    (10630, "BANESE PSI02", "Despesa Direta"),
    (10858, "BANESE PSI02", "Receita Direta"),
    (10859, "BANESE PSI02", "Despesa Direta"),
    (11045, "BANESE PSI02", "Despesa Direta"),
    (11059, "BANESE PSI02", "Despesa Direta"),
    (11060, "BANESE PSI02", "Receita Direta"),
    (11169, "BANESE PSI02", "Despesa Direta"),
]

db_url = os.environ.get("DATABASE_URL") or sys.argv[1] if len(sys.argv) > 1 else None

if not db_url:
    print("ERRO: DATABASE_URL não definida. Passe como argumento: python3 update_lancamentos.py 'postgresql://...'")
    sys.exit(1)

try:
    import psycopg2
except ImportError:
    os.system("sudo pip3 install psycopg2-binary -q")
    import psycopg2

conn = psycopg2.connect(db_url)
cur = conn.cursor()

atualizados = 0
nao_encontrados = []

for (num, projeto, natureza) in lancamentos:
    # Buscar por numLanc no JSONB
    cur.execute("""
        UPDATE entries
        SET data = jsonb_set(
                    jsonb_set(data, '{projeto}', %s::jsonb),
                    '{natureza}', %s::jsonb
                  )
        WHERE (data->>'numLanc')::int = %s
    """, (f'"{projeto}"', f'"{natureza}"', num))
    
    if cur.rowcount > 0:
        atualizados += cur.rowcount
        print(f"  ✓ #{num:06d} → projeto={projeto}, natureza={natureza}")
    else:
        nao_encontrados.append(num)
        print(f"  ✗ #{num:06d} NÃO ENCONTRADO")

conn.commit()
cur.close()
conn.close()

print(f"\n{'='*50}")
print(f"Total atualizados: {atualizados}")
print(f"Não encontrados ({len(nao_encontrados)}): {nao_encontrados}")
