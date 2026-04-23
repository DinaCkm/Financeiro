#!/usr/bin/env python3
"""
Parser de planilhas para o Painel Financeiro Gerencial CKM.
Suporta CSV, XLSX e XLSM.
Compatível com a planilha operacional real CKM (CKM_FLUXO_*.xlsm).

Funcionalidades:
- Lê a primeira aba (ou aba especificada por nome) de XLSX/XLSM
- Converte datas em serial numérico do Excel para ISO (yyyy-mm-dd)
- Normaliza valores monetários com separadores BR (1.234,56) e US (1234.56)
- Detecta e ignora linhas de cabeçalho duplicadas, totais e linhas vazias
- Mapeia colunas por sinônimos (compatível com planilha CKM)
"""
import csv
import io
import json
import sys
import zipfile
import re
import xml.etree.ElementTree as ET
from datetime import date, timedelta

NS = {'m': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}
REL_NS = {'r': 'http://schemas.openxmlformats.org/package/2006/relationships'}

# Palavras que indicam linhas de controle (não são lançamentos)
SKIP_KEYWORDS = {
    'saldo', 'total', 'subtotal', 'stop', 'carlos', 'marta',
    'janeiro', 'fevereiro', 'março', 'abril', 'maio', 'junho',
    'julho', 'agosto', 'setembro', 'outubro', 'novembro', 'dezembro',
}

# Mapeamento de colunas: canonical → lista de sinônimos (lowercase)
COLUMN_ALIASES = {
    'data':           ['data', 'dt', 'date', 'data_movimento', 'data movimento',
                       'vencimento', 'data pagamento', 'data_recebimento',
                       'data recebimento', 'data_emissão', 'data emissão'],
    'descricao':      ['descricao', 'descrição', 'historico', 'histórico',
                       'description', 'obs', 'observacao', 'observação'],
    'cliente':        ['cliente', 'client', 'contratante'],
    'projeto':        ['projeto', 'project', 'contrato', 'frente'],
    'parceiro':       ['parceiro', 'prestador', 'fornecedor', 'beneficiario',
                       'beneficiário', 'fornecedor/parceiro'],
    'conta':          ['conta', 'cartao', 'cartão', 'conta_cartao', 'conta/cartão'],
    'detalhe':        ['detalhe', 'categoria', 'detalhamento'],
    'valor':          ['valor', 'amount', 'vlr', 'valor_total', 'valor total',
                       'valor unitário', 'valor unitario'],
    'centroCusto':    ['centro_custo', 'centro custo', 'cc', 'centrodecusto',
                       'c_custo', 'c custo', 'c.custo'],
    'tipo':           ['tipo', 'receita/despesa', 'tipo_lancamento',
                       'tipo lancamento', 'tp-despesa', 'tp despesa'],
    'dc':             ['d/c', 'dc', 'debito/credito', 'débito/crédito'],
    'status':         ['status'],
    'movto':          ['movto', 'movimento', 'movimentacao', 'movimentação'],
    'pr':             ['pr'],
    'detDespesa':     ['det-despesa', 'det despesa', 'detalhe despesa',
                       'det_despesa', 'det.despesa', 'det-desp'],
    'notaFiscal':     ['nota_fiscal', 'nota fiscal', 'nf', 'nr.nf', 'nº nf',
                       'nf_emitida'],
    'formaPagamento': ['forma_pagamento', 'forma pagamento', 'pagamento'],
}


def normalize_header(h):
    return str(h or '').strip().lower()


def map_column(header):
    h = normalize_header(header)
    for canonical, aliases in COLUMN_ALIASES.items():
        if h in aliases:
            return canonical
    return None


def excel_serial_to_iso(serial):
    """Converte número serial do Excel para string ISO yyyy-mm-dd."""
    try:
        n = float(serial)
        if 20000 < n < 60000:
            d = date(1899, 12, 30) + timedelta(days=int(n))
            return d.isoformat()
    except Exception:
        pass
    return None


def parse_date(value):
    """
    Tenta converter um valor de data para ISO.
    Retorna (iso_string_or_None, tipo_string).
    """
    raw = str(value or '').strip()
    if not raw:
        return None, 'vazio'

    # Verifica se é palavra de controle (não é data)
    if raw.lower() in SKIP_KEYWORDS:
        return None, f'controle:{raw}'

    # Serial numérico do Excel
    serial = excel_serial_to_iso(raw)
    if serial:
        return serial, 'serial_excel'

    # Formato BR: dd/mm/aaaa ou dd/mm/aa
    br = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{2,4})$', raw)
    if br:
        dd, mm, yy = int(br.group(1)), int(br.group(2)), int(br.group(3))
        if len(br.group(3)) == 2:
            yy += 2000
        return f"{yy:04d}-{mm:02d}-{dd:02d}", 'br'

    # Formato ISO
    iso = re.match(r'^(\d{4})-(\d{2})-(\d{2})$', raw)
    if iso:
        return raw, 'iso'

    # Formato mm/aaaa (mês/ano apenas — ex: "01/2022")
    my = re.match(r'^(\d{1,2})/(\d{4})$', raw)
    if my:
        mm, yy = int(my.group(1)), int(my.group(2))
        return f"{yy:04d}-{mm:02d}-01", 'mes_ano'

    return None, f'invalido:{raw[:20]}'


def parse_money(value):
    """Normaliza valor monetário para float."""
    raw = str(value or '').strip()
    if not raw or raw == '-':
        return 0.0
    # Remove R$, espaços
    raw = re.sub(r'[R$\s]', '', raw)
    # Formato BR: 1.234,56
    if ',' in raw and '.' in raw:
        raw = raw.replace('.', '').replace(',', '.')
    elif ',' in raw:
        raw = raw.replace(',', '.')
    try:
        return float(raw)
    except Exception:
        return None


def read_shared_strings(zf):
    if 'xl/sharedStrings.xml' not in zf.namelist():
        return []
    root = ET.fromstring(zf.read('xl/sharedStrings.xml'))
    values = []
    for si in root.findall('m:si', NS):
        txt = ''.join(t.text or '' for t in si.findall('.//m:t', NS))
        values.append(txt)
    return values


def cell_value(cell, shared_strings):
    cell_type = cell.attrib.get('t')
    value_node = cell.find('m:v', NS)
    if value_node is None:
        inline = cell.find('m:is/m:t', NS)
        return inline.text if inline is not None and inline.text else ''
    raw = value_node.text or ''
    if cell_type == 's':
        try:
            return shared_strings[int(raw)]
        except Exception:
            return raw
    return raw


def col_letter_to_index(letters):
    idx = 0
    for ch in letters.upper():
        idx = idx * 26 + (ord(ch) - 64)
    return idx


def parse_xlsx_sheet(zf, sheet_path, shared):
    """Lê uma aba do XLSX/XLSM e retorna lista de listas."""
    ws = ET.fromstring(zf.read(sheet_path))
    data_el = ws.find('m:sheetData', NS)
    rows = []
    for row in data_el.findall('m:row', NS):
        max_col = 0
        row_data = {}
        for cell in row.findall('m:c', NS):
            ref = cell.attrib.get('r', '')
            col_letters = ''.join(ch for ch in ref if ch.isalpha())
            if not col_letters:
                continue
            col_idx = col_letter_to_index(col_letters)
            max_col = max(max_col, col_idx)
            row_data[col_idx] = cell_value(cell, shared)
        if max_col == 0:
            continue
        arr = [row_data.get(i, '') for i in range(1, max_col + 1)]
        rows.append(arr)
    return rows


def find_header_row(rows, min_filled=3):
    """Encontra a primeira linha com pelo menos min_filled células preenchidas."""
    for i, row in enumerate(rows[:15]):
        filled = sum(1 for c in row if str(c).strip())
        if filled >= min_filled:
            return i
    return 0


def rows_to_dicts(rows, header_idx):
    """Converte lista de listas em lista de dicts usando a linha de cabeçalho."""
    headers = [str(h).strip() for h in rows[header_idx]]
    result = []
    for row in rows[header_idx + 1:]:
        if not any(str(c).strip() for c in row):
            continue
        item = {}
        for i, h in enumerate(headers):
            key = h if h else f'col_{i+1}'
            item[key] = str(row[i]).strip() if i < len(row) else ''
        result.append(item)
    return result


def normalize_rows(raw_rows):
    """
    Normaliza os dicts brutos:
    - Mapeia colunas por sinônimos
    - Converte datas seriais do Excel para ISO
    - Normaliza valores monetários
    - Filtra linhas de controle (SALDO, TOTAL, etc.)
    """
    normalized = []
    skipped = 0
    skipped_invalid_value = 0

    for raw in raw_rows:
        entry = {}

        # Mapear colunas
        for orig_key, orig_val in raw.items():
            canon = map_column(orig_key)
            if canon and canon not in entry:
                entry[canon] = str(orig_val).strip()
            elif not canon:
                # Preservar colunas extras com prefixo extra_
                safe_key = re.sub(r'[^a-zA-Z0-9_]', '_', orig_key.lower())[:30]
                if safe_key:
                    entry[f'extra_{safe_key}'] = str(orig_val).strip()

        # Converter data
        raw_date = entry.get('data', '')
        iso_date, date_type = parse_date(raw_date)

        # Pular linhas de controle (SALDO, TOTAL, etc.)
        if date_type.startswith('controle:') or date_type.startswith('invalido:'):
            skipped += 1
            continue

        entry['data'] = iso_date or raw_date
        entry['dataISO'] = iso_date or ''
        entry['dataTipo'] = date_type

        # Converter valor
        raw_valor = entry.get('valor', '0')
        parsed_valor = parse_money(raw_valor)
        if parsed_valor is None:
            skipped += 1
            skipped_invalid_value += 1
            continue
        entry['valor'] = parsed_valor
        entry['valorOriginal'] = raw_valor

        # Normalizar D/C: se tiver coluna D/C, usar para determinar sinal do valor
        dc = entry.get('dc', '').upper().strip()
        if dc == 'D' and entry['valor'] > 0:
            entry['valor'] = -entry['valor']
        elif dc == 'C' and entry['valor'] < 0:
            entry['valor'] = -entry['valor']

        normalized.append(entry)

    return normalized, {'skipped': skipped, 'skippedInvalidValue': skipped_invalid_value}


def parse_xlsx(path, sheet_name=None):
    """Lê XLSX/XLSM e retorna linhas normalizadas."""
    with zipfile.ZipFile(path) as zf:
        workbook = ET.fromstring(zf.read('xl/workbook.xml'))
        sheets_el = workbook.find('m:sheets', NS)
        sheets = []
        for s in sheets_el.findall('m:sheet', NS):
            name = s.attrib.get('name', '')
            rel_id = s.attrib.get(
                '{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id', '')
            sheets.append({'name': name, 'rel_id': rel_id})

        rels_root = ET.fromstring(zf.read('xl/_rels/workbook.xml.rels'))
        rel_map = {}
        for rel in rels_root.findall('r:Relationship', REL_NS):
            rel_map[rel.attrib['Id']] = rel.attrib['Target']

        # Selecionar aba: por nome ou primeira
        selected = None
        if sheet_name:
            for s in sheets:
                if s['name'].lower() == sheet_name.lower():
                    selected = s
                    break
        if not selected:
            selected = sheets[0]

        target = rel_map.get(selected['rel_id'], '')
        if not target.startswith('xl/'):
            target = 'xl/' + target

        shared = read_shared_strings(zf)
        raw_rows = parse_xlsx_sheet(zf, target, shared)

        if len(raw_rows) < 2:
            return [], {'sheet': selected['name'], 'skipped': 0, 'skippedInvalidValue': 0, 'total_raw': 0}

        header_idx = find_header_row(raw_rows)
        dicts = rows_to_dicts(raw_rows, header_idx)
        normalized, stats = normalize_rows(dicts)

        meta = {
            'sheet': selected['name'],
            'total_raw': len(dicts),
            'skipped': stats['skipped'],
            'skippedInvalidValue': stats['skippedInvalidValue'],
            'imported': len(normalized),
        }
        return normalized, meta


def parse_csv(path):
    """Lê CSV e retorna linhas normalizadas."""
    with open(path, 'r', encoding='utf-8-sig') as f:
        sample = f.read()
    delimiter = ';' if sample.count(';') >= sample.count(',') else ','
    reader = csv.DictReader(io.StringIO(sample), delimiter=delimiter)
    raw_rows = []
    for row in reader:
        norm = {str(k).strip(): (str(v).strip() if v is not None else '')
                for k, v in row.items()}
        if any(norm.values()):
            raw_rows.append(norm)

    normalized, stats = normalize_rows(raw_rows)
    meta = {
        'sheet': 'CSV',
        'total_raw': len(raw_rows),
        'skipped': stats['skipped'],
        'skippedInvalidValue': stats['skippedInvalidValue'],
        'imported': len(normalized),
    }
    return normalized, meta


def main():
    if len(sys.argv) < 3:
        print(json.dumps({'error': 'Uso: parse_spreadsheet.py <path> <ext> [out_file]'}))
        sys.exit(1)

    path = sys.argv[1]
    ext = sys.argv[2].lower()
    # 3º argumento: arquivo de saída (evita ENOBUFS em planilhas grandes)
    out_file = sys.argv[3] if len(sys.argv) > 3 else None

    try:
        if ext in ('xlsx', 'xlsm'):
            rows, meta = parse_xlsx(path, None)
        elif ext == 'csv':
            rows, meta = parse_csv(path)
        else:
            raise RuntimeError(f'Extensão não suportada: {ext}')

        result = json.dumps({'rows': rows, 'meta': meta}, ensure_ascii=False)
        if out_file:
            with open(out_file, 'w', encoding='utf-8') as f:
                f.write(result)
        else:
            print(result)
    except Exception as e:
        err = json.dumps({'error': str(e)}, ensure_ascii=False)
        if out_file:
            with open(out_file, 'w', encoding='utf-8') as f:
                f.write(err)
        else:
            print(err)
        sys.exit(2)

if __name__ == '__main__':
    main()
