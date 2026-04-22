#!/usr/bin/env python3
import csv
import io
import json
import sys
import zipfile
import xml.etree.ElementTree as ET

NS = {'m': 'http://schemas.openxmlformats.org/spreadsheetml/2006/main'}


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
            return ''
    return raw


def parse_xlsx(path):
    with zipfile.ZipFile(path) as zf:
        workbook = ET.fromstring(zf.read('xl/workbook.xml'))
        sheets = workbook.find('m:sheets', NS)
        first_sheet = sheets.find('m:sheet', NS)
        rel_id = first_sheet.attrib.get('{http://schemas.openxmlformats.org/officeDocument/2006/relationships}id')

        rels = ET.fromstring(zf.read('xl/_rels/workbook.xml.rels'))
        rel_ns = {'r': 'http://schemas.openxmlformats.org/package/2006/relationships'}
        target = None
        for rel in rels.findall('r:Relationship', rel_ns):
            if rel.attrib.get('Id') == rel_id:
                target = rel.attrib.get('Target')
                break
        if not target:
            raise RuntimeError('Não foi possível localizar a primeira planilha.')
        if not target.startswith('xl/'):
            target = 'xl/' + target

        shared = read_shared_strings(zf)
        ws = ET.fromstring(zf.read(target))
        data = ws.find('m:sheetData', NS)
        rows = []
        for row in data.findall('m:row', NS):
            max_col = 0
            row_data = {}
            for cell in row.findall('m:c', NS):
                ref = cell.attrib.get('r', '')
                col_letters = ''.join(ch for ch in ref if ch.isalpha())
                if not col_letters:
                    continue
                col_idx = 0
                for ch in col_letters:
                    col_idx = col_idx * 26 + (ord(ch.upper()) - 64)
                max_col = max(max_col, col_idx)
                row_data[col_idx] = cell_value(cell, shared)
            if max_col == 0:
                continue
            arr = [row_data.get(i, '') for i in range(1, max_col + 1)]
            rows.append(arr)

        if len(rows) < 2:
            return []
        headers = [str(h).strip() for h in rows[0]]
        result = []
        for r in rows[1:]:
            if not any(str(c).strip() for c in r):
                continue
            item = {}
            for i, h in enumerate(headers):
                key = h if h else f'col_{i+1}'
                item[key] = str(r[i]).strip() if i < len(r) else ''
            result.append(item)
        return result


def parse_csv(path):
    with open(path, 'r', encoding='utf-8-sig') as f:
        sample = f.read()
    delimiter = ';' if sample.count(';') >= sample.count(',') else ','
    reader = csv.DictReader(io.StringIO(sample), delimiter=delimiter)
    rows = []
    for row in reader:
        norm = {str(k).strip(): (str(v).strip() if v is not None else '') for k, v in row.items()}
        if any(norm.values()):
            rows.append(norm)
    return rows


def main():
    if len(sys.argv) != 3:
        print(json.dumps({'error': 'Uso: parse_spreadsheet.py <path> <ext>'}))
        sys.exit(1)

    path = sys.argv[1]
    ext = sys.argv[2].lower()

    try:
        if ext in ('xlsx', 'xlsm'):
            rows = parse_xlsx(path)
        elif ext == 'csv':
            rows = parse_csv(path)
        else:
            raise RuntimeError(f'Extensão não suportada: {ext}')

        print(json.dumps({'rows': rows}, ensure_ascii=False))
    except Exception as e:
        print(json.dumps({'error': str(e)}, ensure_ascii=False))
        sys.exit(2)


if __name__ == '__main__':
    main()
