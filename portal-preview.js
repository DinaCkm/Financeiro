// Visualização de leitura (HTML) para Word .docx e Excel .xlsx no portal de entregas.
function escapeHtml(value) {
  return String(value == null ? '' : value)
    .replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}
function portalFileExt(fileName) {
  const m = String(fileName || '').toLowerCase().match(/\.([a-z0-9]+)$/);
  return m ? m[1] : '';
}
function portalCanPreviewOffice(fileName) {
  const ext = portalFileExt(fileName);
  return ext === 'docx' || ext === 'xlsx' || ext === 'xlsm';
}
// Converte Word (.docx) ou Excel (.xlsx) em uma página HTML de leitura.
async function portalOfficePreviewHtml(fileName, buffer) {
  const ext = portalFileExt(fileName);
  let body = '';
  if (ext === 'docx') {
    const mammoth = require('mammoth');
    const result = await mammoth.convertToHtml({ buffer });
    body = `<article class='doc'>${result.value || '<p>(Documento sem conteúdo de texto.)</p>'}</article>`;
  } else {
    const ExcelJS = require('exceljs');
    const wb = new ExcelJS.Workbook();
    await wb.xlsx.load(buffer);
    const parts = [];
    wb.eachSheet(ws => {
      if (ws.state && ws.state !== 'visible') return;
      const rows = [];
      const maxRow = Math.min(ws.rowCount || 0, 2000);
      const maxCol = Math.min(ws.columnCount || 0, 60);
      for (let r = 1; r <= maxRow; r++) {
        const row = ws.getRow(r);
        const cells = [];
        let hasValue = false;
        for (let c = 1; c <= maxCol; c++) {
          const cell = row.getCell(c);
          let text = '';
          try { text = cell.text == null ? '' : String(cell.text); } catch { text = ''; }
          if (text) hasValue = true;
          cells.push(`<td>${escapeHtml(text)}</td>`);
        }
        if (hasValue) rows.push(`<tr><th>${r}</th>${cells.join('')}</tr>`);
      }
      parts.push(`<h2>${escapeHtml(ws.name)}</h2>` + (rows.length
        ? `<div class='tbl'><table>${rows.join('')}</table></div>`
        : `<p class='muted'>(Aba sem dados.)</p>`));
    });
    body = parts.join('') || "<p class='muted'>(Planilha sem dados.)</p>";
  }
  return `<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
<title>${escapeHtml(fileName || 'Documento')}</title>
<style>
body{margin:0;background:#e2e8f0;font-family:Calibri,Arial,sans-serif;color:#0f172a}
.doc{max-width:820px;margin:20px auto;background:#fff;padding:56px 64px;box-shadow:0 2px 10px rgba(0,0,0,.08);line-height:1.55;font-size:15px}
.doc img{max-width:100%;height:auto}.doc table{border-collapse:collapse;width:100%;margin:10px 0}.doc td,.doc th{border:1px solid #cbd5e1;padding:6px 8px;vertical-align:top}
.doc h1,.doc h2,.doc h3{color:#1e1b4b;line-height:1.25}
h2{font-size:16px;margin:18px 16px 8px}.muted{color:#64748b;margin:0 16px}
.tbl{overflow:auto;margin:0 16px 16px;background:#fff;border:1px solid #cbd5e1}
.tbl table{border-collapse:collapse;font-size:13px}.tbl td,.tbl th{border:1px solid #e2e8f0;padding:4px 8px;white-space:nowrap}.tbl th{background:#f1f5f9;color:#64748b;font-weight:600}
@media(max-width:700px){.doc{margin:0;padding:24px 18px}}
</style></head><body>${body}</body></html>`;
}

function portalWantsView(req) {
  try { return new URL(req.url, 'http://x').searchParams.get('view') === '1'; } catch { return false; }
}
// Responde com a visualização HTML. Retorna true se respondeu.
async function portalSendOfficePreview(req, res, fileName, buffer) {
  if (!portalWantsView(req) || !portalCanPreviewOffice(fileName)) return false;
  try {
    const page = await portalOfficePreviewHtml(fileName, buffer);
    res.writeHead(200, {
      'Content-Type': 'text/html; charset=utf-8',
      'Cache-Control': 'private, no-store',
      'X-Content-Type-Options': 'nosniff',
      'Content-Security-Policy': "default-src 'none'; img-src data:; style-src 'unsafe-inline'"
    });
    res.end(page);
  } catch (e) {
    console.warn('[entregas] Falha ao gerar visualização:', e.message);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'private, no-store' });
    res.end("<!doctype html><meta charset='utf-8'><body style='font-family:Arial;padding:30px;color:#64748b'>Não foi possível gerar a visualização deste arquivo. Use o botão Baixar arquivo original.</body>");
  }
  return true;
}
// Iframe de visualização + botão de download do original.
function portalOfficeViewerHtml(src, viewSrc, style) {
  return `<iframe src='${viewSrc}' title='Visualização do documento' style='${style}'></iframe>
    <div style='margin-top:10px;display:flex;gap:12px;align-items:center;flex-wrap:wrap'>
      <a href='${src}' download style='display:inline-block;padding:9px 16px;border-radius:9px;background:#0f766e;color:#fff;font-weight:700;text-decoration:none'>Baixar arquivo original</a>
      <span style='font-size:12px;color:#64748b'>Visualização para leitura; a formatação exata está no arquivo original.</span>
    </div>`;
}
module.exports = { portalCanPreviewOffice, portalOfficePreviewHtml, portalSendOfficePreview, portalOfficeViewerHtml };
