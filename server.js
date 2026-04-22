const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const os = require('os');
const { spawnSync } = require('child_process');

const PORT = process.env.PORT || 3000;
const DB_PATH = path.join(__dirname, 'data', 'db.json');
const PARSER_PATH = path.join(__dirname, 'scripts', 'parse_spreadsheet.py');
const sessions = new Map();

const TYPE_OPTIONS = ['Cliente', 'Projeto', 'Prestador de Serviço', 'Fornecedor', 'Estrutura Interna', 'Financeiro / Não Operacional', 'Conta / Cartão', 'Pendente de Classificação'];

const OFFICIAL_CLIENTS = ['BRB', 'SEBRAE TO', 'SEBRAE-AC'];
const OFFICIAL_PROJECTS = ['BRB-PDL', 'SEBRAE 10º CICLO', 'SEBRAE 9º CICLO', 'PS SEBRAE 2022', 'CESAMA CARTA-CONTRATO 20/2023 ETAPA 4'];

const ALIAS_RULES = {
  BRB: 'BRB',
  'BRB-PDL': 'BRB',
  'METRÔ': 'METRÔ-SP',
  'METRÔ-SP': 'METRÔ-SP',
  PMSOROCABA: 'SOROCABA',
  SOROCABA: 'SOROCABA',
  'SEBRAE ACRE': 'SEBRAE-AC',
  'SEBRAE-AC': 'SEBRAE-AC'
};

const FORBIDDEN_AS_CLIENT = ['ESCRITÓRIO', 'SALÁRIOS', 'JURÍDICO', 'CONTÁBIL', 'TEF', 'MÚTUO', 'PRONAMPE', 'SALDO ATUAL'];

const COLUMN_ALIASES = {
  data: ['data', 'dt', 'date', 'data_movimento', 'data movimento', 'vencimento'],
  descricao: ['descricao', 'descrição', 'historico', 'histórico', 'description'],
  cliente: ['cliente', 'client', 'contratante'],
  projeto: ['projeto', 'project', 'contrato', 'frente'],
  parceiro: ['parceiro', 'prestador', 'fornecedor', 'beneficiario', 'beneficiário'],
  conta: ['conta', 'cartao', 'cartão', 'conta_cartao', 'conta/cartão'],
  detalhe: ['detalhe', 'categoria', 'detalhamento', 'observacao', 'observação'],
  valor: ['valor', 'amount', 'vlr', 'valor_total', 'valor total'],
  centroCusto: ['centro_custo', 'centro custo', 'cc', 'centrodecusto'],
  formaPagamento: ['forma_pagamento', 'forma pagamento', 'pagamento']
};

function loadDb() {
  return JSON.parse(fs.readFileSync(DB_PATH, 'utf8'));
}

function saveDb(db) {
  fs.writeFileSync(DB_PATH, JSON.stringify(db, null, 2));
}

function parseCookies(req) {
  const pairs = (req.headers.cookie || '').split(';').map((c) => c.trim()).filter(Boolean);
  return Object.fromEntries(pairs.map((p) => {
    const [k, ...rest] = p.split('=');
    return [k, decodeURIComponent(rest.join('='))];
  }));
}

function currentUser(req, db) {
  const sid = parseCookies(req).sid;
  if (!sid) return null;
  const userId = sessions.get(sid);
  return db.users.find((u) => u.id === userId) || null;
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk) => {
      data += chunk;
      if (data.length > 25 * 1024 * 1024) reject(new Error('Body too large'));
    });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

function json(res, status, payload) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

function normalizeName(name) {
  if (!name) return '';
  const n = String(name).trim().toUpperCase();
  return ALIAS_RULES[n] || n;
}

function inferType(name) {
  const n = normalizeName(name);
  if (!n) return 'Pendente de Classificação';
  if (FORBIDDEN_AS_CLIENT.includes(n)) return 'Estrutura Interna';
  if (n.includes('MÚTUO') || n.includes('PRONAMPE')) return 'Financeiro / Não Operacional';
  if (n.includes('CARTÃO') || n.includes('CARTAO') || n.includes('CARD')) return 'Conta / Cartão';
  if (OFFICIAL_PROJECTS.includes(n) || n.includes('CICLO') || n.includes('ETAPA') || n.includes('CONTRATO')) return 'Projeto';
  if (OFFICIAL_CLIENTS.includes(n)) return 'Cliente';
  return 'Pendente de Classificação';
}

function inferNature(entry) {
  const desc = normalizeName(entry.descricao || '');
  if (desc.includes('MÚTUO') || desc.includes('MUTUO')) return 'Movimentação Financeira Não Operacional';
  if (entry.tipo === 'entrada') return 'Receita Operacional';
  if (entry.tipo === 'saida' && entry.projeto) return 'Custo Direto do Projeto';
  if (entry.tipo === 'saida') return 'Despesa Indireta';
  return 'Pendente de Classificação';
}

function canonicalHeader(raw) {
  const key = String(raw || '').trim().toLowerCase().normalize('NFD').replace(/\p{Diacritic}/gu, '');
  for (const [canonical, aliases] of Object.entries(COLUMN_ALIASES)) {
    if (aliases.includes(key)) return canonical;
  }
  return key;
}

function normalizeRowHeaders(row) {
  const out = {};
  for (const [k, v] of Object.entries(row)) {
    out[canonicalHeader(k)] = String(v || '').trim();
  }
  return out;
}

function applySavedRulesToEntry(entry, db) {
  for (const rule of db.savedRules || []) {
    if (!rule.active) continue;
    if (rule.type === 'alias' && normalizeName(entry[rule.field]) === normalizeName(rule.matchValue)) {
      entry[rule.field] = normalizeName(rule.targetValue);
    }
    if (rule.type === 'project_client_link' && normalizeName(entry.projeto) === normalizeName(rule.projectName)) {
      entry.cliente = normalizeName(rule.clientName);
    }
    if (rule.type === 'entry_update' && normalizeName(entry[rule.field]) === normalizeName(rule.matchValue)) {
      Object.assign(entry, rule.updates || {});
    }
  }
}

function parseRowsWithPython(fileName, buffer) {
  const ext = path.extname(fileName).replace('.', '').toLowerCase();
  if (!['csv', 'xlsx', 'xlsm'].includes(ext)) throw new Error('Extensão não suportada. Use CSV, XLSX ou XLSM.');

  const tempPath = path.join(os.tmpdir(), `${crypto.randomUUID()}.${ext}`);
  fs.writeFileSync(tempPath, buffer);

  const proc = spawnSync('python3', [PARSER_PATH, tempPath, ext], { encoding: 'utf8' });
  fs.unlinkSync(tempPath);

  if (proc.error) throw proc.error;
  if (proc.status !== 0 && proc.status !== 2) throw new Error(proc.stderr || 'Falha ao executar parser de planilha.');

  const payload = JSON.parse(proc.stdout || '{}');
  if (payload.error) throw new Error(payload.error);
  return payload.rows || [];
}

function parseEntries(rows, uploadId, db) {
  return rows.map((raw) => {
    const r = normalizeRowHeaders(raw);
    const valor = Number(String(r.valor || '0').replace('.', '').replace(',', '.'));
    const entry = {
      id: crypto.randomUUID(),
      uploadId,
      data: r.data || '',
      descricao: r.descricao || '',
      cliente: normalizeName(r.cliente || ''),
      projeto: normalizeName(r.projeto || ''),
      parceiro: normalizeName(r.parceiro || ''),
      conta: r.conta || '',
      detalhe: r.detalhe || '',
      formaPagamento: r.formaPagamento || '',
      centroCusto: r.centroCusto || '',
      tipo: valor >= 0 ? 'entrada' : 'saida',
      valor,
      natureza: 'Pendente de Classificação',
      categoria: '',
      status: 'importado'
    };
    applySavedRulesToEntry(entry, db);
    entry.natureza = inferNature(entry);
    return entry;
  });
}

function buildIssues(entries) {
  const issues = [];
  for (const e of entries) {
    const desc = normalizeName(e.descricao);
    const client = normalizeName(e.cliente);
    const project = normalizeName(e.projeto);

    if (e.natureza === 'Custo Direto do Projeto' && !project) issues.push({ entryId: e.id, level: 'erro', code: 'DESPESA_SEM_PROJETO', message: 'Despesa direta sem projeto.' });
    if (client && inferType(client) !== 'Cliente' && inferType(client) !== 'Projeto') issues.push({ entryId: e.id, level: 'erro', code: 'CLIENTE_INVALIDO', message: 'Nome lançado no cliente não é cliente válido.' });
    if (desc.includes('MÚTUO') && e.natureza !== 'Movimentação Financeira Não Operacional') issues.push({ entryId: e.id, level: 'erro', code: 'MUTUO_CLASSIFICACAO', message: 'Mútuo classificado incorretamente.' });
    if (!e.data || Number.isNaN(new Date(e.data).getTime())) issues.push({ entryId: e.id, level: 'erro', code: 'DATA_INVALIDA', message: 'Data inválida.' });
    if (!Number.isFinite(e.valor)) issues.push({ entryId: e.id, level: 'erro', code: 'VALOR_INVALIDO', message: 'Valor inválido.' });
    if (inferType(client) === 'Pendente de Classificação') issues.push({ entryId: e.id, level: 'alerta', code: 'NOME_FORA_PADRAO', message: 'Cliente/projeto fora do padrão conhecido.' });
    if (e.conta && e.conta.toUpperCase().includes('CARTAO') && !e.detalhe) issues.push({ entryId: e.id, level: 'alerta', code: 'CARTAO_SEM_DETALHE', message: 'Cartão sem detalhamento completo.' });
  }
  return issues;
}

function buildReviewRegistry(entries) {
  const map = new Map();
  for (const e of entries) {
    [e.cliente, e.projeto, e.parceiro].forEach((name) => {
      if (!name) return;
      const key = normalizeName(name);
      if (map.has(key)) return;
      const type = inferType(name);
      map.set(key, {
        id: crypto.randomUUID(),
        nomeOriginal: name,
        nomeOficial: key,
        tipoSugerido: type,
        tipoFinal: type,
        clienteVinculado: type === 'Projeto' ? normalizeName(e.cliente) : '',
        projetoVinculado: type === 'Projeto' ? key : '',
        manterAlias: true,
        observacao: '',
        statusRevisao: 'pendente'
      });
    });
  }
  return [...map.values()];
}

function mergeRegistry(existing, incoming) {
  const map = new Map(existing.map((r) => [normalizeName(r.nomeOficial), r]));
  for (const item of incoming) {
    const key = normalizeName(item.nomeOficial);
    if (!map.has(key)) map.set(key, item);
  }
  return [...map.values()];
}

function requireAuth(req, res, db) {
  const user = currentUser(req, db);
  if (!user) {
    json(res, 401, { error: 'Não autenticado' });
    return null;
  }
  return user;
}

function serveStatic(req, res) {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const file = path.join(__dirname, 'public', url.pathname.replace('/public/', ''));
  if (!file.startsWith(path.join(__dirname, 'public')) || !fs.existsSync(file)) return false;
  const ext = path.extname(file);
  const type = ext === '.css' ? 'text/css' : 'text/plain';
  res.writeHead(200, { 'Content-Type': type });
  res.end(fs.readFileSync(file));
  return true;
}

function page(title, body, user) {
  return `<!doctype html><html><head><meta charset='utf-8'><title>${title}</title><link rel='stylesheet' href='/public/style.css'></head><body>
<header><h1>Painel Financeiro Gerencial CKM</h1>${user ? `<nav><a href='/'>Home</a><a href='/upload'>Upload</a><a href='/pendencias'>Pendências</a><a href='/cadastros'>Cadastro Revisável</a><a href='/dashboard'>Dashboard</a><a href='/logout'>Sair</a></nav>` : ''}</header>
<main>${body}</main></body></html>`;
}

function reviewTableRows(list) {
  return list.map((r) => `<tr>
<td>${r.nomeOriginal}</td><td>${r.nomeOficial}</td><td>${r.tipoSugerido}</td>
<td><select onchange="alterarTipo('${r.id}', this.value)">${TYPE_OPTIONS.map((t) => `<option ${t === r.tipoFinal ? 'selected' : ''}>${t}</option>`).join('')}</select></td>
<td><input value='${r.clienteVinculado || ''}' onchange="vincularCliente('${r.id}', this.value)"/></td>
<td><input value='${r.projetoVinculado || ''}' onchange="vincularProjeto('${r.id}', this.value)"/></td>
<td>${r.statusRevisao}</td></tr>`).join('');
}

const server = http.createServer(async (req, res) => {
  const db = loadDb();
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (req.url.startsWith('/public/') && serveStatic(req, res)) return;

  if (req.method === 'GET' && url.pathname === '/login') {
    const html = page('Login', `<section><h2>Login</h2><form method='post' action='/login'><label>E-mail <input name='email'></label><label>Senha <input type='password' name='password'></label><button>Entrar</button></form><p>Usuário: owner@ckm.local / 123456</p></section>`);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'POST' && url.pathname === '/login') {
    const form = new URLSearchParams(await readBody(req));
    const user = db.users.find((u) => u.email === form.get('email') && u.password === form.get('password'));
    if (!user) {
      res.writeHead(302, { Location: '/login' });
      res.end();
      return;
    }
    const sid = crypto.randomUUID();
    sessions.set(sid, user.id);
    res.writeHead(302, { 'Set-Cookie': `sid=${sid}; Path=/; HttpOnly`, Location: '/' });
    res.end();
    return;
  }

  if (req.method === 'GET' && url.pathname === '/logout') {
    sessions.delete(parseCookies(req).sid);
    res.writeHead(302, { 'Set-Cookie': 'sid=; Path=/; Max-Age=0', Location: '/login' });
    res.end();
    return;
  }

  const user = currentUser(req, db);
  if (!user) {
    res.writeHead(302, { Location: '/login' });
    res.end();
    return;
  }

  if (req.method === 'GET' && url.pathname === '/') {
    const saldo = db.entries.reduce((acc, cur) => acc + cur.valor, 0);
    const pendencias = db.issues.filter((i) => i.level === 'erro' && i.status === 'aberta').length;
    const html = page('Home', `<section><h2>Resumo</h2><ul><li>Saldo atual: R$ ${saldo.toFixed(2)}</li><li>Pendências obrigatórias: ${pendencias}</li><li>Cadastros pendentes: ${db.reviewRegistry.filter((r) => r.statusRevisao !== 'revisado').length}</li></ul></section>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/upload') {
    const html = page('Upload', `<section>
<h2>Upload de planilha (CSV, XLSX, XLSM)</h2>
<p>Fluxo priorizado para compatibilização com planilha operacional real CKM.</p>
<input type='file' id='file' accept='.csv,.xlsx,.xlsm' />
<button onclick='enviarArquivo()'>Importar</button>
<pre id='out'></pre>
</section>
<script>
async function enviarArquivo(){
  const f = document.getElementById('file').files[0];
  if(!f){ alert('Selecione um arquivo.'); return; }
  const buf = await f.arrayBuffer();
  const bytes = new Uint8Array(buf);
  let binary = '';
  bytes.forEach(b => binary += String.fromCharCode(b));
  const base64 = btoa(binary);
  const r = await fetch('/api/upload',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({fileName:f.name,fileBase64:base64})});
  document.getElementById('out').textContent = JSON.stringify(await r.json(), null, 2);
}
</script>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/upload') {
    if (!requireAuth(req, res, db)) return;
    try {
      const body = JSON.parse(await readBody(req) || '{}');
      const fileName = body.fileName || 'upload.csv';
      const buffer = Buffer.from(body.fileBase64 || '', 'base64');
      const rows = parseRowsWithPython(fileName, buffer);

      const upload = { id: crypto.randomUUID(), fileName, uploadedAt: new Date().toISOString(), rowCount: rows.length };
      const entries = parseEntries(rows, upload.id, db);
      const issues = buildIssues(entries).map((i) => ({ ...i, id: crypto.randomUUID(), uploadId: upload.id, status: 'aberta' }));
      const registry = buildReviewRegistry(entries);

      db.uploads.push(upload);
      db.entries.push(...entries);
      db.issues.push(...issues);
      db.reviewRegistry = mergeRegistry(db.reviewRegistry, registry);
      saveDb(db);

      json(res, 200, {
        uploadId: upload.id,
        fileName,
        importedRows: entries.length,
        foundNames: registry.length,
        pendingErrors: issues.filter((i) => i.level === 'erro').length,
        alerts: issues.filter((i) => i.level === 'alerta').length
      });
    } catch (error) {
      json(res, 400, { error: error.message });
    }
    return;
  }

  if (req.method === 'GET' && url.pathname === '/pendencias') {
    const errItems = db.issues.filter((i) => i.level === 'erro');
    const alertItems = db.issues.filter((i) => i.level === 'alerta');
    const html = page('Pendências', `<section><h2>Pendências obrigatórias</h2><ul>${errItems.map((e) => `<li>${e.code}: ${e.message}</li>`).join('')}</ul><h2>Alertas</h2><ul>${alertItems.map((a) => `<li>${a.code}: ${a.message}</li>`).join('')}</ul></section>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/cadastros') {
    const html = page('Cadastro Revisável', `<section><h2>Cadastro Revisável (centro do fluxo)</h2>
<p>Consolide alias, altere tipo, vincule projeto/cliente e salve regras futuras.</p>
<div class='grid2'>
<div><h3>Consolidar alias</h3><input id='aliasFrom' placeholder='Nome origem'/><input id='aliasTo' placeholder='Nome oficial'/><label><input id='keepAlias' type='checkbox' checked/> manter alias</label><button onclick='consolidarAlias()'>Consolidar + regra</button></div>
<div><h3>Vincular projeto a cliente</h3><input id='projectName' placeholder='Projeto'/><input id='clientName' placeholder='Cliente'/><button onclick='vincularProjetoCliente()'>Vincular + regra</button></div>
</div>
<table><thead><tr><th>Original</th><th>Oficial</th><th>Sugerido</th><th>Tipo Final</th><th>Cliente</th><th>Projeto</th><th>Status</th></tr></thead><tbody>${reviewTableRows(db.reviewRegistry)}</tbody></table>
</section>
<script>
async function alterarTipo(id,tipoFinal){await fetch('/api/review/'+id,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({tipoFinal,statusRevisao:'revisado'})});}
async function vincularCliente(id,clienteVinculado){await fetch('/api/review/'+id,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({clienteVinculado,statusRevisao:'revisado'})});}
async function vincularProjeto(id,projetoVinculado){await fetch('/api/review/'+id,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({projetoVinculado,statusRevisao:'revisado'})});}
async function consolidarAlias(){const sourceName=document.getElementById('aliasFrom').value;const targetName=document.getElementById('aliasTo').value;const keepAlias=document.getElementById('keepAlias').checked;await fetch('/api/review/consolidate',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({sourceName,targetName,keepAlias,applyRule:true})});location.reload();}
async function vincularProjetoCliente(){const projectName=document.getElementById('projectName').value;const clientName=document.getElementById('clientName').value;await fetch('/api/review/link-project',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({projectName,clientName,applyRule:true})});location.reload();}
</script>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'PATCH' && url.pathname.startsWith('/api/review/')) {
    const id = url.pathname.split('/').pop();
    const item = db.reviewRegistry.find((r) => r.id === id);
    if (!item) return json(res, 404, { error: 'Registro não encontrado' });
    Object.assign(item, JSON.parse(await readBody(req) || '{}'));
    saveDb(db);
    return json(res, 200, item);
  }

  if (req.method === 'POST' && url.pathname === '/api/review/consolidate') {
    const { sourceName, targetName, keepAlias, applyRule } = JSON.parse(await readBody(req) || '{}');
    const source = normalizeName(sourceName);
    const target = normalizeName(targetName);
    db.reviewRegistry.forEach((r) => {
      if (normalizeName(r.nomeOficial) === source) {
        r.nomeOficial = target;
        r.statusRevisao = 'revisado';
        r.manterAlias = !!keepAlias;
      }
    });
    db.entries.forEach((e) => {
      ['cliente', 'projeto', 'parceiro'].forEach((f) => {
        if (normalizeName(e[f]) === source) e[f] = target;
      });
    });
    if (applyRule) {
      db.savedRules.push({ id: crypto.randomUUID(), type: 'alias', field: 'cliente', matchValue: source, targetValue: target, active: true });
      db.savedRules.push({ id: crypto.randomUUID(), type: 'alias', field: 'projeto', matchValue: source, targetValue: target, active: true });
      db.savedRules.push({ id: crypto.randomUUID(), type: 'alias', field: 'parceiro', matchValue: source, targetValue: target, active: true });
    }
    saveDb(db);
    return json(res, 200, { ok: true });
  }

  if (req.method === 'POST' && url.pathname === '/api/review/link-project') {
    const { projectName, clientName, applyRule } = JSON.parse(await readBody(req) || '{}');
    const project = normalizeName(projectName);
    const client = normalizeName(clientName);
    db.reviewRegistry.forEach((r) => {
      if (normalizeName(r.nomeOficial) === project) {
        r.tipoFinal = 'Projeto';
        r.projetoVinculado = project;
        r.clienteVinculado = client;
        r.statusRevisao = 'revisado';
      }
    });
    db.entries.forEach((e) => {
      if (normalizeName(e.projeto) === project) e.cliente = client;
    });
    if (applyRule) db.savedRules.push({ id: crypto.randomUUID(), type: 'project_client_link', projectName: project, clientName: client, active: true });
    saveDb(db);
    return json(res, 200, { ok: true });
  }

  if (req.method === 'POST' && url.pathname === '/api/review/save-rule') {
    const body = JSON.parse(await readBody(req) || '{}');
    db.savedRules.push({ id: crypto.randomUUID(), type: 'entry_update', active: true, ...body });
    saveDb(db);
    return json(res, 201, { ok: true });
  }

  if (req.method === 'PATCH' && url.pathname.startsWith('/api/entries/')) {
    const id = url.pathname.split('/').pop();
    const entry = db.entries.find((e) => e.id === id);
    if (!entry) return json(res, 404, { error: 'Lançamento não encontrado' });
    const changes = JSON.parse(await readBody(req) || '{}');
    const editable = ['cliente', 'projeto', 'natureza', 'centroCusto', 'parceiro', 'categoria', 'detalhe', 'conta', 'formaPagamento', 'status'];
    editable.forEach((k) => { if (changes[k] !== undefined) entry[k] = changes[k]; });
    saveDb(db);
    return json(res, 200, entry);
  }

  if (req.method === 'GET' && url.pathname === '/dashboard') {
    const byClient = {};
    const byProject = {};
    db.entries.forEach((e) => {
      const c = e.cliente || 'SEM CLIENTE';
      const p = e.projeto || 'SEM PROJETO';
      byClient[c] = (byClient[c] || 0) + e.valor;
      byProject[p] = (byProject[p] || 0) + e.valor;
    });

    const html = page('Dashboard', `<section><h2>Resultado por cliente</h2><ul>${Object.entries(byClient).map(([k, v]) => `<li>${k}: R$ ${v.toFixed(2)}</li>`).join('')}</ul><h2>Resultado por projeto</h2><ul>${Object.entries(byProject).map(([k, v]) => `<li>${k}: R$ ${v.toFixed(2)}</li>`).join('')}</ul></section>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

server.listen(PORT, () => {
  console.log(`CKM MVP running at http://localhost:${PORT}`);
});
