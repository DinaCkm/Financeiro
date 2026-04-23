const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const os = require('os');
const { spawnSync } = require('child_process');
const { createStorage } = require('./storage');

const PORT = process.env.PORT || 3000;
const PARSER_PATH = path.join(__dirname, 'scripts', 'parse_spreadsheet.py');
const sessions = new Map();
const storage = createStorage({ databaseUrl: process.env.DATABASE_URL });

const TYPE_OPTIONS = ['Cliente', 'Projeto', 'Prestador de Serviço', 'Fornecedor', 'Estrutura Interna', 'Financeiro / Não Operacional', 'Conta / Cartão', 'Pendente de Classificação'];
const BLOCKING_ISSUES = ['DESPESA_SEM_PROJETO', 'ESTRUTURA_COMO_CLIENTE', 'MUTUO_COMO_CLIENTE', 'MUTUO_CLASSIFICACAO', 'VALOR_INVALIDO', 'DATA_INVALIDA'];

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
  formaPagamento: ['forma_pagamento', 'forma pagamento', 'pagamento'],
  dc: ['d/c', 'dc', 'd-c', 'debito_credito', 'debito/credito', 'débito/crédito'],
  tipoOriginal: ['tipo', 'tp-despesa', 'tp despesa', 'tipo_despesa'],
  detDespesa: ['det-despesa', 'det despesa', 'detalhe despesa', 'det_despesa'],
  statusPlanilha: ['status', 'status_planilha']
};

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

function parseDateValue(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;
  if (/^\d{4}-\d{2}-\d{2}$/.test(raw)) {
    return new Date(`${raw}T00:00:00Z`);
  }
  if (/^\d{5}(\.\d+)?$/.test(raw)) {
    const excelSerial = Number(raw);
    if (Number.isFinite(excelSerial)) {
      const epoch = new Date(Date.UTC(1899, 11, 30));
      epoch.setUTCDate(epoch.getUTCDate() + Math.floor(excelSerial));
      return epoch;
    }
  }

  const br = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (br) {
    const dd = Number(br[1]);
    const mm = Number(br[2]);
    const yy = Number(br[3].length === 2 ? `20${br[3]}` : br[3]);
    return new Date(Date.UTC(yy, mm - 1, dd));
  }

  const iso = new Date(raw);
  if (!Number.isNaN(iso.getTime())) {
    return new Date(Date.UTC(iso.getUTCFullYear(), iso.getUTCMonth(), iso.getUTCDate()));
  }

  return null;
}

function inferType(name) {
  const n = normalizeName(name);
  if (!n) return 'Pendente de Classificação';
  if (FORBIDDEN_AS_CLIENT.includes(n)) return 'Estrutura Interna';
  if (n.includes('MÚTUO') || n.includes('MUTUO') || n.includes('PRONAMPE')) return 'Financeiro / Não Operacional';
  if (n.includes('CARTÃO') || n.includes('CARTAO') || n.includes('CARD')) return 'Conta / Cartão';
  if (OFFICIAL_PROJECTS.includes(n) || n.includes('CICLO') || n.includes('ETAPA') || n.includes('CONTRATO') || n.includes('CARTA-CONTRATO') || n.includes('PDL')) return 'Projeto';
  if (OFFICIAL_CLIENTS.includes(n)) return 'Cliente';
  return 'Pendente de Classificação';
}

function inferNature(entry) {
  const desc = normalizeName(entry.descricao || '');
  const centro = normalizeName(entry.centroCusto || '');
  const tipoOriginal = normalizeName(entry.tipoOriginal || '');
  if (desc.includes('MÚTUO') || desc.includes('MUTUO') || tipoOriginal.includes('MÚTUO') || tipoOriginal.includes('MUTUO')) return 'Movimentação Financeira Não Operacional';
  if (entry.tipo === 'entrada') return 'Receita Operacional';
  if (centro.includes('FINANC') || tipoOriginal.includes('JUROS') || tipoOriginal.includes('TARIFA')) return 'Despesa Financeira';
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

function normalizeMoney(value) {
  const raw = String(value || '0').trim();
  if (!raw) return 0;
  const cleaned = raw.replace(/[R$\s]/g, '');
  const normalized = cleaned.includes(',') ? cleaned.replace(/\./g, '').replace(',', '.') : cleaned;
  return Number(normalized);
}

function applySavedRulesToEntry(entry, db) {
  const rules = (db.savedRules || []).filter((rule) => rule.active);
  for (const rule of rules) {
    if (rule.type === 'alias') {
      for (const field of ['cliente', 'projeto', 'parceiro']) {
        if (normalizeName(entry[field]) === normalizeName(rule.matchValue)) {
          entry[field] = normalizeName(rule.targetValue);
        }
      }
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
    if (/^(SALDO|TOTAL)/i.test(r.descricao || '')) return null;
    let valor = normalizeMoney(r.valor);
    const dc = normalizeName(r.dc || '');
    if (dc === 'D' && valor > 0) valor *= -1;
    if (dc === 'C' && valor < 0) valor *= -1;
    const parsedDate = parseDateValue(r.data);
    const entry = {
      id: crypto.randomUUID(),
      uploadId,
      data: r.data || '',
      dataISO: parsedDate ? parsedDate.toISOString().slice(0, 10) : '',
      descricao: r.descricao || '',
      cliente: normalizeName(r.cliente || ''),
      projeto: normalizeName(r.projeto || ''),
      parceiro: normalizeName(r.parceiro || ''),
      conta: r.conta || '',
      detalhe: r.detalhe || '',
      formaPagamento: r.formaPagamento || '',
      centroCusto: r.centroCusto || '',
      dc,
      tipoOriginal: r.tipoOriginal || '',
      detDespesa: r.detDespesa || '',
      statusPlanilha: normalizeName(r.statusPlanilha || ''),
      tipo: valor >= 0 ? 'entrada' : 'saida',
      valor,
      natureza: 'Pendente de Classificação',
      categoria: '',
      status: 'importado'
    };
    applySavedRulesToEntry(entry, db);
    entry.natureza = inferNature(entry);
    return entry;
  }).filter(Boolean);
}

function buildIssues(entries, db) {
  const issues = [];
  const newNames = new Set();
  const knownNames = new Set((db.reviewRegistry || []).map((item) => normalizeName(item.nomeOficial)));

  for (const e of entries) {
    const desc = normalizeName(e.descricao);
    const client = normalizeName(e.cliente);
    const project = normalizeName(e.projeto);

    if (e.natureza === 'Custo Direto do Projeto' && !project) issues.push({ entryId: e.id, level: 'erro', code: 'DESPESA_SEM_PROJETO', message: 'Despesa direta sem projeto.', blocking: true });
    if (client && inferType(client) === 'Estrutura Interna') issues.push({ entryId: e.id, level: 'erro', code: 'ESTRUTURA_COMO_CLIENTE', message: 'Estrutura interna lançada como cliente.', blocking: true });
    if (client && client.includes('MÚTUO')) issues.push({ entryId: e.id, level: 'erro', code: 'MUTUO_COMO_CLIENTE', message: 'Mútuo lançado como cliente.', blocking: true });
    if (desc.includes('MÚTUO') && e.natureza !== 'Movimentação Financeira Não Operacional') issues.push({ entryId: e.id, level: 'erro', code: 'MUTUO_CLASSIFICACAO', message: 'Mútuo classificado incorretamente.', blocking: true });
    if (!e.dataISO) issues.push({ entryId: e.id, level: 'erro', code: 'DATA_INVALIDA', message: 'Data inválida.', blocking: true });
    if (!Number.isFinite(e.valor)) issues.push({ entryId: e.id, level: 'erro', code: 'VALOR_INVALIDO', message: 'Valor inválido.', blocking: true });

    if (e.tipo === 'entrada' && !client && !normalizeName(e.parceiro)) issues.push({ entryId: e.id, level: 'alerta', code: 'RECEITA_SEM_CLIENTE', message: 'Receita sem cliente/parceiro identificado.', blocking: false });
    if (e.statusPlanilha === 'ZZ' && Math.abs(e.valor) > 0) issues.push({ entryId: e.id, level: 'alerta', code: 'CANCELADO_COM_VALOR', message: 'Lançamento cancelado (ZZ) com valor informado.', blocking: false });
    if (client && inferType(client) === 'Pendente de Classificação') issues.push({ entryId: e.id, level: 'alerta', code: 'NOME_FORA_PADRAO', message: 'Cliente/projeto fora do padrão conhecido.', blocking: false });
    if (e.conta && normalizeName(e.conta).includes('CARTAO') && !e.detalhe) issues.push({ entryId: e.id, level: 'alerta', code: 'CARTAO_SEM_DETALHE', message: 'Cartão sem detalhamento completo.', blocking: false });

    [client, project, normalizeName(e.parceiro)].forEach((name) => {
      if (name && !knownNames.has(name)) newNames.add(name);
    });
  }

  for (const name of newNames) {
    issues.push({ entryId: null, level: 'alerta', code: 'NOVO_CADASTRO', message: `Novo cadastro identificado: ${name}.`, blocking: false });
  }

  const aliasTargetMap = new Map();
  for (const rule of db.savedRules || []) {
    if (rule.type !== 'alias' || !rule.active) continue;
    const source = normalizeName(rule.matchValue);
    const target = normalizeName(rule.targetValue);
    if (!aliasTargetMap.has(source)) aliasTargetMap.set(source, new Set());
    aliasTargetMap.get(source).add(target);
  }
  aliasTargetMap.forEach((targets, source) => {
    if (targets.size > 1) {
      issues.push({ entryId: null, level: 'alerta', code: 'CONFLITO_ALIAS', message: `Conflito de alias para ${source}: ${[...targets].join(', ')}`, blocking: false });
    }
  });

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
<header><h1>Painel Financeiro Gerencial CKM</h1>${user ? `<nav><a href='/'>Home</a><a href='/upload'>Upload</a><a href='/pendencias'>Pré-análise</a><a href='/cadastros'>Cadastro Revisável</a><a href='/dashboard'>Dashboard</a><a href='/logout'>Sair</a></nav>` : ''}</header>
<main>${body}</main></body></html>`;
}

function reviewTableRows(list) {
  return list.map((r) => `<tr data-id='${r.id}'>
<td>${r.nomeOriginal}</td><td>${r.nomeOficial}</td><td>${r.tipoSugerido}</td>
<td><select onchange="alterarTipo('${r.id}', this.value)">${TYPE_OPTIONS.map((t) => `<option ${t === r.tipoFinal ? 'selected' : ''}>${t}</option>`).join('')}</select></td>
<td><input value='${r.clienteVinculado || ''}' onchange="vincularCliente('${r.id}', this.value)"/></td>
<td><input value='${r.projetoVinculado || ''}' onchange="vincularProjeto('${r.id}', this.value)"/></td>
<td><select onchange="marcarRevisao('${r.id}', this.value)"><option ${r.statusRevisao === 'pendente' ? 'selected' : ''}>pendente</option><option ${r.statusRevisao === 'revisado' ? 'selected' : ''}>revisado</option></select></td></tr>`).join('');
}

function buildPreAnalysisSummary(db) {
  const openIssues = db.issues.filter((i) => i.status !== 'resolvida');
  const count = (code) => openIssues.filter((i) => i.code === code).length;
  return {
    semProjeto: count('DESPESA_SEM_PROJETO'),
    estruturaComoCliente: count('ESTRUTURA_COMO_CLIENTE'),
    mutuoIncorreto: count('MUTUO_COMO_CLIENTE') + count('MUTUO_CLASSIFICACAO'),
    novosCadastros: count('NOVO_CADASTRO'),
    conflitosAlias: count('CONFLITO_ALIAS'),
    receitaSemCliente: count('RECEITA_SEM_CLIENTE'),
    canceladoComValor: count('CANCELADO_COM_VALOR'),
    bloqueantes: openIssues.filter((i) => i.blocking || BLOCKING_ISSUES.includes(i.code)).length
  };
}

function calculateDashboard(db) {
  const today = new Date().toISOString().slice(0, 10);
  const addDays = (date, n) => {
    const d = new Date(`${date}T00:00:00Z`);
    d.setUTCDate(d.getUTCDate() + n);
    return d.toISOString().slice(0, 10);
  };
  const d7 = addDays(today, 7);
  const d30 = addDays(today, 30);

  const sortedEntries = [...db.entries].sort((a, b) => (a.dataISO || '').localeCompare(b.dataISO || ''));
  let rolling = 0;
  for (const e of sortedEntries) {
    rolling += e.valor;
  }
  const saldoHoje = sortedEntries.filter((e) => e.dataISO <= today).reduce((acc, e) => acc + e.valor, 0);
  const proj7 = sortedEntries.filter((e) => e.dataISO <= d7).reduce((acc, e) => acc + e.valor, 0);
  const proj30 = sortedEntries.filter((e) => e.dataISO <= d30).reduce((acc, e) => acc + e.valor, 0);
  const contasPagar = sortedEntries.filter((e) => e.dataISO > today && e.valor < 0).reduce((acc, e) => acc + Math.abs(e.valor), 0);
  const contasReceber = sortedEntries.filter((e) => e.dataISO > today && e.valor > 0).reduce((acc, e) => acc + e.valor, 0);
  const upcoming7 = sortedEntries.filter((e) => e.dataISO > today && e.dataISO <= d7);
  const riscoCaixa = proj7 < 0 ? 'alto' : proj30 < 0 ? 'moderado' : 'controlado';

  const byClient = {};
  const byProject = {};
  db.entries.forEach((e) => {
    const c = e.cliente || 'SEM CLIENTE';
    const p = e.projeto || 'SEM PROJETO';
    byClient[c] = (byClient[c] || 0) + e.valor;
    byProject[p] = (byProject[p] || 0) + e.valor;
  });

  return { saldoHoje, proj7, proj30, contasPagar, contasReceber, byClient, byProject, rolling, upcoming7, riscoCaixa };
}

const server = http.createServer(async (req, res) => {
  const db = await storage.loadDb();
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
    const summary = buildPreAnalysisSummary(db);
    const metrics = calculateDashboard(db);
    const html = page('Home', `<section><h2>Resumo diário</h2><ul>
<li>Saldo de hoje: R$ ${metrics.saldoHoje.toFixed(2)}</li>
<li>Projeção em 7 dias: R$ ${metrics.proj7.toFixed(2)}</li>
<li>Projeção em 30 dias: R$ ${metrics.proj30.toFixed(2)}</li>
<li>Contas a pagar: R$ ${metrics.contasPagar.toFixed(2)}</li>
<li>Contas a receber: R$ ${metrics.contasReceber.toFixed(2)}</li>
<li>Pendências bloqueantes: ${summary.bloqueantes}</li>
<li>Cadastros pendentes: ${db.reviewRegistry.filter((r) => r.statusRevisao !== 'revisado').length}</li>
</ul></section>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/upload') {
    const html = page('Upload', `<section>
<h2>Upload de planilha (CSV, XLSX, XLSM)</h2>
<p>Compatível com planilha operacional CKM (primeira aba para XLSX/XLSM).</p>
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
      const issues = buildIssues(entries, db).map((i) => ({ ...i, id: crypto.randomUUID(), uploadId: upload.id, status: 'aberta' }));
      const registry = buildReviewRegistry(entries);

      db.uploads.push(upload);
      db.entries.push(...entries);
      db.issues.push(...issues);
      db.reviewRegistry = mergeRegistry(db.reviewRegistry, registry);
      await storage.saveDb(db);

      const summary = buildPreAnalysisSummary(db);
      json(res, 200, {
        uploadId: upload.id,
        fileName,
        importedRows: entries.length,
        foundNames: registry.length,
        pendingErrors: issues.filter((i) => i.level === 'erro').length,
        alerts: issues.filter((i) => i.level === 'alerta').length,
        blockingIssues: summary.bloqueantes
      });
    } catch (error) {
      json(res, 400, { error: error.message });
    }
    return;
  }

  if (req.method === 'GET' && url.pathname === '/pendencias') {
    const openIssues = db.issues.filter((i) => i.status !== 'resolvida');
    const summary = buildPreAnalysisSummary(db);
    const groups = [
      { title: 'Despesas sem projeto', code: 'DESPESA_SEM_PROJETO' },
      { title: 'Estrutura lançada como cliente', code: 'ESTRUTURA_COMO_CLIENTE' },
      { title: 'Mútuos classificados incorretamente', code: 'MUTUO_CLASSIFICACAO' },
      { title: 'Mútuos lançados como cliente', code: 'MUTUO_COMO_CLIENTE' },
      { title: 'Novos cadastros', code: 'NOVO_CADASTRO' },
      { title: 'Conflitos de alias', code: 'CONFLITO_ALIAS' },
      { title: 'Receita sem cliente/parceiro', code: 'RECEITA_SEM_CLIENTE' },
      { title: 'Cancelado (ZZ) com valor', code: 'CANCELADO_COM_VALOR' }
    ];

    const html = page('Pré-análise', `<section><h2>Pré-análise operacional</h2>
<div class='cards'>
<div class='card'><strong>Bloqueantes</strong><span>${summary.bloqueantes}</span></div>
<div class='card'><strong>Sem projeto</strong><span>${summary.semProjeto}</span></div>
<div class='card'><strong>Estrutura como cliente</strong><span>${summary.estruturaComoCliente}</span></div>
<div class='card'><strong>Mútuo incorreto</strong><span>${summary.mutuoIncorreto}</span></div>
<div class='card'><strong>Novos cadastros</strong><span>${summary.novosCadastros}</span></div>
<div class='card'><strong>Conflito de alias</strong><span>${summary.conflitosAlias}</span></div>
<div class='card'><strong>Receita sem cliente</strong><span>${summary.receitaSemCliente}</span></div>
<div class='card'><strong>ZZ com valor</strong><span>${summary.canceladoComValor}</span></div>
</div>
${groups.map((g) => `<h3>${g.title}</h3><ul>${openIssues.filter((i) => i.code === g.code).map((e) => `<li>${e.message}${e.blocking ? ' (bloqueante)' : ''}</li>`).join('') || '<li>Sem itens</li>'}</ul>`).join('')}
<h3>Outras pendências</h3>
<ul>${openIssues.filter((i) => !groups.some((g) => g.code === i.code)).map((e) => `<li>${e.code}: ${e.message}${e.blocking ? ' (bloqueante)' : ''}</li>`).join('') || '<li>Sem itens</li>'}</ul>
</section>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/cadastros') {
    const statusFilter = (url.searchParams.get('status') || 'pendente').trim();
    const typeFilter = (url.searchParams.get('tipo') || '').trim();
    const textFilter = normalizeName(url.searchParams.get('q') || '');
    const filtered = db.reviewRegistry.filter((item) => {
      if (statusFilter && statusFilter !== 'todos' && item.statusRevisao !== statusFilter) return false;
      if (typeFilter && item.tipoFinal !== typeFilter) return false;
      if (textFilter && !normalizeName(`${item.nomeOriginal} ${item.nomeOficial}`).includes(textFilter)) return false;
      return true;
    });
    const reviewed = db.reviewRegistry.filter((item) => item.statusRevisao === 'revisado').length;
    const html = page('Cadastro Revisável', `<section><h2>Cadastro Revisável (fluxo diário)</h2>
<p>Total filtrado: ${filtered.length} | Revisados: ${reviewed}</p>
<form method='get' action='/cadastros' class='grid2'>
  <label>Status<select name='status'><option value='pendente' ${statusFilter === 'pendente' ? 'selected' : ''}>pendente</option><option value='revisado' ${statusFilter === 'revisado' ? 'selected' : ''}>revisado</option><option value='todos' ${statusFilter === 'todos' ? 'selected' : ''}>todos</option></select></label>
  <label>Tipo<select name='tipo'><option value=''>todos</option>${TYPE_OPTIONS.map((t) => `<option ${typeFilter === t ? 'selected' : ''}>${t}</option>`).join('')}</select></label>
  <label>Busca <input name='q' value='${url.searchParams.get('q') || ''}' placeholder='nome original/oficial'/></label>
  <button type='submit'>Filtrar</button>
</form>
<button onclick='revisarEmLote()'>Marcar filtrados como revisado</button>
<div class='grid2'>
<div><h3>Consolidar aliases</h3><input id='aliasFrom' placeholder='Nome origem'/><input id='aliasTo' placeholder='Nome oficial'/><label><input id='keepAlias' type='checkbox' checked/> manter alias</label><button onclick='consolidarAlias()'>Consolidar + regra</button></div>
<div><h3>Vincular projeto a cliente</h3><input id='projectName' placeholder='Projeto'/><input id='clientName' placeholder='Cliente'/><button onclick='vincularProjetoCliente()'>Vincular + regra</button></div>
</div>
<div class='grid2'>
<div><h3>Reclassificar para Estrutura</h3><input id='toEstrutura' placeholder='Nome oficial'/><button onclick='reclassificar("Estrutura Interna")'>Aplicar</button></div>
<div><h3>Reclassificar para Financeiro</h3><input id='toFinanceiro' placeholder='Nome oficial'/><button onclick='reclassificar("Financeiro / Não Operacional")'>Aplicar</button></div>
</div>
<table><thead><tr><th>Original</th><th>Oficial</th><th>Sugerido</th><th>Tipo Final</th><th>Cliente</th><th>Projeto</th><th>Status</th></tr></thead><tbody>${reviewTableRows(filtered)}</tbody></table>
</section>
<script>
async function alterarTipo(id,tipoFinal){await fetch('/api/review/'+id,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({tipoFinal,statusRevisao:'revisado'})});}
async function vincularCliente(id,clienteVinculado){await fetch('/api/review/'+id,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({clienteVinculado,statusRevisao:'revisado'})});}
async function vincularProjeto(id,projetoVinculado){await fetch('/api/review/'+id,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({projetoVinculado,statusRevisao:'revisado'})});}
async function marcarRevisao(id,statusRevisao){await fetch('/api/review/'+id,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({statusRevisao})});}
async function consolidarAlias(){const sourceName=document.getElementById('aliasFrom').value;const targetName=document.getElementById('aliasTo').value;const keepAlias=document.getElementById('keepAlias').checked;await fetch('/api/review/consolidate',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({sourceName,targetName,keepAlias,applyRule:true})});location.reload();}
async function vincularProjetoCliente(){const projectName=document.getElementById('projectName').value;const clientName=document.getElementById('clientName').value;await fetch('/api/review/link-project',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({projectName,clientName,applyRule:true})});location.reload();}
async function reclassificar(tipo){const idField=tipo.includes('Estrutura')?'toEstrutura':'toFinanceiro';const nome=document.getElementById(idField).value;await fetch('/api/review/reclassify',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({nome,tipoFinal:tipo,applyRule:true})});location.reload();}
async function revisarEmLote(){const ids=[...document.querySelectorAll('tr[data-id]')].map(r=>r.getAttribute('data-id'));await fetch('/api/review/bulk-review',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({ids,statusRevisao:'revisado'})});location.reload();}
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
    await storage.saveDb(db);
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
      db.savedRules.push({ id: crypto.randomUUID(), type: 'alias', matchValue: source, targetValue: target, active: true });
    }
    await storage.saveDb(db);
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
    await storage.saveDb(db);
    return json(res, 200, { ok: true });
  }

  if (req.method === 'POST' && url.pathname === '/api/review/reclassify') {
    const { nome, tipoFinal, applyRule } = JSON.parse(await readBody(req) || '{}');
    const normalized = normalizeName(nome);
    db.reviewRegistry.forEach((r) => {
      if (normalizeName(r.nomeOficial) === normalized) {
        r.tipoFinal = tipoFinal;
        r.statusRevisao = 'revisado';
      }
    });
    if (applyRule) {
      db.savedRules.push({
        id: crypto.randomUUID(),
        type: 'entry_update',
        field: 'cliente',
        matchValue: normalized,
        updates: { natureza: tipoFinal === 'Financeiro / Não Operacional' ? 'Movimentação Financeira Não Operacional' : 'Despesa Indireta' },
        active: true
      });
    }
    await storage.saveDb(db);
    return json(res, 200, { ok: true });
  }

  if (req.method === 'POST' && url.pathname === '/api/review/bulk-review') {
    const { ids, statusRevisao, tipoFinal } = JSON.parse(await readBody(req) || '{}');
    const set = new Set(ids || []);
    db.reviewRegistry.forEach((item) => {
      if (!set.has(item.id)) return;
      if (statusRevisao) item.statusRevisao = statusRevisao;
      if (tipoFinal) item.tipoFinal = tipoFinal;
    });
    await storage.saveDb(db);
    return json(res, 200, { ok: true, updated: set.size });
  }

  if (req.method === 'POST' && url.pathname === '/api/review/save-rule') {
    const body = JSON.parse(await readBody(req) || '{}');
    db.savedRules.push({ id: crypto.randomUUID(), type: 'entry_update', active: true, ...body });
    await storage.saveDb(db);
    return json(res, 201, { ok: true });
  }

  if (req.method === 'PATCH' && url.pathname.startsWith('/api/entries/')) {
    const id = url.pathname.split('/').pop();
    const entry = db.entries.find((e) => e.id === id);
    if (!entry) return json(res, 404, { error: 'Lançamento não encontrado' });
    const changes = JSON.parse(await readBody(req) || '{}');
    const editable = ['cliente', 'projeto', 'natureza', 'centroCusto', 'parceiro', 'categoria', 'detalhe', 'conta', 'formaPagamento', 'status'];
    editable.forEach((k) => { if (changes[k] !== undefined) entry[k] = changes[k]; });
    db.manualAdjustments = db.manualAdjustments || [];
    db.manualAdjustments.push({
      id: crypto.randomUUID(),
      entryId: entry.id,
      changedAt: new Date().toISOString(),
      changes
    });
    await storage.saveDb(db);
    return json(res, 200, entry);
  }

  if (req.method === 'GET' && url.pathname === '/dashboard') {
    const metrics = calculateDashboard(db);
    const topItems = (obj) => Object.entries(obj).sort((a, b) => Math.abs(b[1]) - Math.abs(a[1])).slice(0, 10);
    const html = page('Dashboard', `<section><h2>Dashboard gerencial</h2>
<div class='cards'>
<div class='card'><strong>Saldo de hoje</strong><span>R$ ${metrics.saldoHoje.toFixed(2)}</span></div>
<div class='card'><strong>Projeção 7 dias</strong><span>R$ ${metrics.proj7.toFixed(2)}</span></div>
<div class='card'><strong>Projeção 30 dias</strong><span>R$ ${metrics.proj30.toFixed(2)}</span></div>
<div class='card'><strong>Contas a pagar</strong><span>R$ ${metrics.contasPagar.toFixed(2)}</span></div>
<div class='card'><strong>Contas a receber</strong><span>R$ ${metrics.contasReceber.toFixed(2)}</span></div>
<div class='card'><strong>Risco de caixa</strong><span>${metrics.riscoCaixa}</span></div>
</div>
<h3>Próximos 7 dias (agenda financeira)</h3><ul>${metrics.upcoming7.slice(0, 15).map((e) => `<li>${e.dataISO} | ${e.descricao || '-'} | R$ ${e.valor.toFixed(2)}</li>`).join('') || '<li>Sem lançamentos previstos.</li>'}</ul>
<h3>Resultado por cliente</h3><ul>${topItems(metrics.byClient).map(([k, v]) => `<li>${k}: R$ ${v.toFixed(2)}</li>`).join('')}</ul>
<h3>Resultado por projeto</h3><ul>${topItems(metrics.byProject).map(([k, v]) => `<li>${k}: R$ ${v.toFixed(2)}</li>`).join('')}</ul>
</section>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

storage.init().then(() => {
  server.listen(PORT, () => {
    console.log(`CKM MVP running at http://localhost:${PORT}`);
  });
}).catch((error) => {
  console.error('Falha ao iniciar storage:', error);
  process.exit(1);
});
