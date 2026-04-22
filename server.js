const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PORT = process.env.PORT || 3000;
const DB_PATH = path.join(__dirname, 'data', 'db.json');
const sessions = new Map();

const OFFICIAL_CLIENTS = ['BRB', 'SEBRAE TO', 'SEBRAE-AC'];
const OFFICIAL_PROJECTS = [
  'BRB-PDL',
  'SEBRAE 10º CICLO',
  'SEBRAE 9º CICLO',
  'PS SEBRAE 2022',
  'CESAMA CARTA-CONTRATO 20/2023 ETAPA 4'
];
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

function loadDb() {
  return JSON.parse(fs.readFileSync(DB_PATH, 'utf8'));
}

function saveDb(db) {
  fs.writeFileSync(DB_PATH, JSON.stringify(db, null, 2));
}

function json(res, status, payload) {
  res.writeHead(status, { 'Content-Type': 'application/json; charset=utf-8' });
  res.end(JSON.stringify(payload));
}

function parseCookies(req) {
  const cookie = req.headers.cookie || '';
  const pairs = cookie.split(';').map((c) => c.trim()).filter(Boolean);
  return Object.fromEntries(pairs.map((p) => {
    const [k, ...rest] = p.split('=');
    return [k, decodeURIComponent(rest.join('='))];
  }));
}

function currentUser(req, db) {
  const sid = parseCookies(req).sid;
  if (!sid || !sessions.has(sid)) return null;
  const userId = sessions.get(sid);
  return db.users.find((u) => u.id === userId) || null;
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    let data = '';
    req.on('data', (chunk) => {
      data += chunk;
      if (data.length > 10 * 1024 * 1024) {
        reject(new Error('Body too large'));
      }
    });
    req.on('end', () => resolve(data));
    req.on('error', reject);
  });
}

function normalizeName(name) {
  if (!name) return '';
  const upper = String(name).trim().toUpperCase();
  return ALIAS_RULES[upper] || upper;
}

function inferType(name) {
  const n = normalizeName(name);
  if (!n) return 'Pendente de Classificação';
  if (FORBIDDEN_AS_CLIENT.includes(n)) return 'Estrutura Interna';
  if (n.includes('MÚTUO') || n.includes('PRONAMPE')) return 'Financeiro / Não Operacional';
  if (n.includes('CARTÃO') || n.includes('CARD')) return 'Conta / Cartão';
  if (OFFICIAL_PROJECTS.includes(n) || n.includes('CICLO') || n.includes('ETAPA')) return 'Projeto';
  if (OFFICIAL_CLIENTS.includes(n)) return 'Cliente';
  return 'Pendente de Classificação';
}

function inferNature(entry) {
  const desc = normalizeName(entry.descricao || '');
  if (desc.includes('MÚTUO')) return 'Movimentação Financeira Não Operacional';
  if (entry.tipo === 'entrada') return 'Receita Operacional';
  if (entry.tipo === 'saida' && entry.projeto) return 'Custo Direto do Projeto';
  if (entry.tipo === 'saida') return 'Despesa Indireta';
  return 'Pendente de Classificação';
}

function parseCsv(csvText) {
  const lines = csvText.split(/\r?\n/).filter(Boolean);
  if (lines.length < 2) return [];
  const headers = lines[0].split(';').map((h) => h.trim().toLowerCase());
  return lines.slice(1).map((line) => {
    const cols = line.split(';');
    const row = {};
    headers.forEach((h, i) => {
      row[h] = (cols[i] || '').trim();
    });
    return row;
  });
}

function buildIssues(entries) {
  const issues = [];
  for (const e of entries) {
    const description = normalizeName(e.descricao);
    const client = normalizeName(e.cliente);
    const project = normalizeName(e.projeto);

    if (e.natureza === 'Custo Direto do Projeto' && !project) {
      issues.push({ entryId: e.id, level: 'erro', code: 'DESPESA_SEM_PROJETO', message: 'Despesa direta sem projeto.' });
    }
    if (client && inferType(client) !== 'Cliente' && inferType(client) !== 'Projeto') {
      issues.push({ entryId: e.id, level: 'erro', code: 'CLIENTE_INVALIDO', message: 'Nome lançado no cliente não é cliente válido.' });
    }
    if (description.includes('MÚTUO') && e.natureza !== 'Movimentação Financeira Não Operacional') {
      issues.push({ entryId: e.id, level: 'erro', code: 'MUTUO_CLASSIFICACAO', message: 'Mútuo classificado incorretamente.' });
    }
    if (!e.data || Number.isNaN(new Date(e.data).getTime())) {
      issues.push({ entryId: e.id, level: 'erro', code: 'DATA_INVALIDA', message: 'Data inválida.' });
    }
    if (!Number.isFinite(e.valor)) {
      issues.push({ entryId: e.id, level: 'erro', code: 'VALOR_INVALIDO', message: 'Valor inválido.' });
    }
    if (inferType(client) === 'Pendente de Classificação') {
      issues.push({ entryId: e.id, level: 'alerta', code: 'NOME_FORA_PADRAO', message: 'Cliente/projeto fora do padrão conhecido.' });
    }
    if (e.conta && e.conta.toUpperCase().includes('CARTAO') && !e.detalhe) {
      issues.push({ entryId: e.id, level: 'alerta', code: 'CARTAO_SEM_DETALHE', message: 'Cartão sem detalhamento completo.' });
    }
  }
  return issues;
}

function buildReviewRegistry(entries) {
  const names = new Map();
  for (const e of entries) {
    [e.cliente, e.projeto, e.parceiro].forEach((name) => {
      if (!name) return;
      const key = normalizeName(name);
      if (!names.has(key)) {
        names.set(key, {
          id: crypto.randomUUID(),
          nomeOriginal: name,
          nomeOficial: key,
          tipoSugerido: inferType(name),
          tipoFinal: inferType(name),
          clienteVinculado: inferType(name) === 'Projeto' ? normalizeName(e.cliente) : '',
          projetoVinculado: inferType(name) === 'Projeto' ? key : '',
          manterAlias: true,
          observacao: '',
          statusRevisao: 'pendente'
        });
      }
    });
  }
  return [...names.values()];
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
  if (!file.startsWith(path.join(__dirname, 'public'))) return false;
  if (!fs.existsSync(file)) return false;
  const ext = path.extname(file);
  const contentType = ext === '.css' ? 'text/css' : 'text/plain';
  res.writeHead(200, { 'Content-Type': contentType });
  res.end(fs.readFileSync(file));
  return true;
}

function page(title, body, user) {
  return `<!doctype html><html><head><meta charset="utf-8"><title>${title}</title><link rel="stylesheet" href="/public/style.css"></head><body>
  <header><h1>Painel Financeiro Gerencial CKM</h1>${user ? `<nav><a href='/'>Home</a><a href='/upload'>Upload</a><a href='/pendencias'>Pendências</a><a href='/cadastros'>Cadastro Revisável</a><a href='/dashboard'>Dashboard</a><a href='/logout'>Sair</a></nav>` : ''}</header>
  <main>${body}</main></body></html>`;
}

function parseEntries(rows, uploadId) {
  return rows.map((r) => {
    const valor = Number(String(r.valor || r.amount || '0').replace(',', '.'));
    const entry = {
      id: crypto.randomUUID(),
      uploadId,
      data: r.data || r.date || '',
      descricao: r.descricao || r.historico || r.description || '',
      cliente: normalizeName(r.cliente || r.client || ''),
      projeto: normalizeName(r.projeto || r.project || ''),
      parceiro: normalizeName(r.parceiro || r.fornecedor || ''),
      conta: r.conta || r.cartao || '',
      detalhe: r.detalhe || r.categoria || '',
      tipo: valor >= 0 ? 'entrada' : 'saida',
      valor,
      natureza: 'Pendente de Classificação',
      centroCusto: r.centrocusto || '',
      status: 'importado'
    };
    entry.natureza = inferNature(entry);
    return entry;
  });
}

const server = http.createServer(async (req, res) => {
  const db = loadDb();
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (req.url.startsWith('/public/') && serveStatic(req, res)) return;

  if (req.method === 'GET' && url.pathname === '/login') {
    const html = page('Login', `<section><h2>Login</h2><form method='post' action='/login'><label>E-mail <input name='email'></label><label>Senha <input type='password' name='password'></label><button>Entrar</button></form><p>Usuário de teste: owner@ckm.local / 123456</p></section>`);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'POST' && url.pathname === '/login') {
    const raw = await readBody(req);
    const form = new URLSearchParams(raw);
    const email = form.get('email');
    const password = form.get('password');
    const user = db.users.find((u) => u.email === email && u.password === password);
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
    const sid = parseCookies(req).sid;
    sessions.delete(sid);
    res.writeHead(302, { 'Set-Cookie': 'sid=; Max-Age=0; Path=/', Location: '/login' });
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
    const today = new Date().toISOString().slice(0, 10);
    const todays = db.entries.filter((e) => e.data.startsWith(today));
    const entradas = todays.filter((e) => e.valor > 0).reduce((a, b) => a + b.valor, 0);
    const saidas = Math.abs(todays.filter((e) => e.valor < 0).reduce((a, b) => a + b.valor, 0));
    const saldoHoje = db.entries.reduce((a, b) => a + b.valor, 0);
    const pendencias = db.issues.filter((i) => i.level === 'erro').length;
    const html = page('Home', `<section><h2>Resumo de Hoje</h2><ul><li>Saldo de hoje: R$ ${saldoHoje.toFixed(2)}</li><li>Entradas de hoje: R$ ${entradas.toFixed(2)}</li><li>Saídas de hoje: R$ ${saidas.toFixed(2)}</li><li>Pendências obrigatórias: ${pendencias}</li></ul></section>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/upload') {
    const html = page('Upload', `<section><h2>Upload da Planilha (CSV com ;)</h2><p>Formato mínimo: data;descricao;cliente;projeto;valor;conta;detalhe</p><textarea id='csv' rows='12' cols='120' placeholder='Cole o CSV aqui'></textarea><br><button onclick='enviar()'>Importar</button><pre id='out'></pre></section><script>async function enviar(){const csv=document.getElementById('csv').value; const r=await fetch('/api/upload',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({csvText:csv,fileName:'manual.csv'})}); document.getElementById('out').textContent=JSON.stringify(await r.json(),null,2);} </script>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/upload') {
    if (!requireAuth(req, res, db)) return;
    const raw = await readBody(req);
    const { csvText, fileName } = JSON.parse(raw || '{}');
    const rows = parseCsv(csvText || '');
    const upload = { id: crypto.randomUUID(), fileName: fileName || 'upload.csv', uploadedAt: new Date().toISOString(), rowCount: rows.length };
    const entries = parseEntries(rows, upload.id);
    const issues = buildIssues(entries).map((i) => ({ ...i, id: crypto.randomUUID(), uploadId: upload.id, status: 'aberta' }));
    const reviewRegistry = buildReviewRegistry(entries);

    db.uploads.push(upload);
    db.entries.push(...entries);
    db.issues.push(...issues);
    db.reviewRegistry = mergeRegistry(db.reviewRegistry, reviewRegistry);
    saveDb(db);

    json(res, 200, { uploadId: upload.id, importedRows: entries.length, issues: issues.length, reviewItems: reviewRegistry.length });
    return;
  }

  if (req.method === 'GET' && url.pathname === '/pendencias') {
    const errors = db.issues.filter((i) => i.level === 'erro');
    const alerts = db.issues.filter((i) => i.level === 'alerta');
    const html = page('Pendências', `<section><h2>Pendências Obrigatórias</h2><ul>${errors.map((e) => `<li>${e.code}: ${e.message}</li>`).join('')}</ul><h2>Alertas</h2><ul>${alerts.map((a) => `<li>${a.code}: ${a.message}</li>`).join('')}</ul></section>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/cadastros') {
    const rows = db.reviewRegistry.map((r) => `<tr><td>${r.nomeOriginal}</td><td>${r.nomeOficial}</td><td>${r.tipoSugerido}</td><td><select onchange="alterarTipo('${r.id}',this.value)">${['Cliente','Projeto','Prestador de Serviço','Fornecedor','Estrutura Interna','Financeiro / Não Operacional','Conta / Cartão','Pendente de Classificação'].map((t)=>`<option ${r.tipoFinal===t?'selected':''}>${t}</option>`).join('')}</select></td><td>${r.statusRevisao}</td></tr>`).join('');
    const html = page('Cadastro Revisável', `<section><h2>Cadastro Revisável</h2><table><thead><tr><th>Nome Original</th><th>Nome Oficial</th><th>Tipo Sugerido</th><th>Tipo Final</th><th>Status</th></tr></thead><tbody>${rows}</tbody></table></section><script>async function alterarTipo(id,tipoFinal){await fetch('/api/review/'+id,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({tipoFinal,statusRevisao:'revisado'})});}</script>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'PATCH' && url.pathname.startsWith('/api/review/')) {
    const id = url.pathname.split('/').pop();
    const body = JSON.parse(await readBody(req));
    const item = db.reviewRegistry.find((r) => r.id === id);
    if (!item) return json(res, 404, { error: 'Registro não encontrado' });
    Object.assign(item, body);
    saveDb(db);
    return json(res, 200, item);
  }

  if (req.method === 'PATCH' && url.pathname.startsWith('/api/entries/')) {
    const id = url.pathname.split('/').pop();
    const body = JSON.parse(await readBody(req));
    const entry = db.entries.find((r) => r.id === id);
    if (!entry) return json(res, 404, { error: 'Lançamento não encontrado' });
    const editable = ['cliente','projeto','natureza','centroCusto','parceiro','detalhe','conta','status'];
    for (const key of editable) {
      if (body[key] !== undefined) entry[key] = body[key];
    }
    saveDb(db);
    return json(res, 200, entry);
  }

  if (req.method === 'GET' && url.pathname === '/dashboard') {
    const byClient = {};
    const byProject = {};
    for (const e of db.entries) {
      const c = e.cliente || 'SEM CLIENTE';
      const p = e.projeto || 'SEM PROJETO';
      byClient[c] = (byClient[c] || 0) + e.valor;
      byProject[p] = (byProject[p] || 0) + e.valor;
    }
    const cList = Object.entries(byClient).map(([k,v]) => `<li>${k}: R$ ${v.toFixed(2)}</li>`).join('');
    const pList = Object.entries(byProject).map(([k,v]) => `<li>${k}: R$ ${v.toFixed(2)}</li>`).join('');
    const html = page('Dashboard', `<section><h2>Resultado por Cliente</h2><ul>${cList}</ul><h2>Resultado por Projeto</h2><ul>${pList}</ul></section>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

function mergeRegistry(existing, incoming) {
  const map = new Map(existing.map((r) => [normalizeName(r.nomeOficial), r]));
  for (const item of incoming) {
    const key = normalizeName(item.nomeOficial);
    if (!map.has(key)) map.set(key, item);
  }
  return [...map.values()];
}

server.listen(PORT, () => {
  console.log(`CKM MVP running at http://localhost:${PORT}`);
});
