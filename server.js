const http = require('http');
const fs = require('fs');
const path = require('path');
const crypto = require('crypto');
const os = require('os');
const { spawnSync } = require('child_process');
const { createStorage } = require('./storage');
const storage = createStorage({ dbPath: path.join(__dirname, 'data', 'db.json'), databaseUrl: process.env.DATABASE_URL });

const PORT = process.env.PORT || 3000;
const DB_PATH = path.join(__dirname, 'data', 'db.json');
const PARSER_PATH = path.join(__dirname, 'scripts', 'parse_spreadsheet.py');
const sessions = new Map();

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
  data: ['data', 'dt', 'date', 'data_movimento', 'data movimento', 'vencimento',
         'data pagamento', 'data_recebimento', 'data recebimento', 'data_emissão', 'data emissão'],
  descricao: ['descricao', 'descrição', 'historico', 'histórico', 'description',
              'obs', 'observacao', 'observação'],
  cliente: ['cliente', 'client', 'contratante'],
  projeto: ['projeto', 'project', 'contrato', 'frente'],
  parceiro: ['parceiro', 'prestador', 'fornecedor', 'beneficiario', 'beneficiário',
             'fornecedor/parceiro'],
  conta: ['conta', 'cartao', 'cartão', 'conta_cartao', 'conta/cartão'],
  detalhe: ['detalhe', 'categoria', 'detalhamento', 'observacao', 'observação'],
  valor: ['valor', 'amount', 'vlr', 'valor_total', 'valor total', 'valor unitário', 'valor unitario'],
  centroCusto: ['centro_custo', 'centro custo', 'cc', 'centrodecusto', 'c_custo', 'c custo', 'c.custo'],
  tipo: ['tipo', 'receita/despesa', 'tipo_lancamento', 'tipo lancamento', 'tp-despesa', 'tp despesa'],
  dc: ['d/c', 'dc', 'debito/credito', 'débito/crédito'],
  status: ['status'],
  movto: ['movto', 'movimento', 'movimentacao', 'movimentação'],
  pr: ['pr'],
  detDespesa: ['det-despesa', 'det despesa', 'detalhe despesa', 'det_despesa', 'det.despesa'],
  notaFiscal: ['nota_fiscal', 'nota fiscal', 'nf', 'nr.nf', 'nº nf'],
  formaPagamento: ['forma_pagamento', 'forma pagamento', 'pagamento']
};

function loadDb() {
  return JSON.parse(fs.readFileSync(DB_PATH, 'utf8'));
}

function saveDb(db) {
  fs.writeFileSync(DB_PATH, JSON.stringify(db, null, 2));
  // Async persist to PostgreSQL if DATABASE_URL is set (non-blocking)
  if (process.env.DATABASE_URL) {
    storage.saveDb(db).catch((e) => console.error('[storage] saveDb error:', e.message));
  }
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

function parseDateValue(value) {
  const raw = String(value || '').trim();
  if (!raw) return null;

  // ISO yyyy-mm-dd (já processado pelo parser Python — serial Excel convertido)
  const isoMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) {
    return new Date(Date.UTC(Number(isoMatch[1]), Number(isoMatch[2]) - 1, Number(isoMatch[3])));
  }

  // Formato BR dd/mm/aaaa ou dd/mm/aa
  const br = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
  if (br) {
    const dd = Number(br[1]);
    const mm = Number(br[2]);
    const yy = Number(br[3].length === 2 ? `20${br[3]}` : br[3]);
    return new Date(Date.UTC(yy, mm - 1, dd));
  }

  // Fallback: tentar parse genérico
  const iso = new Date(raw);
  if (!Number.isNaN(iso.getTime())) {
    return new Date(Date.UTC(iso.getUTCFullYear(), iso.getUTCMonth(), iso.getUTCDate()));
  }

  return null;
}

function inferType(name) {
  const n = normalizeName(name);
  if (!n) return 'Pendente de Classificação';

  // Estrutura interna (lista explícita + padrões CKM)
  if (FORBIDDEN_AS_CLIENT.includes(n)) return 'Estrutura Interna';
  const estruturaPatterns = ['ESCRITÓRIO', 'SALÁRIOS', 'SALARIO', 'JURÍDICO', 'JURIDICO',
    'CONTÁBIL', 'CONTABIL', 'TEF', 'SALDO', 'PRONAMPE', 'ADTO', 'ADIANTAMENTO',
    'F.FIXO', 'FIXO', 'CX RSV', 'RESERVA', 'CONVÊNIO', 'CONVENIO',
    'PRÓ-LABORE', 'PRO-LABORE', 'PROLABORE'];
  if (estruturaPatterns.some((p) => n.includes(p))) return 'Estrutura Interna';

  // Financeiro / Não Operacional
  const finPatterns = ['MÚTUO', 'MUTUO', 'TEF ENTRE', 'TRANSFERÊNCIA', 'TRANSFERENCIA',
    'APLICAÇÃO', 'APLICACAO', 'APLIC ', 'RESGATE', 'IOF', 'IMPOSTO', 'TRIBUTO',
    'RET_', 'RETENÇÃO', 'RETENCAO', 'DASN', 'SIMPLES NACIONAL', 'IRRF', 'CSLL',
    'PIS', 'COFINS', 'ISS', 'INSS'];
  if (finPatterns.some((p) => n.includes(p))) return 'Financeiro / Não Operacional';

  // Conta / Cartão
  if (n.includes('CARTÃO') || n.includes('CARTAO') || n.includes('CARD') ||
      n.includes('BB') || n.includes('ITAÚ') || n.includes('ITAU') ||
      n.includes('BRB') || n.includes('BRD') || n.includes('STD') ||
      n.includes('CEF') || n.includes('BANRISUL') || n.includes('BRADESCO')) return 'Conta / Cartão';

  // Projeto (padrões CKM)
  if (OFFICIAL_PROJECTS.includes(n)) return 'Projeto';
  if (n.includes('CICLO') || n.includes('ETAPA') || n.includes('PDL') ||
      n.includes('PROCESSO') || n.includes('CONTRATO') || n.includes('LOTE') ||
      n.includes('EDITAL') || n.includes('PS ') || n.includes('CARTA-CONTRATO')) return 'Projeto';

  // Cliente (lista oficial)
  if (OFFICIAL_CLIENTS.includes(n)) return 'Cliente';

  return 'Pendente de Classificação';
}

function inferNature(entry) {
  const desc = normalizeName(entry.descricao || '');
  const cc = normalizeName(entry.centroCusto || '');
  const tipoOriginal = normalizeName(entry.tipoOriginal || '');
  const dc = String(entry.dc || '').toUpperCase().trim();

  // Mútuo / Financeiro não operacional
  if (desc.includes('MÚTUO') || desc.includes('MUTUO') || cc === 'MÚTUO' || cc === 'MUTUO') {
    return 'Movimentação Financeira Não Operacional';
  }

  // TEF (transferência entre contas)
  if (cc === 'TEF' || tipoOriginal.includes('TEF') || desc.includes('TEF=>') || desc.includes('TEF ')) {
    return 'Movimentação Financeira Não Operacional';
  }

  // Pró-labore / Estrutura
  if (cc === 'PRÓ-LABORE' || cc === 'PRO-LABORE' || cc === 'PROLABORE' ||
      tipoOriginal.includes('PRÓ-LABORE') || tipoOriginal.includes('PRO-LABORE')) {
    return 'Despesa Indireta';
  }

  // Usar D/C da planilha CKM: C = crédito (entrada), D = débito (saída)
  const isEntrada = dc === 'C' || (dc !== 'D' && entry.valor > 0);
  const isSaida = dc === 'D' || (dc !== 'C' && entry.valor < 0);

  if (isEntrada) {
    // Receita: faturamento, NF, serviços
    if (tipoOriginal === 'FATURAMENTO' || desc.includes('NF ') || desc.includes('NOTA FISCAL') ||
        desc.includes('FATURAMENTO') || desc.includes('EMISSÃO')) {
      return 'Receita Operacional';
    }
    return 'Receita Operacional';
  }

  if (isSaida) {
    // Custo direto: tem projeto ou CC de projeto
    if (entry.projeto) return 'Custo Direto do Projeto';
    // Impostos / retenções
    if (tipoOriginal.includes('TRIBUTO') || tipoOriginal.includes('IMPOSTO') ||
        cc.startsWith('RET_') || desc.includes('SIMPLES') || desc.includes('DASN')) {
      return 'Despesa Indireta';
    }
    return 'Despesa Indireta';
  }

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
  const normalized = raw.includes(',') ? raw.replace(/\./g, '').replace(',', '.') : raw;
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

  const outPath = path.join(os.tmpdir(), `${crypto.randomUUID()}.json`);
  const proc = spawnSync('python3', [PARSER_PATH, tempPath, ext, outPath], {
    encoding: 'utf8',
    maxBuffer: 64 * 1024 * 1024 // 64 MB
  });
  fs.unlinkSync(tempPath);

  if (proc.error) throw proc.error;
  if (proc.status !== 0 && proc.status !== 2) throw new Error(proc.stderr || 'Falha ao executar parser de planilha.');

  // Ler saída do arquivo temporário se existir, senão usar stdout
  let rawOutput;
  if (fs.existsSync(outPath)) {
    rawOutput = fs.readFileSync(outPath, 'utf8');
    fs.unlinkSync(outPath);
  } else {
    rawOutput = proc.stdout || '{}';
  }

  const payload = JSON.parse(rawOutput);
  if (payload.error) throw new Error(payload.error);
  return payload.rows || [];
}

function parseEntries(rows, uploadId, db) {
  return rows
    .filter((raw) => {
      // Filtrar linhas completamente vazias ou de controle (SALDO, TOTAL)
      const dataVal = String(raw.data || raw.dataISO || '').trim().toUpperCase();
      if (!dataVal || ['SALDO', 'TOTAL', 'SUBTOTAL', 'STOP'].includes(dataVal)) return false;
      return true;
    })
    .map((raw) => {
      // O parser Python já normaliza os headers; mas normalizamos novamente para segurança
      const r = normalizeRowHeaders(raw);

      // Data: preferir dataISO já convertida pelo parser Python
      const dataRaw = r.dataiso || r.data || '';
      const parsedDate = parseDateValue(dataRaw);

      // Valor: o parser Python já aplica sinal via D/C; usar diretamente se for número
      let valor = typeof raw.valor === 'number' ? raw.valor : normalizeMoney(r.valor);

      // D/C da planilha CKM: C = crédito (positivo), D = débito (negativo)
      const dc = String(r.dc || '').toUpperCase().trim();
      if (dc === 'D' && valor > 0) valor = -valor;
      if (dc === 'C' && valor < 0) valor = -valor;

      // Status da planilha: PG=pago, RE=realizado, ZZ=zerado/cancelado, TF=transferência
      const statusPlanilha = String(r.status || '').toUpperCase().trim();
      const statusImport = statusPlanilha || 'importado';

      // Tipo original da planilha (coluna 'Tipo' ou 'RECEITA/DESPESA')
      const tipoOriginal = String(r.tipo || r['receita/despesa'] || '').trim();

      const entry = {
        id: crypto.randomUUID(),
        uploadId,
        data: dataRaw,
        dataISO: parsedDate ? parsedDate.toISOString().slice(0, 10) : '',
        descricao: r.descricao || '',
        cliente: normalizeName(r.cliente || ''),
        projeto: normalizeName(r.projeto || ''),
        parceiro: normalizeName(r.parceiro || ''),
        conta: r.conta || '',
        detalhe: r.detalhe || r.detdespesa || '',
        formaPagamento: r.formapagamento || '',
        centroCusto: r.centrocusto || '',
        dc,
        tipoOriginal,
        statusPlanilha,
        notaFiscal: r.notafiscal || '',
        tipo: valor >= 0 ? 'entrada' : 'saida',
        valor,
        natureza: 'Pendente de Classificação',
        categoria: '',
        status: statusImport
      };

      applySavedRulesToEntry(entry, db);
      entry.natureza = inferNature(entry);
      return entry;
    });
}

function buildIssues(entries, db) {
  const issues = [];
  const newNames = new Set();
  const knownNames = new Set((db.reviewRegistry || []).map((item) => normalizeName(item.nomeOficial)));

  for (const e of entries) {
    const desc = normalizeName(e.descricao);
    const client = normalizeName(e.cliente);
    const project = normalizeName(e.projeto);
    const cc = normalizeName(e.centroCusto || '');
    const statusPlanilha = String(e.statusPlanilha || '').toUpperCase();

    // --- ERROS BLOQUEANTES ---

    // Data inválida
    if (!e.dataISO) {
      issues.push({ entryId: e.id, level: 'erro', code: 'DATA_INVALIDA',
        message: `Data inválida: '${e.data}'.`, blocking: true });
    }

    // Valor inválido
    if (!Number.isFinite(e.valor)) {
      issues.push({ entryId: e.id, level: 'erro', code: 'VALOR_INVALIDO',
        message: 'Valor não numérico.', blocking: true });
    }

    // Estrutura interna lançada como cliente
    if (client && inferType(client) === 'Estrutura Interna') {
      issues.push({ entryId: e.id, level: 'erro', code: 'ESTRUTURA_COMO_CLIENTE',
        message: `Estrutura interna lançada como cliente: '${client}'.`, blocking: true });
    }

    // Mútuo lançado como cliente
    if (client && (client.includes('MÚTUO') || client.includes('MUTUO'))) {
      issues.push({ entryId: e.id, level: 'erro', code: 'MUTUO_COMO_CLIENTE',
        message: `Mútuo lançado como cliente: '${client}'.`, blocking: true });
    }

    // Mútuo classificado incorretamente (não como Movimentação Financeira)
    if ((desc.includes('MÚTUO') || desc.includes('MUTUO') || cc === 'MÚTUO') &&
        e.natureza !== 'Movimentação Financeira Não Operacional') {
      issues.push({ entryId: e.id, level: 'erro', code: 'MUTUO_CLASSIFICACAO',
        message: 'Mútuo classificado incorretamente.', blocking: true });
    }

    // Despesa direta sem projeto
    if (e.natureza === 'Custo Direto do Projeto' && !project) {
      issues.push({ entryId: e.id, level: 'erro', code: 'DESPESA_SEM_PROJETO',
        message: 'Despesa direta sem projeto vinculado.', blocking: true });
    }

    // --- ALERTAS NÃO BLOQUEANTES ---

    // Entrada sem cliente identificado (faturamento sem cliente)
    if (e.tipo === 'entrada' && e.natureza === 'Receita Operacional' && !client && !e.parceiro) {
      issues.push({ entryId: e.id, level: 'alerta', code: 'RECEITA_SEM_CLIENTE',
        message: 'Receita sem cliente identificado.', blocking: false });
    }

    // Cartão sem detalhamento
    if (e.conta && normalizeName(e.conta).includes('CARTAO') && !e.detalhe) {
      issues.push({ entryId: e.id, level: 'alerta', code: 'CARTAO_SEM_DETALHE',
        message: 'Cartão sem detalhamento completo.', blocking: false });
    }

    // Lançamento cancelado (ZZ) com valor não zero
    if (e.statusPlanilha === 'ZZ' && Math.abs(e.valor) > 0) {
      issues.push({ entryId: e.id, level: 'alerta', code: 'CANCELADO_COM_VALOR',
        message: `Lançamento cancelado (ZZ) com valor informado.`, blocking: false });
    }
    // Nome fora do padrão conhecido
    if (client && inferType(client) === 'Pendente de Classificação') {
      issues.push({ entryId: e.id, level: 'alerta', code: 'NOME_FORA_PADRAO',
        message: 'Cliente/projeto fora do padrão conhecido.', blocking: false });
    }
    // Novos cadastros: nomes não presentes no registro revisado
    [client, project, normalizeName(e.parceiro)].forEach((name) => {
      if (name && !knownNames.has(name) && inferType(name) === 'Pendente de Classificação') {
        newNames.add(name);
      }
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
  return `<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>${title} — CKM Financeiro</title><link rel='preconnect' href='https://fonts.googleapis.com'><link rel='stylesheet' href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap'><link rel='stylesheet' href='/public/style.css'></head><body>
<header><h1>Painel <em>CKM</em> Financeiro</h1>${user ? `<nav><a href='/'>Home</a><a href='/upload'>Upload</a><a href='/pendencias'>Pré-análise</a><a href='/cadastros'>Cadastro Revisável</a><a href='/dashboard'>Dashboard</a><a href='/logout' class='sair'>Sair</a></nav>` : ''}</header>
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
  const mutuoEntries = sortedEntries.filter((e) => normalizeName(`${e.descricao} ${e.tipoOriginal} ${e.natureza}`).includes('MUTUO') || normalizeName(`${e.descricao} ${e.tipoOriginal} ${e.natureza}`).includes('MÚTUO'));
  const saldoMutuo = mutuoEntries.reduce((acc, e) => acc + e.valor, 0);

  const byClient = {};
  const byProject = {};
  db.entries.forEach((e) => {
    const c = e.cliente || 'SEM CLIENTE';
    const p = e.projeto || 'SEM PROJETO';
    byClient[c] = (byClient[c] || 0) + e.valor;
    byProject[p] = (byProject[p] || 0) + e.valor;
  });

  return { saldoHoje, proj7, proj30, contasPagar, contasReceber, byClient, byProject, rolling, upcoming7, riscoCaixa, saldoMutuo };
}

function sortEntries(entries, mode = 'date_desc') {
  const list = [...entries];
  if (mode === 'abs_desc') {
    return list.sort((a, b) => Math.abs(b.valor) - Math.abs(a.valor));
  }
  return list.sort((a, b) => (b.dataISO || '').localeCompare(a.dataISO || ''));
}

function entriesTable(entries) {
  const rows = sortEntries(entries).map((e) => `<tr>
    <td>${e.dataISO || e.data || '-'}</td>
    <td>${e.descricao || '-'}</td>
    <td>R$ ${Number(e.valor || 0).toFixed(2)}</td>
    <td>${e.cliente || '-'}</td>
    <td>${e.projeto || '-'}</td>
    <td>${e.parceiro || '-'}</td>
    <td>${e.natureza || '-'}</td>
    <td>${e.status || '-'}</td>
  </tr>`).join('');
  return `<table><thead><tr><th>Data</th><th>Descrição</th><th>Valor</th><th>Cliente</th><th>Projeto</th><th>Parceiro</th><th>Natureza</th><th>Status</th></tr></thead><tbody>${rows || '<tr><td colspan="8">Sem lançamentos no recorte.</td></tr>'}</tbody></table>`;
}

const server = http.createServer(async (req, res) => {
  const db = loadDb();
  const url = new URL(req.url, `http://${req.headers.host}`);

  if (req.url.startsWith('/public/') && serveStatic(req, res)) return;

  if (req.method === 'GET' && url.pathname === '/login') {
    const html = `<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>Login — CKM Financeiro</title><link rel='preconnect' href='https://fonts.googleapis.com'><link rel='stylesheet' href='https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap'><link rel='stylesheet' href='/public/style.css'></head><body class='login-page'><div class='login-card'><div class='brand'><h2>Painel CKM Financeiro</h2><p>Gestão Financeira Gerencial</p></div><form method='post' action='/login'><label>E-mail<input name='email' type='email' placeholder='seu@email.com' autocomplete='username'></label><label>Senha<input type='password' name='password' placeholder='••••••' autocomplete='current-password'></label><button type='submit' style='width:100%;justify-content:center;padding:.75rem'>Entrar</button></form><p class='login-hint'>owner@ckm.local &nbsp;&bull;&nbsp; 123456</p></div></body></html>`;
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
    const cadastrosPendentes = db.reviewRegistry.filter((r) => r.statusRevisao !== 'revisado').length;
    const fmtBRL = (v) => v.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
    const html = page('Home', `
<h2 class='page-title'>Resumo do dia</h2>
<div class='cards'>
  <div class='card'><strong>Saldo hoje</strong><span>${fmtBRL(metrics.saldoHoje)}</span></div>
  <div class='card'><strong>Projeção 7 dias</strong><span>${fmtBRL(metrics.proj7)}</span></div>
  <div class='card'><strong>Projeção 30 dias</strong><span>${fmtBRL(metrics.proj30)}</span></div>
  <div class='card'><strong>A pagar</strong><span style='color:var(--red)'>${fmtBRL(metrics.contasPagar)}</span></div>
  <div class='card'><strong>A receber</strong><span style='color:var(--green)'>${fmtBRL(metrics.contasReceber)}</span></div>
  <div class='card'><strong>Bloqueantes</strong><span style='color:${summary.bloqueantes > 0 ? 'var(--red)' : 'var(--green)'}'>${summary.bloqueantes}</span></div>
  <div class='card'><strong>Cadastros pendentes</strong><span style='color:${cadastrosPendentes > 0 ? 'var(--amber)' : 'var(--green)'}'>${cadastrosPendentes}</span></div>
</div>
<section>
  <h2>Acesso rápido</h2>
  <div class='grid3'>
    <a href='/upload' style='display:flex;flex-direction:column;gap:.5rem;padding:1.25rem;background:var(--gray-50);border:1px solid var(--gray-200);border-radius:10px;text-decoration:none;color:var(--gray-800);transition:box-shadow .15s' onmouseover="this.style.boxShadow='0 4px 12px rgba(0,0,0,.1)'" onmouseout="this.style.boxShadow=''"><strong style='font-size:1rem'>&#128196; Upload</strong><span style='font-size:.85rem;color:var(--gray-600)'>Importar planilha CSV / XLSX / XLSM</span></a>
    <a href='/pendencias' style='display:flex;flex-direction:column;gap:.5rem;padding:1.25rem;background:var(--gray-50);border:1px solid var(--gray-200);border-radius:10px;text-decoration:none;color:var(--gray-800);transition:box-shadow .15s' onmouseover="this.style.boxShadow='0 4px 12px rgba(0,0,0,.1)'" onmouseout="this.style.boxShadow=''"><strong style='font-size:1rem'>&#9888;&#65039; Pré-análise</strong><span style='font-size:.85rem;color:var(--gray-600)'>Verificar pendências e bloqueantes</span></a>
    <a href='/cadastros' style='display:flex;flex-direction:column;gap:.5rem;padding:1.25rem;background:var(--gray-50);border:1px solid var(--gray-200);border-radius:10px;text-decoration:none;color:var(--gray-800);transition:box-shadow .15s' onmouseover="this.style.boxShadow='0 4px 12px rgba(0,0,0,.1)'" onmouseout="this.style.boxShadow=''"><strong style='font-size:1rem'>&#128203; Cadastro Revisável</strong><span style='font-size:.85rem;color:var(--gray-600)'>Revisar e classificar nomes importados</span></a>
  </div>
</section>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/upload') {
    const html = page('Upload', `
<h2 class='page-title'>Importar Planilha</h2>
<section>
  <h2>Selecionar arquivo</h2>
  <p style='color:var(--gray-600);font-size:.9rem;margin-bottom:1rem'>Formatos aceitos: <strong>CSV</strong>, <strong>XLSX</strong> e <strong>XLSM</strong>. A primeira aba da planilha será usada para XLSX/XLSM.</p>
  <div class='upload-area' id='dropzone' onclick="document.getElementById('file').click()">
    <div class='upload-icon'>&#128196;</div>
    <p><strong>Clique para selecionar</strong> ou arraste o arquivo aqui</p>
    <p id='file-name' style='margin-top:.5rem;font-size:.82rem;color:var(--blue-lt)'></p>
  </div>
  <input type='file' id='file' accept='.csv,.xlsx,.xlsm' style='display:none' onchange="document.getElementById('file-name').textContent = this.files[0]?.name || ''" />
  <div style='margin-top:1rem;display:flex;gap:.75rem;align-items:center'>
    <button onclick='enviarArquivo()' id='btn-import'>&#128640;&nbsp; Importar planilha</button>
    <span id='status-msg' style='font-size:.85rem;color:var(--gray-600)'></span>
  </div>
</section>
<section id='result-section' style='display:none'>
  <h2 id='result-title'>Resultado da importação</h2>
  <div class='cards' id='result-cards'></div>
  <div style='margin-top:1rem;display:flex;gap:.75rem'>
    <a href='/pendencias'><button>&#9888;&#65039;&nbsp; Ver Pré-análise</button></a>
    <a href='/cadastros'><button style='background:var(--gray-600)'>&#128203;&nbsp; Ver Cadastros</button></a>
  </div>
</section>
<script>
const dropzone = document.getElementById('dropzone');
dropzone.addEventListener('dragover', e => { e.preventDefault(); dropzone.classList.add('drag-over'); });
dropzone.addEventListener('dragleave', () => dropzone.classList.remove('drag-over'));
dropzone.addEventListener('drop', e => {
  e.preventDefault(); dropzone.classList.remove('drag-over');
  const f = e.dataTransfer.files[0];
  if (f) { document.getElementById('file').files; document.getElementById('file-name').textContent = f.name; window._dropFile = f; }
});
async function enviarArquivo(){
  const f = window._dropFile || document.getElementById('file').files[0];
  if(!f){ alert('Selecione um arquivo primeiro.'); return; }
  const btn = document.getElementById('btn-import');
  const msg = document.getElementById('status-msg');
  btn.disabled = true; btn.textContent = 'Processando...';
  msg.textContent = 'Aguarde, importando ' + f.name + '...';
  try {
    const buf = await f.arrayBuffer();
    const bytes = new Uint8Array(buf);
    let binary = ''; bytes.forEach(b => binary += String.fromCharCode(b));
    const base64 = btoa(binary);
    const r = await fetch('/api/upload',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({fileName:f.name,fileBase64:base64})});
    const data = await r.json();
    if (data.error) { msg.textContent = 'Erro: ' + data.error; msg.style.color = 'var(--red)'; }
    else {
      msg.textContent = '';
      document.getElementById('result-section').style.display = '';
      document.getElementById('result-title').textContent = 'Importação concluída: ' + data.fileName;
      document.getElementById('result-cards').innerHTML = [
        ['Linhas importadas', data.importedRows, ''],
        ['Novos cadastros', data.foundNames, ''],
        ['Erros encontrados', data.pendingErrors, data.pendingErrors > 0 ? 'color:var(--red)' : 'color:var(--green)'],
        ['Alertas', data.alerts, data.alerts > 0 ? 'color:var(--amber)' : ''],
        ['Bloqueantes', data.blockingIssues, data.blockingIssues > 0 ? 'color:var(--red)' : 'color:var(--green)']
      ].map(([k,v,s]) => '<div class="card"><strong>'+k+'</strong><span style="'+s+'">'+v+'</span></div>').join('');
    }
  } catch(e) { msg.textContent = 'Erro: ' + e.message; msg.style.color = 'var(--red)'; }
  btn.disabled = false; btn.textContent = '\u{1F680}\u00a0 Importar planilha';
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
      saveDb(db);

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
      { title: 'Cancelado (ZZ) com valor', code: 'CANCELADO_COM_VALOR' },
      { title: 'Data inválida', code: 'DATA_INVALIDA' },
      { title: 'Valor inválido', code: 'VALOR_INVALIDO' }
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
      db.savedRules.push({ id: crypto.randomUUID(), type: 'alias', matchValue: source, targetValue: target, active: true });
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
    saveDb(db);
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
    saveDb(db);
    return json(res, 200, { ok: true, updated: set.size });
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
    db.manualAdjustments = db.manualAdjustments || [];
    db.manualAdjustments.push({
      id: crypto.randomUUID(),
      entryId: entry.id,
      changedAt: new Date().toISOString(),
      changes
    });
    saveDb(db);
    return json(res, 200, entry);
  }

  if (req.method === 'GET' && url.pathname === '/dashboard') {
    const metrics = calculateDashboard(db);
    const topItems = (obj) => Object.entries(obj).sort((a, b) => Math.abs(b[1]) - Math.abs(a[1])).slice(0, 10);
    const html = page('Dashboard', `<section><h2>Dashboard gerencial</h2>
<div class='cards'>
<a class='card' href='/dashboard/detalhe?view=saldo_hoje'><strong>Saldo de hoje</strong><span>R$ ${metrics.saldoHoje.toFixed(2)}</span></a>
<a class='card' href='/dashboard/detalhe?view=proj_7'><strong>Projeção 7 dias</strong><span>R$ ${metrics.proj7.toFixed(2)}</span></a>
<a class='card' href='/dashboard/detalhe?view=proj_30'><strong>Projeção 30 dias</strong><span>R$ ${metrics.proj30.toFixed(2)}</span></a>
<a class='card' href='/dashboard/detalhe?view=a_pagar'><strong>A pagar</strong><span>R$ ${metrics.contasPagar.toFixed(2)}</span></a>
<a class='card' href='/dashboard/detalhe?view=a_receber'><strong>A receber</strong><span>R$ ${metrics.contasReceber.toFixed(2)}</span></a>
<a class='card' href='/dashboard/detalhe?view=saldo_mutuo'><strong>Saldo de mútuo</strong><span>R$ ${metrics.saldoMutuo.toFixed(2)}</span></a>
<a class='card' href='/dashboard/detalhe?view=risco_caixa'><strong>Risco de caixa</strong><span>${metrics.riscoCaixa}</span></a>
</div>
<h3>Próximos 7 dias (agenda financeira)</h3><ul>${metrics.upcoming7.slice(0, 15).map((e) => `<li>${e.dataISO} | ${e.descricao || '-'} | R$ ${e.valor.toFixed(2)}</li>`).join('') || '<li>Sem lançamentos previstos.</li>'}</ul>
<h3>Resultado por cliente</h3><ul>${topItems(metrics.byClient).map(([k, v]) => `<li><a href='/dashboard/detalhe?view=cliente&chave=${encodeURIComponent(k)}'>${k}: R$ ${v.toFixed(2)}</a></li>`).join('')}</ul>
<h3>Resultado por projeto</h3><ul>${topItems(metrics.byProject).map(([k, v]) => `<li><a href='/dashboard/detalhe?view=projeto&chave=${encodeURIComponent(k)}'>${k}: R$ ${v.toFixed(2)}</a></li>`).join('')}</ul>
</section>`, user);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/dashboard/detalhe') {
    const view = url.searchParams.get('view') || '';
    const chave = url.searchParams.get('chave') || '';
    const today = new Date().toISOString().slice(0, 10);
    const plusDays = (days) => {
      const d = new Date(`${today}T00:00:00Z`);
      d.setUTCDate(d.getUTCDate() + days);
      return d.toISOString().slice(0, 10);
    };
    const d7 = plusDays(7);
    const d30 = plusDays(30);
    let title = 'Detalhamento';
    let list = [];
    if (view === 'a_pagar') {
      title = 'A pagar';
      list = db.entries.filter((e) => e.dataISO > today && e.valor < 0);
    } else if (view === 'a_receber') {
      title = 'A receber';
      list = db.entries.filter((e) => e.dataISO > today && e.valor > 0);
    } else if (view === 'proj_7') {
      title = 'Projeção 7 dias';
      list = db.entries.filter((e) => e.dataISO <= d7);
    } else if (view === 'proj_30') {
      title = 'Projeção 30 dias';
      list = db.entries.filter((e) => e.dataISO <= d30);
    } else if (view === 'saldo_hoje') {
      title = 'Saldo de hoje';
      list = db.entries.filter((e) => e.dataISO <= today);
    } else if (view === 'saldo_mutuo') {
      title = 'Saldo de mútuo';
      list = db.entries.filter((e) => normalizeName(`${e.descricao} ${e.tipoOriginal} ${e.natureza}`).includes('MUTUO') || normalizeName(`${e.descricao} ${e.tipoOriginal} ${e.natureza}`).includes('MÚTUO'));
    } else if (view === 'cliente') {
      title = `Resultado por cliente: ${chave}`;
      list = db.entries.filter((e) => (e.cliente || 'SEM CLIENTE') === chave);
    } else if (view === 'projeto') {
      title = `Resultado por projeto: ${chave}`;
      list = db.entries.filter((e) => (e.projeto || 'SEM PROJETO') === chave);
    } else if (view === 'risco_caixa') {
      title = 'Risco de caixa (projeções)';
      list = db.entries.filter((e) => e.dataISO <= d30);
    }
    const total = list.reduce((acc, e) => acc + Number(e.valor || 0), 0);
    const html = page('Detalhamento do dashboard', `<section><h2>${title}</h2><p>Total do recorte: <strong>R$ ${total.toFixed(2)}</strong></p><p><a href='/dashboard'>← Voltar ao dashboard</a></p>${entriesTable(list)}</section>`, user);
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
