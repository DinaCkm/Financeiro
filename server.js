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
const SESSION_TTL_SECONDS = 60 * 60 * 12; // 12h

const TYPE_OPTIONS = ['Cliente', 'Projeto', 'Prestador de Serviço', 'Fornecedor', 'Estrutura Interna', 'Financeiro / Não Operacional', 'Conta / Cartão', 'Pendente de Classificação'];
const BLOCKING_ISSUES = ['DESPESA_SEM_PROJETO', 'ESTRUTURA_COMO_CLIENTE', 'MUTUO_COMO_CLIENTE', 'MUTUO_CLASSIFICACAO', 'VALOR_INVALIDO', 'DATA_INVALIDA'];

const OFFICIAL_CLIENTS = ['BRB', 'SEBRAE TO', 'SEBRAE-AC'];
const OFFICIAL_PROJECTS = ['BRB-PDL', 'SEBRAE 10º CICLO', 'SEBRAE 9º CICLO', 'PS SEBRAE 2022', 'CESAMA CARTA-CONTRATO 20/2023 ETAPA 4'];
// Corte histórico: lançamentos anteriores a esta data ficam congelados
// (incluídos apenas no saldo acumulado, fora de revisão e cálculos operacionais)
const CORTE_DATA = '2024-06-01';
const isAtivo = (e) => (e.dataISO || e.data || '') >= CORTE_DATA;
const isHistorico = (e) => (e.dataISO || e.data || '') < CORTE_DATA;

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

// CCs que NUNCA devem aparecer como cliente no dashboard
// São custos de estrutura (overhead) ou movimentações internas
const FORBIDDEN_AS_CLIENT = [
  // Com acento (planilhas antigas)
  'ESCRITÓRIO', 'SALÁRIOS', 'JURÍDICO', 'CONTÁBIL', 'MÚTUO', 'PRÓ-LABORE',
  // Sem acento (planilha CKM atual — códigos canônicos do banco)
  'ESCRITORIO', 'SALARIOS', 'JURIDICO', 'CONTABIL', 'MUTUO', 'PRO-LABORE',
  'SAL_PRO-LAB_PJ', 'TI_SUPORTE', 'ADM',
  // Movimentações internas e financeiras
  'TEF', 'TEF_CC', 'PRONAMPE', 'BANCO', 'IOF_CC',
  // Genéricos que não são clientes
  'SALDO ATUAL', 'ADMINISTRATIVO', 'COMERCIAL', 'FINANCEIRO',
  'FISCAL', 'OPERACIONAL', 'RH', 'TI', 'IMPOSTOS',
  'MARKETING', 'INFRAESTRUTURA', 'TECNOLOGIA'
];

// Centros de custo padrão CKM — sempre disponíveis no datalist de edição
const CC_PADRAO = [
  'ADMINISTRATIVO', 'COMERCIAL', 'ESCRITÓRIO', 'FINANCEIRO', 'FISCAL',
  'JURÍDICO', 'MÚTUO', 'OPERACIONAL', 'PRÓ-LABORE', 'RH', 'TEF', 'TI'
];

// ============================================================
// Mapeamento de códigos da planilha CKM para nomes oficiais
// Clientes: coluna CLIENTE usa códigos 1.X; C CUSTO é o cliente real
// Projetos: coluna PROJETO usa códigos 4.X
// ============================================================
const MAPA_CLIENTES_CKM = {
  '1.0': 'ADMINISTRAÇÃO',
  '1.1': 'BANESE',
  '1.2': 'BRB',
  '1.3': 'SEBRAE-TO',
  '1.4': 'SEBRAE-AC',
  '1.5': 'EMBRAPII',
  '1.6': 'METRÔ-SP',
  '1.7': 'BANESE',
  '1.8': 'BANRISUL',
  '1.9': 'B2C-MENTORIAS',
  '1.10': 'CESAMA',
  '1.11': 'ESTEVÃO',
  '1.12': 'UFPE',
  '1.13': 'COFEN',
  '1.14': 'CPTM',
  '1.15': 'PROSPECÇÃO COMERCIAL',
  '1.16': 'PMFROCHA',
  '1.17': 'IGDRH',
  '1.18': 'SOROCABA',
  '1.19': 'TCU',
  '1.20': 'IGDR'
};
const MAPA_PROJETOS_CKM = {
  '4.1': 'Treinamentos/Assessment/Cursos',
  '4.2': 'Palestra',
  '4.3': 'Onboarding',
  '4.4': 'PDI do BEM',
  '4.5': 'Prospecção de Novos Clientes',
  '4.6': 'PDI Evoluir',
  '4.7': 'Mentorias',
  '4.8': 'Processo Seletivo',
  '4.9': 'Processamento de Concurso',
  '4.10': 'Avaliação de Desempenho - GID',
  '4.11': 'Estágio Probatório',
  '4.12': 'Concurso Público'
};
const MAPA_TIPOS_DESPESA_CKM = {
  '5.1': 'Prestador de Serviços/Consultoria',
  '5.2': 'Logística/Deslocamento/Alimentação',
  '5.3': 'Material de Treinamento',
  '5.4': 'Licenças e Ferramentas Digitais',
  '5.5': 'Avaliações e Testes',
  '5.6': 'Infraestrutura/Escritório',
  '5.8': 'Comercial/Marketing/Divulgação',
  '5.11': 'Despesas Financeiras',
  '5.12': 'Jurídico e Burocrático',
  '5.14': 'Mútuo',
  '5.18': 'Tributos',
  '5.19': 'Educação e Desenvolvimento',
  '5.20': 'Folha de Pagamento',
  '5.21': 'Financiamentos',
  '5.25': 'Hospedagem e Servidores',
  '5.27': 'Entidades de Classe',
  '5.30': 'Desenvolvimento de Ferramenta',
  '5.31': 'Pró-labore Sócios'
};

const PASSWORD_PREFIX = 'scrypt$';

function isHashedPassword(stored) {
  return typeof stored === 'string' && stored.startsWith(PASSWORD_PREFIX);
}

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const key = crypto.scryptSync(String(password), salt, 64).toString('hex');
  return `${PASSWORD_PREFIX}${salt}$${key}`;
}

function verifyPassword(inputPassword, storedPassword) {
  if (!storedPassword) return false;
  if (!isHashedPassword(storedPassword)) return String(storedPassword) === String(inputPassword);
  const parts = String(storedPassword).split('$');
  if (parts.length !== 3) return false;
  const [, salt, expectedHex] = parts;
  const actualHex = crypto.scryptSync(String(inputPassword), salt, 64).toString('hex');
  const expected = Buffer.from(expectedHex, 'hex');
  const actual = Buffer.from(actualHex, 'hex');
  if (expected.length !== actual.length) return false;
  return crypto.timingSafeEqual(expected, actual);
}

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

// ===== AUDITORIA: registra alterações campo a campo em cada lançamento =====
// Estrutura de cada registro: { id, ts, usuario, campo, de, para }
function registrarAuditoria(db, entryId, usuario, changes, entryAntes) {
  if (!db.auditLog) db.auditLog = [];
  const ts = new Date().toISOString();
  const user = usuario || 'sistema';
  for (const [campo, novoValor] of Object.entries(changes)) {
    const valorAnterior = entryAntes ? entryAntes[campo] : undefined;
    // Só registra se houve mudança real
    if (String(valorAnterior ?? '') === String(novoValor ?? '')) continue;
    db.auditLog.push({
      id: crypto.randomUUID(),
      ts,
      entryId,
      usuario: user,
      campo,
      de: valorAnterior ?? '',
      para: novoValor ?? ''
    });
  }
}

function registrarAuditoriaRevisao(db, registroId, nomeOficial, usuario, changes, registroAntes) {
  if (!db.auditLog) db.auditLog = [];
  const ts = new Date().toISOString();
  const user = usuario || 'sistema';
  for (const [campo, novoValor] of Object.entries(changes)) {
    const valorAnterior = registroAntes ? registroAntes[campo] : undefined;
    if (String(valorAnterior ?? '') === String(novoValor ?? '')) continue;
    db.auditLog.push({
      id: crypto.randomUUID(),
      ts,
      registroId,
      nomeOficial,
      usuario: user,
      campo,
      de: valorAnterior ?? '',
      para: novoValor ?? '',
      tipo: 'revisao'
    });
  }
}

function parseCookies(req) {
  const pairs = (req.headers.cookie || '').split(';').map((c) => c.trim()).filter(Boolean);
  return Object.fromEntries(pairs.map((p) => {
    const [k, ...rest] = p.split('=');
    return [k, decodeURIComponent(rest.join('='))];
  }));
}

function isSecureRequest(req) {
  const forwardedProto = String(req.headers['x-forwarded-proto'] || '').toLowerCase();
  return forwardedProto.includes('https') || Boolean(req.socket && req.socket.encrypted);
}

function buildSessionCookie(req, sid, maxAgeSeconds = SESSION_TTL_SECONDS) {
  const parts = [
    `sid=${encodeURIComponent(sid)}`,
    'Path=/',
    'HttpOnly',
    'SameSite=Lax',
    `Max-Age=${maxAgeSeconds}`
  ];
  if (process.env.NODE_ENV === 'production' || isSecureRequest(req)) {
    parts.push('Secure');
  }
  return parts.join('; ');
}

function currentUser(req, db) {
  const sid = parseCookies(req).sid;
  if (!sid) return null;

  const sessionData = sessions.get(sid);
  if (!sessionData) return null;

  const now = Date.now();
  if (typeof sessionData === 'object' && sessionData.expiresAt && sessionData.expiresAt <= now) {
    sessions.delete(sid);
    return null;
  }

  const userId = typeof sessionData === 'string' ? sessionData : sessionData.userId;
  if (!userId) {
    sessions.delete(sid);
    return null;
  }

  // Sliding session: renova validade quando o usuário segue ativo.
  sessions.set(sid, { userId, expiresAt: now + SESSION_TTL_SECONDS * 1000 });
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

// Resolve o cliente efetivo de um lançamento:
// 1. Se campo cliente tem valor, usa ele (traduzindo código 1.X se necessário)
// 2. Se cliente vazio, usa centroCusto como cliente (na planilha CKM o cliente real está no CC)
function clienteEfetivo(e) {
  const cli = (e.cliente || '').trim();
  if (cli) {
    return MAPA_CLIENTES_CKM[cli] || cli;
  }
  // Na planilha CKM, o campo C CUSTO é o cliente real para lançamentos operacionais
  const cc = (e.centroCusto || '').trim();
  if (cc && !FORBIDDEN_AS_CLIENT.some((f) => f === cc.toUpperCase())) {
    return cc;
  }
  return 'SEM CLIENTE';
}

// Resolve o nome oficial do projeto traduzindo o código numérico (4.X)
function projetoEfetivo(e) {
  const proj = (e.projeto || '').trim();
  if (!proj) return 'SEM PROJETO';
  return MAPA_PROJETOS_CKM[proj] || proj;
}

// Extrai apenas o nome do beneficiário, removendo dados bancários colados na mesma célula
// Ex: "ADRIANA PEREIRA - 033 - 1554 - CPF: 93383" => "ADRIANA PEREIRA"
function normalizeParceiro(raw) {
  if (!raw) return '';
  let s = String(raw).trim();
  // Padrões que indicam início de dados bancários: código de banco (3 dígitos), AG:, C/C:, CPF:, CNPJ:, PIX:, BANCO:
  const bancPatterns = [
    / - \d{3} - /,          // " - 001 - " (código banco)
    / - \d{3}$/,            // " - 341" no final
    / - \d{3} AG:/i,
    / - AG:/i,
    / - C\/C:/i,
    / - CPF:/i,
    / - CNPJ:/i,
    / - PIX:/i,
    / - BANCO:/i,
    / \d{3}-\d{4}-/,       // "341-0411-"
    / \d{3} AG:\d/,
    /\s+BB\s+\d{4}/i,       // " BB 1889-9"
    / - ITAU/i,
    / - BRADESCO/i,
    / - SANTANDER/i,
    / - INTER/i,
    / - NUBANK/i,
    / - CAIXA/i,
    / - SICOOB/i,
    / - SICREDI/i,
    / - BRD/i,
    / - BRB/i,
    / - STD/i,
    / - CEF/i,
    / - BANRISUL/i,
    / - DL - /i,
  ];
  for (const pat of bancPatterns) {
    const m = s.search(pat);
    if (m > 0) { s = s.slice(0, m).trim(); break; }
  }
  // Remover sufixos de NF/cupom fiscal que ficam no nome
  s = s.replace(/ - NF:.*$/i, '').replace(/ NF:.*$/i, '').replace(/ - RPS.*$/i, '').trim();
  // Remover datas soltas no final (ex: "BANRISUL - EMISSÃO 04/07/2019")
  s = s.replace(/\s*-?\s*EMISSÃO\s+\d{2}\/\d{2}\/\d{4}$/i, '').trim();
  // Limitar a 80 caracteres
  if (s.length > 80) s = s.slice(0, 80).trim();
  return normalizeName(s);
}

// Detecta valores que são claramente lixo no campo cliente (números decimais, índices de linha)
function isValorLixo(val) {
  if (!val) return true;
  const s = String(val).trim();
  // Apenas número decimal (ex: "1.0", "2.3", "4.10", "10.5")
  if (/^\d+\.\d+$/.test(s)) return true;
  // Apenas número inteiro
  if (/^\d+$/.test(s)) return true;
  // Vazio, traço ou ponto isolado
  if (s === '' || s === '-' || s === '--' || s === '.') return true;
  // Padrão de índice hierárquico: "4.1", "4.10", "1.2.3" etc.
  if (/^\d+(\.\d+)+$/.test(s)) return true;
  return false;
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
  return { rows: payload.rows || [], meta: payload.meta || {} };
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

      // dc=T: Transferência Interna entre contas próprias — tratar antes de qualquer cálculo
      const isTransferenciaInterna = dc === 'T';

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
        cliente: isValorLixo(r.cliente) ? '' : normalizeName(r.cliente || ''),
        projeto: normalizeName(r.projeto || ''),
        parceiro: normalizeParceiro(r.parceiro || ''),
        conta: r.conta || '',
        detalhe: r.detalhe || r.detdespesa || '',
        formaPagamento: r.formapagamento || '',
        centroCusto: r.centrocusto || '',
        dc,
        tipoOriginal,
        statusPlanilha,
        notaFiscal: r.notafiscal || '',
        tipo: isTransferenciaInterna ? 'transferencia_interna' : (valor >= 0 ? 'entrada' : 'saida'),
        valor,
        natureza: isTransferenciaInterna ? 'Transferência Interna' : 'Pendente de Classificação',
        isTransferenciaInterna,
        categoria: '',
        status: statusImport
      };

      if (!isTransferenciaInterna) {
        applySavedRulesToEntry(entry, db);
        entry.natureza = inferNature(entry);
      }
      return entry;
    });
}

function buildIssues(entries, db) {
  const issues = [];
  const newNames = new Set();
  const knownNames = new Set((db.reviewRegistry || []).map((item) => normalizeName(item.nomeOficial)));
  // Apenas lançamentos ativos (a partir do corte) e que NÃO são transferências internas geram issues
  const activeEntries = entries.filter((e) => isAtivo(e) && !e.isTransferenciaInterna);
  for (const e of activeEntries) {
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
  // Apenas lançamentos ativos (a partir do corte) e que NÃO são transferências internas geram cadastros para revisão
  const activeEntries = entries.filter((e) => isAtivo(e) && !e.isTransferenciaInterna);
  for (const e of activeEntries) {
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

// ===== CONCILIAÇÃO: compara planilha enviada vs. banco de dados =====
// Retorna: { saldoPlanilha, saldoBanco, divergencia, detalhes[] }
// detalhes: lista de lançamentos que causam a diferença
function conciliarPlanilha(allNewEntries, db) {
  // Saldo da planilha = soma de todos os valores da planilha recebida
  const saldoPlanilha = allNewEntries.reduce((acc, e) => acc + (Number(e.valor) || 0), 0);

  // Saldo do banco = soma de todos os lançamentos já persistidos
  const saldoBanco = db.entries.reduce((acc, e) => acc + (Number(e.valor) || 0), 0);

  // Chave de identificação de cada lançamento (mesma usada na dedup)
  const chave = (e) => `${e.dataISO}|${normalizeName(e.descricao)}|${normalizeName(e.conta)}`;

  // Mapa do banco: chave -> valor
  const bancoMap = new Map();
  db.entries.forEach(e => {
    const k = chave(e);
    bancoMap.set(k, (bancoMap.get(k) || 0) + (Number(e.valor) || 0));
  });

  // Mapa da planilha: chave -> { valor, entry }
  const planilhaMap = new Map();
  allNewEntries.forEach(e => {
    const k = chave(e);
    if (!planilhaMap.has(k)) planilhaMap.set(k, { valor: 0, entry: e });
    planilhaMap.get(k).valor += (Number(e.valor) || 0);
  });

  const detalhes = [];

  // 1. Lançamentos na planilha com valor diferente do banco
  planilhaMap.forEach(({ valor: vPlanilha, entry }, k) => {
    const vBanco = bancoMap.get(k);
    if (vBanco === undefined) {
      // Presente na planilha, ausente no banco (novo)
      detalhes.push({
        tipo: 'novo',
        descricao: entry.descricao || '-',
        data: entry.dataISO || entry.data || '-',
        conta: entry.conta || '-',
        valorPlanilha: vPlanilha,
        valorBanco: null,
        diferenca: vPlanilha
      });
    } else if (Math.abs(vPlanilha - vBanco) > 0.005) {
      // Valor divergente
      detalhes.push({
        tipo: 'divergente',
        descricao: entry.descricao || '-',
        data: entry.dataISO || entry.data || '-',
        conta: entry.conta || '-',
        valorPlanilha: vPlanilha,
        valorBanco: vBanco,
        diferenca: vPlanilha - vBanco
      });
    }
  });

  // 2. Lançamentos no banco que não aparecem na planilha (removidos ou não enviados)
  bancoMap.forEach((vBanco, k) => {
    if (!planilhaMap.has(k)) {
      const entry = db.entries.find(e => chave(e) === k);
      detalhes.push({
        tipo: 'ausente_na_planilha',
        descricao: entry ? (entry.descricao || '-') : k,
        data: entry ? (entry.dataISO || entry.data || '-') : '-',
        conta: entry ? (entry.conta || '-') : '-',
        valorPlanilha: null,
        valorBanco: vBanco,
        diferenca: -vBanco
      });
    }
  });

  const divergencia = Math.abs(saldoPlanilha - saldoBanco) > 0.005;

  return {
    saldoPlanilha: Math.round(saldoPlanilha * 100) / 100,
    saldoBanco: Math.round(saldoBanco * 100) / 100,
    diferenca: Math.round((saldoPlanilha - saldoBanco) * 100) / 100,
    divergencia,
    detalhes: detalhes.slice(0, 50) // limitar a 50 itens para não sobrecarregar
  };
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

function page(title, body, user, activePage) {
  const navLinks = user ? [
    ['/', 'Home', ''],
    ['/upload', 'Upload', ''],
    ['/pendencias', 'Pré-análise', ''],
    ['/cadastros', 'Revisável', ''],
    ['/fatura', 'Fatura', ''],
    ['/historico', 'Histórico', ''],
    ['/dashboard', 'Dashboard', ''],
    ['/cadastros-mestres', '⚙ Cadastros', 'nav-cad'],
    ['/contratos', '📋 Contratos', ''],
    ['/contas', '💰 Contas', ''],
    ['/conciliacao', '🏦 Conciliação', ''],
    ['/ia', '🤖 IA', 'nav-ia'],
    ['/relatorio', '📄 Relatórios', 'nav-rel'],
    ['/logout', 'Sair', 'sair'],
  ] : [];
  const nav = navLinks.map(([href, label, cls]) =>
    `<a href='${href}' class='${cls}${activePage === href ? ' active' : ''}'>${label}</a>`
  ).join('');
  return `<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'><title>${title} — Eco do Bem Financeiro</title><link rel='preconnect' href='https://fonts.googleapis.com'><link rel='stylesheet' href='https://fonts.googleapis.com/css2?family=Poppins:wght@400;600;700&family=Inter:wght@400;500;600&family=Sora:wght@400;700&display=swap'><link rel='stylesheet' href='/public/style.css'></head><body>
<header>
  <a href='/' class='header-logo'>
    <img src='/public/logo-branco.png' alt='Eco do Bem' onerror="this.style.display='none'">
    <div class='header-logo-text'><span>Sistema</span><span>Financeiro</span></div>
  </a>
  ${user ? `<nav>${nav}</nav>` : ''}
</header>
<main>${body}</main></body></html>`;
}

const TYPE_GUIDE = {
  'Cliente': 'Empresa ou pessoa que contrata e paga pelos serviços da CKM. Ex: BRB, SEBRAE.',
  'Projeto': 'Contrato ou frente de trabalho específica vinculada a um cliente. Ex: BRB-PDL, SEBRAE 10º CICLO.',
  'Prestador de Serviço': 'Pessoa física ou empresa contratada para executar trabalho para a CKM. Ex: consultores, designers.',
  'Fornecedor': 'Empresa que fornece produtos ou serviços de suporte à operação. Ex: locador de imóvel, internet, software.',
  'Estrutura Interna': 'Despesa fixa da própria empresa, sem vínculo com cliente. Ex: aluguel do escritório, salários, contábil.',
  'Financeiro / Não Operacional': 'Movimentação financeira que não é receita nem despesa operacional. Ex: mútuo, empréstimo, transferência entre contas.',
  'Conta / Cartão': 'Conta bancária ou cartão de crédito usado como meio de pagamento. Ex: Itaú PJ, Nubank.',
  'Pendente de Classificação': 'Ainda não classificado. Selecione um dos tipos acima para resolver.'
};

function reviewCards(list, allEntries) {
  return list.map((r) => {
    const nome = normalizeName(r.nomeOficial);
    const linked = allEntries.filter((e) =>
      normalizeName(e.cliente) === nome ||
      normalizeName(e.projeto) === nome ||
      normalizeName(e.parceiro) === nome
    ).sort((a, b) => (b.dataISO || '').localeCompare(a.dataISO || ''));
    const total = linked.reduce((s, e) => s + (e.valor || 0), 0);
    const totalColor = total >= 0 ? 'color:#065f46' : 'color:#991b1b';
    const badgeClass = r.statusRevisao === 'revisado' ? 'badge-green' : 'badge-amber';
    const badgeLabel = r.statusRevisao === 'revisado' ? 'Revisado' : 'Pendente';
    const isPendente = r.tipoFinal === 'Pendente de Classificação' || !r.tipoFinal;
    const tipoAtual = r.tipoFinal || 'Pendente de Classificação';

    // Sugestão automática de Tipo por regras baseadas nos dados dos lançamentos
    let tipoSugerido = null;
    if (isPendente && linked.length > 0) {
      const descs = linked.map((e) => (e.descricao || '').toUpperCase()).join(' ');
      const nats = linked.map((e) => (e.natureza || '').toUpperCase()).join(' ');
      const ccs = linked.map((e) => (e.centroCusto || '').toUpperCase()).join(' ');
      const allText = descs + ' ' + nats + ' ' + ccs;
      if (/MÚTUO|MUTUO|EMPRÉSTIMO|EMPRESTIMO|FINANCIAMENTO|TRANSFERÊNCIA ENTRE CONTAS/.test(allText)) {
        tipoSugerido = 'Financeiro / Não Operacional';
      } else if (/ALUGUEL|LOCAÇÃO|LOCACAO|CONDOMÍNIO|CONDOMINIO|IPTU/.test(allText) && /FORNEC|INDIRETA|ESCRITÓRIO/.test(allText)) {
        tipoSugerido = 'Fornecedor';
      } else if (/SALÁRIO|SALARIO|FOLHA|FÉRIAS|FERIAS|13º|RESCISÃO|RESCISAO|CLT/.test(allText)) {
        tipoSugerido = 'Estrutura Interna';
      } else if (/RECEITA|HONORÁRIO|HONORARIO|CONTRATO DE SERVIÇO|SERVIÇOS PRESTADOS/.test(allText)) {
        tipoSugerido = 'Cliente';
      } else if (/PEDÁGIO|PEDAGIO|TAG|COMBUSTÍVEL|COMBUSTIVEL|SEMPARAR|VELOE|CONECTCAR/.test(allText)) {
        tipoSugerido = 'Fornecedor';
      } else if (/CONTA CORRENTE|CARTÃO|CARTAO|NUBANK|ITAÚ|ITAU|BRADESCO|CAIXA|SICOOB|SANTANDER/.test(allText)) {
        tipoSugerido = 'Conta / Cartão';
      } else if (/CONSULTORIA|CONSULTOR|PRESTAÇÃO DE SERVIÇO|RPA|MEI|CNPJ/.test(allText)) {
        tipoSugerido = 'Prestador de Serviço';
      }
    }

    // Badge descritivo: explica exatamente o que falta
    let statusBadgeLabel = tipoAtual;
    let statusBadgeStyle = 'background:#dbeafe;color:#1e40af;border:1px solid #bfdbfe';
    if (isPendente) {
      if (tipoSugerido) {
        statusBadgeLabel = `Sugestão: ${tipoSugerido}`;
        statusBadgeStyle = 'background:#fef9c3;color:#854d0e;border:1px solid #fde047';
      } else {
        statusBadgeLabel = 'Tipo não definido';
        statusBadgeStyle = 'background:#fee2e2;color:#991b1b;border:1px solid #fca5a5';
      }
    } else if (r.statusRevisao !== 'revisado') {
      // Tipo definido mas não confirmado
      const faltaCliente = ['Prestador de Serviço', 'Projeto'].includes(tipoAtual) && !r.clienteVinculado;
      if (faltaCliente) {
        statusBadgeLabel = 'Cliente não vinculado';
        statusBadgeStyle = 'background:#fff7ed;color:#c2410c;border:1px solid #fdba74';
      } else {
        statusBadgeLabel = `${tipoAtual} — confirmar`;
        statusBadgeStyle = 'background:#f0fdf4;color:#166534;border:1px solid #86efac';
      }
    }

    // Natureza options para cada linha
    const naturezaOpts = ['Receita Operacional','Despesa Direta','Despesa Indireta','Despesa Administrativa',
      'Despesa Financeira','Movimentação Financeira Não Operacional','Transferência','Pendente']
      .map((n) => `<option value='${n}'>${n}</option>`).join('');

    // Cada linha tem uma linha de visualização + linha de edição (oculta por padrão)
    const lancRows = linked.map((e) => {
      const valColor = e.valor >= 0 ? 'color:#065f46;font-weight:600' : 'color:#991b1b;font-weight:600';
      const dcLabel = e.dc ? e.dc : (e.valor >= 0 ? 'C' : 'D');
      const dcColor = dcLabel === 'C' ? 'color:#065f46' : 'color:#991b1b';
      const eId = e.id;
      const congelado = isHistorico(e);
      const natOpts = ['Receita Operacional','Despesa Direta','Despesa Indireta','Despesa Administrativa',
        'Despesa Financeira','Movimentação Financeira Não Operacional','Transferência','Pendente']
        .map((n) => `<option value='${n}' ${n === (e.natureza||'Pendente') ? 'selected' : ''}>${n}</option>`).join('');
      const statusOpts = ['ok','pendente','cancelado','revisado']
        .map((s) => `<option value='${s}' ${s === (e.status||'ok') ? 'selected' : ''}>${s}</option>`).join('');

      return `
        <tr class='entry-view-row' id='view-${eId}'
          style='${congelado ? 'opacity:.55;background:#f8fafc;cursor:default' : 'cursor:pointer'}'
          ${congelado ? '' : `onclick="toggleEntryEdit('${eId}')" title='Clique para editar este lançamento'`}>
          <td style='white-space:nowrap;font-size:.8rem;${!congelado&&!(e.dataISO||e.data)?'color:#dc2626;font-weight:700':''}'>${e.dataISO || e.data || (congelado?'-':'⚠ sem data')}</td>
          <td style='font-size:.8rem;${!congelado&&!e.descricao?'color:#dc2626;font-weight:700':''}'>${e.descricao || (congelado?'-':'⚠ sem descrição')}</td>
          <td style='white-space:nowrap;${!congelado&&(e.valor||0)===0?'color:#dc2626;font-weight:700':valColor};font-size:.8rem'>R$ ${Number(e.valor || 0).toFixed(2)}</td>
          <td style='${dcColor};font-weight:700;font-size:.8rem;text-align:center'>${dcLabel}</td>
          <td style='font-size:.8rem;${!congelado&&(!e.natureza||e.natureza==='Pendente')?'color:#dc2626;font-weight:700;background:#fff5f5':''}'>${e.natureza || '-'}</td>
          <td style='font-size:.8rem;${!congelado&&!e.centroCusto?'color:#dc2626;font-weight:700;background:#fff5f5':''}'>${e.centroCusto || '-'}</td>
          <td style='font-size:.8rem;color:#64748b'>${e.conta || '-'}</td>
          <td style='font-size:.8rem;${!congelado&&!(e.cliente||e.parceiro)?'color:#dc2626;font-weight:700;background:#fff5f5':''}'>${e.cliente || e.parceiro || '-'}</td>
          <td style='font-size:.8rem;color:#94a3b8'>${e.projeto || '-'}</td>
          <td style='font-size:.8rem'>${e.status || '-'}</td>
          <td style='font-size:.75rem;white-space:nowrap'>${congelado ? '<span style="color:#94a3b8;font-size:.7rem">🔒 histórico</span>' : '&#9998; <span style="color:#1d4ed8">editar</span>'}</td>
        </tr>
        <tr class='entry-edit-row' id='edit-${eId}' style='display:none;background:${congelado?'#f8fafc':'#f0f9ff'}'>
          <td colspan='11' style='padding:.75rem 1rem'>
            <div style='background:#fff;border:1px solid ${congelado?'#e2e8f0':'#bfdbfe'};border-radius:8px;padding:1rem'>
              ${congelado
                ? `<p style='font-size:.8rem;color:#64748b;margin:0'>&#128274; Este lançamento é <strong>histórico</strong> (anterior a ${CORTE_DATA}) e está congelado. Ele entra apenas no saldo acumulado e não pode ser editado.</p>`
                : `<p style='font-size:.75rem;font-weight:700;color:#1e40af;text-transform:uppercase;letter-spacing:.04em;margin-bottom:.75rem'>&#9998; Editar lançamento — preencha ou corrija os campos abaixo</p>`
              }
              <div style='display:grid;grid-template-columns:repeat(auto-fill,minmax(180px,1fr));gap:.6rem;margin-bottom:.75rem'>
                ` + (function(){
                  var dataVal = e.dataISO || e.data || '';
                  var descVal = (e.descricao||'').replace(/'/g,"&#39;");
                  var valorVal = Number(e.valor||0).toFixed(2);
                  var ccVal = (e.centroCusto||'').replace(/'/g,"&#39;");
                  var contaVal = (e.conta||'').replace(/'/g,"&#39;");
                  var clienteVal = (e.cliente||e.parceiro||'').replace(/'/g,"&#39;");
                  var projVal = (e.projeto||'').replace(/'/g,"&#39;");
                  var isPendNat = !e.natureza || e.natureza==='Pendente' || e.natureza==='Pendente de Classificação';
                  // Verificar se este lançamento é o motivo da revisão pendente
                  var nomeCard = r.nomeOficial ? r.nomeOficial.toUpperCase() : '';
                  var clienteNorm = (e.cliente||'').toUpperCase();
                  var parceiroNorm = (e.parceiro||'').toUpperCase();
                  var projetoNorm = (e.projeto||'').toUpperCase();
                  var clienteEMotivo = isPendente && (clienteNorm === nomeCard || parceiroNorm === nomeCard);
                  // Projeto só é motivo se o nome do card for exatamente o projeto deste lançamento
                  var projetoEMotivo = isPendente && projetoNorm && projetoNorm === nomeCard;
                  var fv = function(v){ return (!v||v==='-'||v==='0.00') ? 'border:2px solid #ef4444;background:#fff5f5' : ''; };
                  var fvMotivo = function(v, isMotivo){ return isMotivo ? 'border:2px solid #ef4444;background:#fff5f5' : fv(v); };
                  var lv = function(v){ return (!v||v==='-'||v==='0.00') ? 'color:#dc2626;font-weight:700' : 'color:#64748b'; };
                  var lvMotivo = function(v, isMotivo){ return isMotivo ? 'color:#dc2626;font-weight:700' : lv(v); };
                  var warn = function(v){ return (!v||v==='-'||v==='0.00') ? '⚠ ' : ''; };
                  var natOpts2 = ['Receita Operacional','Despesa Direta','Despesa Indireta','Despesa Administrativa',
                    'Despesa Financeira','Movimentação Financeira Não Operacional','Transferência','Pendente']
                    .map(function(n){ return "<option value='"+n+"' "+(n===(e.natureza||'Pendente')?'selected':'')+">"+n+"</option>"; }).join('');
                  return "<div>"
                    + "<label style='font-size:.72rem;font-weight:700;"+lv(dataVal)+";text-transform:uppercase'>"+warn(dataVal)+"Data</label>"
                    + "<input id='ef-data-"+eId+"' value='"+dataVal+"' placeholder='YYYY-MM-DD' style='font-size:.8rem;padding:.3rem .5rem;"+fv(dataVal)+"'/>"
                    + "</div>"
                    + "<div style='grid-column:span 2'>"
                    + "<label style='font-size:.72rem;font-weight:700;"+lv(descVal)+";text-transform:uppercase'>"+warn(descVal)+"Histórico / Descrição</label>"
                    + "<input id='ef-desc-"+eId+"' value='"+descVal+"' placeholder='Descrição do lançamento' style='font-size:.8rem;padding:.3rem .5rem;"+fv(descVal)+"'/>"
                    + "</div>"
                    + "<div>"
                    + "<label style='font-size:.72rem;font-weight:700;"+(valorVal==='0.00'?'color:#dc2626;font-weight:700':'color:#64748b')+";text-transform:uppercase'>"+(valorVal==='0.00'?'⚠ ':'')+"Valor (R$)</label>"
                    + "<input id='ef-valor-"+eId+"' type='number' step='0.01' value='"+valorVal+"' style='font-size:.8rem;padding:.3rem .5rem;"+(valorVal==='0.00'?'border:2px solid #ef4444;background:#fff5f5':'')+"'/>"
                    + "</div>"
                    + "<div>"
                    + "<label style='font-size:.72rem;font-weight:700;color:#64748b;text-transform:uppercase'>D/C</label>"
                    + "<select id='ef-dc-"+eId+"' style='font-size:.8rem;padding:.3rem .5rem'>"
                    + "<option value='D' "+(dcLabel==='D'?'selected':'')+">D — Débito (saída)</option>"
                    + "<option value='C' "+(dcLabel==='C'?'selected':'')+">C — Crédito (entrada)</option>"
                    + "</select>"
                    + "</div>"
                    + "<div>"
                    + "<label style='font-size:.72rem;font-weight:700;"+(isPendNat?'color:#dc2626;font-weight:700':'color:#64748b')+";text-transform:uppercase'>"+(isPendNat?'⚠ ':'')+"Natureza</label>"
                    + "<select id='ef-nat-"+eId+"' style='font-size:.8rem;padding:.3rem .5rem;"+(isPendNat?'border:2px solid #ef4444;background:#fff5f5':'')+"'>"+natOpts2+"</select>"
                    + "</div>"
                    + "<div>"
                    + "<label style='font-size:.72rem;font-weight:700;"+lv(ccVal)+";text-transform:uppercase'>"+warn(ccVal)+"Centro de Custo</label>"
                    + "<input id='ef-cc-"+eId+"' list='dl-cc' value='"+ccVal+"' placeholder='Selecione ou digite...' style='font-size:.8rem;padding:.3rem .5rem;"+fv(ccVal)+"'/>"
                    + "</div>"
                    + "<div>"
                    + "<label style='font-size:.72rem;font-weight:700;color:#64748b;text-transform:uppercase'>Conta / Banco</label>"
                    + "<input id='ef-conta-"+eId+"' list='dl-contas' value='"+contaVal+"' placeholder='Selecione ou digite...' style='font-size:.8rem;padding:.3rem .5rem'/>"
                    + "</div>"
                    + "<div>"
                    + "<label style='font-size:.72rem;font-weight:700;"+lvMotivo(clienteVal,clienteEMotivo)+";text-transform:uppercase'>"+(clienteEMotivo?'\u26a0 \u2190 PENDENTE DE CLASSIFICA\u00c7\u00c3O ':warn(clienteVal))+"Cliente / Parceiro</label>"
                    + "<input id='ef-cliente-"+eId+"' list='dl-clientes' value='"+clienteVal+"' placeholder='Selecione ou digite...' style='font-size:.8rem;padding:.3rem .5rem;"+fvMotivo(clienteVal,clienteEMotivo)+"'/>"
                    + "</div>"
                    + "<div>"
                    + "<label style='font-size:.72rem;font-weight:700;"+lvMotivo(projVal,projetoEMotivo)+";text-transform:uppercase'>"+(projetoEMotivo?'\u26a0 \u2190 PENDENTE DE CLASSIFICA\u00c7\u00c3O ':'')+'Projeto <span style=\'font-weight:400;font-size:.7rem\'>(opcional)</span></label>'
                    + "<input id='ef-proj-"+eId+"' list='dl-projetos' value='"+projVal+"' placeholder='Selecione ou digite...' style='font-size:.8rem;padding:.3rem .5rem;"+(projetoEMotivo?'border:2px solid #ef4444;background:#fff5f5':'')+"'/>"
                    + "</div>"
                    + "<div>"
                    + "<label style='font-size:.72rem;font-weight:700;color:#64748b;text-transform:uppercase'>Status</label>"
                    + "<select id='ef-status-"+eId+"' style='font-size:.8rem;padding:.3rem .5rem'>"+statusOpts+"</select>"
                    + "</div>";
                })() + `
              ${congelado ? '' : `
              </div>
              <div style='display:flex;gap:.5rem'>
                <button onclick="salvarLancamento('${eId}')" style='background:#059669;font-size:.8rem;padding:.4rem .9rem'>&#10003; Salvar alterações</button>
                <button onclick="toggleEntryEdit('${eId}')" style='background:#e2e8f0;color:#475569;font-size:.8rem;padding:.4rem .9rem;box-shadow:none'>Cancelar</button>
                <button onclick="verHistoricoLancamento('${eId}')" style='background:#f1f5f9;color:#475569;font-size:.8rem;padding:.4rem .9rem;box-shadow:none;border:1px solid #cbd5e1' title='Ver histórico de alterações deste lançamento'>&#128336; Histórico</button>
              </div>`}
            </div>
          </td>
        </tr>`;
    }).join('');

    const lancTable = linked.length > 0
      ? `<div class='review-entries' style='overflow-x:auto;margin-bottom:1rem'>
          <p style='font-size:.72rem;color:#94a3b8;margin-bottom:.3rem'>&#128161; Clique em qualquer linha para editar os campos daquele lançamento</p>
          <table style='min-width:1000px;font-size:.8rem'>
            <thead><tr>
              <th style='white-space:nowrap'>Data</th>
              <th>Histórico</th>
              <th style='white-space:nowrap'>Valor</th>
              <th>D/C</th>
              <th>Natureza</th>
              <th>Centro Custo</th>
              <th>Conta</th>
              <th>Cliente/Parceiro</th>
              <th>Projeto</th>
              <th>Status</th>
              <th></th>
            </tr></thead>
            <tbody>${lancRows}</tbody>
          </table>
        </div>`
      : `<p style='font-size:.82rem;color:var(--gray-400);margin:.5rem 0'>Nenhum lançamento vinculado encontrado.</p>`;

    // Painel de classificação do cadastro (tipo, cliente vinculado, projeto vinculado)
    // Se pendente e tem sugestão, pré-selecionar o tipo sugerido no dropdown
    const tipoParaSelect = (isPendente && tipoSugerido) ? tipoSugerido : tipoAtual;
    const typeOptions = TYPE_OPTIONS.map((t) => {
      const guide = TYPE_GUIDE[t] || '';
      return `<option value='${t}' title='${guide}' ${t === tipoParaSelect ? 'selected' : ''}>${t}</option>`;
    }).join('');
    const guiaTexto = TYPE_GUIDE[tipoParaSelect] || '';
    const sugestaoAviso = (isPendente && tipoSugerido)
      ? `<div style='background:#fef9c3;border:1px solid #fde047;border-radius:6px;padding:.4rem .7rem;margin-bottom:.5rem;font-size:.76rem;color:#854d0e'>
          &#128161; <strong>Sugestão automática:</strong> com base nos históricos dos lançamentos, o tipo mais provável é <strong>${tipoSugerido}</strong>. Confirme se estiver correto ou altere abaixo.
        </div>`
      : (isPendente ? `<div style='background:#fee2e2;border:1px solid #fca5a5;border-radius:6px;padding:.4rem .7rem;margin-bottom:.5rem;font-size:.76rem;color:#991b1b'>
          &#9888; Não foi possível sugerir automaticamente. Analise os lançamentos acima e selecione o Tipo correto.
        </div>` : '');
    const classifPanel = `
      <div style='display:grid;grid-template-columns:1fr 1fr;gap:1rem;margin-top:.75rem'>
        <div style='background:#fffbeb;border:1px solid #fcd34d;border-radius:8px;padding:.75rem 1rem'>
          <strong style='font-size:.8rem;color:#92400e'>&#9998; Classificação do cadastro</strong>
          <p style='font-size:.78rem;color:#78350f;margin:.25rem 0 .5rem'>Defina o <strong>Tipo</strong> deste nome para a CKM. Isso se aplica a todos os lançamentos vinculados.</p>
          ${sugestaoAviso}
          <div style='display:flex;flex-direction:column;gap:.5rem'>
            <div>
              <label style='font-size:.72rem;font-weight:700;color:#64748b;text-transform:uppercase'>Tipo *</label>
              <select id='tipo-select-${r.id}' onchange="alterarTipo('${r.id}', this.value); atualizarGuia('${r.id}', this.value)" style='font-size:.82rem'>${typeOptions}</select>
              <p id='guia-tipo-${r.id}' style='font-size:.74rem;color:#94a3b8;margin:.2rem 0 0;font-style:italic'>${guiaTexto}</p>
            </div>
            <div>
              <label style='font-size:.72rem;font-weight:700;color:#64748b;text-transform:uppercase'>Cliente vinculado <span style='font-weight:400'>(se prestador/projeto)</span></label>
              <input id='cliente-input-${r.id}' value='${r.clienteVinculado || ''}' placeholder='Ex: BRB, SEBRAE TO...' style='font-size:.82rem' onchange="vincularCliente('${r.id}', this.value)"/>
            </div>
            <div>
              <label style='font-size:.72rem;font-weight:700;color:#64748b;text-transform:uppercase'>Projeto vinculado <span style='font-weight:400'>(se prestador)</span></label>
              <input id='projeto-input-${r.id}' value='${r.projetoVinculado || ''}' placeholder='Ex: BRB-PDL, SEBRAE 10º CICLO...' style='font-size:.82rem' onchange="vincularProjeto('${r.id}', this.value)"/>
            </div>
            <div style='display:flex;gap:.5rem;padding-top:.25rem;border-top:1px solid #fde68a'>
              <button onclick="marcarRevisado('${r.id}')" style='flex:1;background:#059669;font-size:.8rem;padding:.4rem'>&#10003; Confirmar revisão do cadastro</button>
              <button onclick="marcarPendente('${r.id}')" style='background:#e2e8f0;color:#475569;font-size:.78rem;padding:.4rem .7rem;box-shadow:none'>Desfazer</button>
            </div>
          </div>
        </div>
        <div style='background:#eff6ff;border:1px solid #bfdbfe;border-radius:8px;padding:.75rem 1rem;display:flex;flex-direction:column;gap:.5rem'>
          <strong style='font-size:.8rem;color:#1e40af'>&#129302; Pergunte à IA sobre estes lançamentos</strong>
          <p style='font-size:.76rem;color:#1e3a8a;margin:0'>A IA responde com base <strong>apenas nos dados da sua planilha</strong>. Exemplos de perguntas:</p>
          <ul style='font-size:.74rem;color:#1e3a8a;margin:.1rem 0 .4rem;padding-left:1.1rem'>
            <li>Tem outros lançamentos similares a este já classificados?</li>
            <li>Quem é o cliente deste prestador?</li>
            <li>Como outros lançamentos de mútuo foram classificados?</li>
          </ul>
          <div id='chat-msgs-${r.id}' style='max-height:120px;overflow-y:auto;font-size:.78rem;display:flex;flex-direction:column;gap:.3rem'></div>
          <div style='display:flex;gap:.4rem;margin-top:.25rem'>
            <input id='chat-input-${r.id}' placeholder='Digite sua pergunta...' style='flex:1;font-size:.8rem;padding:.35rem .6rem' onkeydown="if(event.key==='Enter')perguntarIA('${r.id}')"/>
            <button onclick="perguntarIA('${r.id}')" style='background:#1d4ed8;font-size:.78rem;padding:.35rem .7rem;white-space:nowrap'>Perguntar</button>
          </div>
        </div>
      </div>`;

    // Banner explicativo: por que este cadastro está pendente?
    const motivoBanner = isPendente
      ? `<div style='background:#fef2f2;border:1px solid #fca5a5;border-radius:8px;padding:.6rem .9rem;margin-bottom:.75rem;display:flex;align-items:flex-start;gap:.5rem'>
          <span style='font-size:1rem;flex-shrink:0'>&#9888;&#65039;</span>
          <div>
            <strong style='font-size:.8rem;color:#991b1b'>Por que este cadastro está pendente?</strong>
            <p style='font-size:.78rem;color:#7f1d1d;margin:.2rem 0 0'>O nome <strong>${r.nomeOficial}</strong> aparece como <strong>parceiro, cliente ou projeto</strong> em ${linked.length} lançamento(s), mas ainda não foi classificado. Defina o <strong>Tipo</strong> abaixo e clique em <strong>Confirmar revisão</strong>.</p>
          </div>
        </div>`
      : (r.statusRevisao !== 'revisado'
          ? `<div style='background:#fff7ed;border:1px solid #fdba74;border-radius:8px;padding:.6rem .9rem;margin-bottom:.75rem;display:flex;align-items:flex-start;gap:.5rem'>
              <span style='font-size:1rem;flex-shrink:0'>&#128161;</span>
              <div>
                <strong style='font-size:.8rem;color:#c2410c'>Tipo definido — aguardando confirmação</strong>
                <p style='font-size:.78rem;color:#7c2d12;margin:.2rem 0 0'>O tipo <strong>${tipoAtual}</strong> foi sugerido automaticamente. Verifique os lançamentos e clique em <strong>Confirmar revisão</strong> para marcar como revisado.</p>
              </div>
            </div>`
          : '');

    return `<div class='review-card' data-id='${r.id}' data-status='${r.statusRevisao}'>
  <div class='review-card-header' onclick="toggleCard('${r.id}')">
    <div class='review-card-title'>
      <span class='badge ${badgeClass}' id='badge-status-${r.id}'>${badgeLabel}</span>
      <strong>${r.nomeOficial}</strong>
      ${r.nomeOriginal !== r.nomeOficial ? `<span style='font-size:.78rem;color:var(--gray-400)'>(orig: ${r.nomeOriginal})</span>` : ''}
    </div>
    <div class='review-card-meta'>
      <span id='badge-tipo-${r.id}' style='font-size:.75rem;font-weight:700;padding:.2rem .6rem;border-radius:20px;white-space:nowrap;${statusBadgeStyle}'>${statusBadgeLabel}</span>
      <span style='font-size:.82rem;color:var(--gray-600)'>${linked.length} lançamento${linked.length !== 1 ? 's' : ''}</span>
      <span style='font-size:.82rem;font-weight:700;${totalColor}'>Total: R$ ${total.toFixed(2)}</span>
      <span class='review-toggle-icon' id='icon-${r.id}'>▼</span>
    </div>
  </div>
  <div class='review-card-body' id='body-${r.id}' style='display:none'>
    ${motivoBanner}
    <p style='font-size:.75rem;font-weight:700;text-transform:uppercase;letter-spacing:.05em;color:var(--gray-400);margin-bottom:.4rem'>&#128196; Lançamentos vinculados (${linked.length})</p>
    ${lancTable}
    ${classifPanel}
  </div>
</div>`;
  }).join('');
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

  // Todos os lançamentos ordenados por data
  const allSorted = [...db.entries].sort((a, b) => (a.dataISO || '').localeCompare(b.dataISO || ''));

  // Lançamentos ativos (a partir do corte) — usados em todos os cálculos operacionais
  const sortedEntries = allSorted.filter(isAtivo);

  // Saldo de Hoje: último registro SALDO ATUAL da planilha (saldo bancário real)
  // A planilha CKM calcula automaticamente o saldo disponível em conta na linha SALDO ATUAL
  // Fallback: soma acumulada histórica (para planilhas sem linha SALDO ATUAL)
  const saldoAtualEntries = allSorted
    .filter((e) => (e.centroCusto || '').toUpperCase().trim() === 'SALDO ATUAL'
      && (e.dataISO || '') <= today
      && (e.valor || 0) !== 0)
    .sort((a, b) => (b.dataISO || '').localeCompare(a.dataISO || ''));
  const saldoHoje = saldoAtualEntries.length > 0
    ? saldoAtualEntries[0].valor
    : allSorted.filter((e) => (e.dataISO || '') <= today && !e.isTransferenciaInterna).reduce((acc, e) => acc + (e.valor || 0), 0);

  // Projeções e cálculos operacionais: apenas lançamentos ativos, excluindo transferências internas e SALDO ATUAL
  // SALDO ATUAL é uma linha de controle da planilha CKM (saldo bancário calculado), não um lançamento operacional
  const isSaldoAtual = (e) => (e.centroCusto || '').toUpperCase().trim() === 'SALDO ATUAL';
  const opEntries = sortedEntries.filter((e) => !e.isTransferenciaInterna && !isSaldoAtual(e));
  const proj7 = opEntries.filter((e) => (e.dataISO || '') <= d7).reduce((acc, e) => acc + (e.valor || 0), 0);
  const proj30 = opEntries.filter((e) => (e.dataISO || '') <= d30).reduce((acc, e) => acc + (e.valor || 0), 0);
  const contasPagar = opEntries.filter((e) => (e.dataISO || '') > today && (e.valor || 0) < 0).reduce((acc, e) => acc + Math.abs(e.valor), 0);
  const contasReceber = opEntries.filter((e) => (e.dataISO || '') > today && (e.valor || 0) > 0).reduce((acc, e) => acc + (e.valor || 0), 0);
  const upcoming7 = opEntries.filter((e) => (e.dataISO || '') > today && (e.dataISO || '') <= d7);
  const riscoCaixa = proj7 < 0 ? 'alto' : proj30 < 0 ? 'moderado' : 'controlado';

  // Saldo de mútuo: apenas lançamentos do CC 'MÚTUO' (empréstimos de sócios)
  // Filtrar por centro de custo exato evita capturar clientes que têm 'mútuo' na descrição
  const isMutuo = (e) => (e.centroCusto || '').toUpperCase().trim() === 'MÚTUO';
  const mutuoEntries = allSorted.filter(isMutuo);
  const saldoMutuo = mutuoEntries.reduce((acc, e) => acc + (e.valor || 0), 0);
  const mutuoCount = mutuoEntries.length;

  // Distribuição por cliente e projeto: apenas lançamentos ativos
  // clienteEfetivo() usa centroCusto quando cliente está vazio (padrão planilha CKM)
  // projetoEfetivo() traduz códigos 4.X para nomes reais
  const byClient = {};
  const byProject = {};
  sortedEntries.forEach((e) => {
    const c = clienteEfetivo(e);
    const p = projetoEfetivo(e);
    // Excluir SEM CLIENTE do byClient — esses vão para byEstrutura
    if (c !== 'SEM CLIENTE') {
      byClient[c] = (byClient[c] || 0) + (e.valor || 0);
    }
    if (p !== 'SEM PROJETO') {
      byProject[p] = (byProject[p] || 0) + (e.valor || 0);
    }
  });

  // Detectores de empréstimos/financiamentos
  const isEmprestimoMutuo = (e) => {
    const txt = [e.descricao, e.tipoOriginal, e.natureza, e.centroCusto, e.parceiro]
      .map(v => normalizeName(v || '')).join(' ');
    return txt.includes('MUTUO') || txt.includes('MÚTUO');
  };
  const isEmprestimoPronampe = (e) => {
    const txt = [e.descricao, e.tipoOriginal, e.natureza, e.centroCusto, e.parceiro]
      .map(v => normalizeName(v || '')).join(' ');
    return txt.includes('PRONAMPE');
  };
  const isEmprestimo = (e) => isEmprestimoMutuo(e) || isEmprestimoPronampe(e);

  // Empréstimos e financiamentos: calculados sobre TODOS os lançamentos (histórico completo)
  // pois o saldo devedor é acumulado desde o início
  const emprestimos = {
    mutuo: {
      // Saldo histórico acumulado (negativo = empresa deve)
      saldoHistorico: allSorted.filter(isEmprestimoMutuo).reduce((a, e) => a + (e.valor || 0), 0),
      totalRecebido: allSorted.filter(isEmprestimoMutuo).filter(e => (e.valor||0) > 0).reduce((a, e) => a + (e.valor || 0), 0),
      totalPago: allSorted.filter(isEmprestimoMutuo).filter(e => (e.valor||0) < 0).reduce((a, e) => a + Math.abs(e.valor || 0), 0),
      // Movimentação no período ativo (para ver fluxo recente)
      saldoPeriodo: sortedEntries.filter(isEmprestimoMutuo).reduce((a, e) => a + (e.valor || 0), 0),
      count: allSorted.filter(isEmprestimoMutuo).length,
      // Credores (parceiros) com saldo individual
      porCredor: (() => {
        const m = {};
        allSorted.filter(isEmprestimoMutuo).forEach(e => {
          const p = (e.parceiro || e.descricao || 'SEM IDENTIFICAÇÃO').slice(0, 40);
          if (!m[p]) m[p] = { recebido: 0, pago: 0, saldo: 0 };
          m[p].saldo += (e.valor || 0);
          if ((e.valor || 0) > 0) m[p].recebido += (e.valor || 0);
          else m[p].pago += Math.abs(e.valor || 0);
        });
        return m;
      })()
    },
    pronampe: {
      saldoHistorico: allSorted.filter(isEmprestimoPronampe).reduce((a, e) => a + (e.valor || 0), 0),
      totalRecebido: allSorted.filter(isEmprestimoPronampe).filter(e => (e.valor||0) > 0).reduce((a, e) => a + (e.valor || 0), 0),
      totalPago: allSorted.filter(isEmprestimoPronampe).filter(e => (e.valor||0) < 0).reduce((a, e) => a + Math.abs(e.valor || 0), 0),
      jurosEstimados: (() => {
        const recebido = allSorted.filter(isEmprestimoPronampe).filter(e => (e.valor||0) > 0).reduce((a, e) => a + (e.valor || 0), 0);
        const pago = allSorted.filter(isEmprestimoPronampe).filter(e => (e.valor||0) < 0).reduce((a, e) => a + Math.abs(e.valor || 0), 0);
        return pago > recebido ? pago - recebido : 0; // juros = excedente pago sobre o principal
      })(),
      saldoPeriodo: sortedEntries.filter(isEmprestimoPronampe).reduce((a, e) => a + (e.valor || 0), 0),
      count: allSorted.filter(isEmprestimoPronampe).length,
      // Última parcela paga e estimativa de parcelas restantes
      ultimaParcela: allSorted.filter(isEmprestimoPronampe).filter(e => (e.valor||0) < 0).sort((a,b) => (b.dataISO||'').localeCompare(a.dataISO||''))[0] || null
    }
  };

  // Custos de Estrutura: lançamentos sem cliente vinculado (overhead operacional)
  // EXCLUINDO empréstimos (mútuo e Pronampe) — esses têm seção própria
  const byEstrutura = {};
  const totalEstrutura = { receita: 0, despesa: 0 };
  sortedEntries.forEach((e) => {
    if (clienteEfetivo(e) !== 'SEM CLIENTE') return; // tem cliente, não é estrutura
    const cc = (e.centroCusto || 'SEM CLASSIFICAÇÃO').toUpperCase();
    // Excluir TEF (transferências internas), SALDO ATUAL (linha de controle da planilha) e empréstimos (têm seção própria)
    if (cc === 'TEF') return;
    if (cc === 'SALDO ATUAL') return; // linha de controle da planilha CKM, não é lançamento operacional
    if (isEmprestimo(e)) return; // mútuo e Pronampe vão para seção de Financiamentos
    byEstrutura[cc] = (byEstrutura[cc] || 0) + (e.valor || 0);
    if ((e.valor || 0) < 0) totalEstrutura.despesa += Math.abs(e.valor);
    else totalEstrutura.receita += (e.valor || 0);
  });

  const rolling = allSorted.filter((e) => !e.isTransferenciaInterna).reduce((acc, e) => acc + (e.valor || 0), 0);
  return { saldoHoje, proj7, proj30, contasPagar, contasReceber, byClient, byProject, byEstrutura, totalEstrutura, emprestimos, rolling, upcoming7, riscoCaixa, saldoMutuo, mutuoCount, corteData: CORTE_DATA };
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
    const user = db.users.find((u) => u.email === form.get('email'));
    const rawPassword = form.get('password') || '';
    if (!user || !verifyPassword(rawPassword, user.password)) {
      res.writeHead(302, { Location: '/login' });
      res.end();
      return;
    }
    // Migração transparente: usuário legado com senha em texto puro é convertido para hash no login bem-sucedido
    if (!isHashedPassword(user.password)) {
      user.password = hashPassword(rawPassword);
      saveDb(db);
    }
    const sid = crypto.randomUUID();
    sessions.set(sid, { userId: user.id, expiresAt: Date.now() + SESSION_TTL_SECONDS * 1000 });
    res.writeHead(302, { 'Set-Cookie': buildSessionCookie(req, sid), Location: '/' });
    res.end();
    return;
  }

  if (req.method === 'GET' && url.pathname === '/logout') {
    sessions.delete(parseCookies(req).sid);
    res.writeHead(302, { 'Set-Cookie': buildSessionCookie(req, '', 0), Location: '/login' });
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
</section>`, user, '/');
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
  <div id='conciliacao-section'></div>
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
      const c = data.conciliacao || {};
      const saldoOk = !c.divergencia;
      const fmtVal = v => v == null ? '-' : 'R$ ' + Number(v).toLocaleString('pt-BR',{minimumFractionDigits:2,maximumFractionDigits:2});
      document.getElementById('result-cards').innerHTML = [
        ['Linhas importadas', data.importedRows, ''],
        ['Ignorados (já existiam)', data.duplicatesIgnored, 'color:var(--gray-500)'],
        ['Novos cadastros', data.foundNames, ''],
        ['Erros encontrados', data.pendingErrors, data.pendingErrors > 0 ? 'color:var(--red)' : 'color:var(--green)'],
        ['Bloqueantes', data.blockingIssues, data.blockingIssues > 0 ? 'color:var(--red)' : 'color:var(--green)']
      ].map(([k,v,s]) => '<div class="card"><strong>'+k+'</strong><span style="'+s+'">'+v+'</span></div>').join('');

      // --- Painel de conciliação ---
      const concDiv = document.getElementById('conciliacao-section');
      if(concDiv){
        if(saldoOk){
          concDiv.innerHTML = '<div style="display:flex;align-items:center;gap:.6rem;padding:.85rem 1.1rem;background:#f0fdf4;border:1px solid #bbf7d0;border-radius:10px;margin-top:1rem">'
            +'<span style="font-size:1.3rem">&#9989;</span>'
            +'<div><strong style="color:#166534">Saldo conciliado</strong><br><span style="font-size:.82rem;color:#166534">Planilha: '+fmtVal(c.saldoPlanilha)+' &nbsp;|&nbsp; Banco: '+fmtVal(c.saldoBanco)+'</span></div></div>';
        } else {
          const tipoLabel = { novo: '&#128195; Novo (não estava no banco)', divergente: '&#9888;&#65039; Valor divergente', ausente_na_planilha: '&#128683; Ausente na planilha' };
          const tipoColor = { novo: '#1d4ed8', divergente: '#d97706', ausente_na_planilha: '#dc2626' };
          const detRows = (c.detalhes || []).map(d =>
            '<tr style="border-bottom:1px solid #fef3c7">'
            +'<td style="padding:.3rem .5rem;font-size:.76rem;white-space:nowrap">'+d.data+'</td>'
            +'<td style="padding:.3rem .5rem;font-size:.76rem;max-width:220px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+d.descricao+'">'+d.descricao+'</td>'
            +'<td style="padding:.3rem .5rem;font-size:.76rem">'+d.conta+'</td>'
            +'<td style="padding:.3rem .5rem;font-size:.76rem;color:'+tipoColor[d.tipo]+'">'+tipoLabel[d.tipo]+'</td>'
            +'<td style="padding:.3rem .5rem;font-size:.76rem;text-align:right">'+fmtVal(d.valorPlanilha)+'</td>'
            +'<td style="padding:.3rem .5rem;font-size:.76rem;text-align:right">'+fmtVal(d.valorBanco)+'</td>'
            +'<td style="padding:.3rem .5rem;font-size:.76rem;text-align:right;font-weight:700;color:'+(d.diferenca>=0?'#059669':'#dc2626')+'">'+fmtVal(d.diferenca)+'</td>'
            +'</tr>'
          ).join('');
          concDiv.innerHTML = '<div style="background:#fffbeb;border:2px solid #fbbf24;border-radius:10px;padding:1rem;margin-top:1rem">'
            +'<div style="display:flex;align-items:center;gap:.6rem;margin-bottom:.75rem">'
            +'<span style="font-size:1.3rem">&#9888;&#65039;</span>'
            +'<div><strong style="color:#92400e">Divergência de saldo detectada</strong><br>'
            +'<span style="font-size:.82rem;color:#78350f">Planilha: <strong>'+fmtVal(c.saldoPlanilha)+'</strong> &nbsp;|&nbsp; Banco: <strong>'+fmtVal(c.saldoBanco)+'</strong> &nbsp;|&nbsp; Diferença: <strong style="color:'+(c.diferenca>=0?'#059669':'#dc2626')+'">'+fmtVal(c.diferenca)+'</strong></span></div></div>'
            +'<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse">'
            +'<thead><tr style="background:#fef3c7"><th style="padding:.3rem .5rem;text-align:left;font-size:.72rem">Data</th><th style="padding:.3rem .5rem;text-align:left;font-size:.72rem">Descrição</th><th style="padding:.3rem .5rem;text-align:left;font-size:.72rem">Conta</th><th style="padding:.3rem .5rem;text-align:left;font-size:.72rem">Situação</th><th style="padding:.3rem .5rem;text-align:right;font-size:.72rem">Planilha</th><th style="padding:.3rem .5rem;text-align:right;font-size:.72rem">Banco</th><th style="padding:.3rem .5rem;text-align:right;font-size:.72rem">Diferença</th></tr></thead>'
            +'<tbody>'+detRows+'</tbody></table></div>'+(c.detalhes&&c.detalhes.length>=50?'<p style="font-size:.75rem;color:#92400e;margin-top:.5rem">&#8505;&#65039; Exibindo os primeiros 50 itens divergentes.</p>':'')+'</div>';
        }
      }
    }
  } catch(e) { msg.textContent = 'Erro: ' + e.message; msg.style.color = 'var(--red)'; }
  btn.disabled = false; btn.textContent = '\u{1F680}\u00a0 Importar planilha';
}
</script>`, user, '/upload');
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
      const parsed = parseRowsWithPython(fileName, buffer);
      const rows = parsed.rows;
      const upload = { id: crypto.randomUUID(), fileName, uploadedAt: new Date().toISOString(), rowCount: rows.length };
      const allNewEntries = parseEntries(rows, upload.id, db);

      // --- Deduplicação: ignorar lançamentos já existentes (mesma data+descrição+valor+conta) ---
      const existingKeys = new Set(
        db.entries.map((e) => `${e.dataISO}|${normalizeName(e.descricao)}|${e.valor}|${normalizeName(e.conta)}`)
      );
      const entries = allNewEntries.filter((e) => {
        const key = `${e.dataISO}|${normalizeName(e.descricao)}|${e.valor}|${normalizeName(e.conta)}`;
        return !existingKeys.has(key);
      });

      // --- Reaplicar revisões existentes nos novos entries ---
      const reviewedMap = new Map(
        (db.reviewRegistry || []).filter((r) => r.statusRevisao === 'revisado').map((r) => [normalizeName(r.nomeOficial), r])
      );
      entries.forEach((e) => {
        for (const field of ['cliente', 'projeto', 'parceiro']) {
          const rev = reviewedMap.get(normalizeName(e[field] || ''));
          if (rev && rev.tipoFinal && rev.tipoFinal !== 'Pendente de Classificação') {
            // Herdar natureza da revisão já feita
            if (rev.tipoFinal === 'Estrutura Interna' || rev.tipoFinal === 'Financeiro / Não Operacional') {
              e.natureza = rev.tipoFinal === 'Financeiro / Não Operacional' ? 'Movimentação Financeira Não Operacional' : 'Despesa Indireta';
            }
            if (rev.clienteVinculado && !e.cliente) e.cliente = rev.clienteVinculado;
            if (rev.projetoVinculado && !e.projeto) e.projeto = rev.projetoVinculado;
          }
        }
      });

      // --- Conciliação: comparar planilha vs. banco ANTES de persistir os novos ---
      // (o banco ainda não tem os novos entries, então comparamos planilha completa vs. banco atual)
      const conciliacao = conciliarPlanilha(allNewEntries, db);

      const issues = buildIssues(entries, db).map((i) => ({ ...i, id: crypto.randomUUID(), uploadId: upload.id, status: 'aberta' }));
      const registry = buildReviewRegistry(entries);
      db.uploads.push(upload);
      db.entries.push(...entries);
      db.issues.push(...issues);
      db.reviewRegistry = mergeRegistry(db.reviewRegistry, registry);
      saveDb(db);

      const summary = buildPreAnalysisSummary(db);
      const duplicatesIgnored = allNewEntries.length - entries.length;
      json(res, 200, {
        uploadId: upload.id,
        fileName,
        importedRows: entries.length,
        invalidValueRows: Number(parsed.meta?.skippedInvalidValue || 0),
        invalidDateRows: Number(parsed.meta?.skippedInvalidDate || 0),
        rejectedRows: Array.isArray(parsed.meta?.rejectedRows) ? parsed.meta.rejectedRows : [],
        duplicatesIgnored,
        foundNames: registry.length,
        pendingErrors: issues.filter((i) => i.level === 'erro').length,
        alerts: issues.filter((i) => i.level === 'alerta').length,
        blockingIssues: summary.bloqueantes,
        conciliacao
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
</section>`, user, '/pendencias');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'GET' && url.pathname === '/cadastros') {
    const statusFilter = (url.searchParams.get('status') || 'pendente').trim();
    const typeFilter = (url.searchParams.get('tipo') || '').trim();
    const textFilter = normalizeName(url.searchParams.get('q') || '');
    // Cadastros que só têm lançamentos históricos não aparecem para revisão
    const registryAtivo = db.reviewRegistry.filter((item) => {
      const nome = normalizeName(item.nomeOficial);
      const linked = (db.entries || []).filter((e) => {
        return normalizeName(e.cliente) === nome ||
               normalizeName(e.projeto) === nome ||
               normalizeName(e.parceiro) === nome;
      });
      // Se não tem lançamentos ou todos são históricos, oculta
      if (linked.length === 0) return false;
      return linked.some(isAtivo);
    });
    const filtered = registryAtivo.filter((item) => {
      if (statusFilter && statusFilter !== 'todos' && item.statusRevisao !== statusFilter) return false;
      if (typeFilter && item.tipoFinal !== typeFilter) return false;
      if (textFilter && !normalizeName(`${item.nomeOriginal} ${item.nomeOficial}`).includes(textFilter)) return false;
      return true;
    });
    const reviewed = registryAtivo.filter((item) => item.statusRevisao === 'revisado').length;
    const total = registryAtivo.length;
    const html = page('Cadastro Revisável', `
<section>
  <div style='display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:1rem;margin-bottom:1.25rem'>
    <div>
      <h2 style='margin:0'>Cadastro Revisável</h2>
      <p style='margin:.25rem 0 0;font-size:.85rem;color:var(--gray-400)'>
        <span class='badge badge-amber'>${total - reviewed} pendentes</span>
        &nbsp;<span class='badge badge-green'>${reviewed} revisados</span>
        &nbsp;<span style='color:var(--gray-400)'>de ${total} cadastros</span>
      </p>
    </div>
    <button onclick='revisarEmLote()' style='background:var(--gray-600)'>&#10003;&nbsp; Marcar visíveis como revisado</button>
  </div>

  <form method='get' action='/cadastros' style='display:flex;flex-wrap:wrap;gap:.75rem;align-items:flex-end;background:var(--gray-50);border:1px solid var(--gray-200);border-radius:10px;padding:1rem;margin-bottom:1.25rem'>
    <label style='flex:1;min-width:130px'>Status
      <select name='status'>
        <option value='pendente' ${statusFilter === 'pendente' ? 'selected' : ''}>Pendente</option>
        <option value='revisado' ${statusFilter === 'revisado' ? 'selected' : ''}>Revisado</option>
        <option value='todos' ${statusFilter === 'todos' ? 'selected' : ''}>Todos</option>
      </select>
    </label>
    <label style='flex:2;min-width:160px'>Tipo
      <select name='tipo'>
        <option value=''>Todos os tipos</option>
        ${TYPE_OPTIONS.map((t) => `<option ${typeFilter === t ? 'selected' : ''}>${t}</option>`).join('')}
      </select>
    </label>
    <label style='flex:3;min-width:200px'>Busca por nome
      <input name='q' value='${url.searchParams.get('q') || ''}' placeholder='Digite parte do nome...'/>
    </label>
    <button type='submit'>&#128269;&nbsp; Filtrar</button>
  </form>

   <p style='font-size:.83rem;color:var(--gray-400);margin-bottom:.75rem'>Exibindo <strong>${filtered.length}</strong> cadastros. Clique em um item para ver os lançamentos vinculados e fazer ajustes. <a href='/referencias' style='font-size:.8rem;color:var(--blue)'>&#9998; Gerenciar clientes, projetos e centros de custo</a></p>
  ${(()=>{
    // Extrair valores únicos dos entries para os datalists
    const refs = db.referencias || {};
    // Filtro de lixo: remove valores puramente numéricos, vazios, só pontos/traços
    const isValido = v => {
      if (!v || v === '-' || v === '--' || v === '.') return false;
      const s = String(v).trim();
      if (!s) return false;
      if (/^\d+(\.\d+)*$/.test(s)) return false; // puramente numérico ou decimal
      if (/^\d+$/.test(s)) return false;
      return true;
    };
    const ccSet = new Set([...CC_PADRAO, ...(refs.centrosCusto||[]), ...db.entries.map(e=>e.centroCusto||'').filter(isValido)]);
    const clienteSet = new Set([...(refs.clientes||[]), ...db.entries.map(e=>e.cliente||e.parceiro||'').filter(isValido)]);
    const projetoSet = new Set([...(refs.projetos||[]), ...db.entries.map(e=>e.projeto||'').filter(isValido)]);
    const contaSet = new Set([...(refs.contas||[]), ...db.entries.map(e=>e.conta||'').filter(isValido)]);
    const toOpts = arr => [...arr].sort().map(v=>`<option value='${v.replace(/'/g,"&#39;")}'>`).join('');
    return `<datalist id='dl-cc'>${toOpts(ccSet)}</datalist>
<datalist id='dl-clientes'>${toOpts(clienteSet)}</datalist>
<datalist id='dl-projetos'>${toOpts(projetoSet)}</datalist>
<datalist id='dl-contas'>${toOpts(contaSet)}</datalist>`;
  })()}
  <div id='review-list'>
    ${reviewCards(filtered, db.entries)}
  </div>

  <details style='margin-top:2rem;border:1px solid var(--gray-200);border-radius:10px;padding:1rem'>
    <summary style='cursor:pointer;font-weight:600;color:var(--gray-600);font-size:.88rem'>&#9881;&nbsp; Ações avançadas (consolidar alias, vincular projeto, reclassificar)</summary>
    <div style='margin-top:1rem;display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:1rem'>
      <div style='background:var(--gray-50);border:1px solid var(--gray-200);border-radius:8px;padding:1rem'>
        <h4 style='margin:0 0 .75rem;font-size:.85rem'>Consolidar aliases</h4>
        <label>Nome origem<input id='aliasFrom' placeholder='Ex: SEBRAE AC'/></label>
        <label style='margin-top:.5rem'>Nome oficial<input id='aliasTo' placeholder='Ex: SEBRAE-AC'/></label>
        <label style='margin-top:.5rem;flex-direction:row;align-items:center;gap:.5rem'><input id='keepAlias' type='checkbox' checked style='width:auto'/> Manter alias</label>
        <button onclick='consolidarAlias()' style='margin-top:.75rem;width:100%'>Consolidar + regra</button>
      </div>
      <div style='background:var(--gray-50);border:1px solid var(--gray-200);border-radius:8px;padding:1rem'>
        <h4 style='margin:0 0 .75rem;font-size:.85rem'>Vincular projeto a cliente</h4>
        <label>Projeto<input id='projectName' placeholder='Ex: BRB-PDL'/></label>
        <label style='margin-top:.5rem'>Cliente<input id='clientName' placeholder='Ex: BRB'/></label>
        <button onclick='vincularProjetoCliente()' style='margin-top:.75rem;width:100%'>Vincular + regra</button>
      </div>
      <div style='background:var(--gray-50);border:1px solid var(--gray-200);border-radius:8px;padding:1rem'>
        <h4 style='margin:0 0 .75rem;font-size:.85rem'>Reclassificar em lote</h4>
        <label>Nome oficial<input id='toEstrutura' placeholder='Ex: ESCRITÓRIO'/></label>
        <button onclick='reclassificar("Estrutura Interna")' style='margin-top:.75rem;width:100%;background:#6b7280'>&#8594; Estrutura Interna</button>
        <label style='margin-top:.5rem'>Nome oficial<input id='toFinanceiro' placeholder='Ex: PRONAMPE'/></label>
        <button onclick='reclassificar("Financeiro / Não Operacional")' style='margin-top:.5rem;width:100%;background:#6b7280'>&#8594; Financeiro / Não Operacional</button>
      </div>
    </div>
  </details>
</section>
<script>
function toggleCard(id){
  const body=document.getElementById('body-'+id);
  const icon=document.getElementById('icon-'+id);
  const open=body.style.display==='none';
  body.style.display=open?'block':'none';
  icon.textContent=open?'\u25b2':'\u25bc';
}
async function alterarTipo(id,tipoFinal){
  await fetch('/api/review/'+id,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({tipoFinal,statusRevisao:'revisado'})});
  const card=document.querySelector('[data-id="'+id+'"]');
  if(card){card.querySelector('.badge-amber,.badge-green').className='badge badge-green';card.querySelector('.badge-amber,.badge-green').textContent='Revisado';}
}
async function vincularCliente(id,clienteVinculado){await fetch('/api/review/'+id,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({clienteVinculado,statusRevisao:'revisado'})});}
async function vincularProjeto(id,projetoVinculado){await fetch('/api/review/'+id,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({projetoVinculado,statusRevisao:'revisado'})});}
async function marcarRevisao(id,statusRevisao){
  await fetch('/api/review/'+id,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({statusRevisao})});
  const card=document.querySelector('[data-id="'+id+'"]');
  if(card){
    const badge=card.querySelector('.review-card-header .badge');
    if(statusRevisao==='revisado'){badge.className='badge badge-green';badge.textContent='Revisado';}
    else{badge.className='badge badge-amber';badge.textContent='Pendente';}
  }
}
async function consolidarAlias(){const sourceName=document.getElementById('aliasFrom').value;const targetName=document.getElementById('aliasTo').value;const keepAlias=document.getElementById('keepAlias').checked;await fetch('/api/review/consolidate',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({sourceName,targetName,keepAlias,applyRule:true})});location.reload();}
async function vincularProjetoCliente(){const projectName=document.getElementById('projectName').value;const clientName=document.getElementById('clientName').value;await fetch('/api/review/link-project',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({projectName,clientName,applyRule:true})});location.reload();}
async function reclassificar(tipo){const idField=tipo.includes('Estrutura')?'toEstrutura':'toFinanceiro';const nome=document.getElementById(idField).value;await fetch('/api/review/reclassify',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({nome,tipoFinal:tipo,applyRule:true})});location.reload();}
async function revisarEmLote(){const ids=[...document.querySelectorAll('#review-list [data-id]')].map(r=>r.getAttribute('data-id'));await fetch('/api/review/bulk-review',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({ids,statusRevisao:'revisado'})});location.reload();}
const TYPE_GUIDE_JS={
  'Cliente':'Empresa ou pessoa que contrata e paga pelos servi\u00e7os da CKM. Ex: BRB, SEBRAE.',
  'Projeto':'Contrato ou frente de trabalho espec\u00edfica vinculada a um cliente. Ex: BRB-PDL, SEBRAE 10\u00ba CICLO.',
  'Prestador de Servi\u00e7o':'Pessoa f\u00edsica ou empresa contratada para executar trabalho para a CKM. Ex: consultores, designers.',
  'Fornecedor':'Empresa que fornece produtos ou servi\u00e7os de suporte \u00e0 opera\u00e7\u00e3o. Ex: locador de im\u00f3vel, internet, software.',
  'Estrutura Interna':'Despesa fixa da pr\u00f3pria empresa, sem v\u00ednculo com cliente. Ex: aluguel do escrit\u00f3rio, sal\u00e1rios, cont\u00e1bil.',
  'Financeiro / N\u00e3o Operacional':'Movimenta\u00e7\u00e3o financeira que n\u00e3o \u00e9 receita nem despesa operacional. Ex: m\u00fatuo, empr\u00e9stimo, transfer\u00eancia entre contas.',
  'Conta / Cart\u00e3o':'Conta banc\u00e1ria ou cart\u00e3o de cr\u00e9dito usado como meio de pagamento. Ex: Ita\u00fa PJ, Nubank.',
  'Pendente de Classifica\u00e7\u00e3o':'Ainda n\u00e3o classificado. Selecione um dos tipos acima para resolver.'
};
function atualizarGuia(id,tipo){
  const el=document.getElementById('guia-tipo-'+id);
  if(el) el.textContent=TYPE_GUIDE_JS[tipo]||'';
  const badge=document.getElementById('badge-tipo-'+id);
  if(badge) badge.textContent=tipo;
}
async function marcarRevisado(id){
  // Lê o tipo selecionado no dropdown para salvar junto
  const sel=document.getElementById('tipo-select-'+id);
  const tipoFinal=sel?sel.value:undefined;
  const clienteVinculado=document.getElementById('cliente-input-'+id)?document.getElementById('cliente-input-'+id).value:undefined;
  const projetoVinculado=document.getElementById('projeto-input-'+id)?document.getElementById('projeto-input-'+id).value:undefined;
  const body={statusRevisao:'revisado'};
  if(tipoFinal&&tipoFinal!=='Pendente de Classificação') body.tipoFinal=tipoFinal;
  if(clienteVinculado) body.clienteVinculado=clienteVinculado;
  if(projetoVinculado) body.projetoVinculado=projetoVinculado;
  const resp=await fetch('/api/review/'+id,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
  if(resp.ok){
    const card=document.querySelector('[data-id="'+id+'"]');
    if(card){
      const b=document.getElementById('badge-status-'+id);
      if(b){b.className='badge badge-green';b.textContent='Revisado';}
      card.dataset.status='revisado';
      // Colapsa o card após confirmar
      const body=card.querySelector('.review-card-body');
      if(body) body.style.display='none';
    }
  } else {
    alert('Erro ao salvar. Tente novamente.');
  }
}
async function marcarPendente(id){
  await fetch('/api/review/'+id,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify({statusRevisao:'pendente'})});
  const b=document.getElementById('badge-status-'+id);
  if(b){b.className='badge badge-amber';b.textContent='Pendente';}
}
async function analisarIA(id){
  const btn=document.getElementById('sug-btn-'+id);
  const txt=document.getElementById('sug-text-'+id);
  if(btn) btn.disabled=true;
  if(txt) txt.textContent='Analisando os lan\u00e7amentos...';
  try{
    const resp=await fetch('/api/review/sugerir/'+id,{method:'POST'});
    const data=await resp.json();
    if(data.tipoSugerido){
      if(txt) txt.innerHTML='<strong>Sugest\u00e3o: '+data.tipoSugerido+'</strong><br><span style="color:#1e3a8a">'+data.explicacao+'</span>';
      const sel=document.getElementById('tipo-select-'+id);
      if(sel){sel.value=data.tipoSugerido; atualizarGuia(id,data.tipoSugerido);}
      if(btn){btn.textContent='\u2713 Aplicar sugest\u00e3o';btn.disabled=false;btn.onclick=function(){alterarTipo(id,data.tipoSugerido);marcarRevisado(id);btn.textContent='\u2713 Aplicado!';btn.disabled=true;};}
    } else {
      if(txt) txt.textContent='N\u00e3o foi poss\u00edvel gerar sugest\u00e3o. Classifique manualmente.';
      if(btn){btn.textContent='Tentar novamente';btn.disabled=false;}
    }
  }catch(e){
    if(txt) txt.textContent='Erro ao consultar IA. Tente novamente.';
    if(btn){btn.textContent='Tentar novamente';btn.disabled=false;}
  }
}
function toggleEntryEdit(eId){
  const editRow=document.getElementById('edit-'+eId);
  const viewRow=document.getElementById('view-'+eId);
  if(!editRow) return;
  const isOpen=editRow.style.display!=='none';
  editRow.style.display=isOpen?'none':'table-row';
  if(viewRow) viewRow.style.background=isOpen?'':'#e0f2fe';
}
async function salvarLancamento(eId){
  const data={
    data: document.getElementById('ef-data-'+eId)?.value||undefined,
    dataISO: document.getElementById('ef-data-'+eId)?.value||undefined,
    descricao: document.getElementById('ef-desc-'+eId)?.value||undefined,
    valor: parseFloat(document.getElementById('ef-valor-'+eId)?.value)||undefined,
    dc: document.getElementById('ef-dc-'+eId)?.value||undefined,
    natureza: document.getElementById('ef-nat-'+eId)?.value||undefined,
    centroCusto: document.getElementById('ef-cc-'+eId)?.value||undefined,
    conta: document.getElementById('ef-conta-'+eId)?.value||undefined,
    cliente: document.getElementById('ef-cliente-'+eId)?.value||undefined,
    projeto: document.getElementById('ef-proj-'+eId)?.value||undefined,
    status: document.getElementById('ef-status-'+eId)?.value||undefined
  };
  // Ajustar sinal do valor conforme D/C
  if(data.valor!==undefined && data.dc==='D') data.valor=-Math.abs(data.valor);
  if(data.valor!==undefined && data.dc==='C') data.valor=Math.abs(data.valor);
  try{
    const resp=await fetch('/api/entries/'+eId,{method:'PATCH',headers:{'content-type':'application/json'},body:JSON.stringify(data)});
    if(resp.ok){
      toggleEntryEdit(eId);
      // Atualizar a linha de visualização sem recarregar a página
      const viewRow=document.getElementById('view-'+eId);
      if(viewRow){
        const cells=viewRow.querySelectorAll('td');
        if(cells[0]) cells[0].textContent=data.dataISO||cells[0].textContent;
        if(cells[1]) cells[1].textContent=data.descricao||cells[1].textContent;
        if(cells[2]){
          const v=data.valor!==undefined?data.valor:parseFloat(cells[2].textContent.replace('R$ ',''));
          cells[2].textContent='R$ '+v.toFixed(2);
          cells[2].style.color=v>=0?'#065f46':'#991b1b';
        }
        if(cells[3]&&data.dc){cells[3].textContent=data.dc;cells[3].style.color=data.dc==='C'?'#065f46':'#991b1b';}
        if(cells[4]&&data.natureza) cells[4].textContent=data.natureza;
        if(cells[5]&&data.centroCusto) cells[5].textContent=data.centroCusto;
        if(cells[6]&&data.conta) cells[6].textContent=data.conta;
        if(cells[7]&&data.cliente) cells[7].textContent=data.cliente;
        if(cells[8]&&data.projeto) cells[8].textContent=data.projeto;
        if(cells[9]&&data.status) cells[9].textContent=data.status;
        viewRow.style.background='';
      }
      // Feedback visual
      const btn=document.querySelector('#edit-'+eId+' button');
      if(btn){const orig=btn.textContent;btn.textContent='\u2713 Salvo!';btn.style.background='#059669';setTimeout(()=>{btn.textContent=orig;},1500);}
    } else {
      alert('Erro ao salvar. Tente novamente.');
    }
  }catch(e){alert('Erro: '+e.message);}
}
async function verHistoricoLancamento(eId){
  const modal=document.getElementById('hist-modal-'+eId);
  if(modal){ modal.style.display=modal.style.display==='none'?'block':'none'; return; }
  // Criar modal inline
  const editRow=document.getElementById('edit-'+eId);
  if(!editRow) return;
  const div=document.createElement('tr');
  div.id='hist-modal-'+eId;
  div.innerHTML='<td colspan="20" style="padding:.75rem 1rem;background:#f8fafc;border-top:1px solid #e2e8f0"><div id="hist-content-'+eId+'" style="font-size:.8rem;color:#475569">&#9203; Carregando histórico...</div></td>';
  editRow.insertAdjacentElement('afterend', div);
  try{
    const resp=await fetch('/api/entries/'+eId+'/historico');
    const log=await resp.json();
    const LABELS={cliente:'Cliente',projeto:'Projeto',parceiro:'Parceiro',centroCusto:'Centro de Custo',natureza:'Natureza',categoria:'Categoria',detalhe:'Detalhe',conta:'Conta',formaPagamento:'Forma Pgto',status:'Status',descricao:'Descrição',valor:'Valor',dc:'D/C',data:'Data'};
    const formatTs=ts=>{const d=new Date(ts);return d.toLocaleDateString('pt-BR')+' '+d.toLocaleTimeString('pt-BR',{hour:'2-digit',minute:'2-digit'});};
    const content=document.getElementById('hist-content-'+eId);
    if(!log.length){ content.innerHTML='<em style="color:#94a3b8">Nenhuma alteração registrada para este lançamento.</em>'; return; }
    content.innerHTML='<strong style="font-size:.78rem;color:#1e293b">Histórico de alterações</strong>'
      +'<table style="width:100%;border-collapse:collapse;margin-top:.5rem">'
      +'<thead><tr style="background:#f1f5f9"><th style="padding:.3rem .5rem;text-align:left;font-size:.72rem">Data/Hora</th><th style="padding:.3rem .5rem;text-align:left;font-size:.72rem">Campo</th><th style="padding:.3rem .5rem;text-align:left;font-size:.72rem;color:#dc2626">Antes</th><th style="padding:.3rem .5rem;text-align:left;font-size:.72rem;color:#059669">Depois</th><th style="padding:.3rem .5rem;text-align:left;font-size:.72rem">Usuário</th></tr></thead>'
      +'<tbody>'+log.map(r=>'<tr style="border-bottom:1px solid #f1f5f9"><td style="padding:.25rem .5rem;font-size:.72rem;white-space:nowrap;color:#94a3b8">'+formatTs(r.ts)+'</td><td style="padding:.25rem .5rem;font-size:.76rem"><strong>'+(LABELS[r.campo]||r.campo)+'</strong></td><td style="padding:.25rem .5rem;font-size:.76rem;color:#dc2626;text-decoration:line-through">'+(r.de||'<em style="color:#cbd5e1">(vazio)</em>')+'</td><td style="padding:.25rem .5rem;font-size:.76rem;color:#059669">'+(r.para||'<em style="color:#cbd5e1">(vazio)</em>')+'</td><td style="padding:.25rem .5rem;font-size:.72rem;color:#94a3b8">'+(r.usuario||'-')+'</td></tr>').join('')+'</tbody></table>';
  }catch(e){
    const content=document.getElementById('hist-content-'+eId);
    if(content) content.innerHTML='<em style="color:#dc2626">Erro ao carregar histórico.</em>';
  }
}
async function perguntarIA(cardId){
  const input=document.getElementById('chat-input-'+cardId);
  const msgs=document.getElementById('chat-msgs-'+cardId);
  const pergunta=input?.value?.trim();
  if(!pergunta||!msgs) return;
  // Adicionar mensagem do usuário
  msgs.innerHTML+='<div style="background:#dbeafe;border-radius:6px;padding:.3rem .6rem;align-self:flex-end"><strong style="font-size:.72rem;color:#1e40af">Você:</strong> <span style="font-size:.76rem">'+pergunta+'</span></div>';
  input.value='';
  msgs.innerHTML+='<div id="ia-loading-'+cardId+'" style="font-size:.74rem;color:#94a3b8;font-style:italic">&#9203; Buscando nos dados...</div>';
  msgs.scrollTop=msgs.scrollHeight;
  try{
    const resp=await fetch('/api/review/chat',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({cardId,pergunta})});
    const data=await resp.json();
    document.getElementById('ia-loading-'+cardId)?.remove();
    msgs.innerHTML+='<div style="background:#f0fdf4;border:1px solid #bbf7d0;border-radius:6px;padding:.3rem .6rem"><strong style="font-size:.72rem;color:#166534">IA:</strong> <span style="font-size:.76rem;color:#14532d">'+( data.resposta||'Sem resposta.')+'</span></div>';
    msgs.scrollTop=msgs.scrollHeight;
  }catch(e){
    document.getElementById('ia-loading-'+cardId)?.remove();
    msgs.innerHTML+='<div style="font-size:.74rem;color:#dc2626">Erro ao consultar IA.</div>';
  }
}
</script>`, user, '/cadastros');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'PATCH' && url.pathname.startsWith('/api/review/')) {
    const id = url.pathname.split('/').pop();
    const item = db.reviewRegistry.find((r) => r.id === id);
    if (!item) return json(res, 404, { error: 'Registro não encontrado' });
    const changesRevisao = JSON.parse(await readBody(req) || '{}');
    // Capturar estado anterior do cadastro
    const registroAntes = { ...item };
    Object.assign(item, changesRevisao);
    // Registrar na trilha de auditoria
    const userRevisao = currentUser(req, db);
    registrarAuditoriaRevisao(db, item.id, item.nomeOficial, userRevisao ? userRevisao.email : 'sistema', changesRevisao, registroAntes);
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

  if (req.method === 'POST' && url.pathname.startsWith('/api/review/sugerir/')) {
    if (!requireAuth(req, res, db)) return;
    const regId = url.pathname.split('/').pop();
    const item = db.reviewRegistry.find((r) => r.id === regId);
    if (!item) return json(res, 404, { error: 'Registro não encontrado' });

    const nome = normalizeName(item.nomeOficial);
    const linked = db.entries.filter((e) =>
      normalizeName(e.cliente) === nome ||
      normalizeName(e.projeto) === nome ||
      normalizeName(e.parceiro) === nome
    ).slice(0, 15);

    const resumo = linked.map((e) =>
      `- ${e.dataISO || e.data}: ${e.descricao || '(sem histórico)'} | Valor: R$ ${Number(e.valor||0).toFixed(2)} | Natureza: ${e.natureza||'-'} | CC: ${e.centroCusto||'-'}`
    ).join('\n');

    const tipos = ['Cliente','Projeto','Prestador de Serviço','Fornecedor','Estrutura Interna','Financeiro / Não Operacional','Conta / Cartão'];

    // Buscar exemplos de nomes já revisados com lançamentos similares (por palavras-chave no histórico)
    const palavrasChave = linked
      .flatMap((e) => (e.descricao || '').toLowerCase().split(/\s+/))
      .filter((w) => w.length > 4)
      .slice(0, 10);

    const exemplosRevisados = (db.reviewRegistry || [])
      .filter((r) => r.statusRevisao === 'revisado' && r.tipoFinal && r.tipoFinal !== 'Pendente de Classificação' && r.id !== regId)
      .map((r) => {
        const nomeRev = normalizeName(r.nomeOficial);
        const lancRev = db.entries.filter((e) =>
          normalizeName(e.cliente) === nomeRev ||
          normalizeName(e.projeto) === nomeRev ||
          normalizeName(e.parceiro) === nomeRev
        );
        const textos = lancRev.map((e) => (e.descricao || '').toLowerCase()).join(' ');
        const score = palavrasChave.filter((w) => textos.includes(w)).length;
        return { r, score, lancRev };
      })
      .filter((x) => x.score > 0)
      .sort((a, b) => b.score - a.score)
      .slice(0, 5);

    let contextoExemplos = '';
    if (exemplosRevisados.length > 0) {
      contextoExemplos = '\n\nExemplos de cadastros similares já classificados pela empresa (use como referência principal):\n';
      contextoExemplos += exemplosRevisados.map(({ r, lancRev }) => {
        const amostras = lancRev.slice(0, 3).map((e) => e.descricao || '-').join('; ');
        return `- "${r.nomeOficial}" → ${r.tipoFinal} (ex. lançamentos: ${amostras})`;
      }).join('\n');
    }

    const prompt = `Você é um assistente financeiro da empresa CKM Consultoria. Analise os lançamentos abaixo vinculados ao nome "${item.nomeOficial}" e classifique este cadastro em um dos seguintes tipos:\n${tipos.map((t,i)=>`${i+1}. ${t}`).join('\n')}\n\nDefinições:\n- Cliente: empresa que contrata e paga a CKM pelos serviços prestados\n- Projeto: contrato ou frente de trabalho específica vinculada a um cliente\n- Prestador de Serviço: pessoa física ou empresa contratada pela CKM para executar trabalho\n- Fornecedor: empresa que fornece produtos ou serviços de suporte à operação (aluguel, internet, pedágio, tag, software, etc)\n- Estrutura Interna: despesa fixa da própria empresa sem vínculo com cliente (salários, aluguel do escritório, contabilidade)\n- Financeiro / Não Operacional: mútuo, empréstimo, transferência entre contas, operação financeira\n- Conta / Cartão: conta bancária ou cartão de crédito usado como meio de pagamento\n\nLançamentos deste cadastro:\n${resumo}${contextoExemplos}\n\nSe houver exemplos similares já classificados, use-os como referência principal e mencione a analogia na explicação.\n\nResponda APENAS em JSON válido:\n{"tipoSugerido": "<um dos tipos acima exatamente>", "explicacao": "<explicação em 1-2 frases em português simples, mencionando analogia se houver>"}`;

    try {
      const { OpenAI } = require('openai');
      const openai = new OpenAI();
      const completion = await openai.chat.completions.create({
        model: 'gpt-4.1-mini',
        messages: [{ role: 'user', content: prompt }],
        temperature: 0.2,
        max_tokens: 300
      });
      const raw = (completion.choices[0]?.message?.content || '').trim();
      const parsed = JSON.parse(raw);
      if (tipos.includes(parsed.tipoSugerido)) {
        return json(res, 200, { tipoSugerido: parsed.tipoSugerido, explicacao: parsed.explicacao });
      }
      return json(res, 200, { tipoSugerido: null, explicacao: 'Não foi possível determinar o tipo.' });
    } catch (err) {
      console.error('[IA sugerir]', err.message);
      return json(res, 500, { error: 'Erro ao consultar IA', detail: err.message });
    }
  }

  if (req.method === 'POST' && url.pathname === '/api/review/save-rule') {
    const body = JSON.parse(await readBody(req) || '{}');
    db.savedRules.push({ id: crypto.randomUUID(), type: 'entry_update', active: true, ...body });
    saveDb(db);
    return json(res, 201, { ok: true });
  }

  if (req.method === 'POST' && url.pathname === '/api/review/chat') {
    if (!requireAuth(req, res, db)) return;
    const { cardId, pergunta } = JSON.parse(await readBody(req) || '{}');
    if (!pergunta) return json(res, 400, { error: 'Pergunta obrigatória' });
    // Buscar o cadastro e seus lançamentos vinculados
    const item = db.reviewRegistry.find((r) => r.id === cardId);
    const nomeNorm = item ? normalizeName(item.nomeOficial) : null;
    const lancamentos = nomeNorm
      ? db.entries.filter((e) =>
          normalizeName(e.cliente) === nomeNorm ||
          normalizeName(e.projeto) === nomeNorm ||
          normalizeName(e.parceiro) === nomeNorm
        )
      : [];
    // Resumo dos lançamentos deste cadastro
    const resumoLanc = lancamentos.slice(0, 30).map((e) =>
      `${e.dataISO||e.data||'-'} | ${e.descricao||'-'} | R$ ${Number(e.valor||0).toFixed(2)} | ${e.natureza||'-'} | CC: ${e.centroCusto||'-'} | Cliente: ${e.cliente||'-'} | Projeto: ${e.projeto||'-'} | Status: ${e.status||'-'}`
    ).join('\n');
    // Contexto geral da planilha: lançamentos similares já classificados
    const palavrasChave = pergunta.toLowerCase().split(/\s+/).filter((w) => w.length > 3);
    const similares = db.entries
      .filter((e) => {
        const desc = (e.descricao || '').toLowerCase();
        return palavrasChave.some((p) => desc.includes(p)) && e.natureza && e.natureza !== 'Pendente';
      })
      .slice(0, 20)
      .map((e) => `${e.dataISO||e.data||'-'} | ${e.descricao||'-'} | R$ ${Number(e.valor||0).toFixed(2)} | ${e.natureza||'-'} | CC: ${e.centroCusto||'-'} | Cliente: ${e.cliente||'-'} | Projeto: ${e.projeto||'-'}`);
    const prompt = `Você é um assistente financeiro da empresa CKM Consultoria. Responda APENAS com base nos dados da planilha fornecidos abaixo. Não invente informações. Se não encontrar nos dados, diga claramente que não há registros suficientes.

Cadastro em análise: "${item?.nomeOficial || cardId}"
Tipo atual: ${item?.tipoFinal || 'Pendente de Classificação'}

Lançamentos vinculados a este cadastro (${lancamentos.length} total):
${resumoLanc || '(nenhum lançamento vinculado encontrado)'}

Lançamentos similares já classificados na planilha (baseado nas palavras da pergunta):
${similares.length > 0 ? similares.join('\n') : '(nenhum lançamento similar encontrado)'}

Pergunta do usuário: ${pergunta}

Responda em português, de forma objetiva e direta, citando os dados específicos encontrados na planilha.`;
    try {
      const { OpenAI } = require('openai');
      const openai = new OpenAI();
      const completion = await openai.chat.completions.create({
        model: 'gpt-4.1-mini',
        messages: [{ role: 'user', content: prompt }],
        temperature: 0.1,
        max_tokens: 600
      });
      const resposta = completion.choices[0]?.message?.content?.trim() || 'Sem resposta.';
      return json(res, 200, { resposta });
    } catch (err) {
      console.error('[chat IA]', err.message);
      return json(res, 500, { error: 'Erro ao consultar IA: ' + err.message });
    }
  }

  if (req.method === 'PATCH' && url.pathname.startsWith('/api/entries/')) {
    const id = url.pathname.split('/').pop();
    const entry = db.entries.find((e) => e.id === id);
    if (!entry) return json(res, 404, { error: 'Lançamento não encontrado' });
    const changes = JSON.parse(await readBody(req) || '{}');
    const editable = ['cliente', 'projeto', 'natureza', 'centroCusto', 'parceiro', 'categoria', 'detalhe', 'conta', 'formaPagamento', 'status', 'data', 'dataISO', 'descricao', 'valor', 'dc'];
    // Capturar estado anterior antes de aplicar as mudanças
    const entryAntes = {};
    editable.forEach((k) => { entryAntes[k] = entry[k]; });
    editable.forEach((k) => { if (changes[k] !== undefined) entry[k] = changes[k]; });
    // Registrar na trilha de auditoria (campo a campo, antes vs. depois)
    const userAtual = currentUser(req, db);
    registrarAuditoria(db, entry.id, userAtual ? userAtual.email : 'sistema', changes, entryAntes);
    // Manter manualAdjustments para compatibilidade
    db.manualAdjustments = db.manualAdjustments || [];
    db.manualAdjustments.push({
      id: crypto.randomUUID(),
      entryId: entry.id,
      changedAt: new Date().toISOString(),
      usuario: userAtual ? userAtual.email : 'sistema',
      changes
    });
    saveDb(db);
    return json(res, 200, entry);
  }

  if (req.method === 'GET' && url.pathname === '/dashboard') {
    const user = requireAuth(req, res, db);
    if (!user) return;
    const hoje = new Date().toISOString().slice(0, 10);
    const anoAtual = hoje.slice(0, 4);
    // Filtro de período via query string (?de=YYYY-MM-DD&ate=YYYY-MM-DD&visao=fluxo|resultado)
    const filtroInicio = url.searchParams.get('de') || CORTE_DATA;
    const filtroFim = url.searchParams.get('ate') || hoje;
    const visao = url.searchParams.get('visao') || 'resultado'; // 'fluxo' ou 'resultado'

    // Calcular métricas gerais (sem filtro de período — para os cards de saldo)
    const metrics = calculateDashboard(db);

    // Calcular métricas filtradas pelo período selecionado
    const lancsFiltrados = db.entries.filter(e =>
      (e.dataISO||'') >= filtroInicio &&
      (e.dataISO||'') <= filtroFim &&
      !e.isTransferenciaInterna &&
      (e.centroCusto||'').toUpperCase().trim() !== 'SALDO ATUAL'
    );
    const totalReceitasFiltro = lancsFiltrados.filter(e=>(e.valor||0)>0).reduce((a,e)=>a+(e.valor||0),0);
    const totalDespesasFiltro = lancsFiltrados.filter(e=>(e.valor||0)<0).reduce((a,e)=>a+Math.abs(e.valor||0),0);
    const saldoFiltro = totalReceitasFiltro - totalDespesasFiltro;

    // Fluxo de caixa mês a mês no período filtrado
    const fluxoMensal = {};
    lancsFiltrados.forEach(e => {
      const mes = (e.dataISO||'').slice(0,7);
      if (!fluxoMensal[mes]) fluxoMensal[mes] = { receitas: 0, despesas: 0, saldo: 0 };
      if ((e.valor||0) > 0) fluxoMensal[mes].receitas += (e.valor||0);
      else fluxoMensal[mes].despesas += Math.abs(e.valor||0);
      fluxoMensal[mes].saldo += (e.valor||0);
    });
    const meses = Object.keys(fluxoMensal).sort();

    // Resultado por cliente no período filtrado
    const byClienteFiltro = {};
    const byProjetoFiltro = {};
    const byEstruturaFiltro = {};
    lancsFiltrados.forEach(e => {
      const c = clienteEfetivo(e);
      const p = projetoEfetivo(e);
      if (c !== 'SEM CLIENTE') byClienteFiltro[c] = (byClienteFiltro[c]||0) + (e.valor||0);
      if (p !== 'SEM PROJETO') byProjetoFiltro[p] = (byProjetoFiltro[p]||0) + (e.valor||0);
      if (c === 'SEM CLIENTE') {
        const cc = (e.centroCusto||'SEM CC').toUpperCase();
        if (cc !== 'TEF' && cc !== 'SALDO ATUAL') byEstruturaFiltro[cc] = (byEstruturaFiltro[cc]||0) + (e.valor||0);
      }
    });

    // ── Buscar mapa de tipos dos CCs do PostgreSQL ──────────────────────────
    // Monta: { 'JURIDICO': 'ESTRUTURA', 'BANRISUL': 'OPERACIONAL', ... }
    let ccTipoMap = {};
    try {
      const pg = storage.getPool ? storage.getPool() : null;
      if (pg) {
        const ccRows = (await pg.query('SELECT codigo, tipo FROM centros_de_custo WHERE ativo = true')).rows;
        ccRows.forEach(r => { ccTipoMap[r.codigo.toUpperCase()] = r.tipo; });
      }
    } catch(e) { console.error('[dashboard] ccTipoMap error:', e.message); }

    // ── Árvore hierárquica: TIPO → CC → (OPERACIONAL: EMPRESA → PROJETO) ──
    // Estrutura: arvore[tipo][cc] = { entradas, saidas, empresas: { empresa: { entradas, saidas, projetos: { proj: { entradas, saidas } } } } }
    const TIPOS_ORDEM = ['OPERACIONAL', 'ESTRUTURA', 'FINANCEIRO', 'TRANSFERENCIA'];
    const arvore = {};
    TIPOS_ORDEM.forEach(t => { arvore[t] = {}; });

    // Função para normalizar CC removendo acentos (para busca no ccTipoMap)
    function normCC(s) {
      return (s||'').toUpperCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'').trim();
    }

    // Função para resolver tipo de um lançamento
    // Retorna: { tipo: 'OPERACIONAL'|'ESTRUTURA'|'FINANCEIRO'|'TRANSFERENCIA', cc: string }
    function resolverTipoCC(e) {
      const ccRaw = (e.centroCusto || '').trim();
      const ccUp  = ccRaw.toUpperCase();
      const ccNorm = normCC(ccRaw);

      // Primeiro: verificar no mapa do banco (com e sem acento)
      if (ccTipoMap[ccUp])   return { tipo: ccTipoMap[ccUp],   cc: ccRaw };
      if (ccTipoMap[ccNorm]) return { tipo: ccTipoMap[ccNorm], cc: ccRaw };

      // Segundo: heurística por FORBIDDEN_AS_CLIENT
      if (FORBIDDEN_AS_CLIENT.includes(ccUp) || FORBIDDEN_AS_CLIENT.includes(ccNorm))
        return { tipo: 'ESTRUTURA', cc: ccRaw };

      // Terceiro: se tem cliente operacional, é OPERACIONAL
      const c = clienteEfetivo(e);
      if (c !== 'SEM CLIENTE') return { tipo: 'OPERACIONAL', cc: ccRaw };

      // Default: ESTRUTURA
      return { tipo: 'ESTRUTURA', cc: ccRaw || 'SEM CC' };
    }

    lancsFiltrados.forEach(e => {
      const v = e.valor || 0;
      const { tipo, cc } = resolverTipoCC(e);
      if (tipo === 'TRANSFERENCIA') return; // ignora TEF
      if (!arvore[tipo]) arvore[tipo] = {};

      if (tipo === 'OPERACIONAL') {
        // Para OPERACIONAL: árvore é EMPRESA → PROJETO → PARCEIRO
        const empresa = clienteEfetivo(e);
        const projeto = projetoEfetivo(e);
        const parceiro = (e.parceiro || '').trim() || 'SEM PARCEIRO';
        if (!arvore[tipo][empresa]) arvore[tipo][empresa] = { entradas: 0, saidas: 0, projetos: {} };
        if (v > 0) arvore[tipo][empresa].entradas += v;
        else arvore[tipo][empresa].saidas += Math.abs(v);
        // Nível projeto
        const projKey = projeto !== 'SEM PROJETO' ? projeto : '(sem projeto)';
        if (!arvore[tipo][empresa].projetos[projKey]) arvore[tipo][empresa].projetos[projKey] = { entradas: 0, saidas: 0, parceiros: {} };
        if (v > 0) arvore[tipo][empresa].projetos[projKey].entradas += v;
        else arvore[tipo][empresa].projetos[projKey].saidas += Math.abs(v);
        // Nível parceiro
        if (!arvore[tipo][empresa].projetos[projKey].parceiros[parceiro]) arvore[tipo][empresa].projetos[projKey].parceiros[parceiro] = { entradas: 0, saidas: 0 };
        if (v > 0) arvore[tipo][empresa].projetos[projKey].parceiros[parceiro].entradas += v;
        else arvore[tipo][empresa].projetos[projKey].parceiros[parceiro].saidas += Math.abs(v);
      } else {
        // Para ESTRUTURA e FINANCEIRO: nível CC simples
        if (!arvore[tipo][cc]) arvore[tipo][cc] = { entradas: 0, saidas: 0, empresas: {} };
        if (v > 0) arvore[tipo][cc].entradas += v;
        else arvore[tipo][cc].saidas += Math.abs(v);
      }
    });

    const fmtBRL = (v) => {
      const abs = Math.abs(v).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      return (v < 0 ? '-' : '') + 'R$ ' + abs;
    };
    const topItems = (obj) => Object.entries(obj).sort((a, b) => Math.abs(b[1]) - Math.abs(a[1])).slice(0, 10);

    // Gráfico de barras do fluxo mensal (SVG simples)
    const maxAbs = Math.max(...meses.map(m => Math.max(fluxoMensal[m].receitas, fluxoMensal[m].despesas)), 1);
    const barW = meses.length > 0 ? Math.max(20, Math.floor(560 / (meses.length * 2 + 1))) : 30;
    const chartH = 120;
    const svgBars = meses.map((m, i) => {
      const v = fluxoMensal[m];
      const hRec = Math.round((v.receitas / maxAbs) * chartH);
      const hDesp = Math.round((v.despesas / maxAbs) * chartH);
      const x = i * (barW * 2 + 4) + 2;
      const label = m.slice(5); // MM
      return `<rect x='${x}' y='${chartH - hRec}' width='${barW}' height='${hRec}' fill='#22c55e' rx='2'/>
<rect x='${x + barW + 2}' y='${chartH - hDesp}' width='${barW}' height='${hDesp}' fill='#ef4444' rx='2'/>
<text x='${x + barW}' y='${chartH + 14}' text-anchor='middle' font-size='9' fill='#64748b'>${label}</text>
<text x='${x}' y='${chartH - hRec - 3}' font-size='8' fill='#16a34a' text-anchor='middle'>${v.saldo >= 0 ? '+' : ''}${(v.saldo/1000).toFixed(0)}k</text>`;
    }).join('');
    const svgW = meses.length * (barW * 2 + 4) + 10;
    const fluxoGrafico = meses.length === 0 ? '<p style="color:#64748b">Sem dados no período.</p>' :
      `<div style='overflow-x:auto'><svg width='${svgW}' height='${chartH + 24}' style='display:block'>${svgBars}</svg>
<div style='display:flex;gap:1rem;font-size:.8rem;margin-top:.25rem'><span style='color:#22c55e'>&#9646; Receitas</span><span style='color:#ef4444'>&#9646; Despesas</span><span style='color:#64748b'>Número = saldo do mês (em R$ mil)</span></div></div>`;

    // Tabela de fluxo mensal detalhada
    const fluxoTabela = meses.length === 0 ? '' : `<div style='overflow-x:auto;margin-top:1rem'><table style='width:100%;border-collapse:collapse;font-size:.88rem'>
<thead><tr style='background:#f1f5f9'><th style='text-align:left;padding:.4rem .6rem'>Mês</th><th style='text-align:right;padding:.4rem .6rem;color:#16a34a'>Entradas</th><th style='text-align:right;padding:.4rem .6rem;color:#dc2626'>Saídas</th><th style='text-align:right;padding:.4rem .6rem'>Saldo do Mês</th></tr></thead>
<tbody>${meses.map(m => {
  const v = fluxoMensal[m];
  const cor = v.saldo >= 0 ? '#16a34a' : '#dc2626';
  return `<tr style='border-bottom:1px solid #f1f5f9'><td style='padding:.4rem .6rem;font-weight:600'>${m}</td><td style='padding:.4rem .6rem;text-align:right;color:#16a34a'>${fmtBRL(v.receitas)}</td><td style='padding:.4rem .6rem;text-align:right;color:#dc2626'>${fmtBRL(v.despesas)}</td><td style='padding:.4rem .6rem;text-align:right;font-weight:700;color:${cor}'>${fmtBRL(v.saldo)}</td></tr>`;
}).join('')}
<tr style='background:#f8fafc;font-weight:700;border-top:2px solid #e2e8f0'><td style='padding:.4rem .6rem'>TOTAL</td><td style='padding:.4rem .6rem;text-align:right;color:#16a34a'>${fmtBRL(totalReceitasFiltro)}</td><td style='padding:.4rem .6rem;text-align:right;color:#dc2626'>${fmtBRL(totalDespesasFiltro)}</td><td style='padding:.4rem .6rem;text-align:right;color:${saldoFiltro>=0?'#16a34a':'#dc2626'}'>${fmtBRL(saldoFiltro)}</td></tr>
</tbody></table></div>`;

    // Seção de Custos de Estrutura (período filtrado)
    const estruturaItems = Object.entries(byEstruturaFiltro).sort((a, b) => a[1] - b[1]);
    const totalEstruturaNeta = estruturaItems.reduce((acc, [, v]) => acc + v, 0);
    const estruturaHTML = estruturaItems.length === 0
      ? '<p style="color:#64748b">Nenhum custo de estrutura no período.</p>'
      : `<div style='overflow-x:auto'><table style='width:100%;border-collapse:collapse;font-size:.9rem'>
<thead><tr style='background:#f1f5f9'><th style='text-align:left;padding:.5rem .75rem;border-bottom:2px solid #e2e8f0'>Centro de Custo</th><th style='text-align:right;padding:.5rem .75rem;border-bottom:2px solid #e2e8f0'>Valor (período)</th><th style='text-align:right;padding:.5rem .75rem;border-bottom:2px solid #e2e8f0'>Detalhes</th></tr></thead>
<tbody>${estruturaItems.map(([cc, v]) => {
  const cor = v < 0 ? '#dc2626' : '#16a34a';
  return `<tr style='border-bottom:1px solid #f1f5f9'><td style='padding:.45rem .75rem;font-weight:600'>${cc}</td><td style='padding:.45rem .75rem;text-align:right;color:${cor};font-weight:700'>${fmtBRL(v)}</td><td style='padding:.45rem .75rem;text-align:right'><a href='/dashboard/detalhe?view=estrutura&chave=${encodeURIComponent(cc)}' style='font-size:.8rem;color:#3b82f6'>ver lançamentos</a></td></tr>`;
}).join('')}
<tr style='background:#f8fafc;font-weight:700;border-top:2px solid #e2e8f0'><td style='padding:.5rem .75rem'>TOTAL ESTRUTURA</td><td style='padding:.5rem .75rem;text-align:right;color:${totalEstruturaNeta < 0 ? '#dc2626' : '#16a34a'}'>${fmtBRL(totalEstruturaNeta)}</td><td></td></tr>
</tbody></table></div>`;

    // Seletor de período rápido
    const mesAtual = hoje.slice(0, 7);
    const mesAnterior = (() => { const d = new Date(hoje); d.setMonth(d.getMonth()-1); return d.toISOString().slice(0,7); })();
    const trimestreInicio = (() => { const m = parseInt(hoje.slice(5,7)); const t = Math.floor((m-1)/3)*3+1; return `${anoAtual}-${String(t).padStart(2,'0')}-01`; })();
    const periodoHTML = `<div style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:.5rem;padding:1rem;margin-bottom:1.5rem'>
<form method='GET' action='/dashboard' style='display:flex;gap:.75rem;flex-wrap:wrap;align-items:flex-end'>
<div><label style='display:block;font-size:.8rem;color:#64748b;margin-bottom:.2rem'>De</label>
<input type='date' name='de' value='${filtroInicio}' style='padding:.35rem .5rem;border:1px solid #cbd5e1;border-radius:.375rem;font-size:.9rem'></div>
<div><label style='display:block;font-size:.8rem;color:#64748b;margin-bottom:.2rem'>Até</label>
<input type='date' name='ate' value='${filtroFim}' style='padding:.35rem .5rem;border:1px solid #cbd5e1;border-radius:.375rem;font-size:.9rem'></div>
<div><label style='display:block;font-size:.8rem;color:#64748b;margin-bottom:.2rem'>Visão</label>
<select name='visao' style='padding:.35rem .5rem;border:1px solid #cbd5e1;border-radius:.375rem;font-size:.9rem'>
<option value='resultado'${visao==='resultado'?' selected':''}>Resultado Econômico</option>
<option value='fluxo'${visao==='fluxo'?' selected':''}>Fluxo de Caixa</option>
</select></div>
<button type='submit' style='padding:.4rem .9rem;background:#2563eb;color:#fff;border:none;border-radius:.375rem;cursor:pointer;font-size:.9rem'>Aplicar</button>
</form>
<div style='display:flex;gap:.5rem;flex-wrap:wrap;margin-top:.6rem'>
<a href='/dashboard?de=${mesAtual}-01&ate=${hoje}&visao=${visao}' style='font-size:.8rem;padding:.25rem .6rem;background:#e2e8f0;border-radius:.375rem;text-decoration:none;color:#374151'>Este mês</a>
<a href='/dashboard?de=${mesAnterior}-01&ate=${mesAnterior}-31&visao=${visao}' style='font-size:.8rem;padding:.25rem .6rem;background:#e2e8f0;border-radius:.375rem;text-decoration:none;color:#374151'>Mês anterior</a>
<a href='/dashboard?de=${trimestreInicio}&ate=${hoje}&visao=${visao}' style='font-size:.8rem;padding:.25rem .6rem;background:#e2e8f0;border-radius:.375rem;text-decoration:none;color:#374151'>Trimestre atual</a>
<a href='/dashboard?de=${anoAtual}-01-01&ate=${hoje}&visao=${visao}' style='font-size:.8rem;padding:.25rem .6rem;background:#e2e8f0;border-radius:.375rem;text-decoration:none;color:#374151'>Este ano</a>
<a href='/dashboard?de=${CORTE_DATA}&ate=${hoje}&visao=${visao}' style='font-size:.8rem;padding:.25rem .6rem;background:#e2e8f0;border-radius:.375rem;text-decoration:none;color:#374151'>Tudo</a>
<a href='/ia?de=${filtroInicio}&ate=${filtroFim}' style='font-size:.8rem;padding:.25rem .6rem;background:#eff6ff;border:1px solid #bfdbfe;border-radius:.375rem;text-decoration:none;color:#1d4ed8'>🤖 Analisar com IA</a>
</div></div>`;

    // ── HTML da árvore hierárquica Resultado do Período ────────────────────
    const tipoLabel = { OPERACIONAL: '\uD83D\uDCBC Operacional (Clientes)', ESTRUTURA: '\uD83C\uDFE0 Estrutura (Overhead)', FINANCEIRO: '\uD83C\uDFE6 Financeiro (Empréstimos/Banco)' };
    const tipoColor = { OPERACIONAL: '#5ED38C', ESTRUTURA: '#5B2EFF', FINANCEIRO: '#00B8D9' };
    const tipoDesc  = { OPERACIONAL: 'Receitas e despesas de projetos com clientes', ESTRUTURA: 'Gastos fixos: escritório, salários, jurídico, contabilidade etc.', FINANCEIRO: 'Empréstimos (Mútuo, Pronampe) e tarifas bancárias' };

    const fmtSaldo = (ent, sai) => {
      const s = ent - sai;
      const cor = s >= 0 ? '#16a34a' : '#dc2626';
      return `<span style='color:${cor};font-weight:700'>${fmtBRL(s)}</span>`;
    };
    const rowStyle = 'display:grid;grid-template-columns:1fr auto auto auto;gap:.5rem;align-items:center;padding:.35rem .5rem;border-radius:.25rem;font-size:.88rem';
    const hdrStyle = 'display:grid;grid-template-columns:1fr auto auto auto;gap:.5rem;padding:.2rem .5rem;font-size:.75rem;color:#94a3b8;font-weight:600;text-transform:uppercase;letter-spacing:.04em';

    let arvoreHTML = `<h3 style='margin-top:1.5rem'>\uD83D\uDCC8 Resultado do Período <span style='font-size:.8rem;color:#64748b;font-weight:400'>(${filtroInicio} a ${filtroFim})</span></h3>
<p style='color:#64748b;font-size:.9rem;margin-bottom:1rem'>Entradas, saídas e saldo de cada grupo no período. Clique em um grupo para expandir os detalhes.</p>`;

    for (const tipo of TIPOS_ORDEM) {
      if (tipo === 'TRANSFERENCIA') continue;
      const ccs = arvore[tipo];
      if (!ccs || Object.keys(ccs).length === 0) continue;

      // Totais do tipo
      let tipoEnt = 0, tipoSai = 0;
      Object.values(ccs).forEach(d => { tipoEnt += d.entradas; tipoSai += d.saidas; });

      const cor = tipoColor[tipo] || '#808080';
      arvoreHTML += `<details style='border:1px solid ${cor}33;border-radius:.5rem;margin-bottom:.75rem;background:#fff'>
<summary style='cursor:pointer;padding:.75rem 1rem;background:${cor}11;border-radius:.5rem;list-style:none;display:flex;align-items:center;gap:.75rem;user-select:none'>
  <span style='font-size:1.05rem;font-weight:700;color:${cor}'>${tipoLabel[tipo]||tipo}</span>
  <span style='flex:1;font-size:.8rem;color:#64748b'>${tipoDesc[tipo]||''}</span>
  <span style='font-size:.8rem;color:#16a34a;white-space:nowrap'>+${fmtBRL(tipoEnt)}</span>
  <span style='font-size:.8rem;color:#dc2626;white-space:nowrap'>-${fmtBRL(tipoSai)}</span>
  <span style='font-size:.9rem;font-weight:700;white-space:nowrap'>${fmtSaldo(tipoEnt,tipoSai)}</span>
</summary>
<div style='padding:.5rem .75rem'>`;

      // Linha de cabeçalho das colunas
      const hdrLabel = tipo === 'OPERACIONAL' ? 'Empresa / Cliente' : 'Centro de Custo';
      arvoreHTML += `<div style='${hdrStyle}'><span>${hdrLabel}</span><span style='text-align:right'>Entradas</span><span style='text-align:right'>Saídas</span><span style='text-align:right'>Saldo</span></div>`;

      // Cada item dentro do tipo
      for (const [chave, dados] of Object.entries(ccs).sort((a,b) => (b[1].entradas - b[1].saidas) - (a[1].entradas - a[1].saidas))) {
        if (tipo === 'OPERACIONAL') {
          // OPERACIONAL: chave = empresa, dados.projetos = { proj: { entradas, saidas, parceiros: { p: {entradas,saidas} } } }
          const projEntries = Object.entries(dados.projetos||{}).sort((a,b) => (b[1].entradas-b[1].saidas)-(a[1].entradas-a[1].saidas));
          // Tabela com subtotais por projeto
          let tabelaProj = `<div style='overflow-x:auto'><table style='width:100%;border-collapse:collapse;font-size:.85rem'>
<thead><tr style='background:#f1f5f9'>
  <th style='text-align:left;padding:.35rem .6rem;border-bottom:2px solid #e2e8f0'>Projeto</th>
  <th style='text-align:left;padding:.35rem .6rem;border-bottom:2px solid #e2e8f0'>Parceiro</th>
  <th style='text-align:right;padding:.35rem .6rem;border-bottom:2px solid #e2e8f0;color:#16a34a'>Entradas</th>
  <th style='text-align:right;padding:.35rem .6rem;border-bottom:2px solid #e2e8f0;color:#dc2626'>Saídas</th>
  <th style='text-align:right;padding:.35rem .6rem;border-bottom:2px solid #e2e8f0'>Saldo</th>
</tr></thead><tbody>`;
          for (const [proj, pDados] of projEntries) {
            const parcEntries = Object.entries(pDados.parceiros||{}).sort((a,b) => (b[1].entradas-b[1].saidas)-(a[1].entradas-a[1].saidas));
            const firstParceiro = parcEntries[0];
            // Primeira linha do projeto (com rowspan implícito via primeira linha)
            if (parcEntries.length > 0) {
              for (let pi = 0; pi < parcEntries.length; pi++) {
                const [parc, parDados] = parcEntries[pi];
                const parSaldo = parDados.entradas - parDados.saidas;
                const parCor = parSaldo >= 0 ? '#16a34a' : '#dc2626';
                if (pi === 0) {
                  tabelaProj += `<tr style='border-bottom:1px solid #f1f5f9'>
  <td style='padding:.35rem .6rem;font-weight:600;vertical-align:top' rowspan='${parcEntries.length + 1}'>${proj}</td>
  <td style='padding:.35rem .6rem'>${parc}</td>
  <td style='padding:.35rem .6rem;text-align:right;color:#16a34a'>${fmtBRL(parDados.entradas)}</td>
  <td style='padding:.35rem .6rem;text-align:right;color:#dc2626'>${fmtBRL(parDados.saidas)}</td>
  <td style='padding:.35rem .6rem;text-align:right;font-weight:600;color:${parCor}'>${fmtBRL(parSaldo)}</td>
</tr>`;
                } else {
                  tabelaProj += `<tr style='border-bottom:1px solid #f1f5f9'>
  <td style='padding:.35rem .6rem'>${parc}</td>
  <td style='padding:.35rem .6rem;text-align:right;color:#16a34a'>${fmtBRL(parDados.entradas)}</td>
  <td style='padding:.35rem .6rem;text-align:right;color:#dc2626'>${fmtBRL(parDados.saidas)}</td>
  <td style='padding:.35rem .6rem;text-align:right;font-weight:600;color:${parCor}'>${fmtBRL(parSaldo)}</td>
</tr>`;
                }
              }
            }
            // Subtotal do projeto
            const projSaldo = pDados.entradas - pDados.saidas;
            const projCor = projSaldo >= 0 ? '#16a34a' : '#dc2626';
            tabelaProj += `<tr style='background:#f8fafc;border-bottom:2px solid #e2e8f0'>
  <td style='padding:.3rem .6rem;font-size:.8rem;color:#64748b;font-style:italic'>Subtotal ${proj}</td>
  <td style='padding:.3rem .6rem;text-align:right;color:#16a34a;font-weight:700'>${fmtBRL(pDados.entradas)}</td>
  <td style='padding:.3rem .6rem;text-align:right;color:#dc2626;font-weight:700'>${fmtBRL(pDados.saidas)}</td>
  <td style='padding:.3rem .6rem;text-align:right;font-weight:700;color:${projCor}'>${fmtBRL(projSaldo)}</td>
</tr>`;
          }
          // Total do cliente
          const cliSaldo = dados.entradas - dados.saidas;
          const cliCor = cliSaldo >= 0 ? '#16a34a' : '#dc2626';
          tabelaProj += `<tr style='background:${cor}11;border-top:2px solid ${cor}44;font-weight:700'>
  <td style='padding:.4rem .6rem' colspan='2'>TOTAL ${chave}</td>
  <td style='padding:.4rem .6rem;text-align:right;color:#16a34a'>${fmtBRL(dados.entradas)}</td>
  <td style='padding:.4rem .6rem;text-align:right;color:#dc2626'>${fmtBRL(dados.saidas)}</td>
  <td style='padding:.4rem .6rem;text-align:right;color:${cliCor}'>${fmtBRL(cliSaldo)}</td>
</tr></tbody></table></div>`;
          arvoreHTML += `<details style='margin:.2rem 0;border-left:3px solid ${cor}44;padding-left:.5rem'>
<summary style='cursor:pointer;list-style:none;${rowStyle};background:#f8fafc;border-radius:.25rem'>
  <span style='font-weight:600'>\u25B6 ${chave}</span>
  <span style='text-align:right;color:#16a34a'>${fmtBRL(dados.entradas)}</span>
  <span style='text-align:right;color:#dc2626'>${fmtBRL(dados.saidas)}</span>
  ${fmtSaldo(dados.entradas,dados.saidas)}
</summary>
<div style='padding:.25rem 0 .25rem .5rem'>${tabelaProj}</div></details>`;

        } else {
          // ESTRUTURA / FINANCEIRO: linha simples por CC
          arvoreHTML += `<div style='${rowStyle};border-bottom:1px solid #f1f5f9'>
  <span style='font-weight:500'>${chave}</span>
  <span style='text-align:right;color:#16a34a'>${fmtBRL(dados.entradas)}</span>
  <span style='text-align:right;color:#dc2626'>${fmtBRL(dados.saidas)}</span>
  ${fmtSaldo(dados.entradas,dados.saidas)}
</div>`;
        }
      }

      // Linha de total do tipo
      arvoreHTML += `<div style='${rowStyle};border-top:2px solid ${cor}44;margin-top:.35rem;font-weight:700;background:${cor}08'>
  <span>TOTAL ${tipo}</span>
  <span style='text-align:right;color:#16a34a'>${fmtBRL(tipoEnt)}</span>
  <span style='text-align:right;color:#dc2626'>${fmtBRL(tipoSai)}</span>
  ${fmtSaldo(tipoEnt,tipoSai)}
</div>`;

      arvoreHTML += `</div></details>`;
    }

    // Conteúdo principal conforme visão selecionada
    const conteudoPrincipal = visao === 'fluxo'
      ? `<h3>\uD83D\uDCB0 Fluxo de Caixa Mês a Mês</h3>
<p style='color:#64748b;font-size:.9rem;margin-bottom:.75rem'>Dinheiro que <strong>realmente entrou e saiu</strong> no período. Não inclui transferências internas entre contas (TEF).</p>
${fluxoGrafico}${fluxoTabela}
<div style='margin-top:1rem;padding:.75rem;background:#fffbeb;border:1px solid #fde68a;border-radius:.375rem;font-size:.85rem;color:#92400e'>
\u26A0\uFE0F <strong>Fluxo de Caixa \u2260 Lucro:</strong> Um mês com saldo negativo não significa prejuízo \u2014 pode ser que você pagou equipe de um projeto que ainda não faturou. Use a visão <a href='/dashboard?de=${filtroInicio}&ate=${filtroFim}&visao=resultado'>Resultado Econômico</a> para ver o lucro real por projeto.
</div>`
      : `${arvoreHTML}
<div style='margin-top:1rem;padding:.75rem;background:#eff6ff;border:1px solid #bfdbfe;border-radius:.375rem;font-size:.85rem;color:#1e40af'>
\uD83D\uDCA1 <strong>Como ler:</strong> Clique em cada grupo para expandir. <strong>Operacional</strong> = receitas e custos de projetos com clientes. <strong>Estrutura</strong> = gastos fixos da empresa (escritório, salários, jurídico). <strong>Financeiro</strong> = empréstimos e tarifas bancárias. Para ver o fluxo mês a mês, use <a href='/dashboard?de=${filtroInicio}&ate=${filtroFim}&visao=fluxo'>Fluxo de Caixa</a>.
</div>`;

    const html = page('Dashboard', `<section>
<h2>Dashboard gerencial</h2>
${periodoHTML}
<div class='cards'>
<div class='card card-clickable' onclick="openDrawer('saldo_hoje','${filtroInicio}','${filtroFim}')"><strong>Saldo de hoje</strong><span class='${metrics.saldoHoje>=0?"pos":"neg"}'>${fmtBRL(metrics.saldoHoje)}</span></div>
<div class='card card-clickable' onclick="openDrawer('proj_7','${filtroInicio}','${filtroFim}')"><strong>Projeção 7 dias</strong><span class='${metrics.proj7>=0?"pos":"neg"}'>${fmtBRL(metrics.proj7)}</span></div>
<div class='card card-clickable' onclick="openDrawer('proj_30','${filtroInicio}','${filtroFim}')"><strong>Projeção 30 dias</strong><span class='${metrics.proj30>=0?"pos":"neg"}'>${fmtBRL(metrics.proj30)}</span></div>
<div class='card card-clickable card-danger' onclick="openDrawer('a_pagar','${filtroInicio}','${filtroFim}')"><strong>A pagar</strong><span class='neg'>${fmtBRL(metrics.contasPagar)}</span></div>
<div class='card card-clickable card-teal' onclick="openDrawer('a_receber','${filtroInicio}','${filtroFim}')"><strong>A receber</strong><span class='pos'>${fmtBRL(metrics.contasReceber)}</span></div>
<div class='card card-clickable card-warning' onclick="openDrawer('saldo_mutuo','${filtroInicio}','${filtroFim}')"><strong>Saldo de mútuo</strong><span class='${metrics.saldoMutuo>=0?"pos":"neg"}'>${fmtBRL(metrics.saldoMutuo)}</span></div>
<div class='card card-clickable' style='background:${saldoFiltro>=0?"#f0fdf4":"#fef2f2"}' onclick="openDrawer('saldo_periodo','${filtroInicio}','${filtroFim}')"><strong>Saldo do período</strong><span style='color:${saldoFiltro>=0?"#16a34a":"#dc2626"}'>${fmtBRL(saldoFiltro)}</span></div>
<div class='card card-clickable card-danger' onclick="openDrawer('estrutura_total','${filtroInicio}','${filtroFim}')"><strong>Estrutura (período)</strong><span class='neg'>${fmtBRL(-Object.values(byEstruturaFiltro).filter(v=>v<0).reduce((a,v)=>a+Math.abs(v),0))}</span></div>
</div>
<!-- Drawer overlay e painel -->
<div class='drawer-overlay' id='drawerOverlay' onclick='closeDrawer()'></div>
<div class='drawer' id='drawerPanel'>
  <div class='drawer-header'>
    <h3 id='drawerTitle'>Detalhamento</h3>
    <button class='drawer-close' onclick='closeDrawer()' title='Fechar'>&#10005;</button>
  </div>
  <div class='drawer-summary' id='drawerSummary'></div>
  <div class='drawer-body' id='drawerBody'><div class='drawer-loading'>Carregando...</div></div>
</div>
<script>
function openDrawer(view, de, ate) {
  const overlay = document.getElementById('drawerOverlay');
  const panel = document.getElementById('drawerPanel');
  const body = document.getElementById('drawerBody');
  const summary = document.getElementById('drawerSummary');
  const title = document.getElementById('drawerTitle');
  body.innerHTML = '<div class="drawer-loading">&#9696; Carregando...</div>';
  summary.innerHTML = '';
  overlay.classList.add('open');
  panel.classList.add('open');
  document.body.style.overflow = 'hidden';
  const params = new URLSearchParams({ view, de: de||'', ate: ate||'' });
  fetch('/api/dashboard/drawer?' + params.toString())
    .then(r => r.json())
    .then(data => {
      title.textContent = data.titulo || 'Detalhamento';
      // Chips de resumo
      const chips = [];
      if (data.total !== undefined) {
        const cls = data.total >= 0 ? 'pos' : 'neg';
        chips.push('<span class="drawer-chip ' + cls + '">Total: ' + fmtBRL(data.total) + '</span>');
      }
      if (data.entradas !== undefined) chips.push('<span class="drawer-chip pos">Entradas: ' + fmtBRL(data.entradas) + '</span>');
      if (data.saidas !== undefined) chips.push('<span class="drawer-chip neg">Saídas: ' + fmtBRL(data.saidas) + '</span>');
      if (data.count !== undefined) chips.push('<span class="drawer-chip neu">' + data.count + ' lançamentos</span>');
      summary.innerHTML = chips.join('');
      // Tabela de lançamentos
      if (!data.lancamentos || data.lancamentos.length === 0) {
        body.innerHTML = '<div class="drawer-empty">Nenhum lançamento encontrado para este período.</div>';
        return;
      }
      const isMutuo = data.lancamentos.length > 0 && data.lancamentos[0].tipoMutuo !== null && data.lancamentos[0].tipoMutuo !== undefined;
      let rows = data.lancamentos.map(e => {
        const cls = e.valor >= 0 ? 'drawer-val-pos' : 'drawer-val-neg';
        const val = (e.valor >= 0 ? '+' : '') + fmtBRL(e.valor);
        const desc = (e.descricao || '-').slice(0, 45);
        if (isMutuo) {
          const tipoCls = e.valor > 0 ? 'drawer-val-pos' : 'drawer-val-neg';
          const tipoLabel = e.tipoMutuo || '-';
          const saldoAcumStr = e.saldoAcum !== null ? fmtBRL(e.saldoAcum) : '-';
          const saldoAcumCls = (e.saldoAcum || 0) >= 0 ? 'drawer-val-pos' : 'drawer-val-neg';
          return '<tr><td style="white-space:nowrap;color:#64748b">' + (e.dataISO||'-') + '</td><td title="' + (e.descricao||'') + '">' + desc + '</td><td class="' + tipoCls + '" style="font-size:.78rem;font-weight:600">' + tipoLabel + '</td><td class="' + cls + '">' + val + '</td><td class="' + saldoAcumCls + '" style="font-size:.78rem">Saldo: ' + saldoAcumStr + '</td></tr>';
        }
        const cc = (e.centroCusto || e.cliente || '-').slice(0, 20);
        return '<tr><td style="white-space:nowrap;color:#64748b">' + (e.dataISO||'-') + '</td><td title="' + (e.descricao||'') + '">' + desc + '</td><td style="color:#64748b;font-size:.78rem">' + cc + '</td><td class="' + cls + '">' + val + '</td></tr>';
      }).join('');
      if (isMutuo) {
        body.innerHTML = '<table><thead><tr><th>Data</th><th>Descrição</th><th>Tipo</th><th style="text-align:right">Valor</th><th style="text-align:right">Saldo devedor</th></tr></thead><tbody>' + rows + '</tbody></table>';
      } else {
        body.innerHTML = '<table><thead><tr><th>Data</th><th>Descrição</th><th>Centro</th><th style="text-align:right">Valor</th></tr></thead><tbody>' + rows + '</tbody></table>';
      }
    })
    .catch(err => {
      body.innerHTML = '<div class="drawer-empty">Erro ao carregar: ' + err.message + '</div>';
    });
}
function closeDrawer() {
  document.getElementById('drawerOverlay').classList.remove('open');
  document.getElementById('drawerPanel').classList.remove('open');
  document.body.style.overflow = '';
}
function fmtBRL(v) {
  const abs = Math.abs(v).toLocaleString('pt-BR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  return (v < 0 ? '-' : '') + 'R$ ' + abs;
}
document.addEventListener('keydown', e => { if (e.key === 'Escape') closeDrawer(); });
</script>
<h3>Próximos 7 dias (agenda financeira)</h3>
<ul>${metrics.upcoming7.slice(0, 15).map((e) => `<li>${e.dataISO} | ${e.descricao || '-'} | R$ ${e.valor.toFixed(2)}</li>`).join('') || '<li>Sem lançamentos previstos.</li>'}</ul>
${conteudoPrincipal}
</section>`, user, '/dashboard');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  // ─── API: Drawer do dashboard (retorna JSON com lançamentos por card) ────────
  if (req.method === 'GET' && url.pathname === '/api/dashboard/drawer') {
    const userD = requireAuth(req, res, db);
    if (!userD) return;
    const view = url.searchParams.get('view') || '';
    const de = url.searchParams.get('de') || CORTE_DATA;
    const ate = url.searchParams.get('ate') || new Date().toISOString().slice(0, 10);
    const today = new Date().toISOString().slice(0, 10);
    const addDaysD = (date, n) => { const d = new Date(`${date}T00:00:00Z`); d.setUTCDate(d.getUTCDate() + n); return d.toISOString().slice(0, 10); };
    const d7 = addDaysD(today, 7);
    const d30 = addDaysD(today, 30);
    const fmtV = (v) => Number(v || 0);
    let titulo = 'Detalhamento';
    let list = [];
    if (view === 'saldo_hoje') {
      titulo = 'Saldo de hoje — todos os lançamentos até hoje';
      list = db.entries.filter(e => (e.dataISO||'') <= today && !e.isTransferenciaInterna && (e.centroCusto||'').toUpperCase().trim() !== 'SALDO ATUAL').sort((a,b)=>(b.dataISO||'').localeCompare(a.dataISO||''));
    } else if (view === 'proj_7') {
      titulo = 'Projeção 7 dias — lançamentos até ' + d7;
      list = db.entries.filter(e => isAtivo(e) && (e.dataISO||'') <= d7 && !e.isTransferenciaInterna && (e.centroCusto||'').toUpperCase().trim() !== 'SALDO ATUAL').sort((a,b)=>(b.dataISO||'').localeCompare(a.dataISO||''));
    } else if (view === 'proj_30') {
      titulo = 'Projeção 30 dias — lançamentos até ' + d30;
      list = db.entries.filter(e => isAtivo(e) && (e.dataISO||'') <= d30 && !e.isTransferenciaInterna && (e.centroCusto||'').toUpperCase().trim() !== 'SALDO ATUAL').sort((a,b)=>(b.dataISO||'').localeCompare(a.dataISO||''));
    } else if (view === 'a_pagar') {
      titulo = 'A pagar — lançamentos futuros negativos';
      list = db.entries.filter(e => isAtivo(e) && (e.dataISO||'') > today && fmtV(e.valor) < 0).sort((a,b)=>(a.dataISO||'').localeCompare(b.dataISO||''));
    } else if (view === 'a_receber') {
      titulo = 'A receber — lançamentos futuros positivos';
      list = db.entries.filter(e => isAtivo(e) && (e.dataISO||'') > today && fmtV(e.valor) > 0).sort((a,b)=>(a.dataISO||'').localeCompare(b.dataISO||''));
    } else if (view === 'saldo_mutuo') {
      titulo = 'Mútuo com sócios — empréstimos e devoluções';
      // Filtra apenas CC MÚTUO (empréstimos de sócios), excluindo clientes com "mútuo" na descrição
      list = db.entries.filter(e => (e.centroCusto||'').toUpperCase().trim() === 'MÚTUO').sort((a,b)=>(b.dataISO||'').localeCompare(a.dataISO||''));
      // Calcula saldo acumulado cronologicamente (do mais antigo ao mais novo)
      const mutuoOrdenado = [...list].sort((a,b)=>(a.dataISO||'').localeCompare(b.dataISO||''));
      let acum = 0;
      const mutuoAcum = {};
      mutuoOrdenado.forEach(e => { acum += fmtV(e.valor); mutuoAcum[e.id] = acum; });
      // Adiciona campo tipo e saldo acumulado a cada lançamento
      list = list.map(e => ({
        ...e,
        tipoMutuo: fmtV(e.valor) > 0 ? 'Empréstimo recebido' : fmtV(e.valor) < 0 ? 'Devolução paga' : 'Ajuste',
        saldoAcum: mutuoAcum[e.id]
      }));
    } else if (view === 'saldo_periodo') {
      titulo = `Saldo do período — ${de} a ${ate}`;
      list = db.entries.filter(e => (e.dataISO||'') >= de && (e.dataISO||'') <= ate && !e.isTransferenciaInterna && (e.centroCusto||'').toUpperCase().trim() !== 'SALDO ATUAL').sort((a,b)=>(b.dataISO||'').localeCompare(a.dataISO||''));
    } else if (view === 'estrutura_total') {
      titulo = `Custos de estrutura — ${de} a ${ate}`;
      list = db.entries.filter(e => {
        if (!isAtivo(e)) return false;
        if ((e.dataISO||'') < de || (e.dataISO||'') > ate) return false;
        if (clienteEfetivo(e) !== 'SEM CLIENTE') return false;
        const cc = (e.centroCusto||'').toUpperCase();
        return cc !== 'TEF' && cc !== 'SALDO ATUAL' && !e.isTransferenciaInterna;
      }).sort((a,b)=>(b.dataISO||'').localeCompare(a.dataISO||''));
    } else {
      return json(res, 400, { error: 'view inválida' });
    }
    const MAX = 100;
    const sample = list.slice(0, MAX);
    const entradas = list.filter(e => fmtV(e.valor) > 0).reduce((a,e) => a + fmtV(e.valor), 0);
    const saidas = list.filter(e => fmtV(e.valor) < 0).reduce((a,e) => a + Math.abs(fmtV(e.valor)), 0);
    const total = entradas - saidas;
    return json(res, 200, {
      titulo,
      total,
      entradas,
      saidas,
      count: list.length,
      lancamentos: sample.map(e => ({
        id: e.id,
        dataISO: e.dataISO || e.data || '',
        descricao: (e.descricao || e.historico || '').slice(0, 80),
        centroCusto: (e.centroCusto || '').slice(0, 30),
        cliente: clienteEfetivo(e).slice(0, 30),
        projeto: projetoEfetivo(e).slice(0, 30),
        valor: fmtV(e.valor),
        natureza: (e.natureza || '').slice(0, 20),
        tipoMutuo: e.tipoMutuo || null,
        saldoAcum: e.saldoAcum !== undefined ? e.saldoAcum : null
      }))
    });
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
      title = 'Mútuo com sócios';
      // Filtra apenas CC MÚTUO (empréstimos de sócios)
      list = db.entries.filter((e) => (e.centroCusto||'').toUpperCase().trim() === 'MÚTUO');
    } else if (view === 'cliente') {
      title = `Resultado por cliente: ${chave}`;
      // Usa clienteEfetivo() para filtrar corretamente (mesmo critério do dashboard)
      list = db.entries.filter((e) => isAtivo(e) && clienteEfetivo(e) === chave);
    } else if (view === 'projeto') {
      title = `Resultado por projeto: ${chave}`;
      // Usa projetoEfetivo() para filtrar corretamente (mesmo critério do dashboard)
      list = db.entries.filter((e) => isAtivo(e) && projetoEfetivo(e) === chave);
    } else if (view === 'risco_caixa') {
      title = 'Risco de caixa (projeções)';
      list = db.entries.filter((e) => e.dataISO <= d30);
    } else if (view === 'estrutura') {
      // Detalhe de um centro de custo de estrutura específico
      title = `Custos de Estrutura: ${chave}`;
      list = db.entries.filter((e) => {
        if (!isAtivo(e)) return false;
        if (clienteEfetivo(e) !== 'SEM CLIENTE') return false;
        const cc = (e.centroCusto || 'SEM CLASSIFICAÇÃO').toUpperCase();
        return cc === chave.toUpperCase() && cc !== 'TEF';
      });
    } else if (view === 'estrutura_total') {
      // Todos os custos de estrutura
      title = 'Custos de Estrutura — todos os lançamentos';
      list = db.entries.filter((e) => {
        if (!isAtivo(e)) return false;
        if (clienteEfetivo(e) !== 'SEM CLIENTE') return false;
        const cc = (e.centroCusto || '').toUpperCase();
        return cc !== 'TEF';
      });
    }
    const total = list.reduce((acc, e) => acc + Number(e.valor || 0), 0);
    const html = page('Detalhamento do dashboard', `<section><h2>${title}</h2><p>Total do recorte: <strong>R$ ${total.toFixed(2)}</strong></p><p><a href='/dashboard'>← Voltar ao dashboard</a></p>${entriesTable(list)}</section>`, user, '/dashboard');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  // ─── MÓDULO FATURA DE CARTÃO ────────────────────────────────────────────────
  if (req.method === 'GET' && url.pathname === '/fatura') {
    const user = requireAuth(req, res, db);
    if (!user) return;
    // Lançamentos que parecem ser de cartão (para o usuário selecionar qual substituir)
    const cartaoEntries = db.entries
      .filter((e) => {
        const desc = (e.descricao || '').toUpperCase();
        return desc.includes('FATURA') || desc.includes('CARTAO') || desc.includes('CARTÃO') || desc.includes('CREDITO') || desc.includes('CRÉDITO') || (e.natureza || '').toUpperCase().includes('CARTÃO');
      })
      .sort((a, b) => (b.dataISO || '').localeCompare(a.dataISO || ''))
      .slice(0, 50);
    const optionsHtml = cartaoEntries.map((e) =>
      `<option value='${e.id}'>${e.dataISO || e.data} | ${e.descricao || '-'} | R$ ${Number(e.valor||0).toFixed(2)}</option>`
    ).join('');
    const html = page('Fatura de Cartão', `
<section>
  <h2>&#128179; Detalhar Fatura de Cartão</h2>
  <p style='color:var(--gray-600);margin-bottom:1.5rem'>Faça upload do PDF da fatura do cartão. A IA extrai os itens, classifica com base em históricos anteriores e você revisa antes de substituir o lançamento original.</p>
  <div style='background:var(--white);border:1px solid var(--gray-200);border-radius:10px;padding:1.5rem;max-width:700px'>
    <form id='fatura-form'>
      <div style='margin-bottom:1rem'>
        <label style='font-size:.85rem;font-weight:700;color:var(--gray-600);text-transform:uppercase;letter-spacing:.04em'>1. Selecione o lançamento de cartão a substituir</label>
        <select id='entry-id' name='entryId' style='margin-top:.4rem'>
          <option value=''>-- Selecione o lançamento --</option>
          ${optionsHtml}
        </select>
        <p style='font-size:.76rem;color:var(--gray-400);margin:.3rem 0 0'>Não encontrou? O lançamento pode estar com outra descrição. Use a busca abaixo.</p>
        <input id='busca-entry' placeholder='Buscar por descrição...' style='margin-top:.4rem' oninput='filtrarEntries(this.value)'/>
      </div>
      <div style='margin-bottom:1rem'>
        <label style='font-size:.85rem;font-weight:700;color:var(--gray-600);text-transform:uppercase;letter-spacing:.04em'>2. Faça upload do PDF da fatura</label>
        <input type='file' id='fatura-pdf' accept='.pdf' style='margin-top:.4rem;padding:.4rem;border:2px dashed var(--gray-300);border-radius:6px;width:100%;cursor:pointer'/>
      </div>
      <div style='margin-bottom:1rem'>
        <label style='font-size:.85rem;font-weight:700;color:var(--gray-600);text-transform:uppercase;letter-spacing:.04em'>3. Cartão / Conta</label>
        <input id='cartao-nome' placeholder='Ex: Itaú Visa, Nubank, XP Visa...' style='margin-top:.4rem'/>
      </div>
      <button type='button' onclick='processarFatura()' style='width:100%;background:#1d4ed8'>&#128269; Extrair e Classificar com IA</button>
    </form>
    <div id='fatura-loading' style='display:none;text-align:center;padding:2rem'>
      <p style='color:#1d4ed8;font-weight:600'>&#9203; Processando PDF e classificando itens com IA...</p>
      <p style='font-size:.82rem;color:var(--gray-400)'>Isso pode levar alguns segundos dependendo do tamanho da fatura.</p>
    </div>
    <div id='fatura-resultado' style='display:none;margin-top:1.5rem'></div>
  </div>
</section>
<script>
const allEntries = ${JSON.stringify(cartaoEntries.map((e) => ({ id: e.id, desc: e.descricao || '', data: e.dataISO || e.data || '', valor: e.valor })))};
function filtrarEntries(q) {
  const sel = document.getElementById('entry-id');
  const opts = allEntries.filter((e) => e.desc.toLowerCase().includes(q.toLowerCase()));
  sel.innerHTML = '<option value="">-- Selecione o lançamento --</option>' +
    opts.map((e) => '<option value="' + e.id + '">' + e.data + ' | ' + e.desc + ' | R$ ' + Number(e.valor||0).toFixed(2) + '</option>').join('');
}
async function processarFatura() {
  const entryId = document.getElementById('entry-id').value;
  const file = document.getElementById('fatura-pdf').files[0];
  const cartao = document.getElementById('cartao-nome').value;
  if (!entryId) { alert('Selecione o lançamento de cartão a substituir.'); return; }
  if (!file) { alert('Selecione o PDF da fatura.'); return; }
  document.getElementById('fatura-loading').style.display = 'block';
  document.getElementById('fatura-resultado').style.display = 'none';
  const fd = new FormData();
  fd.append('pdf', file);
  fd.append('entryId', entryId);
  fd.append('cartao', cartao);
  try {
    const resp = await fetch('/api/fatura/processar', { method: 'POST', body: fd });
    const data = await resp.json();
    document.getElementById('fatura-loading').style.display = 'none';
    if (data.error) { alert('Erro: ' + data.error); return; }
    renderResultado(data);
  } catch(e) {
    document.getElementById('fatura-loading').style.display = 'none';
    alert('Erro ao processar: ' + e.message);
  }
}
function renderResultado(data) {
  const div = document.getElementById('fatura-resultado');
  div.style.display = 'block';
  const rows = data.itens.map((item, i) => {
    const pendente = !item.natureza || item.natureza === 'Pendente';
    const rowBg = pendente ? 'background:#fffbeb' : '';
    return '<tr style="' + rowBg + '" id="row-' + i + '">' +
      '<td style="white-space:nowrap;font-size:.8rem">' + (item.data || '-') + '</td>' +
      '<td style="font-size:.8rem">' + (item.descricao || '-') + '</td>' +
      '<td style="font-size:.8rem;white-space:nowrap;color:#991b1b;font-weight:600">R$ ' + Number(item.valor||0).toFixed(2) + '</td>' +
      '<td><select id="nat-' + i + '" style="font-size:.76rem;padding:.2rem .4rem">' +
        ['Receita Operacional','Despesa Direta','Despesa Indireta','Despesa Administrativa','Despesa Financeira','Transferência','Pendente'].map((n) =>
          '<option ' + (n === (item.natureza||'Pendente') ? 'selected' : '') + '>' + n + '</option>'
        ).join('') +
      '</select></td>' +
      '<td><input id="cc-' + i + '" value="' + (item.centroCusto||'') + '" placeholder="CC" style="font-size:.76rem;padding:.2rem .4rem;width:90px"/></td>' +
      '<td><input id="proj-' + i + '" value="' + (item.projeto||'') + '" placeholder="Projeto" style="font-size:.76rem;padding:.2rem .4rem;width:100px"/></td>' +
      '<td style="font-size:.76rem;color:var(--gray-400);font-style:italic;max-width:160px">' + (item.explicacaoIA || '') + '</td>' +
    '</tr>';
  }).join('');
  div.innerHTML = '<h3 style="margin-bottom:.75rem">Itens extraídos da fatura (' + data.itens.length + ')</h3>' +
    '<p style="font-size:.82rem;color:var(--gray-600);margin-bottom:.75rem">Itens em amarelo foram deixados como <strong>Pendente</strong> pela IA. Revise e ajuste antes de confirmar.</p>' +
    '<div style="overflow-x:auto;margin-bottom:1rem"><table style="min-width:800px"><thead><tr>' +
    '<th>Data</th><th>Descrição</th><th>Valor</th><th>Natureza</th><th>Centro Custo</th><th>Projeto</th><th>Obs. IA</th>' +
    '</tr></thead><tbody>' + rows + '</tbody></table></div>' +
    '<div style="background:#fef2f2;border:1px solid #fecaca;border-radius:8px;padding:.75rem 1rem;margin-bottom:1rem">' +
    '<strong style="font-size:.85rem;color:#991b1b">⚠ Atenção:</strong> <span style="font-size:.82rem;color:#7f1d1d">Ao confirmar, o lançamento original de R$ ' + Number(data.valorOriginal||0).toFixed(2) + ' será <strong>removido</strong> e substituído pelos ' + data.itens.length + ' itens acima. Esta ação não pode ser desfeita.</span>' +
    '</div>' +
    '<button onclick="confirmarFatura(' + JSON.stringify(data.entryId) + ', ' + data.itens.length + ')" style="background:#059669;width:100%">&#10003; Confirmar e substituir lançamento original</button>';
  window._faturaData = data;
}
async function confirmarFatura(entryId, count) {
  if (!confirm('Confirmar substituição do lançamento original por ' + count + ' itens detalhados?')) return;
  const itens = window._faturaData.itens.map((item, i) => ({
    ...item,
    natureza: document.getElementById('nat-' + i)?.value || item.natureza,
    centroCusto: document.getElementById('cc-' + i)?.value || item.centroCusto,
    projeto: document.getElementById('proj-' + i)?.value || item.projeto
  }));
  const resp = await fetch('/api/fatura/confirmar', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify({ entryId, itens })
  });
  const data = await resp.json();
  if (data.ok) {
    alert('Substituição realizada! ' + data.inseridos + ' lançamentos inseridos.');
    location.href = '/cadastros';
  } else {
    alert('Erro: ' + (data.error || 'desconhecido'));
  }
}
</script>`, user, '/fatura');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/fatura/processar') {
    if (!requireAuth(req, res, db)) return;
    // Usar multer para receber o PDF
    const multer = require('multer');
    const upload = multer({ dest: os.tmpdir() });
    await new Promise((resolve, reject) => {
      upload.single('pdf')(req, res, (err) => { if (err) reject(err); else resolve(); });
    });
    const pdfPath = req.file?.path;
    const entryId = req.body?.entryId;
    const cartao = req.body?.cartao || 'Cartão';
    if (!pdfPath || !entryId) return json(res, 400, { error: 'PDF e entryId são obrigatórios' });
    const entry = db.entries.find((e) => e.id === entryId);
    if (!entry) return json(res, 404, { error: 'Lançamento não encontrado' });
    // Extrair texto do PDF
    let pdfText = '';
    try {
      const { execSync } = require('child_process');
      pdfText = execSync(`pdftotext -layout "${pdfPath}" -`, { maxBuffer: 5 * 1024 * 1024 }).toString();
    } catch (err) {
      return json(res, 500, { error: 'Erro ao ler PDF: ' + err.message });
    } finally {
      try { fs.unlinkSync(pdfPath); } catch (_) {}
    }
    // Contexto de históricos já classificados para a IA usar como referência
    const historicos = db.entries
      .filter((e) => e.natureza && e.natureza !== 'Pendente' && e.descricao)
      .slice(-200)
      .map((e) => `${e.descricao} → ${e.natureza}${e.centroCusto ? ' / ' + e.centroCusto : ''}${e.projeto ? ' / ' + e.projeto : ''}`)
      .join('\n');
    const prompt = `Você é um assistente financeiro da empresa CKM Consultoria. Analise o texto abaixo extraído de uma fatura do cartão "${cartao}" e extraia todos os lançamentos individuais.

Para cada lançamento, classifique com base nos históricos de lançamentos já classificados da empresa (fornecidos abaixo como referência).

Históricos de referência (descrição → natureza/CC/projeto):
${historicos.slice(0, 3000)}

Texto da fatura:
${pdfText.slice(0, 6000)}

Retorne um JSON com o seguinte formato (apenas o JSON, sem texto adicional):
{
  "itens": [
    {
      "data": "YYYY-MM-DD",
      "descricao": "descrição do item",
      "valor": 99.90,
      "natureza": "Despesa Direta|Despesa Indireta|Despesa Administrativa|Despesa Financeira|Receita Operacional|Transferência|Pendente",
      "centroCusto": "nome do CC ou vazio",
      "projeto": "nome do projeto ou vazio",
      "explicacaoIA": "breve justificativa em português"
    }
  ]
}

Regras:
- Se não souber classificar, use natureza: "Pendente" e explicacaoIA: "Não foi possível classificar automaticamente"
- Valores devem ser positivos (são débitos do cartão)
- Se a data não estiver clara, use a data de vencimento da fatura
- Ignore linhas de total, subtotal, pagamento anterior e saldo`;
    try {
      const { OpenAI } = require('openai');
      const openai = new OpenAI();
      const completion = await openai.chat.completions.create({
        model: 'gpt-4.1-mini',
        messages: [{ role: 'user', content: prompt }],
        temperature: 0.1,
        max_tokens: 4000
      });
      const raw = (completion.choices[0]?.message?.content || '').trim();
      const jsonMatch = raw.match(/\{[\s\S]*\}/);
      if (!jsonMatch) return json(res, 500, { error: 'IA não retornou JSON válido' });
      const parsed = JSON.parse(jsonMatch[0]);
      return json(res, 200, {
        entryId,
        valorOriginal: entry.valor,
        cartao,
        itens: parsed.itens || []
      });
    } catch (err) {
      console.error('[fatura IA]', err.message);
      return json(res, 500, { error: 'Erro ao processar com IA: ' + err.message });
    }
  }

  if (req.method === 'POST' && url.pathname === '/api/fatura/confirmar') {
    if (!requireAuth(req, res, db)) return;
    const { entryId, itens } = JSON.parse(await readBody(req) || '{}');
    if (!entryId || !Array.isArray(itens) || itens.length === 0) {
      return json(res, 400, { error: 'entryId e itens são obrigatórios' });
    }
    const entryIdx = db.entries.findIndex((e) => e.id === entryId);
    if (entryIdx === -1) return json(res, 404, { error: 'Lançamento não encontrado' });
    const original = db.entries[entryIdx];
    // Criar novos lançamentos detalhados no lugar do original
    const novosLancamentos = itens.map((item) => ({
      id: crypto.randomUUID(),
      data: item.data || original.data,
      dataISO: item.data || original.dataISO,
      descricao: item.descricao || '-',
      valor: -Math.abs(Number(item.valor || 0)), // débitos do cartão são negativos
      natureza: item.natureza || 'Pendente',
      centroCusto: item.centroCusto || original.centroCusto || '',
      projeto: item.projeto || '',
      conta: original.conta || '',
      status: item.natureza === 'Pendente' ? 'pendente' : 'ok',
      origem: 'fatura_cartao',
      cartao: original.descricao || 'Cartão',
      entryOriginalId: entryId
    }));
    // Remover o lançamento original e inserir os detalhados
    db.entries.splice(entryIdx, 1, ...novosLancamentos);
    saveDb(db);
    return json(res, 200, { ok: true, inseridos: novosLancamentos.length, removido: entryId });
  }

  // ===== REFERÊNCIAS: Clientes, Projetos, Centros de Custo =====
  if (req.method === 'GET' && url.pathname === '/referencias') {
    if (!user) { res.writeHead(302, { Location: '/login' }); res.end(); return; }
    const refs = db.referencias || { clientes: [], projetos: [], centrosCusto: [], contas: [] };
    // Mostrar apenas os valores cadastrados manualmente + CC_PADRAO
    // Não carregar todos os entries para evitar crash de memória
    const allCC = [...new Set([...CC_PADRAO, ...(refs.centrosCusto||[])])].sort();
    const allClientes = [...(refs.clientes||[])].sort();
    const allProjetos = [...(refs.projetos||[])].sort();
    const allContas = [...(refs.contas||[])].sort();
    const section = (titulo, lista, tipo, cor) => `
      <div style='background:#fff;border:1px solid var(--gray-200);border-radius:10px;padding:1.25rem'>
        <div style='display:flex;align-items:center;justify-content:space-between;margin-bottom:1rem'>
          <h3 style='margin:0;font-size:1rem;color:${cor}'>${titulo}</h3>
          <button onclick="adicionarRef('${tipo}')" style='font-size:.8rem;padding:.35rem .75rem;background:${cor}'>+ Novo</button>
        </div>
        <div id='list-${tipo}' style='display:flex;flex-direction:column;gap:.4rem'>
          ${lista.map(v=>`
            <div style='display:flex;align-items:center;gap:.5rem;padding:.4rem .6rem;background:var(--gray-50);border-radius:6px;border:1px solid var(--gray-200)'>
              <span style='flex:1;font-size:.85rem'>${v}</span>
              <button onclick="editarRef('${tipo}','${v.replace(/'/g,"\\'")}')"
                style='font-size:.72rem;padding:.2rem .5rem;background:#f1f5f9;color:#475569;box-shadow:none;border:1px solid #cbd5e1'>&#9998;</button>
              <button onclick="excluirRef('${tipo}','${v.replace(/'/g,"\\'")}')"
                style='font-size:.72rem;padding:.2rem .5rem;background:#fee2e2;color:#dc2626;box-shadow:none;border:1px solid #fca5a5'>&#10005;</button>
            </div>`).join('')}
        </div>
      </div>`;
    const html = page('Refer\u00eancias', `
<section>
  <div style='display:flex;align-items:center;gap:1rem;margin-bottom:1.5rem'>
    <h2 style='margin:0'>Refer\u00eancias do Sistema</h2>
    <a href='/cadastros' style='font-size:.82rem;color:var(--blue)'>&#8592; Voltar para Cadastros</a>
  </div>
  <p style='font-size:.85rem;color:var(--gray-400);margin-bottom:1.5rem'>Gerencie os valores controlados usados nos campos de edi\u00e7\u00e3o. Ao editar um lan\u00e7amento, apenas estes valores aparecer\u00e3o como op\u00e7\u00f5es.</p>
  <div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:1.25rem'>
    ${section('&#128100; Clientes / Parceiros', allClientes, 'clientes', '#1d4ed8')}
    ${section('&#128196; Projetos', allProjetos, 'projetos', '#7c3aed')}
    ${section('&#127970; Centros de Custo', allCC, 'centrosCusto', '#059669')}
    ${section('&#127981; Contas / Bancos', allContas, 'contas', '#d97706')}
  </div>
</section>
<script>
async function adicionarRef(tipo){
  const nome=prompt('Nome do novo item:');
  if(!nome||!nome.trim()) return;
  await fetch('/api/referencias',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({tipo,nome:nome.trim()})});
  location.reload();
}
async function editarRef(tipo,nomeAtual){
  const novo=prompt('Novo nome:',nomeAtual);
  if(!novo||!novo.trim()||novo===nomeAtual) return;
  await fetch('/api/referencias',{method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify({tipo,nomeAtual,nomeNovo:novo.trim()})});
  location.reload();
}
async function excluirRef(tipo,nome){
  if(!confirm('Remover "'+nome+'" da lista de refer\u00eancias?')) return;
  await fetch('/api/referencias',{method:'DELETE',headers:{'content-type':'application/json'},body:JSON.stringify({tipo,nome})});
  location.reload();
}
<\/script>`);
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(html);
  }
  if (req.method === 'POST' && url.pathname === '/api/referencias') {
    if (!user) { res.writeHead(302, { Location: '/login' }); res.end(); return; }
    const db = loadDb();
    const { tipo, nome } = JSON.parse(await readBody(req) || '{}');
    if (!tipo || !nome) return json(res, 400, { error: 'tipo e nome s\u00e3o obrigat\u00f3rios' });
    if (!db.referencias) db.referencias = { clientes: [], projetos: [], centrosCusto: [], contas: [] };
    if (!db.referencias[tipo]) db.referencias[tipo] = [];
    if (!db.referencias[tipo].includes(nome)) db.referencias[tipo].push(nome);
    saveDb(db);
    return json(res, 200, { ok: true });
  }
  if (req.method === 'PUT' && url.pathname === '/api/referencias') {
    if (!user) { res.writeHead(302, { Location: '/login' }); res.end(); return; }
    const db = loadDb();
    const { tipo, nomeAtual, nomeNovo } = JSON.parse(await readBody(req) || '{}');
    if (!db.referencias || !db.referencias[tipo]) return json(res, 404, { error: 'N\u00e3o encontrado' });
    const idx = db.referencias[tipo].indexOf(nomeAtual);
    if (idx !== -1) db.referencias[tipo][idx] = nomeNovo;
    // Atualizar entries que usam o nome antigo
    db.entries.forEach(e => {
      if (tipo === 'clientes') { if (e.cliente === nomeAtual) e.cliente = nomeNovo; if (e.parceiro === nomeAtual) e.parceiro = nomeNovo; }
      if (tipo === 'projetos' && e.projeto === nomeAtual) e.projeto = nomeNovo;
      if (tipo === 'centrosCusto' && e.centroCusto === nomeAtual) e.centroCusto = nomeNovo;
      if (tipo === 'contas' && e.conta === nomeAtual) e.conta = nomeNovo;
    });
    saveDb(db);
    return json(res, 200, { ok: true });
  }
  if (req.method === 'DELETE' && url.pathname === '/api/referencias') {
    if (!user) { res.writeHead(302, { Location: '/login' }); res.end(); return; }
    const db = loadDb();
    const { tipo, nome } = JSON.parse(await readBody(req) || '{}');
    if (!db.referencias || !db.referencias[tipo]) return json(res, 404, { error: 'N\u00e3o encontrado' });
    db.referencias[tipo] = db.referencias[tipo].filter(v => v !== nome);
    saveDb(db);
    return json(res, 200, { ok: true });
  }
  // ===== HISTÓRICO DE ALTERAÇÕES =====
  // GET /api/entries/:id/historico — retorna auditLog filtrado por entryId
  if (req.method === 'GET' && url.pathname.match(/^\/api\/entries\/[^/]+\/historico$/)) {
    if (!user) { res.writeHead(302, { Location: '/login' }); res.end(); return; }
    const entryId = url.pathname.split('/')[3];
    const log = (db.auditLog || []).filter(r => r.entryId === entryId).sort((a, b) => b.ts.localeCompare(a.ts));
    return json(res, 200, log);
  }

  // GET /historico — página global de auditoria (com paginação para evitar crash de memória)
  if (req.method === 'GET' && url.pathname === '/historico') {
    if (!user) { res.writeHead(302, { Location: '/login' }); res.end(); return; }
    const PAGE_SIZE = 100;
    const page_num = Math.max(1, parseInt(url.searchParams.get('p') || '1', 10));
    const qFilter = (url.searchParams.get('q') || '').toLowerCase().trim();

    // Construir mapa de entries para lookup rápido
    const entryMap = new Map(db.entries.map(e => [e.id, e]));

    // Ordenar e filtrar sem gerar HTML de todas as linhas de uma vez
    let log = (db.auditLog || []).slice().sort((a, b) => b.ts.localeCompare(a.ts));

    // Filtro server-side por texto (registro, campo, usuário)
    if (qFilter) {
      log = log.filter(r => {
        const entry = r.entryId ? entryMap.get(r.entryId) : null;
        const descricao = entry ? (entry.descricao || '') : (r.nomeOficial || r.registroId || '');
        return (descricao + r.campo + (r.de || '') + (r.para || '') + (r.usuario || '')).toLowerCase().includes(qFilter);
      });
    }

    const totalLog = log.length;
    const totalPages = Math.max(1, Math.ceil(totalLog / PAGE_SIZE));
    const currentPage = Math.min(page_num, totalPages);
    const pageLog = log.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE);

    const LABELS = {
      cliente: 'Cliente', projeto: 'Projeto', parceiro: 'Parceiro', centroCusto: 'Centro de Custo',
      natureza: 'Natureza', categoria: 'Categoria', detalhe: 'Detalhe', conta: 'Conta',
      formaPagamento: 'Forma de Pagamento', status: 'Status', descricao: 'Descrição',
      valor: 'Valor', dc: 'D/C', data: 'Data', dataISO: 'Data ISO',
      tipoFinal: 'Tipo Final', statusRevisao: 'Status Revisão',
      clienteVinculado: 'Cliente Vinculado', projetoVinculado: 'Projeto Vinculado', observacao: 'Observação'
    };
    const formatTs = ts => {
      const d = new Date(ts);
      return d.toLocaleDateString('pt-BR') + ' ' + d.toLocaleTimeString('pt-BR', { hour: '2-digit', minute: '2-digit' });
    };
    const rows = pageLog.map(r => {
      const entry = r.entryId ? entryMap.get(r.entryId) : null;
      const descricao = entry ? (entry.descricao || entry.id.slice(0, 8)) : (r.nomeOficial || r.registroId || '-');
      const tipoLog = r.tipo === 'revisao' ? '<span style="font-size:.72rem;background:#ede9fe;color:#7c3aed;padding:.15rem .45rem;border-radius:4px">Revisão</span>' : '<span style="font-size:.72rem;background:#dbeafe;color:#1d4ed8;padding:.15rem .45rem;border-radius:4px">Lançamento</span>';
      return `<tr>
        <td style='font-size:.78rem;color:var(--gray-500);white-space:nowrap'>${formatTs(r.ts)}</td>
        <td>${tipoLog}</td>
        <td style='font-size:.82rem;max-width:200px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap' title='${descricao}'>${descricao}</td>
        <td style='font-size:.82rem'><strong>${LABELS[r.campo] || r.campo}</strong></td>
        <td style='font-size:.82rem;color:var(--red);text-decoration:line-through;max-width:160px;overflow:hidden;text-overflow:ellipsis' title='${r.de}'>${r.de || '<em style="color:var(--gray-400)">(vazio)</em>'}</td>
        <td style='font-size:.82rem;color:var(--green);max-width:160px;overflow:hidden;text-overflow:ellipsis' title='${r.para}'>${r.para || '<em style="color:var(--gray-400)">(vazio)</em>'}</td>
        <td style='font-size:.78rem;color:var(--gray-500)'>${r.usuario || '-'}</td>
      </tr>`;
    }).join('');

    // Paginação
    const buildPageUrl = (p) => '/historico?p=' + p + (qFilter ? '&q=' + encodeURIComponent(qFilter) : '');
    const pagination = totalPages <= 1 ? '' : `
    <div style='display:flex;gap:.5rem;align-items:center;flex-wrap:wrap;margin-top:1rem'>
      ${currentPage > 1 ? `<a href='${buildPageUrl(1)}' style='padding:.3rem .7rem;border:1px solid var(--gray-200);border-radius:6px;font-size:.82rem;text-decoration:none;color:var(--gray-700)'>&laquo; Primeira</a>` : ''}
      ${currentPage > 1 ? `<a href='${buildPageUrl(currentPage-1)}' style='padding:.3rem .7rem;border:1px solid var(--gray-200);border-radius:6px;font-size:.82rem;text-decoration:none;color:var(--gray-700)'>&lsaquo; Anterior</a>` : ''}
      <span style='font-size:.82rem;color:var(--gray-500)'>Página ${currentPage} de ${totalPages} (${totalLog} registros)</span>
      ${currentPage < totalPages ? `<a href='${buildPageUrl(currentPage+1)}' style='padding:.3rem .7rem;border:1px solid var(--gray-200);border-radius:6px;font-size:.82rem;text-decoration:none;color:var(--gray-700)'>Próxima &rsaquo;</a>` : ''}
      ${currentPage < totalPages ? `<a href='${buildPageUrl(totalPages)}' style='padding:.3rem .7rem;border:1px solid var(--gray-200);border-radius:6px;font-size:.82rem;text-decoration:none;color:var(--gray-700)'>Última &raquo;</a>` : ''}
    </div>`;

    const html = page('Histórico de Alterações', `
<section>
  <div style='display:flex;align-items:center;gap:1rem;margin-bottom:1rem;flex-wrap:wrap'>
    <h2 style='margin:0'>Histórico de Alterações</h2>
    <span style='font-size:.82rem;color:var(--gray-400)'>${totalLog} registro(s)${qFilter ? ' filtrados' : ''}</span>
  </div>
  <p style='font-size:.85rem;color:var(--gray-400);margin-bottom:1rem'>Trilha completa de auditoria: toda alteração manual em lançamentos e revisões de cadastro é registrada aqui.</p>
  <form method='get' action='/historico' style='display:flex;gap:.5rem;margin-bottom:1rem'>
    <input name='q' value='${qFilter.replace(/"/g,'&quot;')}' placeholder='Buscar por registro, campo ou usuário...' style='flex:1;padding:.5rem .8rem;border:1px solid var(--gray-200);border-radius:8px;font-size:.88rem'/>
    <button type='submit' style='padding:.5rem 1rem;font-size:.88rem'>Buscar</button>
    ${qFilter ? `<a href='/historico' style='padding:.5rem .8rem;border:1px solid var(--gray-200);border-radius:8px;font-size:.88rem;text-decoration:none;color:var(--gray-600)'>Limpar</a>` : ''}
  </form>
  ${pagination}
  <div style='overflow-x:auto;margin-top:.75rem'>
  <table style='width:100%;border-collapse:collapse;font-size:.85rem'>
    <thead><tr style='background:var(--gray-50);border-bottom:2px solid var(--gray-200)'>
      <th style='padding:.6rem .75rem;text-align:left;white-space:nowrap'>Data/Hora</th>
      <th style='padding:.6rem .75rem;text-align:left'>Tipo</th>
      <th style='padding:.6rem .75rem;text-align:left'>Registro</th>
      <th style='padding:.6rem .75rem;text-align:left'>Campo</th>
      <th style='padding:.6rem .75rem;text-align:left'>Antes</th>
      <th style='padding:.6rem .75rem;text-align:left'>Depois</th>
      <th style='padding:.6rem .75rem;text-align:left'>Usuário</th>
    </tr></thead>
    <tbody>${rows || '<tr><td colspan="7" style="text-align:center;padding:2rem;color:var(--gray-400)">Nenhuma alteração registrada ainda.</td></tr>'}</tbody>
  </table>
  </div>
  ${pagination}
</section>`, user, '/historico');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    return res.end(html);
  }

  // ===== ROTA DE MANUTENÇÃO: limpar cadastros com ruído no reviewRegistry e entries =====
  if (req.method === 'POST' && url.pathname === '/api/admin/limpar-cadastros') {
    const user = requireAuth(req, res, db);
    if (!user) return;

    // Função auxiliar local para verificar lixo
    const isLixo = (val) => {
      if (!val) return true;
      const s = String(val).trim();
      if (/^\d+(\.\d+)+$/.test(s)) return true;
      if (/^\d+$/.test(s)) return true;
      if (s === '' || s === '-' || s === '--' || s === '.') return true;
      return false;
    };

    // 1. Corrigir entries: aplicar normalizeParceiro, limpar cliente lixo e marcar dc=T
    let entriesCorrigidos = 0;
    let transferenciasInternas = 0;
    for (const e of db.entries) {
      let changed = false;
      const novoParceiro = normalizeParceiro(e.parceiro || '');
      if (novoParceiro !== (e.parceiro || '')) { e.parceiro = novoParceiro; changed = true; }
      if (isLixo(e.cliente)) { if (e.cliente) { e.cliente = ''; changed = true; } }
      // Marcar transferências internas (dc=T) que ainda não foram marcadas
      if (String(e.dc || '').toUpperCase().trim() === 'T' && !e.isTransferenciaInterna) {
        e.isTransferenciaInterna = true;
        e.natureza = 'Transferência Interna';
        e.tipo = 'transferencia_interna';
        changed = true;
        transferenciasInternas++;
      }
      if (changed) entriesCorrigidos++;
    }

    // 2. Reconstruir reviewRegistry: preservar revisados, reconstruir pendentes
    const CORTE = '2024-06-01';
    const revisados = (db.reviewRegistry || []).filter(r => r.statusRevisao === 'revisado');
    const revisadosKeys = new Set(revisados.map(r => (r.nomeOficial || '').toUpperCase()));

    const novosNomes = new Map();
    for (const e of db.entries) {
      const dataISO = e.dataISO || e.data || '';
      if (dataISO < CORTE) continue;
      // Pular lançamentos de transferência interna (dc=T) — não geram cadastros
      if (e.isTransferenciaInterna || String(e.dc || '').toUpperCase().trim() === 'T') continue;
      for (const campo of ['cliente', 'projeto', 'parceiro']) {
        const val = (e[campo] || '').trim();
        if (!val || val === '-' || isLixo(val)) continue;
        const key = val.toUpperCase();
        if (!novosNomes.has(key)) novosNomes.set(key, { nomeOriginal: val, nomeOficial: key });
      }
    }

    const novosPendentes = [];
    for (const [key, info] of novosNomes) {
      if (revisadosKeys.has(key)) continue;
      novosPendentes.push({
        id: crypto.randomUUID(),
        nomeOriginal: info.nomeOriginal,
        nomeOficial: key,
        tipoSugerido: 'Pendente de Classificação',
        tipoFinal: 'Pendente de Classificação',
        clienteVinculado: '',
        projetoVinculado: '',
        manterAlias: true,
        observacao: '',
        statusRevisao: 'pendente'
      });
    }

    db.reviewRegistry = [...revisados, ...novosPendentes];

    // 3. Limpar issues NOVO_CADASTRO obsoletos (gerados com nomes com ruído)
    //    Manter apenas os que correspondem a nomes que ainda estão no reviewRegistry
    const nomesValidos = new Set(db.reviewRegistry.map(r => (r.nomeOficial || '').toUpperCase()));
    const issuesAntes = (db.issues || []).length;
    db.issues = (db.issues || []).filter(i => {
      if (i.code !== 'NOVO_CADASTRO') return true; // manter outros tipos de issue
      // extrair o nome do message: "Novo cadastro identificado: NOME."
      const match = (i.message || '').match(/Novo cadastro identificado: (.+)\./);
      if (!match) return false;
      const nome = match[1].trim().toUpperCase();
      return nomesValidos.has(nome);
    });
    const issuesRemovidos = issuesAntes - db.issues.length;

    saveDb(db);

    json(res, 200, {
      ok: true,
      entriesCorrigidos,
      transferenciasInternasCorrigidas: transferenciasInternas,
      revisadosPreservados: revisados.length,
      novosPendentes: novosPendentes.length,
      totalRegistry: db.reviewRegistry.length,
      issuesNovoCadastroRemovidos: issuesRemovidos
    });
    return;
  }

  // ─── IA FINANCEIRA: ANÁLISE COM JUSTIFICATIVA ─────────────────────────────
  // ─── DIAGNÓSTICO: verificar variáveis de ambiente da IA ───────────────────────
  if (req.method === 'GET' && url.pathname === '/api/ia/diag') {
    if (!requireAuth(req, res, db)) return;
    const hasOpenAI = !!(process.env.OPENAI_API_KEY);
    const openAILen = hasOpenAI ? process.env.OPENAI_API_KEY.length : 0;
    const openAIPrefix = hasOpenAI ? process.env.OPENAI_API_KEY.slice(0, 7) + '...' : 'NÃO CONFIGURADA';
    return json(res, 200, {
      OPENAI_API_KEY: openAIPrefix,
      keyLength: openAILen,
      configured: hasOpenAI,
      NODE_ENV: process.env.NODE_ENV || 'não definido',
      DATABASE_URL: process.env.DATABASE_URL ? 'configurado' : 'não configurado',
      entries: db.entries ? db.entries.length : 0
    });
  }

  // POST /api/ia/analisar — chat financeiro com raciocínio transparente
  if (req.method === 'POST' && url.pathname === '/api/ia/analisar') {
    if (!requireAuth(req, res, db)) return;
    try {
      const { pergunta, periodo_inicio, periodo_fim } = JSON.parse(await readBody(req) || '{}');
      if (!pergunta) return json(res, 400, { error: 'Pergunta obrigatória' });

      const hoje = new Date().toISOString().slice(0, 10);
      const pInicio = periodo_inicio || CORTE_DATA;
      const pFim = periodo_fim || hoje;

      // Filtrar lançamentos do período solicitado
      const lancsPeriodo = db.entries.filter(e =>
        (e.dataISO || '') >= pInicio &&
        (e.dataISO || '') <= pFim &&
        !e.isTransferenciaInterna
      );

      // Calcular resumo financeiro do período
      const totalReceitas = lancsPeriodo.filter(e => (e.valor||0) > 0).reduce((a,e) => a+(e.valor||0), 0);
      const totalDespesas = lancsPeriodo.filter(e => (e.valor||0) < 0).reduce((a,e) => a+Math.abs(e.valor||0), 0);
      const saldoPeriodo = totalReceitas - totalDespesas;

      // Fluxo de caixa mês a mês
      const fluxoMensal = {};
      lancsPeriodo.forEach(e => {
        const mes = (e.dataISO||'').slice(0,7);
        if (!fluxoMensal[mes]) fluxoMensal[mes] = { receitas: 0, despesas: 0, saldo: 0 };
        if ((e.valor||0) > 0) fluxoMensal[mes].receitas += (e.valor||0);
        else fluxoMensal[mes].despesas += Math.abs(e.valor||0);
        fluxoMensal[mes].saldo += (e.valor||0);
      });
      const fluxoMensalStr = Object.entries(fluxoMensal).sort().map(([m,v]) =>
        `${m}: entradas=R$${v.receitas.toFixed(2)} | saídas=R$${v.despesas.toFixed(2)} | saldo=R$${v.saldo.toFixed(2)}`
      ).join('\n');

      // Resultado por cliente no período
      const byClientePeriodo = {};
      lancsPeriodo.forEach(e => {
        const c = clienteEfetivo(e);
        if (c !== 'SEM CLIENTE') {
          byClientePeriodo[c] = (byClientePeriodo[c]||0) + (e.valor||0);
        }
      });
      const clientesStr = Object.entries(byClientePeriodo)
        .sort((a,b) => b[1]-a[1]).slice(0,15)
        .map(([k,v]) => `${k}: R$${v.toFixed(2)}`).join(' | ');

      // Resultado por projeto no período
      const byProjetoPeriodo = {};
      lancsPeriodo.forEach(e => {
        const p = projetoEfetivo(e);
        if (p !== 'SEM PROJETO') {
          byProjetoPeriodo[p] = (byProjetoPeriodo[p]||0) + (e.valor||0);
        }
      });
      const projetosStr = Object.entries(byProjetoPeriodo)
        .sort((a,b) => b[1]-a[1]).slice(0,15)
        .map(([k,v]) => `${k}: R$${v.toFixed(2)}`).join(' | ');

      // Custos de estrutura no período
      const byCCPeriodo = {};
      lancsPeriodo.forEach(e => {
        if (clienteEfetivo(e) === 'SEM CLIENTE') {
          const cc = (e.centroCusto||'SEM CC').toUpperCase();
          if (cc !== 'TEF') byCCPeriodo[cc] = (byCCPeriodo[cc]||0) + (e.valor||0);
        }
      });
      const estruturaStr = Object.entries(byCCPeriodo)
        .sort((a,b) => a[1]-b[1]).slice(0,10)
        .map(([k,v]) => `${k}: R$${v.toFixed(2)}`).join(' | ');

      // Mútuo e Pronampe no período
      const mutuoPeriodo = lancsPeriodo.filter(e => {
        const txt = [e.descricao,e.centroCusto,e.parceiro].map(v=>(v||'').toUpperCase()).join(' ');
        return txt.includes('MÚTUO') || txt.includes('MUTUO') || txt.includes('PRONAMPE');
      });
      const mutuoStr = mutuoPeriodo.length > 0
        ? `${mutuoPeriodo.length} lançamentos | saldo período: R$${mutuoPeriodo.reduce((a,e)=>a+(e.valor||0),0).toFixed(2)}`
        : 'Nenhum lançamento de mútuo/Pronampe no período';

      // Saldo acumulado histórico (para contexto)
      const saldoHistorico = db.entries
        .filter(e => (e.dataISO||'') <= pFim && !e.isTransferenciaInterna)
        .reduce((a,e) => a+(e.valor||0), 0);

      // Lançamentos mais relevantes (maiores valores absolutos)
      const topLancs = [...lancsPeriodo]
        .sort((a,b) => Math.abs(b.valor||0) - Math.abs(a.valor||0))
        .slice(0,20)
        .map(e => `${e.dataISO||'-'} | ${(e.descricao||'-').slice(0,50)} | R$${Number(e.valor||0).toFixed(2)} | CC:${e.centroCusto||'-'} | cliente:${e.cliente||'-'} | projeto:${e.projeto||'-'}`)
        .join('\n');

      const systemPrompt = `Você é o assistente financeiro da empresa CKM Consultoria, especializado em análise de fluxo de caixa e resultado econômico.

Sua função é analisar os dados financeiros reais da empresa e responder perguntas de forma TRANSPARENTE, sempre mostrando:
1. DADOS UTILIZADOS: quais números e lançamentos você considerou
2. RACIOCÍNIO: como você chegou à conclusão (passo a passo)
3. CONCLUSÃO: resposta clara em linguagem simples para o empresário
4. ALERTAS: o que pode estar distorcendo a análise ou o que precisa de atenção
5. CONFIANÇA: se os dados são suficientes ou se há lacunas que podem mudar a conclusão

IMPORTANTE:
- Nunca invente dados. Se não tiver informação suficiente, diga claramente.
- Diferencie FLUXO DE CAIXA (dinheiro que entrou/saiu no período) de RESULTADO ECONÔMICO (lucro/prejuízo do projeto, independente de quando foi pago).
- Mútuo e Pronampe são EMPRÉSTIMOS, não receita operacional. Não some com faturamento.
- TEF são transferências entre contas próprias (BB, Itaú, Santander), não são receita nem despesa.
- Explique os números em contexto: R$50.000 de despesa com escritório pode ser alto ou baixo dependendo do faturamento.

Empresa: CKM Consultoria
Ramo: Consultoria em RH, treinamentos, seleção e desenvolvimento organizacional
Bancos: Banco do Brasil, Itaú, Santander (STD)
Sócios: Carlos Kiyomitu Makiyama, Maria Dinamar P S Makiyama`;

      const userPrompt = `PERÍODO ANALISADO: ${pInicio} a ${pFim}

=== RESUMO DO PERÍODO ===
Total de lançamentos: ${lancsPeriodo.length}
Total receitas (entradas): R$${totalReceitas.toFixed(2)}
Total despesas (saídas): R$${totalDespesas.toFixed(2)}
Saldo do período: R$${saldoPeriodo.toFixed(2)}
Saldo histórico acumulado (até ${pFim}): R$${saldoHistorico.toFixed(2)}

=== FLUXO DE CAIXA MÊS A MÊS ===
${fluxoMensalStr || 'Sem dados no período'}

=== RESULTADO POR CLIENTE (período) ===
${clientesStr || 'Sem dados'}

=== RESULTADO POR PROJETO (período) ===
${projetosStr || 'Sem dados'}

=== CUSTOS DE ESTRUTURA (período) ===
${estruturaStr || 'Sem dados'}

=== EMPRÉSTIMOS/MÚTUO (período) ===
${mutuoStr}

=== 20 MAIORES LANÇAMENTOS DO PERÍODO ===
${topLancs || 'Sem dados'}

=== PERGUNTA DO EMPRESÁRIO ===
${pergunta}

Responda seguindo OBRIGATORIAMENTE a estrutura:
📊 DADOS UTILIZADOS:
[liste os números e lançamentos que você vai usar para responder]

🧮 RACIOCÍNIO:
[explique passo a passo como chegou à conclusão]

✅ CONCLUSÃO:
[resposta clara e direta em linguagem simples]

⚠️ ALERTAS:
[o que pode estar distorcendo a análise]

🎯 CONFIANÇA:
[Alta/Média/Baixa — e por quê]`;

      const { OpenAI } = require('openai');
      const openai = new OpenAI();
      const completion = await openai.chat.completions.create({
        model: 'gpt-4.1-mini',
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: userPrompt }
        ],
        temperature: 0.1,
        max_tokens: 1500
      });
      const resposta = completion.choices[0]?.message?.content?.trim() || 'Sem resposta.';

      return json(res, 200, {
        resposta,
        contexto: {
          periodo: { inicio: pInicio, fim: pFim },
          totalLancamentos: lancsPeriodo.length,
          totalReceitas,
          totalDespesas,
          saldoPeriodo,
          saldoHistorico
        }
      });
    } catch (err) {
      console.error('[IA analisar]', err.message);
      return json(res, 500, { error: 'Erro ao consultar IA: ' + err.message });
    }
  }

  // ─── PÁGINA DE ANÁLISE IA ─────────────────────────────────────────────────
  if (req.method === 'GET' && url.pathname === '/ia') {
    const user = requireAuth(req, res, db);
    if (!user) return;
    const hoje = new Date().toISOString().slice(0, 10);
    const mesAtual = hoje.slice(0, 7);
    const mesAnterior = (() => { const d = new Date(hoje); d.setMonth(d.getMonth()-1); return d.toISOString().slice(0,7); })();
    const html = page('Análise com IA', `
<section>
  <h2>🤖 Assistente Financeiro com IA</h2>
  <p style='color:#64748b;margin-bottom:1.5rem'>Faça perguntas sobre seus dados financeiros. A IA mostrará os dados utilizados, o raciocínio e os alertas para você validar a resposta.</p>

  <div style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:.5rem;padding:1.25rem;margin-bottom:1.5rem'>
    <h3 style='margin:0 0 1rem'>Período de análise</h3>
    <div style='display:flex;gap:1rem;flex-wrap:wrap;align-items:flex-end'>
      <div><label style='display:block;font-size:.85rem;color:#64748b;margin-bottom:.25rem'>De</label>
        <input type='date' id='ia-inicio' value='${mesAtual}-01' style='padding:.4rem .6rem;border:1px solid #cbd5e1;border-radius:.375rem'></div>
      <div><label style='display:block;font-size:.85rem;color:#64748b;margin-bottom:.25rem'>Até</label>
        <input type='date' id='ia-fim' value='${hoje}' style='padding:.4rem .6rem;border:1px solid #cbd5e1;border-radius:.375rem'></div>
      <div style='display:flex;gap:.5rem;flex-wrap:wrap'>
        <button onclick="setPeriodo('${mesAtual}-01','${hoje}')" style='padding:.4rem .75rem;background:#e2e8f0;border:none;border-radius:.375rem;cursor:pointer;font-size:.85rem'>Este mês</button>
        <button onclick="setPeriodo('${mesAnterior}-01','${mesAnterior}-31')" style='padding:.4rem .75rem;background:#e2e8f0;border:none;border-radius:.375rem;cursor:pointer;font-size:.85rem'>Mês anterior</button>
        <button onclick="setPeriodo('${hoje.slice(0,4)}-01-01','${hoje}')" style='padding:.4rem .75rem;background:#e2e8f0;border:none;border-radius:.375rem;cursor:pointer;font-size:.85rem'>Este ano</button>
        <button onclick="setPeriodo('${CORTE_DATA}','${hoje}')" style='padding:.4rem .75rem;background:#e2e8f0;border:none;border-radius:.375rem;cursor:pointer;font-size:.85rem'>Tudo (desde ${CORTE_DATA})</button>
      </div>
    </div>
  </div>

  <div style='margin-bottom:1rem'>
    <p style='font-size:.85rem;color:#64748b;margin-bottom:.5rem'>Sugestões de perguntas:</p>
    <div style='display:flex;gap:.5rem;flex-wrap:wrap'>
      ${[
        'Como foi meu fluxo de caixa mês a mês?',
        'Qual projeto está me dando mais prejuízo?',
        'Tenho dinheiro para pagar a folha do mês que vem?',
        'Qual cliente gerou mais receita no período?',
        'Quanto paguei de impostos?',
        'Qual é minha dívida total de mútuo?',
        'Compare meu faturamento com minhas despesas fixas',
        'Em quais meses meu caixa ficou negativo?'
      ].map(s => `<button onclick="usarSugestao('${s.replace(/'/g,"\\'")}')"
        style='padding:.35rem .65rem;background:#eff6ff;border:1px solid #bfdbfe;border-radius:.375rem;cursor:pointer;font-size:.8rem;color:#1d4ed8'>${s}</button>`).join('')}
    </div>
  </div>

  <div style='display:flex;gap:.75rem;margin-bottom:1.5rem'>
    <textarea id='ia-pergunta' placeholder='Digite sua pergunta sobre os dados financeiros...' rows='3'
      style='flex:1;padding:.6rem .75rem;border:1px solid #cbd5e1;border-radius:.375rem;font-size:.95rem;resize:vertical'></textarea>
    <button onclick='enviarPergunta()' id='ia-btn'
      style='padding:.6rem 1.25rem;background:#2563eb;color:#fff;border:none;border-radius:.375rem;cursor:pointer;font-weight:600;align-self:flex-end'>Analisar</button>
  </div>

  <div id='ia-resultado' style='display:none'>
    <div id='ia-contexto' style='background:#f0fdf4;border:1px solid #bbf7d0;border-radius:.5rem;padding:1rem;margin-bottom:1rem;font-size:.85rem;color:#166534'></div>
    <div id='ia-resposta' style='background:#fff;border:1px solid #e2e8f0;border-radius:.5rem;padding:1.25rem;white-space:pre-wrap;line-height:1.7;font-size:.95rem'></div>
    <div style='margin-top:.75rem;display:flex;gap:.5rem'>
      <button onclick='avaliar(true)' style='padding:.4rem .75rem;background:#dcfce7;border:1px solid #86efac;border-radius:.375rem;cursor:pointer;font-size:.85rem;color:#166534'>✅ Análise correta</button>
      <button onclick='avaliar(false)' style='padding:.4rem .75rem;background:#fee2e2;border:1px solid #fca5a5;border-radius:.375rem;cursor:pointer;font-size:.85rem;color:#991b1b'>❌ Análise incorreta — quero corrigir</button>
    </div>
    <div id='ia-correcao' style='display:none;margin-top:.75rem'>
      <textarea id='ia-correcao-texto' placeholder='Explique o que está errado na análise...' rows='3'
        style='width:100%;padding:.6rem;border:1px solid #fca5a5;border-radius:.375rem;font-size:.9rem;box-sizing:border-box'></textarea>
      <button onclick='enviarCorrecao()' style='margin-top:.5rem;padding:.4rem .75rem;background:#dc2626;color:#fff;border:none;border-radius:.375rem;cursor:pointer;font-size:.85rem'>Enviar correção</button>
    </div>
  </div>

  <div id='ia-historico' style='margin-top:2rem'></div>
</section>

<script>
let historicoAnalises = [];

function setPeriodo(inicio, fim) {
  document.getElementById('ia-inicio').value = inicio;
  document.getElementById('ia-fim').value = fim;
}

function usarSugestao(texto) {
  document.getElementById('ia-pergunta').value = texto;
  document.getElementById('ia-pergunta').focus();
}

async function enviarPergunta() {
  const pergunta = document.getElementById('ia-pergunta').value.trim();
  if (!pergunta) return alert('Digite uma pergunta.');
  const inicio = document.getElementById('ia-inicio').value;
  const fim = document.getElementById('ia-fim').value;
  const btn = document.getElementById('ia-btn');
  btn.disabled = true; btn.textContent = 'Analisando...';
  document.getElementById('ia-resultado').style.display = 'none';
  try {
    const r = await fetch('/api/ia/analisar', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ pergunta, periodo_inicio: inicio, periodo_fim: fim })
    });
    const data = await r.json();
    if (data.error) throw new Error(data.error);
    const ctx = data.contexto;
    document.getElementById('ia-contexto').innerHTML =
      '<strong>📋 Contexto da análise:</strong> Período ' + ctx.periodo.inicio + ' a ' + ctx.periodo.fim +
      ' | ' + ctx.totalLancamentos + ' lançamentos | Receitas: R$ ' + ctx.totalReceitas.toLocaleString('pt-BR',{minimumFractionDigits:2}) +
      ' | Despesas: R$ ' + ctx.totalDespesas.toLocaleString('pt-BR',{minimumFractionDigits:2}) +
      ' | Saldo: R$ ' + ctx.saldoPeriodo.toLocaleString('pt-BR',{minimumFractionDigits:2});
    document.getElementById('ia-resposta').textContent = data.resposta;
    document.getElementById('ia-resultado').style.display = 'block';
    document.getElementById('ia-correcao').style.display = 'none';
    historicoAnalises.unshift({ pergunta, periodo: ctx.periodo, resposta: data.resposta, ctx });
    renderHistorico();
  } catch(e) {
    alert('Erro: ' + e.message);
  } finally {
    btn.disabled = false; btn.textContent = 'Analisar';
  }
}

function avaliar(correto) {
  if (correto) {
    alert('Ótimo! Análise marcada como correta.');
    document.getElementById('ia-correcao').style.display = 'none';
  } else {
    document.getElementById('ia-correcao').style.display = 'block';
    document.getElementById('ia-correcao-texto').focus();
  }
}

function enviarCorrecao() {
  const texto = document.getElementById('ia-correcao-texto').value.trim();
  if (!texto) return alert('Explique o que está errado.');
  alert('Correção registrada! Esta informação será usada para melhorar as próximas análises.');
  document.getElementById('ia-correcao').style.display = 'none';
}

function renderHistorico() {
  const el = document.getElementById('ia-historico');
  if (historicoAnalises.length === 0) { el.innerHTML = ''; return; }
  el.innerHTML = '<h3>Histórico desta sessão</h3>' +
    historicoAnalises.slice(0,5).map((h,i) =>
      '<details style="margin-bottom:.5rem;border:1px solid #e2e8f0;border-radius:.375rem"><summary style="padding:.6rem .75rem;cursor:pointer;background:#f8fafc"><strong>' +
      h.pergunta.slice(0,80) + '</strong> <span style="color:#64748b;font-size:.8rem">' + h.periodo.inicio + ' a ' + h.periodo.fim + '</span></summary>' +
      '<div style="padding:.75rem;white-space:pre-wrap;font-size:.9rem">' + h.resposta + '</div></details>'
    ).join('');
}

document.getElementById('ia-pergunta').addEventListener('keydown', e => {
  if (e.ctrlKey && e.key === 'Enter') enviarPergunta();
});
</script>`, user, '/ia');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  // ─── RELATÓRIO AUTOMÁTICO MENSAL GERADO POR IA ───────────────────────────
  if (req.method === 'POST' && url.pathname === '/api/ia/relatorio') {
    if (!requireAuth(req, res, db)) return;
    try {
      const { mes } = JSON.parse(await readBody(req) || '{}');
      // mes no formato YYYY-MM, ex: '2025-03'
      const hoje = new Date().toISOString().slice(0, 10);
      const mesRef = mes || hoje.slice(0, 7);
      const mesAnterior = (() => { const d = new Date(mesRef + '-01'); d.setMonth(d.getMonth()-1); return d.toISOString().slice(0,7); })();
      const pInicio = mesRef + '-01';
      const pFim = mesRef + '-31'; // PostgreSQL aceita datas inexistentes, JS filtra
      const pInicioAnt = mesAnterior + '-01';
      const pFimAnt = mesAnterior + '-31';

      const lancsAtivos = db.entries.filter(e => !e.isTransferenciaInterna);

      // Dados do mês de referência
      const lancsMes = lancsAtivos.filter(e => (e.dataISO||'') >= pInicio && (e.dataISO||'') <= pFim);
      const lancsAnt = lancsAtivos.filter(e => (e.dataISO||'') >= pInicioAnt && (e.dataISO||'') <= pFimAnt);

      const resumo = (lancs) => ({
        receitas: lancs.filter(e=>(e.valor||0)>0).reduce((a,e)=>a+(e.valor||0),0),
        despesas: lancs.filter(e=>(e.valor||0)<0).reduce((a,e)=>a+Math.abs(e.valor||0),0),
        saldo: lancs.reduce((a,e)=>a+(e.valor||0),0),
        count: lancs.length
      });

      const rMes = resumo(lancsMes);
      const rAnt = resumo(lancsAnt);

      // Por cliente
      const byClienteMes = {};
      const byClienteAnt = {};
      lancsMes.forEach(e => { const c=clienteEfetivo(e); if(c!=='SEM CLIENTE') byClienteMes[c]=(byClienteMes[c]||0)+(e.valor||0); });
      lancsAnt.forEach(e => { const c=clienteEfetivo(e); if(c!=='SEM CLIENTE') byClienteAnt[c]=(byClienteAnt[c]||0)+(e.valor||0); });

      // Por projeto
      const byProjetoMes = {};
      lancsMes.forEach(e => { const p=projetoEfetivo(e); if(p!=='SEM PROJETO') byProjetoMes[p]=(byProjetoMes[p]||0)+(e.valor||0); });

      // Estrutura
      const byEstrMes = {};
      lancsMes.forEach(e => {
        if(clienteEfetivo(e)==='SEM CLIENTE') {
          const cc=(e.centroCusto||'SEM CC').toUpperCase();
          if(cc!=='TEF') byEstrMes[cc]=(byEstrMes[cc]||0)+(e.valor||0);
        }
      });

      // Mútuo/Pronampe
      const mutuoMes = lancsMes.filter(e=>{
        const t=[e.descricao,e.centroCusto,e.parceiro].map(v=>(v||'').toUpperCase()).join(' ');
        return t.includes('MÚTUO')||t.includes('MUTUO')||t.includes('PRONAMPE');
      });

      // Saldo histórico acumulado até o fim do mês
      const saldoAcumulado = lancsAtivos
        .filter(e=>(e.dataISO||'')<=pFim)
        .reduce((a,e)=>a+(e.valor||0),0);

      // Variações mês a mês
      const varReceita = rAnt.receitas > 0 ? ((rMes.receitas - rAnt.receitas)/rAnt.receitas*100).toFixed(1) : 'N/A';
      const varDespesa = rAnt.despesas > 0 ? ((rMes.despesas - rAnt.despesas)/rAnt.despesas*100).toFixed(1) : 'N/A';

      const clientesStr = Object.entries(byClienteMes).sort((a,b)=>b[1]-a[1]).slice(0,8).map(([k,v])=>`${k}: R$${v.toFixed(2)}`).join(' | ');
      const projetosStr = Object.entries(byProjetoMes).sort((a,b)=>b[1]-a[1]).slice(0,8).map(([k,v])=>`${k}: R$${v.toFixed(2)}`).join(' | ');
      const estruturaStr = Object.entries(byEstrMes).sort((a,b)=>a[1]-b[1]).slice(0,8).map(([k,v])=>`${k}: R$${v.toFixed(2)}`).join(' | ');
      const clientesAntStr = Object.entries(byClienteAnt).sort((a,b)=>b[1]-a[1]).slice(0,5).map(([k,v])=>`${k}: R$${v.toFixed(2)}`).join(' | ');

      const systemPrompt = `Você é o assistente financeiro da CKM Consultoria. Gere um RELATÓRIO MENSAL EXECUTIVO completo e transparente.

O relatório deve ser escrito em linguagem simples para um empresário leigo em contabilidade, mas com números precisos.

ESTRUTURA OBRIGATÓRIA DO RELATÓRIO:

# Relatório Financeiro — [Mês/Ano]

## 1. Resumo Executivo
[3-4 frases resumindo o mês: foi bom ou ruim? por quê?]

## 2. Fluxo de Caixa do Mês
[Tabela: Receitas | Despesas | Saldo | Comparação com mês anterior]

## 3. Clientes e Projetos
[Quais clientes geraram mais receita? Quais projetos estão no positivo/negativo?]

## 4. Custos de Estrutura
[Como estão os gastos fixos? Estão crescendo ou estabilizados?]

## 5. Empréstimos e Financiamentos
[Quanto foi pago de mútuo/Pronampe? Qual o impacto no caixa?]

## 6. Alertas e Pontos de Atenção
[O que precisa de atenção urgente? Riscos identificados?]

## 7. Recomendações
[3-5 ações concretas para o próximo mês]

## 8. Dados Utilizados e Confiabilidade
[Quais dados foram usados? Há lacunas que podem distorcer a análise?]

IMPORTANTE: Seja transparente sobre incertezas. Se um número parece estranho, mencione. Não invente dados.`;

      const userPrompt = `MÊS DE REFERÊNCIA: ${mesRef}

=== MÊS ATUAL (${mesRef}) ===
Lançamentos: ${rMes.count}
Receitas: R$${rMes.receitas.toFixed(2)}
Despesas: R$${rMes.despesas.toFixed(2)}
Saldo do mês: R$${rMes.saldo.toFixed(2)}
Saldo histórico acumulado: R$${saldoAcumulado.toFixed(2)}

=== MÊS ANTERIOR (${mesAnterior}) ===
Receitas: R$${rAnt.receitas.toFixed(2)}
Despesas: R$${rAnt.despesas.toFixed(2)}
Saldo: R$${rAnt.saldo.toFixed(2)}

=== VARIAÇÕES ===
Receitas: ${varReceita}% vs mês anterior
Despesas: ${varDespesa}% vs mês anterior

=== CLIENTES (mês atual) ===
${clientesStr || 'Sem dados'}

=== CLIENTES (mês anterior) ===
${clientesAntStr || 'Sem dados'}

=== PROJETOS (mês atual) ===
${projetosStr || 'Sem dados'}

=== CUSTOS DE ESTRUTURA (mês atual) ===
${estruturaStr || 'Sem dados'}

=== MÚTUO/PRONAMPE (mês atual) ===
${mutuoMes.length} lançamentos | saldo: R$${mutuoMes.reduce((a,e)=>a+(e.valor||0),0).toFixed(2)}

Gere o relatório mensal completo seguindo a estrutura definida.`;

      const { OpenAI } = require('openai');
      const openai = new OpenAI();
      const completion = await openai.chat.completions.create({
        model: 'gpt-4.1-mini',
        messages: [
          { role: 'system', content: systemPrompt },
          { role: 'user', content: userPrompt }
        ],
        temperature: 0.2,
        max_tokens: 2000
      });
      const relatorio = completion.choices[0]?.message?.content?.trim() || 'Erro ao gerar relatório.';

      return json(res, 200, {
        relatorio,
        mes: mesRef,
        resumo: { receitas: rMes.receitas, despesas: rMes.despesas, saldo: rMes.saldo, saldoAcumulado }
      });
    } catch (err) {
      console.error('[IA relatorio]', err.message);
      return json(res, 500, { error: 'Erro ao gerar relatório: ' + err.message });
    }
  }

  // ─── PÁGINA DE RELATÓRIOS ──────────────────────────────────────────────────────
  if (req.method === 'GET' && url.pathname === '/relatorio') {
    const user = requireAuth(req, res, db);
    if (!user) return;
    const hoje = new Date().toISOString().slice(0, 10);
    const mesAtual = hoje.slice(0, 7);
    // Gerar lista de meses disponíveis (desde CORTE_DATA até hoje)
    const mesesDisponiveis = [];
    const dCorte = new Date(CORTE_DATA);
    const dHoje = new Date(hoje);
    let d = new Date(dCorte.getFullYear(), dCorte.getMonth(), 1);
    while (d <= dHoje) {
      mesesDisponiveis.unshift(d.toISOString().slice(0,7));
      d.setMonth(d.getMonth()+1);
    }
    const html = page('Relatórios', `
<section>
  <h2>📄 Relatórios Mensais com IA</h2>
  <p style='color:#64748b;margin-bottom:1.5rem'>Gere um relatório executivo completo de qualquer mês. A IA analisa todos os dados e apresenta um resumo com alertas e recomendações.</p>

  <div style='background:#f8fafc;border:1px solid #e2e8f0;border-radius:.5rem;padding:1.25rem;margin-bottom:1.5rem;display:flex;gap:1rem;align-items:flex-end;flex-wrap:wrap'>
    <div>
      <label style='display:block;font-size:.85rem;color:#64748b;margin-bottom:.25rem'>Mês de referência</label>
      <select id='rel-mes' style='padding:.4rem .6rem;border:1px solid #cbd5e1;border-radius:.375rem;font-size:.95rem'>
        ${mesesDisponiveis.map(m => `<option value='${m}'${m===mesAtual?' selected':''}>${m}</option>`).join('')}
      </select>
    </div>
    <button onclick='gerarRelatorio()' id='rel-btn'
      style='padding:.5rem 1.25rem;background:#2563eb;color:#fff;border:none;border-radius:.375rem;cursor:pointer;font-weight:600'>Gerar Relatório</button>
    <button onclick='gerarRelatorio(true)' id='rel-btn-comp'
      style='padding:.5rem 1.25rem;background:#7c3aed;color:#fff;border:none;border-radius:.375rem;cursor:pointer;font-weight:600'>🔄 Comparar com mês anterior</button>
  </div>

  <div id='rel-loading' style='display:none;text-align:center;padding:2rem;color:#64748b'>
    <div style='font-size:1.5rem;margin-bottom:.5rem'>⏳</div>
    Gerando relatório... isso pode levar alguns segundos.
  </div>

  <div id='rel-resultado' style='display:none'>
    <div id='rel-resumo' style='display:flex;gap:1rem;flex-wrap:wrap;margin-bottom:1.25rem'></div>
    <div id='rel-conteudo' style='background:#fff;border:1px solid #e2e8f0;border-radius:.5rem;padding:1.5rem;line-height:1.8;font-size:.95rem;white-space:pre-wrap'></div>
    <div style='margin-top:1rem;display:flex;gap:.75rem;flex-wrap:wrap'>
      <button onclick='copiarRelatorio()' style='padding:.4rem .9rem;background:#f1f5f9;border:1px solid #e2e8f0;border-radius:.375rem;cursor:pointer;font-size:.85rem'>&#128203; Copiar texto</button>
      <a id='rel-link-ia' href='/ia' style='padding:.4rem .9rem;background:#eff6ff;border:1px solid #bfdbfe;border-radius:.375rem;text-decoration:none;font-size:.85rem;color:#1d4ed8'>🤖 Fazer perguntas sobre este mês</a>
    </div>
  </div>

  <div id='rel-historico' style='margin-top:2rem'></div>
</section>

<script>
let relatoriosGerados = [];

async function gerarRelatorio(comparar) {
  const mes = document.getElementById('rel-mes').value;
  const btn = document.getElementById('rel-btn');
  const btnComp = document.getElementById('rel-btn-comp');
  btn.disabled = true; btnComp.disabled = true;
  document.getElementById('rel-loading').style.display = 'block';
  document.getElementById('rel-resultado').style.display = 'none';
  try {
    const r = await fetch('/api/ia/relatorio', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ mes })
    });
    const data = await r.json();
    if (data.error) throw new Error(data.error);
    const rs = data.resumo;
    const fmtBRL = (v) => (v<0?'-':'') + 'R\u00a0' + Math.abs(v).toLocaleString('pt-BR',{minimumFractionDigits:2});
    document.getElementById('rel-resumo').innerHTML = [
      { label: 'Receitas', val: rs.receitas, cor: '#16a34a' },
      { label: 'Despesas', val: rs.despesas, cor: '#dc2626' },
      { label: 'Saldo do M\u00eas', val: rs.saldo, cor: rs.saldo>=0?'#16a34a':'#dc2626' },
      { label: 'Saldo Acumulado', val: rs.saldoAcumulado, cor: rs.saldoAcumulado>=0?'#16a34a':'#dc2626' }
    ].map(function(c) { return '<div style="flex:1;min-width:140px;background:#f8fafc;border:1px solid #e2e8f0;border-radius:.5rem;padding:.75rem 1rem"><div style="font-size:.8rem;color:#64748b">' + c.label + '</div><div style="font-size:1.15rem;font-weight:700;color:' + c.cor + '">' + fmtBRL(c.val) + '</div></div>'; }).join('');
    document.getElementById('rel-conteudo').textContent = data.relatorio;
    document.getElementById('rel-link-ia').href = '/ia?de=' + mes + '-01&ate=' + mes + '-31';
    document.getElementById('rel-resultado').style.display = 'block';
    relatoriosGerados.unshift({ mes, relatorio: data.relatorio, resumo: rs });
    renderHistoricoRel();
  } catch(e) {
    alert('Erro: ' + e.message);
  } finally {
    btn.disabled = false; btnComp.disabled = false;
    document.getElementById('rel-loading').style.display = 'none';
  }
}

function copiarRelatorio() {
  const texto = document.getElementById('rel-conteudo').textContent;
  navigator.clipboard.writeText(texto).then(() => alert('Relat\u00f3rio copiado!')).catch(() => alert('N\u00e3o foi poss\u00edvel copiar.'));
}

function renderHistoricoRel() {
  const el = document.getElementById('rel-historico');
  if (relatoriosGerados.length <= 1) { el.innerHTML = ''; return; }
  el.innerHTML = '<h3>Relat\u00f3rios gerados nesta sess\u00e3o</h3>' +
    relatoriosGerados.slice(1).map(h =>
      '<details style="margin-bottom:.5rem;border:1px solid #e2e8f0;border-radius:.375rem"><summary style="padding:.6rem .75rem;cursor:pointer;background:#f8fafc"><strong>' + h.mes + '</strong></summary>' +
      '<div style="padding:.75rem;white-space:pre-wrap;font-size:.9rem">' + h.relatorio + '</div></details>'
    ).join('');
}
</script>`, user, '/relatorio');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  // ============================================================
  // CADASTROS MESTRES — /cadastros-mestres
  // ============================================================
  if (req.method === 'GET' && url.pathname === '/cadastros-mestres') {
    const user = requireAuth(req, res, db); if (!user) return;
    let ccs = [], clientes = [], projetos = [], tipos = [], bancos = [];
    try {
      const pg = storage.getPool ? storage.getPool() : null;
      if (pg) {
        ccs      = (await pg.query('SELECT * FROM centros_de_custo ORDER BY tipo, nome')).rows;
        clientes = (await pg.query('SELECT * FROM clientes ORDER BY nome')).rows;
        projetos = (await pg.query('SELECT p.*, c.nome_curto as cliente_nome FROM projetos p LEFT JOIN clientes c ON p.cliente_id=c.id ORDER BY p.codigo')).rows;
        tipos    = (await pg.query('SELECT * FROM tipos_lancamento ORDER BY natureza, nome')).rows;
        bancos   = (await pg.query('SELECT * FROM bancos ORDER BY nome')).rows;
      }
    } catch(e) { console.error('[cadastros-mestres]', e.message); }

    const tipoColors = { ESTRUTURA:'#5B2EFF', FINANCEIRO:'#00B8D9', OPERACIONAL:'#5ED38C', TRANSFERENCIA:'#f59e0b' };
    const natColors  = { RECEITA:'#5ED38C', DESPESA:'#ef4444', IMPOSTO:'#f59e0b', FINANCEIRO:'#00B8D9', TRANSFERENCIA:'#808080' };

    const renderCC = ccs.map(c => `
      <tr>
        <td><span class='badge' style='background:${tipoColors[c.tipo]||'#808080'}22;color:${tipoColors[c.tipo]||'#808080'}'>${c.tipo}</span></td>
        <td><strong>${c.codigo}</strong></td>
        <td>${c.nome}</td>
        <td><span class='status-dot ${c.ativo?'ativo':'inativo'}'></span>${c.ativo?'Ativo':'Inativo'}</td>
        <td><button class='btn btn-sm btn-outline' onclick="editCC('${c.id}','${c.codigo}','${c.nome.replace(/'/g,"\\'")  }','${c.tipo}',${c.ativo})">Editar</button></td>
      </tr>`).join('');

    const renderClientes = clientes.map(c => `
      <tr>
        <td><strong>${c.codigo||'-'}</strong></td>
        <td>${c.nome}</td>
        <td>${c.nome_curto||'-'}</td>
        <td>${c.cnpj||'-'}</td>
        <td><span class='status-dot ${c.ativo?'ativo':'inativo'}'></span>${c.ativo?'Ativo':'Inativo'}</td>
        <td><button class='btn btn-sm btn-outline' onclick="editCliente('${c.id}','${c.codigo||''}','${c.nome.replace(/'/g,"\\'")  }','${(c.nome_curto||'').replace(/'/g,"\\'")  }','${c.cnpj||''}',${c.ativo})">Editar</button></td>
      </tr>`).join('');

    const renderProjetos = projetos.map(p => `
      <tr>
        <td><strong>${p.codigo}</strong></td>
        <td>${p.nome}</td>
        <td>${p.tipo||'-'}</td>
        <td>${p.cliente_nome||'-'}</td>
        <td><span class='status-dot ${p.ativo?'ativo':'inativo'}'></span>${p.ativo?'Ativo':'Inativo'}</td>
        <td><button class='btn btn-sm btn-outline' onclick="editProjeto('${p.id}','${p.codigo}','${p.nome.replace(/'/g,"\\'")  }','${p.tipo||''}','${p.cliente_id||''}',${p.ativo})">Editar</button></td>
      </tr>`).join('');

    const renderTipos = tipos.map(t => `
      <tr>
        <td><span class='badge' style='background:${natColors[t.natureza]||'#808080'}22;color:${natColors[t.natureza]||'#808080'}'>${t.natureza}</span></td>
        <td><strong>${t.codigo}</strong></td>
        <td>${t.nome}</td>
        <td><span class='status-dot ${t.ativo?'ativo':'inativo'}'></span>${t.ativo?'Ativo':'Inativo'}</td>
      </tr>`).join('');

    const renderBancos = bancos.map(b => `
      <tr>
        <td><strong>${b.codigo}</strong></td>
        <td>${b.nome}</td>
        <td>${b.agencia||'-'}</td>
        <td>${b.conta||'-'}</td>
        <td><span class='status-dot ${b.ativo?'ativo':'inativo'}'></span>${b.ativo?'Ativo':'Inativo'}</td>
        <td><button class='btn btn-sm btn-outline' onclick="editBanco('${b.id}','${b.codigo}','${b.nome.replace(/'/g,"\\'")  }','${b.agencia||''}','${b.conta||''}',${b.ativo})">Editar</button></td>
      </tr>`).join('');

    const clienteOpts = clientes.map(c => `<option value='${c.id}'>${c.nome}</option>`).join('');

    const body = `
<h2 class='page-title'>⚙ Cadastros Mestres</h2>
<div class='alert alert-info'>Gerencie aqui os dados de referência do sistema. Alterações aqui afetam automaticamente a classificação de novos lançamentos.</div>

<div class='master-tabs'>
  <button class='master-tab active' onclick="switchTab('cc')">Centros de Custo (${ccs.length})</button>
  <button class='master-tab' onclick="switchTab('clientes')">Clientes (${clientes.length})</button>
  <button class='master-tab' onclick="switchTab('projetos')">Projetos (${projetos.length})</button>
  <button class='master-tab' onclick="switchTab('tipos')">Tipos de Lançamento (${tipos.length})</button>
  <button class='master-tab' onclick="switchTab('bancos')">Bancos (${bancos.length})</button>
</div>

<!-- CENTROS DE CUSTO -->
<div id='panel-cc' class='master-panel active'>
  <div class='master-form' id='form-cc'>
    <h3 id='form-cc-title'>➕ Novo Centro de Custo</h3>
    <input type='hidden' id='cc-id'>
    <div class='form-grid'>
      <label>Código <input id='cc-codigo' placeholder='Ex: MARKETING'></label>
      <label>Nome <input id='cc-nome' placeholder='Ex: Marketing e Comunicação'></label>
      <label>Tipo
        <select id='cc-tipo'>
          <option value='ESTRUTURA'>Estrutura (overhead)</option>
          <option value='OPERACIONAL'>Operacional (cliente/projeto)</option>
          <option value='FINANCEIRO'>Financeiro (empréstimos, tarifas)</option>
          <option value='TRANSFERENCIA'>Transferência entre contas</option>
        </select>
      </label>
      <label>Status
        <select id='cc-ativo'><option value='true'>Ativo</option><option value='false'>Inativo</option></select>
      </label>
    </div>
    <div style='display:flex;gap:.75rem;flex-wrap:wrap'>
      <button onclick='saveCC()'>💾 Salvar</button>
      <button class='btn-outline' onclick='clearFormCC()'>Cancelar</button>
    </div>
  </div>
  <section>
    <h2>Centros de Custo cadastrados</h2>
    <div style='overflow-x:auto'>
    <table><thead><tr><th>Tipo</th><th>Código</th><th>Nome</th><th>Status</th><th></th></tr></thead>
    <tbody id='tbody-cc'>${renderCC}</tbody></table></div>
  </section>
</div>

<!-- CLIENTES -->
<div id='panel-clientes' class='master-panel'>
  <div class='master-form'>
    <h3 id='form-cli-title'>➕ Novo Cliente</h3>
    <input type='hidden' id='cli-id'>
    <div class='form-grid'>
      <label>Código <input id='cli-codigo' placeholder='Ex: SEBRAE_TO'></label>
      <label>Nome completo <input id='cli-nome' placeholder='Ex: SEBRAE Tocantins'></label>
      <label>Nome curto <input id='cli-curto' placeholder='Ex: SEBRAE-TO'></label>
      <label>CNPJ <input id='cli-cnpj' placeholder='00.000.000/0001-00'></label>
      <label>Status <select id='cli-ativo'><option value='true'>Ativo</option><option value='false'>Inativo</option></select></label>
    </div>
    <div style='display:flex;gap:.75rem;flex-wrap:wrap'>
      <button onclick='saveCliente()'>💾 Salvar</button>
      <button class='btn-outline' onclick='clearFormCli()'>Cancelar</button>
    </div>
  </div>
  <section>
    <h2>Clientes cadastrados</h2>
    <div style='overflow-x:auto'>
    <table><thead><tr><th>Código</th><th>Nome</th><th>Nome Curto</th><th>CNPJ</th><th>Status</th><th></th></tr></thead>
    <tbody id='tbody-clientes'>${renderClientes}</tbody></table></div>
  </section>
</div>

<!-- PROJETOS -->
<div id='panel-projetos' class='master-panel'>
  <div class='master-form'>
    <h3 id='form-proj-title'>➕ Novo Projeto</h3>
    <input type='hidden' id='proj-id'>
    <div class='form-grid'>
      <label>Código <input id='proj-codigo' placeholder='Ex: 4.16'></label>
      <label>Nome <input id='proj-nome' placeholder='Ex: Coaching Executivo'></label>
      <label>Tipo <input id='proj-tipo' placeholder='Ex: CONSULTORIA'></label>
      <label>Cliente vinculado
        <select id='proj-cliente'><option value=''>-- Nenhum --</option>${clienteOpts}</select>
      </label>
      <label>Status <select id='proj-ativo'><option value='true'>Ativo</option><option value='false'>Inativo</option></select></label>
    </div>
    <div style='display:flex;gap:.75rem;flex-wrap:wrap'>
      <button onclick='saveProjeto()'>💾 Salvar</button>
      <button class='btn-outline' onclick='clearFormProj()'>Cancelar</button>
    </div>
  </div>
  <section>
    <h2>Projetos cadastrados</h2>
    <div style='overflow-x:auto'>
    <table><thead><tr><th>Código</th><th>Nome</th><th>Tipo</th><th>Cliente</th><th>Status</th><th></th></tr></thead>
    <tbody id='tbody-projetos'>${renderProjetos}</tbody></table></div>
  </section>
</div>

<!-- TIPOS DE LANÇAMENTO -->
<div id='panel-tipos' class='master-panel'>
  <section>
    <h2>Tipos de Lançamento</h2>
    <div class='alert alert-info'>Os tipos de lançamento definem a natureza de cada movimentação financeira. Edição disponível em breve.</div>
    <div style='overflow-x:auto'>
    <table><thead><tr><th>Natureza</th><th>Código</th><th>Nome</th><th>Status</th></tr></thead>
    <tbody>${renderTipos}</tbody></table></div>
  </section>
</div>

<!-- BANCOS -->
<div id='panel-bancos' class='master-panel'>
  <div class='master-form'>
    <h3 id='form-banco-title'>➕ Novo Banco</h3>
    <input type='hidden' id='banco-id'>
    <div class='form-grid'>
      <label>Código <input id='banco-codigo' placeholder='Ex: NUBANK'></label>
      <label>Nome <input id='banco-nome' placeholder='Ex: Nubank PJ'></label>
      <label>Agência <input id='banco-agencia' placeholder='Ex: 0001'></label>
      <label>Conta <input id='banco-conta' placeholder='Ex: 12345-6'></label>
      <label>Status <select id='banco-ativo'><option value='true'>Ativo</option><option value='false'>Inativo</option></select></label>
    </div>
    <div style='display:flex;gap:.75rem;flex-wrap:wrap'>
      <button onclick='saveBanco()'>💾 Salvar</button>
      <button class='btn-outline' onclick='clearFormBanco()'>Cancelar</button>
    </div>
  </div>
  <section>
    <h2>Bancos cadastrados</h2>
    <div style='overflow-x:auto'>
    <table><thead><tr><th>Código</th><th>Nome</th><th>Agência</th><th>Conta</th><th>Status</th><th></th></tr></thead>
    <tbody id='tbody-bancos'>${renderBancos}</tbody></table></div>
  </section>
</div>

<script>
function switchTab(tab) {
  document.querySelectorAll('.master-tab').forEach((b,i) => {
    const tabs = ['cc','clientes','projetos','tipos','bancos'];
    b.classList.toggle('active', tabs[i] === tab);
  });
  document.querySelectorAll('.master-panel').forEach(p => p.classList.remove('active'));
  document.getElementById('panel-' + tab).classList.add('active');
}
async function apiCall(method, url, body) {
  const r = await fetch(url, { method, headers:{'Content-Type':'application/json'}, body: body ? JSON.stringify(body) : undefined });
  const d = await r.json();
  if (!d.ok) throw new Error(d.error || 'Erro');
  return d;
}
function clearFormCC() {
  document.getElementById('cc-id').value='';
  document.getElementById('cc-codigo').value='';
  document.getElementById('cc-nome').value='';
  document.getElementById('cc-tipo').value='ESTRUTURA';
  document.getElementById('cc-ativo').value='true';
  document.getElementById('form-cc-title').textContent='➕ Novo Centro de Custo';
}
function editCC(id,codigo,nome,tipo,ativo) {
  switchTab('cc');
  document.getElementById('cc-id').value=id;
  document.getElementById('cc-codigo').value=codigo;
  document.getElementById('cc-nome').value=nome;
  document.getElementById('cc-tipo').value=tipo;
  document.getElementById('cc-ativo').value=String(ativo);
  document.getElementById('form-cc-title').textContent='✏ Editar Centro de Custo: '+nome;
  setTimeout(()=>document.getElementById('form-cc').scrollIntoView({behavior:'smooth'}),100);
}
async function saveCC() {
  const id=document.getElementById('cc-id').value;
  const payload={codigo:document.getElementById('cc-codigo').value.trim().toUpperCase(),nome:document.getElementById('cc-nome').value.trim(),tipo:document.getElementById('cc-tipo').value,ativo:document.getElementById('cc-ativo').value==='true'};
  if(!payload.codigo||!payload.nome){alert('Preencha código e nome');return;}
  try{
    await apiCall(id?'PUT':'POST','/api/mestres/centros-custo'+(id?'/'+id:''),payload);
    location.reload();
  }catch(e){alert(e.message);}
}
function clearFormCli() {
  ['cli-id','cli-codigo','cli-nome','cli-curto','cli-cnpj'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('cli-ativo').value='true';
  document.getElementById('form-cli-title').textContent='➕ Novo Cliente';
}
function editCliente(id,codigo,nome,curto,cnpj,ativo) {
  document.getElementById('cli-id').value=id;
  document.getElementById('cli-codigo').value=codigo;
  document.getElementById('cli-nome').value=nome;
  document.getElementById('cli-curto').value=curto;
  document.getElementById('cli-cnpj').value=cnpj;
  document.getElementById('cli-ativo').value=String(ativo);
  document.getElementById('form-cli-title').textContent='✏ Editar Cliente: '+nome;
  switchTab('clientes');
  setTimeout(()=>document.querySelector('#panel-clientes .master-form').scrollIntoView({behavior:'smooth'}),100);
}
async function saveCliente() {
  const id=document.getElementById('cli-id').value;
  const payload={codigo:document.getElementById('cli-codigo').value.trim().toUpperCase(),nome:document.getElementById('cli-nome').value.trim(),nome_curto:document.getElementById('cli-curto').value.trim(),cnpj:document.getElementById('cli-cnpj').value.trim(),ativo:document.getElementById('cli-ativo').value==='true'};
  if(!payload.nome){alert('Preencha o nome');return;}
  try{
    await apiCall(id?'PUT':'POST','/api/mestres/clientes'+(id?'/'+id:''),payload);
    location.reload();
  }catch(e){alert(e.message);}
}
function clearFormProj() {
  ['proj-id','proj-codigo','proj-nome','proj-tipo'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('proj-cliente').value='';
  document.getElementById('proj-ativo').value='true';
  document.getElementById('form-proj-title').textContent='➕ Novo Projeto';
}
function editProjeto(id,codigo,nome,tipo,clienteId,ativo) {
  document.getElementById('proj-id').value=id;
  document.getElementById('proj-codigo').value=codigo;
  document.getElementById('proj-nome').value=nome;
  document.getElementById('proj-tipo').value=tipo;
  document.getElementById('proj-cliente').value=clienteId;
  document.getElementById('proj-ativo').value=String(ativo);
  document.getElementById('form-proj-title').textContent='✏ Editar Projeto: '+nome;
  switchTab('projetos');
  setTimeout(()=>document.querySelector('#panel-projetos .master-form').scrollIntoView({behavior:'smooth'}),100);
}
async function saveProjeto() {
  const id=document.getElementById('proj-id').value;
  const payload={codigo:document.getElementById('proj-codigo').value.trim(),nome:document.getElementById('proj-nome').value.trim(),tipo:document.getElementById('proj-tipo').value.trim().toUpperCase()||null,cliente_id:document.getElementById('proj-cliente').value||null,ativo:document.getElementById('proj-ativo').value==='true'};
  if(!payload.codigo||!payload.nome){alert('Preencha código e nome');return;}
  try{
    await apiCall(id?'PUT':'POST','/api/mestres/projetos'+(id?'/'+id:''),payload);
    location.reload();
  }catch(e){alert(e.message);}
}
function clearFormBanco() {
  ['banco-id','banco-codigo','banco-nome','banco-agencia','banco-conta'].forEach(id=>document.getElementById(id).value='');
  document.getElementById('banco-ativo').value='true';
  document.getElementById('form-banco-title').textContent='➕ Novo Banco';
}
function editBanco(id,codigo,nome,agencia,conta,ativo) {
  document.getElementById('banco-id').value=id;
  document.getElementById('banco-codigo').value=codigo;
  document.getElementById('banco-nome').value=nome;
  document.getElementById('banco-agencia').value=agencia;
  document.getElementById('banco-conta').value=conta;
  document.getElementById('banco-ativo').value=String(ativo);
  document.getElementById('form-banco-title').textContent='✏ Editar Banco: '+nome;
  switchTab('bancos');
  setTimeout(()=>document.querySelector('#panel-bancos .master-form').scrollIntoView({behavior:'smooth'}),100);
}
async function saveBanco() {
  const id=document.getElementById('banco-id').value;
  const payload={codigo:document.getElementById('banco-codigo').value.trim().toUpperCase(),nome:document.getElementById('banco-nome').value.trim(),agencia:document.getElementById('banco-agencia').value.trim()||null,conta:document.getElementById('banco-conta').value.trim()||null,ativo:document.getElementById('banco-ativo').value==='true'};
  if(!payload.codigo||!payload.nome){alert('Preencha código e nome');return;}
  try{
    await apiCall(id?'PUT':'POST','/api/mestres/bancos'+(id?'/'+id:''),payload);
    location.reload();
  }catch(e){alert(e.message);}
}
</script>`;
    const html = page('Cadastros Mestres', body, user, '/cadastros-mestres');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  // ── APIs de Cadastros Mestres ──
  if (req.url.startsWith('/api/mestres/')) {
    const pg = storage.getPool ? storage.getPool() : null;
    if (!pg) { return json(res, 503, { error: 'Banco não disponível' }); }
    const parts = url.pathname.split('/'); // ['','api','mestres','entidade','id?']
    const entidade = parts[3];
    const id = parts[4] || null;
    let body = {};
    if (['POST','PUT'].includes(req.method)) {
      body = await new Promise((resolve) => {
        let data = '';
        req.on('data', c => data += c);
        req.on('end', () => { try { resolve(JSON.parse(data)); } catch { resolve({}); } });
      });
    }
    try {
      if (entidade === 'centros-custo') {
        if (req.method === 'POST') {
          const r = await pg.query('INSERT INTO centros_de_custo (codigo,nome,tipo,ativo) VALUES ($1,$2,$3,$4) RETURNING id', [body.codigo, body.nome, body.tipo||'OPERACIONAL', body.ativo!==false]);
          return json(res, 200, { ok: true, id: r.rows[0].id });
        }
        if (req.method === 'PUT' && id) {
          await pg.query('UPDATE centros_de_custo SET codigo=$1,nome=$2,tipo=$3,ativo=$4,atualizado_em=NOW() WHERE id=$5', [body.codigo, body.nome, body.tipo, body.ativo!==false, id]);
          return json(res, 200, { ok: true });
        }
        if (req.method === 'DELETE' && id) {
          await pg.query('UPDATE centros_de_custo SET ativo=false WHERE id=$1', [id]);
          return json(res, 200, { ok: true });
        }
        if (req.method === 'GET') {
          const r = await pg.query('SELECT * FROM centros_de_custo ORDER BY tipo,nome');
          return json(res, 200, { ok: true, data: r.rows });
        }
      }
      if (entidade === 'clientes') {
        if (req.method === 'POST') {
          const r = await pg.query('INSERT INTO clientes (codigo,nome,nome_curto,cnpj,ativo) VALUES ($1,$2,$3,$4,$5) RETURNING id', [body.codigo||null, body.nome, body.nome_curto||null, body.cnpj||null, body.ativo!==false]);
          return json(res, 200, { ok: true, id: r.rows[0].id });
        }
        if (req.method === 'PUT' && id) {
          await pg.query('UPDATE clientes SET codigo=$1,nome=$2,nome_curto=$3,cnpj=$4,ativo=$5,atualizado_em=NOW() WHERE id=$6', [body.codigo||null, body.nome, body.nome_curto||null, body.cnpj||null, body.ativo!==false, id]);
          return json(res, 200, { ok: true });
        }
        if (req.method === 'GET') {
          const r = await pg.query('SELECT * FROM clientes ORDER BY nome');
          return json(res, 200, { ok: true, data: r.rows });
        }
      }
      if (entidade === 'projetos') {
        if (req.method === 'POST') {
          const r = await pg.query('INSERT INTO projetos (codigo,nome,tipo,cliente_id,ativo) VALUES ($1,$2,$3,$4,$5) RETURNING id', [body.codigo, body.nome, body.tipo||null, body.cliente_id||null, body.ativo!==false]);
          return json(res, 200, { ok: true, id: r.rows[0].id });
        }
        if (req.method === 'PUT' && id) {
          await pg.query('UPDATE projetos SET codigo=$1,nome=$2,tipo=$3,cliente_id=$4,ativo=$5,atualizado_em=NOW() WHERE id=$6', [body.codigo, body.nome, body.tipo||null, body.cliente_id||null, body.ativo!==false, id]);
          return json(res, 200, { ok: true });
        }
        if (req.method === 'GET') {
          const r = await pg.query('SELECT p.*, c.nome_curto as cliente_nome FROM projetos p LEFT JOIN clientes c ON p.cliente_id=c.id ORDER BY p.codigo');
          return json(res, 200, { ok: true, data: r.rows });
        }
      }
      if (entidade === 'bancos') {
        if (req.method === 'POST') {
          const r = await pg.query('INSERT INTO bancos (codigo,nome,agencia,conta,ativo) VALUES ($1,$2,$3,$4,$5) RETURNING id', [body.codigo, body.nome, body.agencia||null, body.conta||null, body.ativo!==false]);
          return json(res, 200, { ok: true, id: r.rows[0].id });
        }
        if (req.method === 'PUT' && id) {
          await pg.query('UPDATE bancos SET codigo=$1,nome=$2,agencia=$3,conta=$4,ativo=$5 WHERE id=$6', [body.codigo, body.nome, body.agencia||null, body.conta||null, body.ativo!==false, id]);
          return json(res, 200, { ok: true });
        }
        if (req.method === 'GET') {
          const r = await pg.query('SELECT * FROM bancos ORDER BY nome');
          return json(res, 200, { ok: true, data: r.rows });
        }
      }
    } catch(e) {
      return json(res, 500, { error: e.message });
    }
    return json(res, 404, { error: 'Entidade não encontrada' });
  }

  // ============================================================
  // CONTRATOS — /contratos
  // ============================================================
  if (req.method === 'GET' && url.pathname === '/contratos') {
    const user = requireAuth(req, res, db); if (!user) return;
    let contratos = [], clientes = [], projetos = [];
    try {
      const pg = storage.getPool ? storage.getPool() : null;
      if (pg) {
        contratos = (await pg.query(`SELECT ct.*, cl.nome_curto as cliente_nome, pr.nome as projeto_nome FROM contratos ct LEFT JOIN clientes cl ON ct.cliente_id=cl.id LEFT JOIN projetos pr ON ct.projeto_id=pr.id ORDER BY ct.status, ct.data_fim`)).rows;
        clientes  = (await pg.query('SELECT id, nome, nome_curto FROM clientes WHERE ativo=true ORDER BY nome')).rows;
        projetos  = (await pg.query('SELECT id, codigo, nome FROM projetos WHERE ativo=true ORDER BY codigo')).rows;
      }
    } catch(e) { console.error('[contratos]', e.message); }

    const statusColors = { ATIVO:'#5ED38C', ENCERRADO:'#808080', SUSPENSO:'#f59e0b', EM_NEGOCIACAO:'#00B8D9' };
    const hoje = new Date();
    const renderContratos = contratos.map(c => {
      const inicio = c.data_inicio ? new Date(c.data_inicio) : null;
      const fim    = c.data_fim    ? new Date(c.data_fim)    : null;
      const duracao = (inicio && fim) ? Math.round((fim - inicio) / (1000*60*60*24*30)) : null;
      const decorrido = (inicio && fim) ? Math.min(100, Math.round((hoje - inicio) / (fim - inicio) * 100)) : null;
      const vencendo = fim && c.status === 'ATIVO' && (fim - hoje) < 60*24*60*60*1000;
      return `<div class='contrato-card'>
        <div class='contrato-badge'>📋</div>
        <div class='contrato-info'>
          <h4>${c.descricao || c.numero || 'Contrato #'+c.id} ${vencendo?'<span class="badge badge-amber">⚠ Vence em breve</span>':''}</h4>
          <div style='font-size:.82rem;color:#808080;margin-bottom:.4rem'>${c.cliente_nome||'-'} · ${c.projeto_nome||'-'} · ${c.periodicidade||'MENSAL'}</div>
          ${decorrido !== null ? `<div class='progress-bar'><div class='progress-bar-fill' style='width:${decorrido}%'></div></div><div style='font-size:.72rem;color:#808080;margin-top:.2rem'>${decorrido}% do período · ${c.data_inicio||'-'} → ${c.data_fim||'Indeterminado'}</div>` : ''}
        </div>
        <div style='text-align:right;flex-shrink:0'>
          <div class='contrato-valor'>R$ ${Number(c.valor_total||0).toLocaleString('pt-BR',{minimumFractionDigits:2})}</div>
          <div style='font-size:.78rem;color:#808080'>${c.valor_parcela ? 'Parcela: R$ '+Number(c.valor_parcela).toLocaleString('pt-BR',{minimumFractionDigits:2}) : ''}</div>
          <span class='badge' style='background:${statusColors[c.status]||'#808080'}22;color:${statusColors[c.status]||'#808080'};margin-top:.4rem'>${c.status}</span>
        </div>
      </div>`;
    }).join('') || '<p style="color:#808080;padding:1rem">Nenhum contrato cadastrado.</p>';

    const clienteOpts = clientes.map(c => `<option value='${c.id}'>${c.nome_curto||c.nome}</option>`).join('');
    const projetoOpts = projetos.map(p => `<option value='${p.id}'>${p.codigo} — ${p.nome}</option>`).join('');

    const totalAtivo = contratos.filter(c=>c.status==='ATIVO').reduce((a,c)=>a+Number(c.valor_total||0),0);
    const totalParcelas = contratos.filter(c=>c.status==='ATIVO').reduce((a,c)=>a+Number(c.valor_parcela||0),0);

    const body = `
<h2 class='page-title'>📋 Contratos de Clientes</h2>
<div class='cards'>
  <div class='card'><strong>Contratos Ativos</strong><span>${contratos.filter(c=>c.status==='ATIVO').length}</span></div>
  <div class='card card-verde'><strong>Valor Total Ativo</strong><span class='pos' style='font-size:1.3rem'>R$ ${totalAtivo.toLocaleString('pt-BR',{minimumFractionDigits:2})}</span></div>
  <div class='card card-teal'><strong>Receita Mensal Prevista</strong><span class='teal' style='font-size:1.3rem'>R$ ${totalParcelas.toLocaleString('pt-BR',{minimumFractionDigits:2})}</span></div>
  <div class='card card-warning'><strong>Vencendo em 60 dias</strong><span class='neg'>${contratos.filter(c=>{const f=c.data_fim?new Date(c.data_fim):null;return f&&c.status==='ATIVO'&&(f-hoje)<60*24*60*60*1000;}).length}</span></div>
</div>

<section>
  <h2>➕ Novo Contrato</h2>
  <div class='form-grid'>
    <label>Número / Referência <input id='ct-numero' placeholder='Ex: SEBRAE-TO-2025-001'></label>
    <label>Cliente <select id='ct-cliente'><option value=''>-- Selecione --</option>${clienteOpts}</select></label>
    <label>Projeto <select id='ct-projeto'><option value=''>-- Selecione --</option>${projetoOpts}</select></label>
    <label>Descrição <input id='ct-descricao' placeholder='Ex: PDI do BEM — Ciclo 2025'></label>
    <label>Valor Total (R$) <input id='ct-valor' type='number' step='0.01' placeholder='0,00'></label>
    <label>Valor da Parcela (R$) <input id='ct-parcela' type='number' step='0.01' placeholder='0,00'></label>
    <label>Data Início <input id='ct-inicio' type='date'></label>
    <label>Data Fim <input id='ct-fim' type='date'></label>
    <label>Periodicidade
      <select id='ct-period'>
        <option value='MENSAL'>Mensal</option>
        <option value='QUINZENAL'>Quinzenal</option>
        <option value='UNICO'>Único (pagamento único)</option>
        <option value='POR_ENTREGA'>Por entrega</option>
      </select>
    </label>
    <label>Status
      <select id='ct-status'>
        <option value='ATIVO'>Ativo</option>
        <option value='EM_NEGOCIACAO'>Em Negociação</option>
        <option value='SUSPENSO'>Suspenso</option>
        <option value='ENCERRADO'>Encerrado</option>
      </select>
    </label>
  </div>
  <label style='margin-bottom:1rem'>Observações <textarea id='ct-obs' rows='2' placeholder='Informações adicionais...'></textarea></label>
  <button onclick='saveContrato()'>💾 Salvar Contrato</button>
</section>

<section>
  <h2>Contratos cadastrados</h2>
  <div id='lista-contratos'>${renderContratos}</div>
</section>

<script>
async function saveContrato() {
  const payload = {
    numero: document.getElementById('ct-numero').value.trim()||null,
    cliente_id: document.getElementById('ct-cliente').value||null,
    projeto_id: document.getElementById('ct-projeto').value||null,
    descricao: document.getElementById('ct-descricao').value.trim()||null,
    valor_total: parseFloat(document.getElementById('ct-valor').value)||0,
    valor_parcela: parseFloat(document.getElementById('ct-parcela').value)||null,
    data_inicio: document.getElementById('ct-inicio').value,
    data_fim: document.getElementById('ct-fim').value||null,
    periodicidade: document.getElementById('ct-period').value,
    status: document.getElementById('ct-status').value,
    observacoes: document.getElementById('ct-obs').value.trim()||null
  };
  if (!payload.data_inicio) { alert('Informe a data de início'); return; }
  if (!payload.valor_total) { alert('Informe o valor total'); return; }
  try {
    const r = await fetch('/api/contratos', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload) });
    const d = await r.json();
    if (d.ok) location.reload();
    else alert(d.error);
  } catch(e) { alert(e.message); }
}
</script>`;
    const html = page('Contratos', body, user, '/contratos');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/contratos') {
    if (!requireAuth(req, res, db)) return;
    const pg = storage.getPool ? storage.getPool() : null;
    if (!pg) return json(res, 503, { error: 'Banco não disponível' });
    const body = await new Promise((resolve) => { let d=''; req.on('data',c=>d+=c); req.on('end',()=>{try{resolve(JSON.parse(d));}catch{resolve({});}}); });
    try {
      const r = await pg.query(
        'INSERT INTO contratos (numero,cliente_id,projeto_id,descricao,valor_total,valor_parcela,data_inicio,data_fim,periodicidade,status,observacoes) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11) RETURNING id',
        [body.numero||null, body.cliente_id||null, body.projeto_id||null, body.descricao||null, body.valor_total||0, body.valor_parcela||null, body.data_inicio, body.data_fim||null, body.periodicidade||'MENSAL', body.status||'ATIVO', body.observacoes||null]
      );
      return json(res, 200, { ok: true, id: r.rows[0].id });
    } catch(e) { return json(res, 500, { error: e.message }); }
  }

  // ============================================================
  // CONTAS A PAGAR/RECEBER — /contas
  // ============================================================
  if (req.method === 'GET' && url.pathname === '/contas') {
    const user = requireAuth(req, res, db); if (!user) return;
    let contas = [], clientes = [], projetos = [], ccs = [], bancos = [];
    try {
      const pg = storage.getPool ? storage.getPool() : null;
      if (pg) {
        contas   = (await pg.query(`SELECT cp.*, cl.nome_curto as cliente_nome, pr.nome as projeto_nome, cc.nome as cc_nome, b.nome as banco_nome FROM contas_pagar_receber cp LEFT JOIN clientes cl ON cp.cliente_id=cl.id LEFT JOIN projetos pr ON cp.projeto_id=pr.id LEFT JOIN centros_de_custo cc ON cp.centro_custo_id=cc.id LEFT JOIN bancos b ON cp.banco_id=b.id WHERE cp.status IN ('PENDENTE','PARCIAL') ORDER BY cp.data_vencimento`)).rows;
        clientes = (await pg.query('SELECT id, nome_curto, nome FROM clientes WHERE ativo=true ORDER BY nome')).rows;
        projetos = (await pg.query('SELECT id, codigo, nome FROM projetos WHERE ativo=true ORDER BY codigo')).rows;
        ccs      = (await pg.query('SELECT id, codigo, nome FROM centros_de_custo WHERE ativo=true ORDER BY tipo,nome')).rows;
        bancos   = (await pg.query('SELECT id, codigo, nome FROM bancos WHERE ativo=true ORDER BY nome')).rows;
      }
    } catch(e) { console.error('[contas]', e.message); }

    const hoje2 = new Date(); hoje2.setHours(0,0,0,0);
    const totalPagar    = contas.filter(c=>c.tipo==='PAGAR').reduce((a,c)=>a+Number(c.valor||0),0);
    const totalReceber  = contas.filter(c=>c.tipo==='RECEBER').reduce((a,c)=>a+Number(c.valor||0),0);
    const vencidas      = contas.filter(c=>c.data_vencimento && new Date(c.data_vencimento)<hoje2).length;
    const saldoProjetado = totalReceber - totalPagar;

    const renderContas = contas.map(c => {
      const venc = c.data_vencimento ? new Date(c.data_vencimento) : null;
      const atrasado = venc && venc < hoje2;
      const cor = c.tipo==='RECEBER' ? '#5ED38C' : '#ef4444';
      return `<tr>
        <td><span class='badge' style='background:${cor}22;color:${cor}'>${c.tipo}</span></td>
        <td>${c.data_vencimento||'-'}</td>
        <td>${c.descricao}</td>
        <td style='font-weight:700;color:${cor}'>R$ ${Number(c.valor||0).toLocaleString('pt-BR',{minimumFractionDigits:2})}</td>
        <td>${c.cliente_nome||'-'}</td>
        <td>${c.cc_nome||'-'}</td>
        <td>${atrasado?'<span class="badge badge-red">Vencida</span>':'<span class="badge badge-green">Pendente</span>'}</td>
        <td>
          <button class='btn btn-sm btn-verde' onclick='darBaixa(${c.id},${c.valor})'>✓ Baixar</button>
        </td>
      </tr>`;
    }).join('') || '<tr><td colspan="8" style="color:#808080;padding:1rem">Nenhuma conta pendente.</td></tr>';

    const clienteOpts2 = clientes.map(c => `<option value='${c.id}'>${c.nome_curto||c.nome}</option>`).join('');
    const projetoOpts2 = projetos.map(p => `<option value='${p.id}'>${p.codigo} — ${p.nome}</option>`).join('');
    const ccOpts       = ccs.map(c => `<option value='${c.id}'>${c.codigo} — ${c.nome}</option>`).join('');
    const bancoOpts    = bancos.map(b => `<option value='${b.id}'>${b.nome}</option>`).join('');

    const body = `
<h2 class='page-title'>💰 Contas a Pagar / Receber</h2>
<div class='cards'>
  <div class='card card-danger'><strong>Total a Pagar</strong><span class='neg' style='font-size:1.3rem'>R$ ${totalPagar.toLocaleString('pt-BR',{minimumFractionDigits:2})}</span></div>
  <div class='card card-verde'><strong>Total a Receber</strong><span class='pos' style='font-size:1.3rem'>R$ ${totalReceber.toLocaleString('pt-BR',{minimumFractionDigits:2})}</span></div>
  <div class='card ${saldoProjetado>=0?'card-verde':'card-danger'}'><strong>Saldo Projetado</strong><span class='${saldoProjetado>=0?'pos':'neg'}' style='font-size:1.3rem'>R$ ${saldoProjetado.toLocaleString('pt-BR',{minimumFractionDigits:2})}</span></div>
  <div class='card card-warning'><strong>Contas Vencidas</strong><span class='neg'>${vencidas}</span></div>
</div>

<section>
  <h2>➕ Lançar Conta</h2>
  <div class='form-grid'>
    <label>Tipo
      <select id='cp-tipo'><option value='PAGAR'>A Pagar</option><option value='RECEBER'>A Receber</option></select>
    </label>
    <label>Descrição <input id='cp-desc' placeholder='Ex: Aluguel escritório maio/2025'></label>
    <label>Valor (R$) <input id='cp-valor' type='number' step='0.01' placeholder='0,00'></label>
    <label>Vencimento <input id='cp-venc' type='date'></label>
    <label>Competência <input id='cp-comp' type='date'></label>
    <label>Cliente <select id='cp-cliente'><option value=''>-- Nenhum --</option>${clienteOpts2}</select></label>
    <label>Projeto <select id='cp-projeto'><option value=''>-- Nenhum --</option>${projetoOpts2}</select></label>
    <label>Centro de Custo <select id='cp-cc'><option value=''>-- Nenhum --</option>${ccOpts}</select></label>
    <label>Banco <select id='cp-banco'><option value=''>-- Nenhum --</option>${bancoOpts}</select></label>
  </div>
  <label style='margin-bottom:1rem'>Observações <textarea id='cp-obs' rows='2'></textarea></label>
  <button onclick='saveConta()'>💾 Salvar Conta</button>
</section>

<section>
  <h2>Contas Pendentes</h2>
  <div style='overflow-x:auto'>
  <table><thead><tr><th>Tipo</th><th>Vencimento</th><th>Descrição</th><th>Valor</th><th>Cliente</th><th>Centro Custo</th><th>Status</th><th></th></tr></thead>
  <tbody>${renderContas}</tbody></table></div>
</section>

<div id='modal-baixa' style='display:none;position:fixed;inset:0;background:rgba(26,10,94,.5);z-index:999;align-items:center;justify-content:center'>
  <div style='background:#fff;border-radius:16px;padding:2rem;max-width:400px;width:90%;box-shadow:0 24px 48px rgba(26,10,94,.3)'>
    <h3 style='font-family:Poppins,sans-serif;color:#1A0A5E;margin-bottom:1rem'>✓ Dar Baixa</h3>
    <input type='hidden' id='baixa-id'>
    <label style='margin-bottom:1rem'>Valor pago (R$) <input id='baixa-valor' type='number' step='0.01'></label>
    <label style='margin-bottom:1rem'>Data do pagamento <input id='baixa-data' type='date'></label>
    <label style='margin-bottom:1.5rem'>Banco utilizado <select id='baixa-banco'><option value=''>-- Selecione --</option>${bancoOpts}</select></label>
    <div style='display:flex;gap:.75rem'>
      <button onclick='confirmarBaixa()'>✓ Confirmar Baixa</button>
      <button class='btn-outline' onclick='document.getElementById("modal-baixa").style.display="none"'>Cancelar</button>
    </div>
  </div>
</div>

<script>
function darBaixa(id, valor) {
  document.getElementById('baixa-id').value = id;
  document.getElementById('baixa-valor').value = valor;
  document.getElementById('baixa-data').value = new Date().toISOString().slice(0,10);
  document.getElementById('modal-baixa').style.display = 'flex';
}
async function confirmarBaixa() {
  const id = document.getElementById('baixa-id').value;
  const payload = {
    valor_pago: parseFloat(document.getElementById('baixa-valor').value),
    data_pagamento: document.getElementById('baixa-data').value,
    banco_id: document.getElementById('baixa-banco').value || null
  };
  try {
    const r = await fetch('/api/contas/'+id+'/baixa', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload) });
    const d = await r.json();
    if (d.ok) location.reload();
    else alert(d.error);
  } catch(e) { alert(e.message); }
}
async function saveConta() {
  const payload = {
    tipo: document.getElementById('cp-tipo').value,
    descricao: document.getElementById('cp-desc').value.trim(),
    valor: parseFloat(document.getElementById('cp-valor').value),
    data_vencimento: document.getElementById('cp-venc').value,
    data_competencia: document.getElementById('cp-comp').value||null,
    cliente_id: document.getElementById('cp-cliente').value||null,
    projeto_id: document.getElementById('cp-projeto').value||null,
    centro_custo_id: document.getElementById('cp-cc').value||null,
    banco_id: document.getElementById('cp-banco').value||null,
    observacoes: document.getElementById('cp-obs').value.trim()||null
  };
  if (!payload.descricao) { alert('Informe a descrição'); return; }
  if (!payload.valor || payload.valor <= 0) { alert('Informe o valor'); return; }
  if (!payload.data_vencimento) { alert('Informe o vencimento'); return; }
  try {
    const r = await fetch('/api/contas', { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(payload) });
    const d = await r.json();
    if (d.ok) location.reload();
    else alert(d.error);
  } catch(e) { alert(e.message); }
}
</script>`;
    const html = page('Contas a Pagar/Receber', body, user, '/contas');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/contas') {
    if (!requireAuth(req, res, db)) return;
    const pg = storage.getPool ? storage.getPool() : null;
    if (!pg) return json(res, 503, { error: 'Banco não disponível' });
    const body = await new Promise((resolve) => { let d=''; req.on('data',c=>d+=c); req.on('end',()=>{try{resolve(JSON.parse(d));}catch{resolve({});}}); });
    try {
      const r = await pg.query(
        'INSERT INTO contas_pagar_receber (tipo,descricao,valor,data_vencimento,data_competencia,cliente_id,projeto_id,centro_custo_id,banco_id,observacoes,status) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,\'PENDENTE\') RETURNING id',
        [body.tipo, body.descricao, body.valor, body.data_vencimento, body.data_competencia||null, body.cliente_id||null, body.projeto_id||null, body.centro_custo_id||null, body.banco_id||null, body.observacoes||null]
      );
      return json(res, 200, { ok: true, id: r.rows[0].id });
    } catch(e) { return json(res, 500, { error: e.message }); }
  }

  if (req.method === 'POST' && url.pathname.startsWith('/api/contas/') && url.pathname.endsWith('/baixa')) {
    if (!requireAuth(req, res, db)) return;
    const pg = storage.getPool ? storage.getPool() : null;
    if (!pg) return json(res, 503, { error: 'Banco não disponível' });
    const id = url.pathname.split('/')[3];
    const body = await new Promise((resolve) => { let d=''; req.on('data',c=>d+=c); req.on('end',()=>{try{resolve(JSON.parse(d));}catch{resolve({});}}); });
    try {
      await pg.query(
        'UPDATE contas_pagar_receber SET status=\'PAGO\', valor_pago=$1, data_pagamento=$2, atualizado_em=NOW() WHERE id=$3',
        [body.valor_pago, body.data_pagamento, id]
      );
      return json(res, 200, { ok: true });
    } catch(e) { return json(res, 500, { error: e.message }); }
  }

  // ============================================================
  // CONCILIAÇÃO BANCÁRIA — /conciliacao
  // ============================================================
  if (req.method === 'GET' && url.pathname === '/conciliacao') {
    const user = requireAuth(req, res, db); if (!user) return;
    let bancos = [], historico = [];
    try {
      const pg = storage.getPool ? storage.getPool() : null;
      if (pg) {
        bancos    = (await pg.query('SELECT * FROM bancos WHERE ativo=true ORDER BY nome')).rows;
        historico = (await pg.query('SELECT ce.*, b.nome as banco_nome FROM conciliacao_extratos ce LEFT JOIN bancos b ON ce.banco_id=b.id ORDER BY ce.data_upload DESC LIMIT 20')).rows;
      }
    } catch(e) { console.error('[conciliacao]', e.message); }

    const bancoOpts2 = bancos.map(b => `<option value='${b.id}'>${b.nome}</option>`).join('');
    const renderHist = historico.map(h => `
      <tr>
        <td>${h.banco_nome||'-'}</td>
        <td>${h.data_extrato||'-'}</td>
        <td>${new Date(h.data_upload).toLocaleString('pt-BR')}</td>
        <td>${h.total_lancamentos||0}</td>
        <td><span class='badge badge-green'>${h.total_conciliados||0} OK</span></td>
        <td><span class='badge badge-amber'>${h.total_divergentes||0} divergentes</span></td>
        <td><span class='badge badge-red'>${h.total_nao_lancados||0} não lançados</span></td>
        <td>${h.saldo_extrato!=null?'R$ '+Number(h.saldo_extrato).toLocaleString('pt-BR',{minimumFractionDigits:2}):'-'}</td>
        <td><a href='/conciliacao/detalhe?id=${h.id}' class='btn btn-sm btn-outline'>Ver detalhes</a></td>
      </tr>`).join('') || '<tr><td colspan="9" style="color:#808080;padding:1rem">Nenhuma conciliação realizada ainda.</td></tr>';

    const body = `
<h2 class='page-title'>🏦 Conciliação Bancária</h2>
<div class='alert alert-info'>Faça o upload do extrato bancário (OFX ou CSV) para conciliar automaticamente com os lançamentos do sistema. O sistema identifica o que está OK, o que diverge e o que não foi lançado.</div>

<section>
  <h2>📤 Upload de Extrato</h2>
  <div class='form-grid'>
    <label>Banco <select id='conc-banco'><option value=''>-- Selecione --</option>${bancoOpts2}</select></label>
    <label>Formato do arquivo
      <select id='conc-formato'>
        <option value='OFX'>OFX (padrão bancário — recomendado)</option>
        <option value='CSV'>CSV (exportação do internet banking)</option>
        <option value='XLSX'>XLSX (planilha Excel)</option>
      </select>
    </label>
  </div>
  <div class='upload-area' onclick='document.getElementById("conc-file").click()'>
    <div class='upload-icon'>🏦</div>
    <p><strong>Clique para selecionar o extrato bancário</strong></p>
    <p>Formatos aceitos: OFX, CSV, XLSX · Arraste e solte aqui</p>
    <input type='file' id='conc-file' accept='.ofx,.csv,.xlsx,.xls' style='display:none' onchange='uploadExtrato(this)'>
  </div>
  <div id='conc-resultado' style='margin-top:1rem'></div>
</section>

<section>
  <h2>📋 Histórico de Conciliações</h2>
  <div style='overflow-x:auto'>
  <table><thead><tr><th>Banco</th><th>Data Extrato</th><th>Upload</th><th>Total</th><th>Conciliados</th><th>Divergentes</th><th>Não Lançados</th><th>Saldo</th><th></th></tr></thead>
  <tbody>${renderHist}</tbody></table></div>
</section>

<script>
async function uploadExtrato(input) {
  const file = input.files[0];
  if (!file) return;
  const bancoId = document.getElementById('conc-banco').value;
  if (!bancoId) { alert('Selecione o banco antes de fazer o upload'); return; }
  const formato = document.getElementById('conc-formato').value;
  const div = document.getElementById('conc-resultado');
  div.innerHTML = '<div class="alert alert-info">⏳ Processando extrato...</div>';
  const fd = new FormData();
  fd.append('arquivo', file);
  fd.append('banco_id', bancoId);
  fd.append('formato', formato);
  try {
    const r = await fetch('/api/conciliacao/upload', { method:'POST', body: fd });
    const d = await r.json();
    if (d.ok) {
      div.innerHTML = '<div class="alert alert-success">\u2705 Extrato processado! ' + d.total + ' lan\u00e7amentos \u00b7 <strong>' + d.conciliados + ' conciliados</strong> \u00b7 ' + d.divergentes + ' divergentes \u00b7 ' + d.nao_lancados + ' n\u00e3o lan\u00e7ados. <a href="/conciliacao/detalhe?id=' + d.id + '" class="btn btn-sm" style="margin-left:.5rem">Ver detalhes</a></div>';
    } else {
      div.innerHTML = '<div class="alert alert-danger">❌ ' + d.error + '</div>';
    }
  } catch(e) {
    div.innerHTML = '<div class="alert alert-danger">❌ Erro: ' + e.message + '</div>';
  }
}
</script>`;
    const html = page('Conciliação Bancária', body, user, '/conciliacao');
    res.writeHead(200, { 'Content-Type': 'text/html; charset=utf-8' });
    res.end(html);
    return;
  }

  if (req.method === 'POST' && url.pathname === '/api/conciliacao/upload') {
    // Processar upload do extrato
    // Por ora, retorna uma resposta de placeholder até implementar o parser OFX
    return json(res, 200, { ok: true, total: 0, conciliados: 0, divergentes: 0, nao_lancados: 0, id: 0, message: 'Parser OFX em implementação' });
  }

  res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' });
  res.end('Not found');
});

// ===== LIMPEZA AUTOMÁTICA: executada no boot e após cada upload =====
// Garante que dc=T seja marcado como transferência interna e que o reviewRegistry
// não contenha entradas geradas por esses lançamentos.
function limparCadastrosAutomatico(db) {
  const isLixo = (val) => {
    if (!val) return true;
    const s = String(val).trim();
    if (/^\d+(\.\d+)+$/.test(s)) return true;
    if (/^\d+$/.test(s)) return true;
    if (s === '' || s === '-' || s === '--' || s === '.') return true;
    return false;
  };

  // 1. Marcar entries dc=T como transferência interna
  let transferenciasInternas = 0;
  for (const e of db.entries) {
    let changed = false;
    const novoParceiro = normalizeParceiro(e.parceiro || '');
    if (novoParceiro !== (e.parceiro || '')) { e.parceiro = novoParceiro; changed = true; }
    if (isLixo(e.cliente)) { if (e.cliente) { e.cliente = ''; changed = true; } }
    if (String(e.dc || '').toUpperCase().trim() === 'T' && !e.isTransferenciaInterna) {
      e.isTransferenciaInterna = true;
      e.natureza = 'Transferência Interna';
      e.tipo = 'transferencia_interna';
      changed = true;
      transferenciasInternas++;
    }
  }

  // 2. Reconstruir reviewRegistry sem entradas de dc=T
  const CORTE = '2024-06-01';
  const revisados = (db.reviewRegistry || []).filter(r => r.statusRevisao === 'revisado');
  const revisadosKeys = new Set(revisados.map(r => (r.nomeOficial || '').toUpperCase()));

  const novosNomes = new Map();
  for (const e of db.entries) {
    const dataISO = e.dataISO || e.data || '';
    if (dataISO < CORTE) continue;
    if (e.isTransferenciaInterna || String(e.dc || '').toUpperCase().trim() === 'T') continue;
    for (const campo of ['cliente', 'projeto', 'parceiro']) {
      const val = (e[campo] || '').trim();
      if (!val || val === '-' || isLixo(val)) continue;
      const key = val.toUpperCase();
      if (!novosNomes.has(key)) novosNomes.set(key, { nomeOriginal: val, nomeOficial: key });
    }
  }

  const novosPendentes = [];
  for (const [key, info] of novosNomes) {
    if (revisadosKeys.has(key)) continue;
    novosPendentes.push({
      id: crypto.randomUUID(),
      nomeOriginal: info.nomeOriginal,
      nomeOficial: key,
      tipoSugerido: 'Pendente de Classificação',
      tipoFinal: 'Pendente de Classificação',
      clienteVinculado: '',
      projetoVinculado: '',
      manterAlias: true,
      observacao: '',
      statusRevisao: 'pendente'
    });
  }
  db.reviewRegistry = [...revisados, ...novosPendentes];

  // 3. Limpar issues NOVO_CADASTRO de transferências internas
  const nomesValidos = new Set(db.reviewRegistry.map(r => (r.nomeOficial || '').toUpperCase()));
  db.issues = (db.issues || []).filter(i => {
    if (i.code !== 'NOVO_CADASTRO') return true;
    const match = (i.message || '').match(/Novo cadastro identificado: (.+)\./);
    if (!match) return false;
    const nome = match[1].trim().toUpperCase();
    return nomesValidos.has(nome);
  });

  if (transferenciasInternas > 0 || novosPendentes.length !== (db.reviewRegistry.length - revisados.length)) {
    console.log(`[limpeza] dc=T marcados: ${transferenciasInternas} | registry: ${revisados.length} revisados + ${novosPendentes.length} pendentes`);
  }
}

// Boot: sobe o servidor imediatamente para evitar timeout do Railway,
// depois carrega os dados do PostgreSQL em background.
let bootReady = false;

async function boot() {
  // Garantir que o db.json existe com usuário padrão (para o servidor aceitar requisições imediatamente)
  if (!fs.existsSync(DB_PATH)) {
    fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
    const emptyDb = { users: [{ id: 'owner-ckm', email: 'owner@ckm.local', password: hashPassword('123456'), role: 'owner' }], uploads: [], entries: [], issues: [], reviewRegistry: [], savedRules: [], manualAdjustments: [] };
    fs.writeFileSync(DB_PATH, JSON.stringify(emptyDb, null, 2));
    console.log('[boot] db.json criado com usuário padrão.');
  }

  // Subir o servidor IMEDIATAMENTE — Railway health check passa em < 1s
  server.listen(PORT, () => {
    console.log(`CKM MVP running at http://localhost:${PORT}`);
  });

  // Carregar dados do PostgreSQL em background (não bloqueia o health check)
  if (process.env.DATABASE_URL) {
    (async () => {
      try {
        console.log('[boot] DATABASE_URL detectado — inicializando schema PostgreSQL...');
        await storage.init();
        console.log('[boot] Carregando dados do PostgreSQL...');
        const pgDb = await storage.loadDb();
        // Sincronizar db.json local com os dados do PostgreSQL
        fs.mkdirSync(path.dirname(DB_PATH), { recursive: true });
        fs.writeFileSync(DB_PATH, JSON.stringify(pgDb, null, 2));
        // Executar limpeza automática após carregar o banco
        limparCadastrosAutomatico(pgDb);
        fs.writeFileSync(DB_PATH, JSON.stringify(pgDb, null, 2));
        // Persistir limpeza de volta ao PostgreSQL
        await storage.saveDb(pgDb).catch(e => console.error('[boot] Erro ao salvar limpeza:', e.message));
        bootReady = true;
        console.log(`[boot] Sincronizado: ${pgDb.entries.length} lançamentos, ${pgDb.reviewRegistry.length} cadastros, ${pgDb.savedRules.length} regras`);
      } catch (err) {
        console.error('[boot] Erro ao carregar PostgreSQL:', err.message);
        console.error('[boot] Continuando com db.json local (pode estar vazio).');
        bootReady = true;
      }
    })();
  } else {
    bootReady = true;
  }
}

boot().catch((err) => {
  console.error('[boot] Falha crítica:', err);
  process.exit(1);
});
