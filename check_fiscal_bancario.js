const { createStorage } = require('./storage');
const storage = createStorage({ dbPath: './data/db.json', databaseUrl: process.env.DATABASE_URL });

storage.loadDb().then(db => {
  const CORTE = '2024-06-01';
  const ativos = db.entries.filter(e => (e.dataISO||'') >= CORTE);

  const norm = (v) => (v||'').toUpperCase().normalize('NFD').replace(/[\u0300-\u036f]/g,'');

  // ─── IMPOSTOS ────────────────────────────────────────────────────────────────
  const IMPOSTO_KEYWORDS = ['IMPOSTO','TRIBUTO','INSS','FGTS','ISS','ISSQN','PIS','COFINS',
    'CSLL','IRPJ','IRRF','IRPF','SIMPLES','DAS','DARF','GPS','SEFAZ','RECEITA FEDERAL',
    'IOF','ICMS','IPI','CONTRIBUICAO','CONTRIBUIÇÃO'];

  const isImposto = (e) => {
    const txt = [e.descricao, e.tipoOriginal, e.natureza, e.centroCusto, e.parceiro]
      .map(v => norm(v||'')).join(' ');
    return IMPOSTO_KEYWORDS.some(k => txt.includes(k));
  };

  const impostos = ativos.filter(isImposto);
  const byImposto = {};
  impostos.forEach(e => {
    // Identificar o tipo de imposto pela descrição
    const txt = norm(e.descricao||'');
    let tipo = 'OUTROS IMPOSTOS';
    if (txt.includes('INSS') || txt.includes('GPS')) tipo = 'INSS / GPS';
    else if (txt.includes('FGTS')) tipo = 'FGTS';
    else if (txt.includes('ISS') || txt.includes('ISSQN')) tipo = 'ISS/ISSQN';
    else if (txt.includes('SIMPLES') || txt.includes('DAS')) tipo = 'Simples Nacional / DAS';
    else if (txt.includes('DARF') || txt.includes('IRPJ') || txt.includes('IRRF') || txt.includes('IRPF')) tipo = 'IRPJ/IRRF/DARF';
    else if (txt.includes('IOF')) tipo = 'IOF';
    else if (txt.includes('PIS') || txt.includes('COFINS') || txt.includes('CSLL')) tipo = 'PIS/COFINS/CSLL';
    byImposto[tipo] = (byImposto[tipo]||0) + (e.valor||0);
  });

  console.log('=== IMPOSTOS (desde ' + CORTE + ') ===');
  console.log('Total lançamentos:', impostos.length);
  console.log('Total valor: R$' + impostos.reduce((a,e)=>a+(e.valor||0),0).toFixed(2));
  Object.entries(byImposto).sort((a,b)=>a[1]-b[1]).forEach(([k,v]) => console.log(`  ${k}: R$${v.toFixed(2)}`));

  // Amostra
  console.log('\nAmostra:');
  impostos.slice(0,10).forEach(e => console.log(`  ${e.dataISO} | R$${e.valor} | ${(e.descricao||'').slice(0,60)} | CC:${e.centroCusto}`));

  // ─── FATURAMENTO (RECEITAS) ───────────────────────────────────────────────────
  const FAT_KEYWORDS = ['NF','NOTA FISCAL','FATURA','HONORARIO','HONORÁRIO','CONTRATO',
    'SERVICO','SERVIÇO','CONSULTORIA','TREINAMENTO','PALESTRA','PROJETO'];

  const isFaturamento = (e) => {
    if ((e.valor||0) <= 0) return false; // só receitas
    const txt = [e.descricao, e.tipoOriginal, e.natureza, e.centroCusto, e.parceiro]
      .map(v => norm(v||'')).join(' ');
    return FAT_KEYWORDS.some(k => txt.includes(k));
  };

  const faturamento = ativos.filter(isFaturamento);
  const byMes = {};
  faturamento.forEach(e => {
    const mes = (e.dataISO||'').slice(0,7);
    byMes[mes] = (byMes[mes]||0) + (e.valor||0);
  });

  console.log('\n=== FATURAMENTO / RECEITAS (desde ' + CORTE + ') ===');
  console.log('Total lançamentos:', faturamento.length);
  console.log('Total valor: R$' + faturamento.reduce((a,e)=>a+(e.valor||0),0).toFixed(2));
  console.log('\nPor mês:');
  Object.entries(byMes).sort().forEach(([k,v]) => console.log(`  ${k}: R$${v.toFixed(2)}`));

  // ─── DESPESAS BANCÁRIAS ───────────────────────────────────────────────────────
  const BANCO_KEYWORDS = ['TARIFA','TAR ','TAR.','MANUTENCAO','MANUTENÇÃO','ANUIDADE',
    'TAXA BANCARIA','TAXA BANCO','TAXA SERVICO','TAXA SERVIÇO','PACOTE','TED','DOC',
    'COBRANCA','COBRANÇA','EXTRATO','BOLETO BANCARIO','BANCO','BB ','ITAU','BRADESCO',
    'SANTANDER','CAIXA ECONOMICA','RENDIMENTO','APLICACAO','APLICAÇÃO','CDB','LCI',
    'JUROS BANCO','JUROS BB','JUROS ITAU','JUROS BRADESCO'];

  const isBancario = (e) => {
    const txt = [e.descricao, e.tipoOriginal, e.natureza, e.centroCusto, e.parceiro]
      .map(v => norm(v||'')).join(' ');
    // Excluir TEF (já tratado), mútuo e Pronampe
    if (txt.includes('MUTUO') || txt.includes('MÚTUO') || txt.includes('PRONAMPE') || txt.includes('TEF')) return false;
    return BANCO_KEYWORDS.some(k => txt.includes(k));
  };

  const bancario = ativos.filter(isBancario);
  const byBanco = {};
  bancario.forEach(e => {
    const desc = norm(e.descricao||'');
    let tipo = 'OUTROS BANCÁRIOS';
    if (desc.includes('TARIFA') || desc.includes('TAR ') || desc.includes('MANUTENCAO') || desc.includes('MANUTENÇÃO')) tipo = 'Tarifas e Manutenção de Conta';
    else if (desc.includes('RENDIMENTO') || desc.includes('APLICACAO') || desc.includes('CDB')) tipo = 'Rendimentos / Aplicações';
    else if (desc.includes('IOF')) tipo = 'IOF Bancário';
    else if (desc.includes('JUROS')) tipo = 'Juros Bancários';
    else if (desc.includes('TED') || desc.includes('DOC')) tipo = 'TED/DOC';
    byBanco[tipo] = (byBanco[tipo]||0) + (e.valor||0);
  });

  console.log('\n=== DESPESAS BANCÁRIAS (desde ' + CORTE + ') ===');
  console.log('Total lançamentos:', bancario.length);
  console.log('Total valor: R$' + bancario.reduce((a,e)=>a+(e.valor||0),0).toFixed(2));
  Object.entries(byBanco).sort((a,b)=>a[1]-b[1]).forEach(([k,v]) => console.log(`  ${k}: R$${v.toFixed(2)}`));

  console.log('\nAmostra:');
  bancario.slice(0,15).forEach(e => console.log(`  ${e.dataISO} | R$${e.valor} | ${(e.descricao||'').slice(0,60)} | CC:${e.centroCusto}`));

  // ─── TODOS OS CCs ÚNICOS (para entender o que ainda não está classificado) ────
  const ccs = {};
  ativos.forEach(e => {
    const cc = (e.centroCusto||'SEM CC').toUpperCase();
    ccs[cc] = (ccs[cc]||0) + 1;
  });
  console.log('\n=== TODOS OS CENTROS DE CUSTO (ativos) ===');
  Object.entries(ccs).sort((a,b)=>b[1]-a[1]).forEach(([k,v]) => console.log(`  ${k}: ${v} lançamentos`));

  process.exit(0);
}).catch(e => { console.error(e.message); process.exit(1); });
