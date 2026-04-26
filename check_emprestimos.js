const { createStorage } = require('./storage');
const storage = createStorage({ dbPath: './data/db.json', databaseUrl: process.env.DATABASE_URL });

storage.loadDb().then(db => {
  const ALL = db.entries;
  const CORTE = '2024-06-01';

  // ─── MÚTUO ───────────────────────────────────────────────────────────────────
  const isMutuo = (e) => {
    const txt = [e.descricao, e.tipoOriginal, e.natureza, e.centroCusto, e.parceiro]
      .map(v => (v||'').toUpperCase()).join(' ');
    return txt.includes('MÚTUO') || txt.includes('MUTUO');
  };
  const mutuo = ALL.filter(isMutuo);
  const mutuoAtivo = mutuo.filter(e => (e.dataISO||'') >= CORTE);

  console.log('=== ANÁLISE DE MÚTUO (todos os períodos) ===');
  console.log('Total lançamentos mútuo:', mutuo.length);
  
  // Separar por crédito (recebimento de empréstimo) e débito (devolução/pagamento)
  const mutuoCreditos = mutuo.filter(e => (e.valor||0) > 0);
  const mutuoDebitos = mutuo.filter(e => (e.valor||0) < 0);
  const saldoMutuo = mutuo.reduce((a,e) => a+(e.valor||0), 0);
  
  console.log('Créditos (empréstimos recebidos): R$' + mutuoCreditos.reduce((a,e)=>a+(e.valor||0),0).toFixed(2));
  console.log('Débitos (devoluções/pagamentos): R$' + mutuoDebitos.reduce((a,e)=>a+(e.valor||0),0).toFixed(2));
  console.log('SALDO DEVEDOR (negativo = empresa deve): R$' + saldoMutuo.toFixed(2));
  console.log('');
  
  // Agrupar por parceiro (credor)
  const byParceiro = {};
  mutuo.forEach(e => {
    const p = e.parceiro || e.descricao || 'SEM IDENTIFICAÇÃO';
    if (!byParceiro[p]) byParceiro[p] = { recebido: 0, pago: 0, saldo: 0, count: 0 };
    byParceiro[p].count++;
    byParceiro[p].saldo += (e.valor||0);
    if ((e.valor||0) > 0) byParceiro[p].recebido += (e.valor||0);
    else byParceiro[p].pago += Math.abs(e.valor||0);
  });
  
  console.log('Por credor (parceiro):');
  Object.entries(byParceiro).sort((a,b) => a[1].saldo - b[1].saldo).forEach(([k,v]) => {
    console.log(`  ${k.slice(0,50)}: recebido=R$${v.recebido.toFixed(2)} | pago=R$${v.pago.toFixed(2)} | saldo=R$${v.saldo.toFixed(2)}`);
  });

  console.log('');
  console.log('Período ativo (desde ' + CORTE + '):');
  console.log('  Lançamentos:', mutuoAtivo.length);
  console.log('  Saldo período: R$' + mutuoAtivo.reduce((a,e)=>a+(e.valor||0),0).toFixed(2));

  // ─── PRONAMPE ────────────────────────────────────────────────────────────────
  const isPronampe = (e) => {
    const txt = [e.descricao, e.tipoOriginal, e.natureza, e.centroCusto, e.parceiro]
      .map(v => (v||'').toUpperCase()).join(' ');
    return txt.includes('PRONAMPE');
  };
  const pronampe = ALL.filter(isPronampe);
  const pronampeAtivo = pronampe.filter(e => (e.dataISO||'') >= CORTE);

  console.log('');
  console.log('=== ANÁLISE DE PRONAMPE (todos os períodos) ===');
  console.log('Total lançamentos Pronampe:', pronampe.length);
  
  const pronampeCreditos = pronampe.filter(e => (e.valor||0) > 0);
  const pronampeDebitos = pronampe.filter(e => (e.valor||0) < 0);
  const saldoPronampe = pronampe.reduce((a,e) => a+(e.valor||0), 0);
  
  console.log('Créditos (empréstimo recebido): R$' + pronampeCreditos.reduce((a,e)=>a+(e.valor||0),0).toFixed(2));
  console.log('Débitos (parcelas pagas): R$' + pronampeDebitos.reduce((a,e)=>a+(e.valor||0),0).toFixed(2));
  console.log('SALDO (negativo = empresa pagou mais do que recebeu): R$' + saldoPronampe.toFixed(2));
  console.log('');
  
  // Agrupar por parceiro
  const byParceiroP = {};
  pronampe.forEach(e => {
    const p = e.parceiro || e.descricao || 'SEM IDENTIFICAÇÃO';
    if (!byParceiroP[p]) byParceiroP[p] = { recebido: 0, pago: 0, saldo: 0, count: 0 };
    byParceiroP[p].count++;
    byParceiroP[p].saldo += (e.valor||0);
    if ((e.valor||0) > 0) byParceiroP[p].recebido += (e.valor||0);
    else byParceiroP[p].pago += Math.abs(e.valor||0);
  });
  
  console.log('Por credor:');
  Object.entries(byParceiroP).sort((a,b) => a[1].saldo - b[1].saldo).forEach(([k,v]) => {
    console.log(`  ${k.slice(0,60)}: recebido=R$${v.recebido.toFixed(2)} | pago=R$${v.pago.toFixed(2)} | saldo=R$${v.saldo.toFixed(2)}`);
  });

  console.log('');
  console.log('Período ativo (desde ' + CORTE + '):');
  console.log('  Lançamentos:', pronampeAtivo.length);
  console.log('  Saldo período: R$' + pronampeAtivo.reduce((a,e)=>a+(e.valor||0),0).toFixed(2));
  
  // Amostra de lançamentos Pronampe
  console.log('');
  console.log('Amostra de lançamentos Pronampe:');
  pronampe.slice(0, 15).forEach(e => {
    console.log(`  ${e.dataISO} | R$${e.valor} | ${(e.descricao||'').slice(0,50)} | parceiro: ${(e.parceiro||'').slice(0,30)}`);
  });

  process.exit(0);
}).catch(e => { console.error(e.message); process.exit(1); });
