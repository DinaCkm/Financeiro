const { createStorage } = require('./storage');
const storage = createStorage({ dbPath: './data/db.json', databaseUrl: process.env.DATABASE_URL });

storage.loadDb().then(db => {
  const CORTE = '2024-06-01';
  const ativos = db.entries.filter(e => (e.dataISO||'') >= CORTE);

  // Todos os lançamentos com CC = TEF
  const tef = ativos.filter(e => (e.centroCusto||'').toUpperCase() === 'TEF');

  console.log('=== ANÁLISE DOS LANÇAMENTOS TEF (desde ' + CORTE + ') ===');
  console.log('Total de lançamentos TEF:', tef.length);
  console.log('Soma total: R$' + tef.reduce((a,e)=>a+(e.valor||0),0).toFixed(2));
  console.log('');

  // Separar por D/C
  const creditos = tef.filter(e => (e.dc||'').toUpperCase() === 'C');
  const debitos = tef.filter(e => (e.dc||'').toUpperCase() === 'D');
  const outros = tef.filter(e => !['C','D'].includes((e.dc||'').toUpperCase()));

  console.log('Créditos (C):', creditos.length, '| Soma: R$' + creditos.reduce((a,e)=>a+(e.valor||0),0).toFixed(2));
  console.log('Débitos (D):', debitos.length, '| Soma: R$' + debitos.reduce((a,e)=>a+(e.valor||0),0).toFixed(2));
  console.log('Outros:', outros.length);
  console.log('');

  // Verificar o campo isTransferenciaInterna
  const marcados = tef.filter(e => e.isTransferenciaInterna);
  console.log('Já marcados como isTransferenciaInterna:', marcados.length);
  console.log('');

  // Agrupar por conta para entender os 3 bancos
  const byConta = {};
  tef.forEach(e => {
    const conta = e.conta || 'SEM CONTA';
    if (!byConta[conta]) byConta[conta] = { count: 0, soma: 0, exemplos: [] };
    byConta[conta].count++;
    byConta[conta].soma += (e.valor||0);
    if (byConta[conta].exemplos.length < 3) {
      byConta[conta].exemplos.push({
        data: e.dataISO,
        desc: (e.descricao||'').slice(0,50),
        valor: e.valor,
        dc: e.dc,
        parceiro: e.parceiro
      });
    }
  });

  console.log('Por CONTA:');
  Object.entries(byConta).sort((a,b)=>b[1].count-a[1].count).forEach(([k,v]) => {
    console.log(`  ${k}: ${v.count} lançamentos | R$${v.soma.toFixed(2)}`);
    v.exemplos.forEach(ex => {
      console.log(`    - ${ex.data} | ${ex.dc} | R$${ex.valor} | ${ex.desc} | parceiro: ${ex.parceiro||'-'}`);
    });
  });

  // Verificar a descrição para entender se são transferências entre contas
  console.log('');
  console.log('Amostra de descrições TEF:');
  tef.slice(0, 20).forEach(e => {
    console.log(`  ${e.dataISO} | ${e.dc} | R$${e.valor} | CC:${e.centroCusto} | desc: ${(e.descricao||'').slice(0,60)} | parceiro: ${(e.parceiro||'').slice(0,30)}`);
  });

  process.exit(0);
}).catch(e => { console.error(e.message); process.exit(1); });
