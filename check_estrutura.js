const { createStorage } = require('./storage');
const storage = createStorage({ dbPath: './data/db.json', databaseUrl: process.env.DATABASE_URL });

storage.loadDb().then(db => {
  const CORTE = '2024-06-01';
  const FORBIDDEN = ['ESCRITÓRIO','SALÁRIOS','JURÍDICO','CONTÁBIL','TEF','MÚTUO','PRONAMPE','SALDO ATUAL','ADMINISTRATIVO','COMERCIAL','FINANCEIRO','FISCAL','OPERACIONAL','PRÓ-LABORE','RH','TI'];
  const ativos = db.entries.filter(e => (e.dataISO||'') >= CORTE);
  
  // Lançamentos de estrutura: sem cliente E com CC de overhead
  const estrutura = ativos.filter(e => {
    const cli = (e.cliente||'').trim();
    const cc = (e.centroCusto||'').trim().toUpperCase();
    if (cli) return false;
    return FORBIDDEN.some(f => f === cc) || cc === '';
  });

  // Agrupar por centroCusto
  const byCC = {};
  estrutura.forEach(e => {
    const cc = e.centroCusto || 'SEM CC';
    byCC[cc] = (byCC[cc]||0) + (e.valor||0);
  });
  const sorted = Object.entries(byCC).sort((a,b) => a[1]-b[1]);
  
  console.log('=== CUSTOS DE ESTRUTURA (desde ' + CORTE + ') ===');
  console.log('Total lançamentos:', estrutura.length);
  console.log('Total valor: R$' + estrutura.reduce((a,e)=>a+(e.valor||0),0).toFixed(2));
  console.log('');
  console.log('Por Centro de Custo:');
  sorted.forEach(([k,v]) => console.log('  ' + k + ': R$' + v.toFixed(2)));
  
  // Também mostrar os CCs que NÃO são estrutura (clientes)
  const clientesCCs = {};
  ativos.filter(e => {
    const cli = (e.cliente||'').trim();
    const cc = (e.centroCusto||'').trim().toUpperCase();
    return !cli && !FORBIDDEN.some(f => f === cc) && cc;
  }).forEach(e => {
    const cc = e.centroCusto;
    clientesCCs[cc] = (clientesCCs[cc]||0) + (e.valor||0);
  });
  console.log('');
  console.log('CCs tratados como CLIENTES (não estrutura):');
  Object.entries(clientesCCs).sort((a,b)=>b[1]-a[1]).slice(0,15).forEach(([k,v]) => console.log('  ' + k + ': R$' + v.toFixed(2)));
  
  process.exit(0);
}).catch(e => { console.error(e.message); process.exit(1); });
