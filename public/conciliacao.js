// Script da tela de conciliação — carregado via <script src>
// EXTRATO_ID e TODOS_TIPOS_CONCIL são injetados via <script> inline antes deste arquivo

function filtrarTiposConc(grupoId, tipoId, valorAtual) {
  var gSel = document.getElementById(grupoId);
  var tSel = document.getElementById(tipoId);
  if (!gSel || !tSel) return;
  var grupoCod = gSel.value;
  var filtrados = grupoCod ? TODOS_TIPOS_CONCIL.filter(function(t){ return t.grupo_cod === grupoCod; }) : TODOS_TIPOS_CONCIL;
  var opts = "<option value=''>-- Selecione o Tipo --</option>";
  filtrados.forEach(function(t){ opts += "<option value='" + t.codigo + "'" + (t.codigo === valorAtual ? ' selected' : '') + ">" + t.nome + "</option>"; });
  tSel.innerHTML = opts;
}

document.addEventListener('change', function(e) {
  var tipoTarget = e.target.getAttribute('data-tipo-target');
  if (tipoTarget) { filtrarTiposConc(e.target.id, tipoTarget, ''); }
});

function abrirFormLanc(i) {
  document.getElementById('nl-form-' + i).style.display = 'table-row';
  var b = document.getElementById('nl-row-' + i).querySelector('button');
  if (b) b.style.display = 'none';
}

function fecharFormLanc(i) {
  document.getElementById('nl-form-' + i).style.display = 'none';
  var b = document.getElementById('nl-row-' + i).querySelector('button');
  if (b) b.style.display = '';
}

async function salvarLanc(i, fitid) {
  var data = document.getElementById('nf-data-' + i).value;
  var dc = document.getElementById('nf-dc-' + i).value;
  var valor = parseFloat(document.getElementById('nf-valor-' + i).value);
  var cc = document.getElementById('nf-cc-' + i).value;
  var cli = document.getElementById('nf-cli-' + i).value;
  var proj = document.getElementById('nf-proj-' + i).value;
  var nat = document.getElementById('nf-nat-' + i).value;
  var cat = document.getElementById('nf-cat-' + i).value;
  var tipo = document.getElementById('nf-tipo-' + i).value;
  var desc = document.getElementById('nf-desc-' + i).value;
  var obs = document.getElementById('nf-obs-' + i).value;
  var msg = document.getElementById('nf-msg-' + i);
  if (!data || !dc || !valor || !cc) {
    msg.innerHTML = '<span style="color:#dc2626">Preencha Data, D/C, Valor e Centro de Custo.</span>';
    return;
  }
  msg.innerHTML = '<span style="color:#1d4ed8">Salvando...</span>';
  try {
    var r = await fetch('/api/entries', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({data:data, dataISO:data, dc:dc, valor:dc==='D'?-Math.abs(valor):Math.abs(valor),
        centroCusto:cc, cliente:cli, projeto:proj, natureza:nat, categoria:cat, tipo:tipo,
        descritivo:desc, observacoes:obs, status:'confirmado'})});
    var d = await r.json();
    if (d.ok || d.id || d.numLanc) {
      msg.innerHTML = '<span style="color:#059669;font-weight:700">✅ Lançamento #' + (d.numLanc||d.id||'') + ' criado com sucesso!</span>';
      if (fitid && EXTRATO_ID) {
        fetch('/api/conciliacao/item-status', {method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({extratoId:EXTRATO_ID, fitid:fitid, novoStatus:'LANCAMENTO_CONFIRMADO', lancamentoId:d.id||d.numLanc})});
      }
      var cells = document.getElementById('nl-row-' + i).cells;
      if (cells[4]) cells[4].innerHTML = '<span style="color:#059669;font-weight:700">✅ Lançamento Confirmado</span>';
      document.getElementById('nl-form-' + i).style.display = 'none';
    } else {
      msg.innerHTML = '<span style="color:#dc2626">Erro: ' + (d.error||JSON.stringify(d)) + '</span>';
    }
  } catch(e) {
    msg.innerHTML = '<span style="color:#dc2626">Erro: ' + e.message + '</span>';
  }
}

async function marcarEmAnalise(i, fitid) {
  if (EXTRATO_ID && fitid) {
    await fetch('/api/conciliacao/item-status', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({extratoId:EXTRATO_ID, fitid:fitid, novoStatus:'EM_ANALISE'})});
  }
  var cells = document.getElementById('nl-row-' + i).cells;
  cells[4].innerHTML = '<span style="color:#6366f1;font-weight:700">🔍 Em Análise</span>';
}

function abrirPainelDiv(i) {
  document.getElementById('dv-painel-' + i).style.display = 'table-row';
  var b = document.getElementById('dv-row-' + i).querySelector('button');
  if (b) b.style.display = 'none';
}

function fecharPainelDiv(i) {
  document.getElementById('dv-painel-' + i).style.display = 'none';
  var b = document.getElementById('dv-row-' + i).querySelector('button');
  if (b) b.style.display = '';
}

function abrirBuscaLanc(i) {
  var el = document.getElementById('dv-busca-lanc-' + i);
  if (el) el.style.display = el.style.display === 'none' ? 'block' : 'none';
}

async function salvarCorrecaoDiv(i, fitid, lancId) {
  var dc = document.getElementById('dv-dc-' + i).value;
  var valor = parseFloat(document.getElementById('dv-valor-' + i).value);
  var cc = document.getElementById('dv-cc-' + i).value;
  var cli = document.getElementById('dv-cli-' + i).value;
  var proj = document.getElementById('dv-proj-' + i).value;
  var nat = document.getElementById('dv-nat-' + i) ? document.getElementById('dv-nat-' + i).value : '';
  var cat = document.getElementById('dv-cat-' + i) ? document.getElementById('dv-cat-' + i).value : '';
  var tipo = document.getElementById('dv-tipo-' + i) ? document.getElementById('dv-tipo-' + i).value : '';
  var desc = document.getElementById('dv-desc-' + i).value;
  var obs = document.getElementById('dv-obs-' + i) ? document.getElementById('dv-obs-' + i).value : '';
  var msg = document.getElementById('dv-msg-' + i);
  if (!lancId) { if(msg) msg.innerHTML = '<span style="color:#dc2626">ID do lançamento não encontrado.</span>'; return; }
  if(msg) msg.innerHTML = '<span style="color:#1d4ed8">Salvando...</span>';
  try {
    var r = await fetch('/api/entries/' + lancId, {method:'PUT', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({dc:dc, valor:dc==='D'?-Math.abs(valor):Math.abs(valor),
        centroCusto:cc, cliente:cli, projeto:proj, natureza:nat, categoria:cat, tipo:tipo,
        descritivo:desc, observacoes:obs})});
    var d = await r.json();
    if (d.ok || d.id) {
      if(msg) msg.innerHTML = '<span style="color:#059669;font-weight:700">✅ Correção salva com sucesso!</span>';
      if (fitid && EXTRATO_ID) {
        await fetch('/api/conciliacao/item-status', {method:'POST', headers:{'Content-Type':'application/json'},
          body: JSON.stringify({extratoId:EXTRATO_ID, fitid:fitid, novoStatus:'OK_CONCILIADO', lancamentoId:lancId})});
      }
      var cells = document.getElementById('dv-row-' + i).cells;
      if (cells[4]) cells[4].innerHTML = '<span style="color:#059669;font-weight:700">✔️ OK — Conciliado</span>';
      document.getElementById('dv-painel-' + i).style.display = 'none';
    } else {
      if(msg) msg.innerHTML = '<span style="color:#dc2626">Erro: ' + (d.error||JSON.stringify(d)) + '</span>';
    }
  } catch(e) {
    if(msg) msg.innerHTML = '<span style="color:#dc2626">Erro: ' + e.message + '</span>';
  }
}

async function confirmarOkConciliado(i, fitid) {
  if (EXTRATO_ID && fitid) {
    await fetch('/api/conciliacao/item-status', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({extratoId:EXTRATO_ID, fitid:fitid, novoStatus:'OK_CONCILIADO'})});
  }
  var cells = document.getElementById('dv-row-' + i).cells;
  cells[4].innerHTML = '<span style="color:#059669;font-weight:700">✔️ OK — Conciliado</span>';
  cells[5].innerHTML = '';
  document.getElementById('dv-painel-' + i).style.display = 'none';
}

async function confirmarConciliadoOk(i, fitid) {
  var btn = document.getElementById('cc-btn-' + i);
  if (btn) { btn.disabled = true; btn.textContent = 'Salvando...'; btn.style.opacity = '.6'; }
  if (EXTRATO_ID && fitid) {
    await fetch('/api/conciliacao/item-status', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({extratoId:EXTRATO_ID, fitid:fitid, novoStatus:'OK_CONCILIADO'})});
  }
  var statusCell = document.getElementById('cc-status-' + i);
  if (statusCell) statusCell.innerHTML = '<span style="color:#059669;font-weight:700">✔️ OK — Conciliado</span>';
  if (btn) btn.remove();
}

async function buscarLancamentoManual(i) {
  var q = document.getElementById('dv-busca-num-' + i).value.trim();
  if (!q) return;
  var res = document.getElementById('dv-busca-res-' + i);
  res.innerHTML = '<span style="color:#64748b;font-size:.78rem">Buscando...</span>';
  try {
    var r = await fetch('/api/entries/busca?q=' + encodeURIComponent(q));
    var d = await r.json();
    if (!d.results || d.results.length === 0) {
      res.innerHTML = '<span style="color:#dc2626;font-size:.78rem">Nenhum lançamento encontrado.</span>';
      return;
    }
    var html = '<div style="margin-top:.35rem;display:flex;flex-direction:column;gap:.3rem">';
    d.results.forEach(function(l) {
      var num = l.numLanc ? '#' + String(l.numLanc).padStart(6,'0') : '-';
      var val = l.valor ? 'R$ ' + Math.abs(parseFloat(l.valor)).toLocaleString('pt-BR',{minimumFractionDigits:2}) : '-';
      var info = num + ' | ' + (l.dataISO||'?') + ' | ' + val + ' | ' + (l.cliente||'-') + ' | ' + (l.centroCusto||'-');
      html += '<div style="display:flex;align-items:center;gap:.5rem;background:#fff;border:1px solid #bae6fd;border-radius:.3rem;padding:.3rem .5rem;font-size:.78rem">';
      html += '<span style="flex:1">' + info + '</span>';
      html += '<button onclick="vincularLancamentoManual(' + i + ',\'' + (l.id||'') + '\',\'' + (l.numLanc||'') + '\')" style="background:#0369a1;color:#fff;border:none;padding:.2rem .6rem;border-radius:.25rem;cursor:pointer;font-size:.75rem;white-space:nowrap">Usar este</button>';
      html += '</div>';
    });
    html += '</div>';
    res.innerHTML = html;
  } catch(e) {
    res.innerHTML = '<span style="color:#dc2626;font-size:.78rem">Erro: ' + e.message + '</span>';
  }
}

async function vincularLancamentoManual(i, lancId, numLanc) {
  if (!EXTRATO_ID || !lancId) return;
  var fitid = document.getElementById('dv-row-' + i) ? document.getElementById('dv-row-' + i).dataset.fitid : '';
  await fetch('/api/conciliacao/item-status', {method:'POST', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({extratoId:EXTRATO_ID, fitid:fitid, novoStatus:'DIVERGENTE', lancamentoId:lancId})});
  var cells = document.getElementById('dv-row-' + i).cells;
  if (cells[5]) cells[5].innerHTML = '<a href="/lancamentos?num=' + numLanc + '" style="color:#3b82f6;font-size:.78rem;font-weight:600">🔗 Ver #' + String(numLanc).padStart(6,'0') + '</a>';
  document.getElementById('dv-busca-lanc-' + i).style.display = 'none';
  var msg = document.getElementById('dv-msg-' + i);
  if (msg) msg.innerHTML = '<span style="color:#059669;font-size:.8rem">✅ Lançamento vinculado! Preencha os dados abaixo e salve.</span>';
}

async function marcarEmAnaliseDiv(i, fitid) {
  if (EXTRATO_ID && fitid) {
    await fetch('/api/conciliacao/item-status', {method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({extratoId:EXTRATO_ID, fitid:fitid, novoStatus:'EM_ANALISE'})});
  }
  var cells = document.getElementById('dv-row-' + i).cells;
  cells[4].innerHTML = '<span style="color:#6366f1;font-weight:700">🔍 Em Análise</span>';
  document.getElementById('dv-painel-' + i).style.display = 'none';
}
