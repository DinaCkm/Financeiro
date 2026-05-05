// Script auxiliar para gerar o código da seção de transferências sem par
// Executar: node gen_transf_section.js
// Saída: trecho a ser inserido no server.js

const section = `
      + (transferenciasSemPar.length > 0
        ? '<section style="margin-bottom:2rem">'
          +'<div style="background:#fefce8;border:2px solid #eab308;border-radius:.6rem;padding:1rem 1.25rem;margin-bottom:.75rem">'
          +'<h3 style="margin:0 0 .25rem;color:#854d0e;font-size:1rem">\\u{1F504} TRANSFER\\u00caNCIAS ENTRE CONTAS ('+transferenciasSemPar.length+') \\u2014 AGUARDANDO LAN\\u00c7AMENTO</h3>'
          +'<p style="margin:0;font-size:.8rem;color:#713f12">Estes lan\\u00e7amentos foram identificados como transfer\\u00eancias entre suas pr\\u00f3prias contas (TED/TEF/PIX entre bancos). Clique em <strong>+ Registrar Transfer\\u00eancia</strong> para criar o lan\\u00e7amento como Transfer\\u00eancia Interna. N\\u00e3o afetam o saldo operacional.</p>'
          +'</div>'
          +'<div style="overflow-x:auto"><table style="width:100%;border-collapse:collapse">'
          +'<thead><tr style="background:#fef9c3">'
          +'<th style="padding:.4rem .6rem;text-align:left;font-size:.78rem;color:#854d0e">Data</th>'
          +'<th style="padding:.4rem .6rem;text-align:left;font-size:.78rem;color:#854d0e">D/C</th>'
          +'<th style="padding:.4rem .6rem;text-align:left;font-size:.78rem;color:#854d0e">Hist\\u00f3rico do Extrato</th>'
          +'<th style="padding:.4rem .6rem;text-align:right;font-size:.78rem;color:#854d0e">Valor</th>'
          +'<th style="padding:.4rem .6rem;font-size:.78rem;color:#854d0e">A\\u00e7\\u00e3o</th>'
          +'</tr></thead><tbody>'
          + transferenciasSemPar.map((it, tspIdx) => {
              const cor = it.dc==='C' ? '#059669' : '#dc2626';
              const extratoIdEnc = extratoId;
              const fitidEnc = encodeURIComponent(it.fitid||'');
              const memoEnc = encodeURIComponent(it.memo||'');
              const valorAbs = Math.abs(it.valor||0);
              return '<tr id="tsp-row-'+tspIdx+'" style="border-bottom:1px solid #fef9c3">'
                +'<td style="padding:.4rem .6rem;font-size:.82rem">'+esc(fmtData(it.dataISO||''))+'</td>'
                +'<td style="padding:.4rem .6rem;font-weight:700;color:'+cor+'">'+esc(it.dc||'')+'</td>'
                +'<td style="padding:.4rem .6rem;font-size:.82rem;max-width:320px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="'+esc(it.memo||'')+'">'+esc((it.memo||'').slice(0,70))+'</td>'
                +'<td style="padding:.4rem .6rem;text-align:right;font-weight:700;color:'+cor+'">'+fmtVal(it.valor)+'</td>'
                +'<td style="padding:.4rem .6rem">'
                  +'<button onclick="registrarTransferencia('+tspIdx+','+extratoIdEnc+',decodeURIComponent(\\''+fitidEnc+'\\'),\\''+esc(it.dataISO||'')+'\\',\\''+esc(it.dc||'')+'\\','+valorAbs+',decodeURIComponent(\\''+memoEnc+'\\'))" '
                  +'style="background:#eab308;color:#fff;border:none;border-radius:.4rem;padding:.3rem .8rem;font-size:.78rem;font-weight:700;cursor:pointer" '
                  +'id="tsp-btn-'+tspIdx+'">+ Registrar Transfer\\u00eancia</button>'
                  +'<span id="tsp-ok-'+tspIdx+'" style="display:none;color:#059669;font-weight:700;font-size:.82rem">\\u2705 Registrado</span>'
                +'</td>'
                +'</tr>';
            }).join('')
          +'</tbody></table></div></section>'
        : '')
`;

console.log(section);
