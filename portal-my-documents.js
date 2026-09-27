const crypto = require('crypto');
const { isSmtpConfigured, sendPortalNotificationEmail } = require('./email-service');

const SESSION_SECONDS = 12 * 60 * 60;

function escapeHtml(value) {
  return String(value ?? '').replace(/&/g, '&amp;').replace(/</g, '&lt;')
    .replace(/>/g, '&gt;').replace(/"/g, '&quot;').replace(/'/g, '&#39;');
}

function cookies(req) {
  return Object.fromEntries(String(req.headers.cookie || '').split(';').map(x => x.trim())
    .filter(Boolean).map(x => { const i = x.indexOf('='); return [x.slice(0, i), x.slice(i + 1)]; }));
}

function cookie(req, token, maxAge = SESSION_SECONDS) {
  return `portal_session=${token}; Path=/; HttpOnly; SameSite=Lax; Max-Age=${maxAge}` +
    ((process.env.NODE_ENV === 'production' || String(req.headers['x-forwarded-proto'] || '').includes('https')) ? '; Secure' : '');
}

function html(res, status, title, body, headers = {}) {
  res.writeHead(status, {
    'Content-Type': 'text/html; charset=utf-8', 'Cache-Control': 'private, no-store',
    'Referrer-Policy': 'no-referrer', 'X-Frame-Options': 'SAMEORIGIN', ...headers
  });
  res.end(`<!doctype html><html lang='pt-BR'><head><meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
    <title>${escapeHtml(title)} — CKM Talents</title><style>
    *{box-sizing:border-box}body{margin:0;background:#f8fafc;color:#1e293b;font-family:Arial,sans-serif;line-height:1.5}
    header{background:#111827;color:white;padding:18px 24px}main{max-width:1060px;margin:auto;padding:24px}
    .card{background:white;border:1px solid #dbe3ea;border-radius:14px;padding:20px;margin:16px 0;box-shadow:0 2px 8px #0f172a0b}
    .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(260px,1fr));gap:16px}.grid .card{margin:0}
    h1{font-size:1.75rem}h2{font-size:1.25rem}label{display:block;font-weight:600;margin:12px 0}
    input{display:block;width:100%;padding:12px;border:1px solid #94a3b8;border-radius:8px;font:inherit;max-width:420px}
    button,.button{display:inline-block;background:#0f766e;color:#fff;border:0;border-radius:8px;padding:11px 16px;font:inherit;font-weight:700;cursor:pointer;text-decoration:none}
    .muted{color:#64748b}.tag{display:inline-block;background:#e0f2fe;color:#075985;border-radius:999px;padding:3px 10px;font-size:.8rem;font-weight:700}
    .error{color:#991b1b}a{color:#0f766e}form.inline{display:inline}</style></head><body>
    <header><strong>CKM Talents — Entregas e Validações</strong></header><main>${body}</main></body></html>`);
}

function redirect(res, location, headers = {}) {
  res.writeHead(303, { Location: location, 'Cache-Control': 'private, no-store', ...headers });
  res.end();
}

async function form(req) {
  let body = '';
  for await (const chunk of req) {
    body += chunk;
    if (body.length > 4096) throw new Error('Formulário acima do limite.');
  }
  return new URLSearchParams(body);
}

function normalizeEmail(value) { return String(value || '').trim().toLowerCase().slice(0, 254); }
function hashToken(token) { return crypto.createHash('sha256').update(token).digest('hex'); }

async function getSessionEmail(pg, req) {
  const token = cookies(req).portal_session;
  if (!token || !/^[a-f0-9]{64}$/i.test(token)) return null;
  const rows = await pg.query('SELECT email FROM portal_email_sessions WHERE token_hash=$1 AND expires_at>NOW()', [hashToken(token)]);
  return rows.rows[0]?.email || null;
}

async function handleMyDocuments(req, res, url, pg) {
  if (!url.pathname.startsWith('/meus-documentos')) return false;
  if (/^\/meus-documentos\/documento\/[0-9a-f-]{36}(?:\/(?:pdf|mensagem|decisao))?$/i.test(url.pathname)) return false;

  if (req.method === 'GET' && url.pathname === '/meus-documentos') {
    const email = await getSessionEmail(pg, req);
    if (!email) {
      html(res, 200, 'Meus documentos', `<h1>Meus documentos</h1><p>Confirme seu e-mail para consultar as entregas destinadas a você, inclusive documentos já validados.</p>
        <div class='card'><form method='post' action='/meus-documentos/iniciar'>
        <label>E-mail usado no convite<input type='email' name='email' autocomplete='email' required></label>
        <button type='submit'>Enviar código de acesso</button></form></div>`);
      return true;
    }

    const rows = (await pg.query(`
      SELECT g.id AS guest_id, e.id AS entrega_id, e.titulo, e.status AS entrega_status,
             e.current_version, v.version_number, v.status AS version_status, v.uploaded_at,
             c.nome AS cliente_nome, p.nome AS projeto_nome, val.protocol, val.validated_at,
             EXISTS (SELECT 1 FROM portal_entrega_decisions d WHERE d.version_id=v.id AND d.convidado_id=g.id AND d.decision='validated') AS approved,
             EXISTS (SELECT 1 FROM portal_entrega_decisions d WHERE d.version_id=v.id AND d.convidado_id=g.id AND d.decision='changes_requested') AS requested_changes
        FROM portal_entrega_convidados g
        JOIN portal_entrega_versions v ON v.id=g.version_id
        JOIN portal_entregas e ON e.id=g.entrega_id
        LEFT JOIN clientes c ON c.id=e.cliente_id
        LEFT JOIN projetos p ON p.id=e.projeto_id
        LEFT JOIN portal_entrega_validations val ON val.version_id=v.id
       WHERE lower(trim(g.email))=$1 AND g.revoked_at IS NULL
       ORDER BY e.created_at DESC, v.version_number DESC`, [email])).rows;

    const byDelivery = new Map();
    for (const row of rows) {
      if (!byDelivery.has(row.entrega_id)) byDelivery.set(row.entrega_id, []);
      byDelivery.get(row.entrega_id).push(row);
    }
    const current = [...byDelivery.values()].map(versions => versions.find(v => Number(v.version_number) === Number(v.current_version))).filter(Boolean);
    const waiting = current.filter(v => !v.protocol);
    const finished = current.filter(v => !!v.protocol);
    const priorApproved = rows.filter(v => Number(v.version_number) !== Number(v.current_version) && v.approved);
    function card(v) {
      const state = v.protocol ? 'Validado por todos' : v.requested_changes ? 'Aguardando documento ajustado' : v.approved ? 'Minha validação registrada — aguardando os demais' : 'Aguardando minha análise';
      return `<article class='card'><div class='tag'>${escapeHtml(state)}</div><h3>${escapeHtml(v.titulo)} — V${Number(v.version_number)}</h3>
        <p class='muted'>${escapeHtml(v.cliente_nome || '')}${v.projeto_nome ? ' · ' + escapeHtml(v.projeto_nome) : ''}</p>
        ${v.protocol ? `<p>Protocolo: <strong>${escapeHtml(v.protocol)}</strong></p>` : ''}
        <a class='button' href='/meus-documentos/documento/${encodeURIComponent(v.guest_id)}'>${v.protocol || v.approved ? 'Consultar documento' : 'Analisar documento'}</a></article>`;
    }
    function section(title, items, empty) { return `<section><h2>${title} (${items.length})</h2>${items.length ? `<div class='grid'>${items.map(card).join('')}</div>` : `<p class='muted'>${empty}</p>`}</section>`; }
    html(res, 200, 'Meus documentos', `<h1>Meus documentos</h1><p class='muted'>Acesso confirmado para ${escapeHtml(email)}.</p>
      <form class='inline' method='post' action='/meus-documentos/sair'><button type='submit'>Sair</button></form>
      ${section('Aguardando minha análise ou conclusão', waiting, 'Nenhum documento aguardando.')}
      ${section('Documentos concluídos', finished, 'Nenhum documento concluído.')}
      ${priorApproved.length ? section('Versões anteriores que validei', priorApproved, '') : ''}`);
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/meus-documentos/iniciar') {
    if (!isSmtpConfigured()) {
      html(res, 503, 'Acesso temporariamente indisponível', `<h1>Acesso por e-mail temporariamente indisponível</h1>
        <p>A CKM está concluindo a configuração dos avisos por e-mail. Você ainda pode consultar o documento pelo seu link individual enquanto ele estiver válido.</p>`);
      return true;
    }
    const data = await form(req);
    let email = normalizeEmail(data.get('email'));
    const invite = String(data.get('convite') || '');
    if (invite && /^[a-f0-9]{64}$/i.test(invite)) {
      const invited = await pg.query(`SELECT email FROM portal_entrega_convidados
        WHERE token_hash=$1 AND revoked_at IS NULL AND expires_at>NOW() LIMIT 1`, [hashToken(invite)]);
      email = normalizeEmail(invited.rows[0]?.email);
    }
    let id = crypto.randomUUID();
    const ip = String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim().slice(0, 64);
    if (email && /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
      const recentIp = await pg.query(`SELECT count(*)::int AS total FROM portal_email_challenges WHERE ip_address=$1 AND requested_at>NOW()-INTERVAL '1 hour'`, [ip]);
      const recentEmail = await pg.query(`SELECT id FROM portal_email_challenges WHERE email=$1 AND requested_at>NOW()-INTERVAL '60 seconds' ORDER BY requested_at DESC LIMIT 1`, [email]);
      if (recentEmail.rows.length) id = recentEmail.rows[0].id;
      if (Number(recentIp.rows[0].total) < 10 && !recentEmail.rows.length) {
        const exists = await pg.query(`SELECT 1 FROM portal_entrega_convidados WHERE lower(trim(email))=$1 AND revoked_at IS NULL LIMIT 1`, [email]);
        if (exists.rows.length) {
          const code = String(crypto.randomInt(0, 1000000)).padStart(6, '0');
          const salt = crypto.randomBytes(16).toString('hex');
          const codeHash = crypto.scryptSync(code, salt, 32).toString('hex');
          await pg.query(`INSERT INTO portal_email_challenges (id,email,code_salt,code_hash,expires_at,ip_address)
            VALUES ($1,$2,$3,$4,NOW()+INTERVAL '10 minutes',$5)`, [id,email,salt,codeHash,ip]);
          await sendPortalNotificationEmail({ to:email, subject:'Código de acesso — Meus documentos CKM',
            title:'Acesso aos seus documentos', lines:[`Seu código de acesso é: ${code}`, 'Ele vence em 10 minutos. Se você não solicitou o acesso, ignore esta mensagem.'] });
        }
      }
    }
    redirect(res, `/meus-documentos/verificar?solicitacao=${id}`);
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/meus-documentos/verificar') {
    const id = String(url.searchParams.get('solicitacao') || '');
    html(res, 200, 'Confirmar e-mail', `<h1>Confirme seu e-mail</h1><p>Se houver documentos destinados a este e-mail, você receberá um código de seis dígitos. Ele vence em 10 minutos.</p>
      <div class='card'><form method='post' action='/meus-documentos/verificar'><input type='hidden' name='solicitacao' value='${escapeHtml(id)}'>
      <label>Código recebido<input name='codigo' inputmode='numeric' pattern='[0-9]{6}' maxlength='6' autocomplete='one-time-code' required></label>
      <button type='submit'>Acessar meus documentos</button></form></div><a href='/meus-documentos'>Solicitar outro código</a>`);
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/meus-documentos/verificar') {
    const data = await form(req);
    const id = String(data.get('solicitacao') || '');
    const code = String(data.get('codigo') || '');
    let verified = null;
    if (/^[0-9a-f-]{36}$/i.test(id) && /^[0-9]{6}$/.test(code)) {
      const row = (await pg.query(`UPDATE portal_email_challenges SET attempts=attempts+1
        WHERE id=$1 AND expires_at>NOW() AND attempts<5 RETURNING *`, [id])).rows[0];
      if (row) {
        const hash = crypto.scryptSync(code, row.code_salt, 32);
        const expected = Buffer.from(row.code_hash, 'hex');
        if (expected.length === hash.length && crypto.timingSafeEqual(hash, expected)) {
          verified = (await pg.query('DELETE FROM portal_email_challenges WHERE id=$1 RETURNING email', [id])).rows[0]?.email || null;
        }
      }
    }
    if (!verified) {
      html(res, 401, 'Código inválido', `<h1>Não foi possível confirmar o código</h1><p>Verifique o código recebido ou solicite outro acesso. O código vence após 10 minutos ou cinco tentativas.</p><a class='button' href='/meus-documentos'>Solicitar outro código</a>`);
      return true;
    }
    const session = crypto.randomBytes(32).toString('hex');
    await pg.query(`INSERT INTO portal_email_sessions (token_hash,email,expires_at)
      VALUES ($1,$2,NOW()+INTERVAL '12 hours')`, [hashToken(session), verified]);
    redirect(res, '/meus-documentos', { 'Set-Cookie': cookie(req, session) });
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/meus-documentos/sair') {
    const token = cookies(req).portal_session;
    if (token && /^[a-f0-9]{64}$/i.test(token)) await pg.query('DELETE FROM portal_email_sessions WHERE token_hash=$1', [hashToken(token)]);
    redirect(res, '/meus-documentos', { 'Set-Cookie': cookie(req, '', 0) });
    return true;
  }

  html(res, 404, 'Página não encontrada', '<h1>Página não encontrada</h1><a href="/meus-documentos">Meus documentos</a>');
  return true;
}

module.exports = { handleMyDocuments, getSessionEmail };
