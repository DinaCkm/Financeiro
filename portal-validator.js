const crypto = require('crypto');
const { getPrivateObjectBuffer } = require('./portal-storage');
const { sendPortalNotificationEmail } = require('./email-service');

const SESSION_TTL_SECONDS = 60 * 60 * 12;
const CODE_TTL_MINUTES = 10;

function normalizeCpf(value) {
  return String(value || '').replace(/\D/g, '');
}

function isValidCpf(value) {
  const cpf = normalizeCpf(value);
  if (!/^\d{11}$/.test(cpf) || /^(\d)\1{10}$/.test(cpf)) return false;
  const calc = (base, factor) => {
    let total = 0;
    for (const digit of base) total += Number(digit) * factor--;
    const mod = (total * 10) % 11;
    return mod === 10 ? 0 : mod;
  };
  return calc(cpf.slice(0, 9), 10) === Number(cpf[9]) &&
    calc(cpf.slice(0, 10), 11) === Number(cpf[10]);
}

function maskCpf(value) {
  const cpf = normalizeCpf(value);
  if (cpf.length !== 11) return '***.***.***-**';
  return `***.${cpf.slice(3, 6)}.${cpf.slice(6, 9)}-**`;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;')
    .replace(/'/g, '&#39;');
}

function parseCookies(req) {
  const pairs = String(req.headers.cookie || '').split(';').map(v => v.trim()).filter(Boolean);
  return Object.fromEntries(pairs.map(pair => {
    const [key, ...rest] = pair.split('=');
    return [key, decodeURIComponent(rest.join('='))];
  }));
}

function isSecureRequest(req) {
  const forwarded = String(req.headers['x-forwarded-proto'] || '').toLowerCase();
  return forwarded.includes('https') || Boolean(req.socket && req.socket.encrypted);
}

function sessionCookie(req, sid, maxAge = SESSION_TTL_SECONDS) {
  const parts = [
    `validator_sid=${encodeURIComponent(sid || '')}`,
    'Path=/',
    'HttpOnly',
    'SameSite=Lax',
    `Max-Age=${maxAge}`
  ];
  if (process.env.NODE_ENV === 'production' || isSecureRequest(req)) parts.push('Secure');
  return parts.join('; ');
}

function portalBaseUrl(req) {
  const configured = String(process.env.APP_BASE_URL || '').trim().replace(/\/+$/, '');
  if (configured) return configured;
  const proto = String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim() || (isSecureRequest(req) ? 'https' : 'http');
  return `${proto}://${req.headers.host}`;
}

function readBody(req, maxBytes = 2 * 1024 * 1024) {
  return new Promise((resolve, reject) => {
    let data = '';
    let done = false;
    const finish = value => { if (!done) { done = true; resolve(value); } };
    req.on('data', chunk => {
      if (done) return;
      data += chunk;
      if (data.length > maxBytes) {
        done = true;
        reject(new Error('Body too large'));
      }
    });
    req.on('end', () => finish(data));
    req.on('aborted', () => finish(''));
    req.on('error', err => reject(err));
  });
}

function html(res, body, status = 200) {
  res.writeHead(status, {
    'Content-Type': 'text/html; charset=utf-8',
    'Cache-Control': 'private, no-store',
    'Referrer-Policy': 'no-referrer'
  });
  res.end(body);
}

function redirect(res, location, headers = {}) {
  res.writeHead(302, { Location: location, ...headers });
  res.end();
}

async function ensureSchema(pg) {
  await pg.query(`
    ALTER TABLE portal_contatos_validacao ADD COLUMN IF NOT EXISTS cpf TEXT;
    ALTER TABLE portal_contatos_validacao ADD COLUMN IF NOT EXISTS cadastro_em TIMESTAMPTZ;
    ALTER TABLE portal_contatos_validacao ADD COLUMN IF NOT EXISTS aceite_lgpd_em TIMESTAMPTZ;

    CREATE TABLE IF NOT EXISTS portal_validator_access_codes (
      id TEXT PRIMARY KEY,
      email TEXT NOT NULL,
      code_hash TEXT NOT NULL,
      expires_at TIMESTAMPTZ NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      consumed_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS idx_portal_validator_access_codes_email
      ON portal_validator_access_codes(lower(email), created_at DESC);

    CREATE TABLE IF NOT EXISTS portal_validator_sessions (
      id TEXT PRIMARY KEY,
      email TEXT NOT NULL,
      read_only BOOLEAN NOT NULL DEFAULT FALSE,
      support_user_id TEXT,
      expires_at TIMESTAMPTZ NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
      last_access_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );

    CREATE INDEX IF NOT EXISTS idx_portal_validator_sessions_email
      ON portal_validator_sessions(lower(email), expires_at);
  `);
}

function page(title, body, session = null) {
  const support = session && session.read_only;
  return `<!doctype html><html lang='pt-BR'><head>
    <meta charset='utf-8'><meta name='viewport' content='width=device-width,initial-scale=1'>
    <title>${escapeHtml(title)} — CKM Talents</title>
    <link rel='stylesheet' href='/public/style.css'>
    <style>
      *{box-sizing:border-box}body{margin:0;background:#f8fafc;color:#1e293b;font-family:Inter,Arial,sans-serif}
      .vp-head{background:#24116f;color:#fff;padding:15px 22px}.vp-head a{color:#fff}
      .vp-wrap{max-width:1280px;margin:0 auto;padding:22px}
      .vp-card{background:#fff;border:1px solid #e2e8f0;border-radius:12px;padding:16px}
      .vp-btn{display:inline-flex;align-items:center;justify-content:center;border:0;border-radius:9px;padding:10px 14px;background:#24116f;color:#fff;text-decoration:none;font-weight:700;cursor:pointer}
      .vp-btn.secondary{background:#fff;color:#24116f;border:1px solid #24116f}
      .vp-muted{color:#64748b}.vp-grid{display:grid;gap:12px}.vp-doc{display:block;text-decoration:none;color:inherit;padding:12px 0;border-top:1px solid #e2e8f0}
      input,textarea{width:100%;padding:10px;border:1px solid #cbd5e1;border-radius:8px;font:inherit}
      label{display:block;font-weight:700;font-size:13px;margin:10px 0 5px}
      .vp-alert{padding:10px 12px;border-radius:9px;margin:10px 0}.vp-support{background:#fff7ed;border:1px solid #fdba74;color:#9a3412}
      .vp-ok{background:#ecfdf5;border:1px solid #a7f3d0;color:#065f46}.vp-err{background:#fef2f2;border:1px solid #fecaca;color:#991b1b}
      @media(max-width:760px){.vp-wrap{padding:14px}}
    </style>
  </head><body>
    <header class='vp-head'><strong>CKM Talents — Portal de Validação</strong>
      ${session ? `<span style='float:right'><a href='/validacao/sair'>Sair</a></span>` : ''}
    </header>
    <main class='vp-wrap'>
      ${support ? "<div class='vp-alert vp-support'><strong>Modo de suporte — somente leitura.</strong> Você está visualizando exatamente os documentos disponíveis para este validador. Comentários, solicitações e validações estão bloqueados.</div>" : ''}
      ${body}
    </main>
  </body></html>`;
}

async function getSession(pg, req) {
  const sid = parseCookies(req).validator_sid;
  if (!sid) return null;
  const row = (await pg.query(
    `SELECT * FROM portal_validator_sessions
      WHERE id=$1 AND expires_at>NOW()
      LIMIT 1`,
    [sid]
  )).rows[0];
  if (!row) return null;
  await pg.query(
    `UPDATE portal_validator_sessions
        SET last_access_at=NOW(), expires_at=NOW()+INTERVAL '12 hours'
      WHERE id=$1`,
    [sid]
  );
  return row;
}

async function createSession(pg, email, readOnly = false, supportUserId = null) {
  const sid = crypto.randomUUID();
  await pg.query(
    `INSERT INTO portal_validator_sessions
      (id,email,read_only,support_user_id,expires_at)
     VALUES ($1,lower($2),$3,$4,NOW()+INTERVAL '12 hours')`,
    [sid, email, !!readOnly, supportUserId || null]
  );
  return sid;
}

async function getContactByEmail(pg, email) {
  return (await pg.query(
    `SELECT pc.*, c.nome AS cliente_nome, c.nome_curto AS cliente_nome_curto
       FROM portal_contatos_validacao pc
       LEFT JOIN clientes c ON c.id=pc.cliente_id
      WHERE lower(pc.email)=lower($1) AND pc.ativo=true
      ORDER BY pc.updated_at DESC
      LIMIT 1`,
    [email]
  )).rows[0] || null;
}

async function getDocuments(pg, email) {
  return (await pg.query(
    `SELECT DISTINCT e.id,e.titulo,e.descricao,e.status,e.current_version,e.responsavel_user_id,
            v.id AS version_id,v.version_number,v.status AS version_status,
            g.id AS convidado_id,g.can_comment,g.can_request_changes,g.can_validate,
            c.nome AS cliente_nome,c.nome_curto AS cliente_nome_curto,p.nome AS projeto_nome,
            EXISTS(
              SELECT 1 FROM portal_entrega_decisions d
              JOIN portal_entrega_convidados gd ON gd.id=d.convidado_id
              WHERE d.version_id=v.id AND d.decision='validated' AND lower(gd.email)=lower($1)
            ) AS validado_por_mim
       FROM portal_entregas e
       JOIN portal_entrega_versions v ON v.entrega_id=e.id AND v.version_number=e.current_version
       JOIN LATERAL (
         SELECT gx.* FROM portal_entrega_convidados gx
          WHERE gx.entrega_id=e.id AND gx.version_id=v.id AND lower(gx.email)=lower($1)
            AND gx.revoked_at IS NULL
          ORDER BY gx.invited_at DESC LIMIT 1
       ) g ON TRUE
       LEFT JOIN clientes c ON c.id=e.cliente_id
       LEFT JOIN projetos p ON p.id=e.projeto_id
      WHERE e.status<>'cancelado'
      ORDER BY c.nome,p.nome,e.titulo`,
    [email]
  )).rows;
}

async function recordAudit(pg, req, data) {
  try {
    await pg.query(
      `INSERT INTO portal_audit_events
        (id,entrega_id,version_id,actor_type,actor_id,action,ip_address,user_agent,details)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb)`,
      [
        crypto.randomUUID(), data.entregaId || null, data.versionId || null,
        data.actorType || 'cliente', String(data.actorId || ''), data.action,
        String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim().slice(0,64),
        String(req.headers['user-agent'] || '').slice(0,500),
        JSON.stringify(data.details || {})
      ]
    );
  } catch (e) {
    console.warn('[portal-validator] auditoria:', e.message);
  }
}

async function notifyResponsible(pg, req, doc, subject, title, lines) {
  try {
    if (!doc || !doc.responsavel_user_id) return;
    const responsible = (await pg.query(
      'SELECT email,name FROM users WHERE id=$1 AND status=\'ativo\' LIMIT 1',
      [doc.responsavel_user_id]
    )).rows[0];
    if (!responsible || !responsible.email) return;
    await sendPortalNotificationEmail({
      to: responsible.email,
      subject,
      title,
      lines,
      actionLabel: 'Abrir entrega',
      actionLink: `${portalBaseUrl(req)}/entregas/${encodeURIComponent(doc.id)}`
    });
  } catch (e) {
    console.warn('[portal-validator] aviso ao responsável:', e.message);
  }
}

function createSignature({ entregaId, versionId, convidadoId, fileHash, name, email, cpf }) {
  const securityCode = `CKM-VAL-${new Date().getUTCFullYear()}-${crypto.randomBytes(5).toString('hex').toUpperCase()}`;
  const signedAt = new Date().toISOString();
  const nonce = crypto.randomBytes(16).toString('hex');
  const signatureHash = crypto.createHash('sha256')
    .update([entregaId,versionId,convidadoId,fileHash,name,email,cpf,signedAt,nonce].join('|'))
    .digest('hex');
  return { securityCode, signedAt, signatureHash };
}

async function handlePublic(req, res, ctx) {
  const pg = ctx.pg;
  const url = new URL(req.url, `http://${req.headers.host}`);
  if (!url.pathname.startsWith('/validacao')) return false;

  await ensureSchema(pg);

  if (req.method === 'GET' && url.pathname === '/validacao') {
    const session = await getSession(pg, req);
    if (session) {
      redirect(res, '/validacao/documentos');
      return true;
    }
    const erro = String(url.searchParams.get('erro') || '');
    const msg = erro === 'nao-cadastrado'
      ? 'Este e-mail ainda não foi autorizado como validador.'
      : erro === 'envio'
        ? 'Não foi possível enviar o código. Tente novamente.'
        : '';
    html(res, page('Acesso', `
      <div class='vp-card' style='max-width:520px;margin:24px auto'>
        <h1 style='margin-top:0'>Portal de Validação</h1>
        <p class='vp-muted'>Use sempre este endereço para acessar os documentos que foram disponibilizados para você.</p>
        ${msg ? `<div class='vp-alert vp-err'>${escapeHtml(msg)}</div>` : ''}
        <form method='post' action='/validacao/codigo'>
          <label>E-mail</label>
          <input name='email' type='email' autocomplete='email' required>
          <button class='vp-btn' type='submit' style='width:100%;margin-top:14px'>Receber código de acesso</button>
        </form>
      </div>`));
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/validacao/codigo') {
    const form = new URLSearchParams(await readBody(req));
    const email = String(form.get('email') || '').trim().toLowerCase();
    const contact = await getContactByEmail(pg, email);
    if (!contact) {
      redirect(res, '/validacao?erro=nao-cadastrado');
      return true;
    }

    const code = String(crypto.randomInt(100000, 1000000));
    const codeHash = crypto.createHash('sha256').update(code).digest('hex');
    await pg.query(
      `UPDATE portal_validator_access_codes
          SET consumed_at=COALESCE(consumed_at,NOW())
        WHERE lower(email)=lower($1) AND consumed_at IS NULL`,
      [email]
    );
    await pg.query(
      `INSERT INTO portal_validator_access_codes
        (id,email,code_hash,expires_at)
       VALUES ($1,lower($2),$3,NOW()+INTERVAL '10 minutes')`,
      [crypto.randomUUID(), email, codeHash]
    );

    try {
      await sendPortalNotificationEmail({
        to: email,
        subject: 'Código de acesso — Portal de Validação CKM Talents',
        title: 'Seu código de acesso',
        lines: [
          `Olá, ${contact.nome}.`,
          `Seu código de acesso é: ${code}`,
          `O código é válido por ${CODE_TTL_MINUTES} minutos.`
        ],
        actionLabel: 'Abrir Portal de Validação',
        actionLink: `${portalBaseUrl(req)}/validacao/entrar?email=${encodeURIComponent(email)}`
      });
    } catch (e) {
      console.warn('[portal-validator] envio do código:', e.message);
      redirect(res, '/validacao?erro=envio');
      return true;
    }

    redirect(res, `/validacao/entrar?email=${encodeURIComponent(email)}&enviado=1`);
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/validacao/entrar') {
    const email = String(url.searchParams.get('email') || '').trim().toLowerCase();
    const erro = url.searchParams.get('erro');
    html(res, page('Confirmar acesso', `
      <div class='vp-card' style='max-width:520px;margin:24px auto'>
        <h1 style='margin-top:0'>Confirmar acesso</h1>
        <p class='vp-muted'>Código enviado para <strong>${escapeHtml(email)}</strong>.</p>
        ${erro ? "<div class='vp-alert vp-err'>Código inválido ou expirado.</div>" : ''}
        <form method='post' action='/validacao/entrar'>
          <input type='hidden' name='email' value='${escapeHtml(email)}'>
          <label>Código de 6 dígitos</label>
          <input name='code' inputmode='numeric' maxlength='6' autocomplete='one-time-code' required>
          <button class='vp-btn' type='submit' style='width:100%;margin-top:14px'>Entrar</button>
        </form>
      </div>`));
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/validacao/entrar') {
    const form = new URLSearchParams(await readBody(req));
    const email = String(form.get('email') || '').trim().toLowerCase();
    const code = String(form.get('code') || '').trim();
    const codeHash = crypto.createHash('sha256').update(code).digest('hex');
    const access = (await pg.query(
      `SELECT * FROM portal_validator_access_codes
        WHERE lower(email)=lower($1) AND consumed_at IS NULL AND expires_at>NOW()
        ORDER BY created_at DESC LIMIT 1`,
      [email]
    )).rows[0];

    if (!access || access.attempts >= 5 || access.code_hash !== codeHash) {
      if (access) await pg.query('UPDATE portal_validator_access_codes SET attempts=attempts+1 WHERE id=$1', [access.id]);
      redirect(res, `/validacao/entrar?email=${encodeURIComponent(email)}&erro=codigo`);
      return true;
    }

    await pg.query('UPDATE portal_validator_access_codes SET consumed_at=NOW() WHERE id=$1', [access.id]);
    const sid = await createSession(pg, email, false, null);
    const contact = await getContactByEmail(pg, email);
    const target = contact && contact.cpf ? '/validacao/documentos' : '/validacao/ativar';
    redirect(res, target, { 'Set-Cookie': sessionCookie(req, sid) });
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/validacao/ativar') {
    const session = await getSession(pg, req);
    if (!session) { redirect(res, '/validacao'); return true; }
    if (session.read_only) { redirect(res, '/validacao/documentos'); return true; }
    const contact = await getContactByEmail(pg, session.email);
    if (!contact) { redirect(res, '/validacao?erro=nao-cadastrado'); return true; }
    if (contact.cpf) { redirect(res, '/validacao/documentos'); return true; }
    const erro = String(url.searchParams.get('erro') || '');
    html(res, page('Ativar acesso', `
      <div class='vp-card' style='max-width:620px;margin:20px auto'>
        <h1 style='margin-top:0'>Confirme seus dados</h1>
        <p class='vp-muted'>Este preenchimento é necessário apenas no primeiro acesso.</p>
        ${erro ? "<div class='vp-alert vp-err'>Confira o CPF e os campos obrigatórios.</div>" : ''}
        <form method='post' action='/validacao/ativar'>
          <label>Nome completo</label><input name='nome' value='${escapeHtml(contact.nome || '')}' required maxlength='150'>
          <label>E-mail</label><input value='${escapeHtml(session.email)}' disabled>
          <label>CPF</label><input name='cpf' inputmode='numeric' maxlength='14' placeholder='000.000.000-00' required>
          <label>Cargo / Função</label><input name='cargo' value='${escapeHtml(contact.cargo || '')}' required maxlength='120'>
          <label>Telefone</label><input name='telefone' value='${escapeHtml(contact.telefone || '')}' maxlength='40'>
          <label style='display:flex;gap:8px;align-items:flex-start;font-weight:400'><input type='checkbox' name='aceite' style='width:auto;margin-top:3px' required> Confirmo meus dados e concordo com seu uso pela CKM Talents exclusivamente para análise e validação das entregas.</label>
          <button class='vp-btn' type='submit' style='width:100%;margin-top:14px'>Ativar meu acesso</button>
        </form>
      </div>`, session));
    return true;
  }

  if (req.method === 'POST' && url.pathname === '/validacao/ativar') {
    const session = await getSession(pg, req);
    if (!session || session.read_only) { redirect(res, '/validacao'); return true; }
    const form = new URLSearchParams(await readBody(req));
    const nome = String(form.get('nome') || '').trim().replace(/\s+/g, ' ').slice(0,150);
    const cpf = normalizeCpf(form.get('cpf'));
    const cargo = String(form.get('cargo') || '').trim().slice(0,120);
    const telefone = String(form.get('telefone') || '').trim().slice(0,40);
    const aceite = form.get('aceite') === 'on';
    if (!nome || !cargo || !aceite || !isValidCpf(cpf)) {
      redirect(res, '/validacao/ativar?erro=dados');
      return true;
    }

    const existingCpf = (await pg.query(
      `SELECT DISTINCT cpf FROM portal_contatos_validacao
        WHERE lower(email)=lower($1) AND ativo=true AND cpf IS NOT NULL AND cpf<>''`,
      [session.email]
    )).rows.map(r => normalizeCpf(r.cpf)).filter(Boolean);
    if (existingCpf.some(v => v !== cpf)) {
      redirect(res, '/validacao/ativar?erro=dados');
      return true;
    }

    await pg.query(
      `UPDATE portal_contatos_validacao
          SET nome=$2,cpf=$3,cargo=$4,telefone=$5,cadastro_em=COALESCE(cadastro_em,NOW()),
              aceite_lgpd_em=COALESCE(aceite_lgpd_em,NOW()),updated_at=NOW()
        WHERE lower(email)=lower($1) AND ativo=true`,
      [session.email, nome, cpf, cargo, telefone || null]
    );
    await recordAudit(pg, req, {
      actorType:'cliente', actorId:session.email, action:'portal_validador_ativado',
      details:{ email:session.email, cpfMascarado:maskCpf(cpf) }
    });
    redirect(res, '/validacao/documentos');
    return true;
  }

  if (req.method === 'GET' && url.pathname === '/validacao/documentos') {
    const session = await getSession(pg, req);
    if (!session) { redirect(res, '/validacao'); return true; }
    const contact = await getContactByEmail(pg, session.email);
    if (!session.read_only && (!contact || !contact.cpf)) { redirect(res, '/validacao/ativar'); return true; }
    const docs = await getDocuments(pg, session.email);
    const groups = new Map();
    for (const d of docs) {
      const key = `${d.cliente_nome_curto || d.cliente_nome || 'Cliente'}||${d.projeto_nome || 'Sem projeto'}`;
      if (!groups.has(key)) groups.set(key, []);
      groups.get(key).push(d);
    }
    const sections = [...groups.entries()].map(([key, items]) => {
      const [client, project] = key.split('||');
      return `<section class='vp-card' style='margin-bottom:14px'>
        <div class='vp-muted' style='font-size:12px;text-transform:uppercase;font-weight:700'>${escapeHtml(client)}</div>
        <h2 style='margin:.2rem 0 .7rem'>${escapeHtml(project)}</h2>
        ${items.map(d => {
          const label = d.validado_por_mim ? 'Validado' : (d.status === 'ajustes_solicitados' || d.version_status === 'ajustes_solicitados') ? 'Alteração solicitada' : 'Pendente';
          return `<a class='vp-doc' href='/validacao/documentos/${encodeURIComponent(d.id)}'><strong>${escapeHtml(d.titulo)}</strong><span class='vp-muted' style='float:right'>${escapeHtml(label)}</span><div class='vp-muted' style='font-size:12px'>V${Number(d.version_number || 1)}</div></a>`;
        }).join('')}
      </section>`;
    }).join('');

    html(res, page('Meus documentos', `
      <div style='display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap'>
        <div><h1 style='margin:0'>Meus documentos</h1><p class='vp-muted'>${escapeHtml(session.email)}</p></div>
      </div>
      ${sections || "<div class='vp-card'>Nenhum documento disponível para este acesso.</div>"}
    `, session));
    return true;
  }

  const docMatch = url.pathname.match(/^\/validacao\/documentos\/([0-9a-f-]{36})(?:\/(pdf|manifestacao|decisao))?$/i);
  if (docMatch) {
    const session = await getSession(pg, req);
    if (!session) { redirect(res, '/validacao'); return true; }
    const deliveryId = docMatch[1];
    const action = docMatch[2] || '';
    const docs = await getDocuments(pg, session.email);
    const doc = docs.find(d => String(d.id) === deliveryId);
    if (!doc) { html(res, page('Documento não disponível', "<div class='vp-card'>Este documento não está disponível para este acesso.</div>", session), 404); return true; }

    if (action === 'pdf' && req.method === 'GET') {
      const version = (await pg.query('SELECT * FROM portal_entrega_versions WHERE id=$1 LIMIT 1',[doc.version_id])).rows[0];
      if (!version) { res.writeHead(404); res.end(); return true; }
      let pdf;
      try {
        pdf = version.storage_key ? await getPrivateObjectBuffer(version.storage_key) : version.file_data;
      } catch (e) {
        res.writeHead(503); res.end('Documento temporariamente indisponível.'); return true;
      }
      await recordAudit(pg, req, { entregaId:doc.id, versionId:doc.version_id, actorType:session.read_only?'ckm':'cliente', actorId:session.support_user_id || session.email, action:session.read_only?'visualizacao_suporte_documento':'documento_visualizado', details:{via:'portal_permanente'} });
      res.writeHead(200, {'Content-Type':'application/pdf','Content-Length':Number(pdf.length),'Cache-Control':'private, no-store','X-Content-Type-Options':'nosniff'});
      res.end(pdf); return true;
    }

    if ((action === 'manifestacao' || action === 'decisao') && session.read_only) {
      res.writeHead(403, {'Content-Type':'text/plain; charset=utf-8'}); res.end('Modo de suporte é somente leitura.'); return true;
    }

    if (action === 'manifestacao' && req.method === 'POST') {
      const form = new URLSearchParams(await readBody(req));
      const message = String(form.get('message') || '').trim();
      const type = String(form.get('manifestationType') || '').trim();
      if (!message || message.length > 5000) { redirect(res, `/validacao/documentos/${doc.id}?erro=mensagem`); return true; }
      const contact = await getContactByEmail(pg, session.email);
      if (type === 'contribution' && doc.can_comment) {
        await pg.query(
          `INSERT INTO portal_entrega_messages
            (id,entrega_id,version_id,actor_type,convidado_id,actor_name,actor_email,message,created_at)
           VALUES ($1,$2,$3,'cliente',$4,$5,$6,$7,NOW())`,
          [crypto.randomUUID(),doc.id,doc.version_id,doc.convidado_id,contact ? contact.nome : session.email,session.email,`CONTRIBUIÇÃO: ${message}`]
        );
        await notifyResponsible(pg, req, doc,
          `Nova contribuição — ${doc.titulo}`,
          'Nova contribuição do cliente',
          [`${contact ? contact.nome : session.email} enviou uma contribuição sobre "${doc.titulo}".`, message]
        );
      } else if (type === 'changes_requested' && doc.can_request_changes) {
        const client = await pg.connect();
        try {
          await client.query('BEGIN');
          await client.query(
            `INSERT INTO portal_entrega_decisions
              (id,entrega_id,version_id,convidado_id,decision,decision_text,created_at)
             VALUES ($1,$2,$3,$4,'changes_requested',$5,NOW())`,
            [crypto.randomUUID(),doc.id,doc.version_id,doc.convidado_id,message]
          );
          await client.query(
            `INSERT INTO portal_entrega_messages
              (id,entrega_id,version_id,actor_type,convidado_id,actor_name,actor_email,message,created_at)
             VALUES ($1,$2,$3,'cliente',$4,$5,$6,$7,NOW())`,
            [crypto.randomUUID(),doc.id,doc.version_id,doc.convidado_id,contact ? contact.nome : session.email,session.email,`ALTERAÇÃO SOLICITADA: ${message}`]
          );
          await client.query("UPDATE portal_entrega_versions SET status='ajustes_solicitados' WHERE id=$1",[doc.version_id]);
          await client.query("UPDATE portal_entregas SET status='ajustes_solicitados' WHERE id=$1",[doc.id]);
          await client.query('COMMIT');
          await notifyResponsible(pg, req, doc,
            `Alteração solicitada — ${doc.titulo}`,
            'Cliente solicitou alteração',
            [`${contact ? contact.nome : session.email} solicitou alteração em "${doc.titulo}".`, message, 'Acesse a entrega para preparar e publicar a nova versão.']
          );
        } catch (e) {
          await client.query('ROLLBACK');
          res.writeHead(500); res.end('Não foi possível registrar a solicitação.'); return true;
        } finally { client.release(); }
      } else {
        res.writeHead(403); res.end('Ação não permitida.'); return true;
      }
      redirect(res, `/validacao/documentos/${doc.id}?manifestacao=ok`);
      return true;
    }

    if (action === 'decisao' && req.method === 'POST') {
      if (!doc.can_validate) { res.writeHead(403); res.end('Validação não permitida.'); return true; }
      if (doc.status === 'ajustes_solicitados' || doc.version_status === 'ajustes_solicitados') { res.writeHead(409); res.end('Esta versão possui alteração solicitada.'); return true; }
      if (doc.validado_por_mim) { redirect(res, `/validacao/documentos/${doc.id}?decisao=ja_validado`); return true; }

      const form = new URLSearchParams(await readBody(req));
      const acknowledgement = form.get('acknowledgement') === 'on';
      const noMoreAdjustments = form.get('noMoreAdjustments') === 'on';
      if (!acknowledgement || !noMoreAdjustments) { redirect(res, `/validacao/documentos/${doc.id}?erro=declaracoes`); return true; }

      const contact = await getContactByEmail(pg, session.email);
      if (!contact || !isValidCpf(contact.cpf)) { redirect(res, '/validacao/ativar'); return true; }
      const version = (await pg.query('SELECT file_hash FROM portal_entrega_versions WHERE id=$1 LIMIT 1',[doc.version_id])).rows[0];
      const existingSig = (await pg.query(
        'SELECT id FROM portal_entrega_signatures WHERE version_id=$1 AND lower(validator_email)=lower($2) LIMIT 1',
        [doc.version_id,session.email]
      )).rows[0];

      if (!existingSig) {
        const sig = createSignature({entregaId:doc.id,versionId:doc.version_id,convidadoId:doc.convidado_id,fileHash:version.file_hash,name:contact.nome,email:session.email,cpf:contact.cpf});
        await pg.query(
          `INSERT INTO portal_entrega_signatures
            (id,entrega_id,version_id,convidado_id,security_code,signature_hash,file_hash,validator_name,validator_email,validator_cpf,declaration_text,ip_address,user_agent,signed_at)
           VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)`,
          [crypto.randomUUID(),doc.id,doc.version_id,doc.convidado_id,sig.securityCode,sig.signatureHash,version.file_hash,contact.nome,session.email,contact.cpf,
           'Declaro que li a versão apresentada, estou ciente da validação e confirmo que não há outros ajustes necessários.',
           String(req.headers['x-forwarded-for'] || req.socket.remoteAddress || '').split(',')[0].trim().slice(0,64),
           String(req.headers['user-agent'] || '').slice(0,500),sig.signedAt]
        );
      }

      await pg.query(
        `INSERT INTO portal_entrega_decisions
          (id,entrega_id,version_id,convidado_id,decision,decision_text,created_at)
         VALUES ($1,$2,$3,$4,'validated',$5,NOW())`,
        [crypto.randomUUID(),doc.id,doc.version_id,doc.convidado_id,'De acordo e validar entrega']
      ).catch(() => {});
      await pg.query(
        `UPDATE portal_entrega_convidados SET status='validado',first_access_at=COALESCE(first_access_at,NOW()),last_access_at=NOW()
          WHERE entrega_id=$1 AND version_id=$2 AND lower(email)=lower($3)`,
        [doc.id,doc.version_id,session.email]
      );

      const totalRequired = Number((await pg.query(
        `SELECT COUNT(DISTINCT lower(email))::int AS c FROM portal_entrega_convidados
          WHERE entrega_id=$1 AND version_id=$2 AND can_validate=true`,
        [doc.id,doc.version_id]
      )).rows[0].c || 0);
      const totalApproved = Number((await pg.query(
        `SELECT COUNT(DISTINCT lower(gd.email))::int AS c
           FROM portal_entrega_decisions d
           JOIN portal_entrega_convidados gd ON gd.id=d.convidado_id
          WHERE d.version_id=$1 AND d.decision='validated'`,
        [doc.version_id]
      )).rows[0].c || 0);
      if (totalRequired > 0 && totalApproved >= totalRequired) {
        const existingValidation = (await pg.query('SELECT id FROM portal_entrega_validations WHERE version_id=$1 LIMIT 1',[doc.version_id])).rows[0];
        if (!existingValidation) {
          const protocol = `CKM-DOC-${new Date().getUTCFullYear()}-${crypto.randomUUID().split('-')[0].toUpperCase()}`;
          await pg.query(
            `INSERT INTO portal_entrega_validations
              (id,entrega_id,version_id,convidado_id,protocol,file_hash,validator_name,validator_email,validator_cpf,declaration_text,validated_at)
             VALUES ($1,$2,$3,$4,$5,$6,$7,$8,NULL,$9,NOW())`,
            [crypto.randomUUID(),doc.id,doc.version_id,doc.convidado_id,protocol,version.file_hash,contact.nome,session.email,'Todos os validadores indicados registraram concordância com esta versão.']
          );
          await pg.query("UPDATE portal_entrega_versions SET status='validated' WHERE id=$1",[doc.version_id]);
          await pg.query("UPDATE portal_entregas SET status='validado' WHERE id=$1",[doc.id]);
        }
      }
      await recordAudit(pg, req, {entregaId:doc.id,versionId:doc.version_id,actorType:'cliente',actorId:doc.convidado_id,action:'concordancia_registrada',details:{email:session.email,via:'portal_permanente'}});
      redirect(res, `/validacao/documentos/${doc.id}?decisao=validado`);
      return true;
    }

    if (req.method === 'GET' && !action) {
      const messages = (await pg.query(
        'SELECT * FROM portal_entrega_messages WHERE version_id=$1 ORDER BY created_at ASC',[doc.version_id]
      )).rows;
      const signature = (await pg.query(
        'SELECT * FROM portal_entrega_signatures WHERE version_id=$1 AND lower(validator_email)=lower($2) ORDER BY signed_at DESC LIMIT 1',
        [doc.version_id,session.email]
      )).rows[0] || null;
      const msgHtml = messages.map(m => {
        const raw = String(m.message || '');
        const clean = raw.replace(/^ALTERAÇÃO SOLICITADA:\s*/i,'').replace(/^AJUSTES SOLICITADOS:\s*/i,'').replace(/^CONTRIBUIÇÃO:\s*/i,'');
        return `<div style='padding:10px;border:1px solid #e2e8f0;border-radius:9px;margin:8px 0'><div class='vp-muted' style='font-size:12px'><strong>${escapeHtml(m.actor_name)}</strong> · ${escapeHtml(new Date(m.created_at).toLocaleString('pt-BR'))}</div><div style='white-space:pre-wrap;margin-top:4px'>${escapeHtml(clean)}</div></div>`;
      }).join('');
      const blockedByAdjustments = doc.status === 'ajustes_solicitados' || doc.version_status === 'ajustes_solicitados';
      const readonly = !!session.read_only;
      const actionArea = readonly ? '' :
        `<div class='vp-card' style='margin-top:14px'><h3 style='margin-top:0'>Conversa</h3>${msgHtml || "<p class='vp-muted'>Nenhuma conversa registrada.</p>"}${(doc.can_comment || doc.can_request_changes) ? `<form method='post' action='/validacao/documentos/${doc.id}/manifestacao'><label>Mensagem</label><textarea name='message' rows='4' maxlength='5000' required></textarea><div style='display:flex;gap:8px;flex-wrap:wrap;margin-top:8px'>${doc.can_request_changes ? "<button class='vp-btn secondary' name='manifestationType' value='changes_requested'>Solicitar alteração</button>" : ''}${doc.can_comment ? "<button class='vp-btn' name='manifestationType' value='contribution'>Contribuição</button>" : ''}</div></form>` : ''}
        ${blockedByAdjustments ? "<div class='vp-alert vp-support'><strong>Aguardando nova versão.</strong> A validação fica bloqueada enquanto esta versão possui alteração solicitada.</div>" :
          doc.validado_por_mim ? `<div class='vp-alert vp-ok'><strong>Sua validação está registrada.</strong>${signature ? `<br>CPF: ${escapeHtml(maskCpf(signature.validator_cpf))}<br>Registro: ${escapeHtml(signature.security_code)}` : ''}</div>` :
          doc.can_validate ? `<form method='post' action='/validacao/documentos/${doc.id}/decisao' style='margin-top:14px;border-top:1px solid #e2e8f0;padding-top:14px'><h3>Validação</h3><label style='display:flex;gap:8px;align-items:flex-start;font-weight:400'><input type='checkbox' name='noMoreAdjustments' required style='width:auto;margin-top:3px'> Confirmo que revisei o documento e não há mais ajustes necessários.</label><label style='display:flex;gap:8px;align-items:flex-start;font-weight:400'><input type='checkbox' name='acknowledgement' required style='width:auto;margin-top:3px'> Estou ciente de que esta confirmação registra minha validação eletrônica desta versão.</label><button class='vp-btn' type='submit' style='margin-top:10px'>Confirmar e registrar validação</button></form>` : ''}
        </div>`;
      html(res, page(doc.titulo, `
        <a href='/validacao/documentos'>← Voltar aos documentos</a>
        <h1>${escapeHtml(doc.titulo)}</h1><p class='vp-muted'>Versão V${Number(doc.version_number || 1)}</p>
        <div class='vp-card'><iframe src='/validacao/documentos/${doc.id}/pdf' style='width:100%;height:70vh;border:0;border-radius:8px;background:#e2e8f0'></iframe></div>
        ${readonly ? `<div class='vp-card' style='margin-top:14px'><h3>Conversa</h3>${msgHtml || "<p class='vp-muted'>Nenhuma conversa registrada.</p>"}</div>` : actionArea}
      `, session));
      return true;
    }
  }

  if (req.method === 'GET' && url.pathname === '/validacao/sair') {
    const sid = parseCookies(req).validator_sid;
    if (sid) await pg.query('DELETE FROM portal_validator_sessions WHERE id=$1',[sid]).catch(()=>{});
    redirect(res, '/validacao', { 'Set-Cookie': sessionCookie(req, '', 0) });
    return true;
  }

  return true;
}

async function startSupportSession(req, res, ctx, contactId, supportUserId) {
  const pg = ctx.pg;
  await ensureSchema(pg);
  const contact = (await pg.query(
    'SELECT id,nome,email,ativo FROM portal_contatos_validacao WHERE id=$1 LIMIT 1',[contactId]
  )).rows[0];
  if (!contact || !contact.ativo) {
    res.writeHead(404, {'Content-Type':'text/plain; charset=utf-8'}); res.end('Contato não encontrado ou inativo.'); return;
  }
  const sid = await createSession(pg, contact.email, true, supportUserId);
  await recordAudit(pg, req, {actorType:'ckm',actorId:supportUserId,action:'suporte_visualizar_como_validador',details:{contactId:contact.id,email:contact.email}});
  redirect(res, '/validacao/documentos', { 'Set-Cookie': sessionCookie(req, sid) });
}

module.exports = {
  ensureSchema,
  handlePublic,
  startSupportSession
};
