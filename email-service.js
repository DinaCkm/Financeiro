const nodemailer = require('nodemailer');

function isSmtpConfigured() {
  return Boolean(process.env.SMTP_USER && process.env.SMTP_PASS);
}

function getTransporter() {
  const host = process.env.SMTP_HOST || 'smtp.gmail.com';
  const port = Number(process.env.SMTP_PORT || 587);
  const user = process.env.SMTP_USER;
  const pass = process.env.SMTP_PASS;

  if (!user || !pass) return null;

  return nodemailer.createTransport({
    host,
    port,
    secure: port === 465,
    auth: { user, pass },
  });
}

async function sendPasswordResetEmail({ to, resetLink }) {
  const transporter = getTransporter();
  if (!transporter) {
    console.warn('[email] SMTP não configurado. Redefinição de senha não enviada.');
    return false;
  }

  const fromEmail = process.env.SMTP_FROM || process.env.SMTP_USER;
  const fromName = process.env.SMTP_FROM_NAME || 'CKM Talents';

  const subject = 'Redefinição de senha — Sistema Financeiro CKM';
  const text = [
    'Recebemos uma solicitação para redefinir sua senha.',
    '',
    'Use o link abaixo para cadastrar uma nova senha:',
    resetLink,
    '',
    'Este link expira em 1 hora.',
    'Se você não solicitou esta alteração, ignore esta mensagem.',
  ].join('\n');

  const html = `
    <div style="font-family:Arial,sans-serif;max-width:620px;margin:0 auto;color:#1f2937">
      <h2 style="margin-bottom:12px">Redefinição de senha</h2>
      <p>Recebemos uma solicitação para redefinir sua senha no Sistema Financeiro CKM.</p>
      <p style="margin:28px 0">
        <a href="${resetLink}" style="background:#111827;color:#fff;text-decoration:none;padding:12px 18px;border-radius:8px;display:inline-block">
          Criar nova senha
        </a>
      </p>
      <p>Este link expira em <strong>1 hora</strong>.</p>
      <p style="color:#6b7280;font-size:13px">Se você não solicitou esta alteração, ignore esta mensagem.</p>
    </div>
  `;

  try {
    await transporter.sendMail({
      from: `"${fromName}" <${fromEmail}>`,
      to,
      subject,
      text,
      html,
    });
    return true;
  } catch (error) {
    console.warn('[email] Falha ao enviar redefinição de senha:', error && error.message ? error.message : error);
    return false;
  }
}


async function sendConsultantInviteEmail({ to, name, activationLink }) {
  const transporter = getTransporter();
  if (!transporter) {
    console.warn('[email] SMTP não configurado. Convite de consultor não enviado.');
    return false;
  }

  const fromEmail = process.env.SMTP_FROM || process.env.SMTP_USER;
  const fromName = process.env.SMTP_FROM_NAME || 'CKM Talents';
  const safeName = String(name || '').trim();
  const greeting = safeName ? `Olá, ${safeName}.` : 'Olá.';

  const subject = 'Acesso ao Portal de Entregas e Validações — CKM Talents';
  const text = [
    greeting,
    '',
    'Você recebeu acesso ao Portal de Entregas e Validações da CKM Talents.',
    'Use o link abaixo para cadastrar sua senha de acesso:',
    activationLink,
    '',
    'Este link expira em 1 hora.',
    'Seu acesso é restrito ao módulo de Entregas e Validações.',
  ].join('\n');

  const html = `
    <div style="font-family:Arial,sans-serif;max-width:620px;margin:0 auto;color:#1f2937">
      <h2 style="margin-bottom:12px">Portal de Entregas e Validações</h2>
      <p>${greeting}</p>
      <p>Você recebeu acesso ao Portal de Entregas e Validações da CKM Talents.</p>
      <p>Seu perfil é restrito a esse módulo e não permite acesso às informações financeiras da CKM.</p>
      <p style="margin:28px 0">
        <a href="${activationLink}" style="background:#111827;color:#fff;text-decoration:none;padding:12px 18px;border-radius:8px;display:inline-block">
          Cadastrar minha senha
        </a>
      </p>
      <p>Este link expira em <strong>1 hora</strong>.</p>
      <p style="color:#6b7280;font-size:13px">Se você não reconhece este convite, ignore esta mensagem.</p>
    </div>
  `;

  try {
    await transporter.sendMail({
      from: `"${fromName}" <${fromEmail}>`,
      to,
      subject,
      text,
      html,
    });
    return true;
  } catch (error) {
    console.warn('[email] Falha ao enviar convite de consultor:', error && error.message ? error.message : error);
    return false;
  }
}


async function sendDeliveryInviteEmail({ to, name, documentTitle, accessLink, senderName }) {
  const transporter = getTransporter();
  if (!transporter) {
    console.warn('[email] SMTP não configurado. Convite de entrega não enviado.');
    return false;
  }

  const fromEmail = process.env.SMTP_FROM || process.env.SMTP_USER;
  const fromName = process.env.SMTP_FROM_NAME || 'CKM Talents';
  const greeting = name ? `Olá, ${String(name).trim()}.` : 'Olá.';
  const sender = senderName ? String(senderName).trim() : 'Equipe CKM Talents';

  const subject = `Documento para análise — ${documentTitle}`;
  const text = [
    greeting,
    '',
    `${sender} disponibilizou o documento "${documentTitle}" para sua análise.`,
    'No portal você poderá ler o documento, conversar com a CKM, solicitar ajustes ou validar a entrega.',
    '',
    accessLink,
    '',
    'Este link é individual. Não encaminhe para outras pessoas.',
  ].join('\n');

  const html = `
    <div style="font-family:Arial,sans-serif;max-width:640px;margin:0 auto;color:#1f2937">
      <h2 style="margin-bottom:12px">Documento para análise</h2>
      <p>${greeting}</p>
      <p><strong>${sender}</strong> disponibilizou o documento <strong>${documentTitle}</strong> para sua análise.</p>
      <p>No portal você poderá ler o documento, conversar com a CKM, solicitar ajustes ou validar a entrega.</p>
      <p style="margin:28px 0">
        <a href="${accessLink}" style="background:#111827;color:#fff;text-decoration:none;padding:12px 18px;border-radius:8px;display:inline-block">
          Acessar documento
        </a>
      </p>
      <p style="color:#6b7280;font-size:13px">Este link é individual e dá acesso somente a esta entrega. Não o encaminhe.</p>
    </div>
  `;

  try {
    await transporter.sendMail({
      from: `"${fromName}" <${fromEmail}>`,
      to,
      subject,
      text,
      html,
    });
    return true;
  } catch (error) {
    console.warn('[email] Falha ao enviar convite da entrega:', error && error.message ? error.message : error);
    return false;
  }
}


async function sendPortalNotificationEmail({ to, subject, title, lines = [], actionLabel, actionLink }) {
  const transporter = getTransporter();
  if (!transporter) {
    console.warn('[email] SMTP não configurado. Aviso do portal não enviado.');
    return false;
  }

  const fromEmail = process.env.SMTP_FROM || process.env.SMTP_USER;
  const fromName = process.env.SMTP_FROM_NAME || 'CKM Talents';
  const safeLines = (lines || []).map(v => String(v || '')).filter(Boolean);
  const text = [
    String(title || subject || 'Aviso do Portal de Entregas'),
    '',
    ...safeLines,
    ...(actionLink ? ['', actionLink] : []),
  ].join('\n');

  const htmlLines = safeLines
    .map(line => `<p style="margin:8px 0;line-height:1.5">${line}</p>`)
    .join('');

  const actionHtml = actionLink
    ? `<p style="margin:24px 0"><a href="${actionLink}" style="background:#111827;color:#fff;text-decoration:none;padding:12px 18px;border-radius:8px;display:inline-block">${actionLabel || 'Acessar'}</a></p>`
    : '';

  try {
    await transporter.sendMail({
      from: `"${fromName}" <${fromEmail}>`,
      to,
      subject,
      text,
      html: `
        <div style="font-family:Arial,sans-serif;max-width:640px;margin:0 auto;color:#1f2937">
          <h2 style="margin-bottom:14px">${title || subject}</h2>
          ${htmlLines}
          ${actionHtml}
          <p style="color:#6b7280;font-size:12px;margin-top:24px">Mensagem automática do Portal de Entregas e Validações da CKM Talents.</p>
        </div>
      `,
    });
    return true;
  } catch (error) {
    console.warn('[email] Falha ao enviar aviso do portal:', error && error.message ? error.message : error);
    return false;
  }
}


async function sendSmtpTestEmail({ to }) {
  return sendPortalNotificationEmail({
    to,
    subject: 'Teste de e-mail — Sistema Financeiro CKM',
    title: 'Configuração de e-mail funcionando',
    lines: [
      'Este é um teste automático do Sistema Financeiro CKM.',
      'Se você recebeu esta mensagem, a configuração SMTP está ativa e pronta para os avisos do Portal de Entregas e Validações.'
    ]
  });
}

module.exports = {
  isSmtpConfigured,
  sendPasswordResetEmail,
  sendConsultantInviteEmail,
  sendDeliveryInviteEmail,
  sendPortalNotificationEmail,
  sendSmtpTestEmail,
};
