const nodemailer = require('nodemailer');

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

module.exports = {
  sendPasswordResetEmail,
  sendConsultantInviteEmail,
};
