const crypto = require('crypto');

const PASSWORD_PREFIX = 'scrypt$';
const HASH_KEY_LENGTH = 64;
const SALT_LENGTH = 16;
const MIN_PASSWORD_LENGTH = 8;
const MAX_FAILED_LOGIN_ATTEMPTS = 5;
const LOGIN_BLOCK_DURATION_MINUTES = 15;

function normalizePassword(password) {
  return String(password ?? '').normalize('NFKC').trim();
}

function validatePasswordStrength(password) {
  const normalized = normalizePassword(password);

  if (!normalized) {
    return { isValid: false, message: 'Informe a senha.' };
  }

  if (normalized.length < MIN_PASSWORD_LENGTH) {
    return {
      isValid: false,
      message: `A senha deve ter pelo menos ${MIN_PASSWORD_LENGTH} caracteres.`,
    };
  }

  if (!/[0-9]/.test(normalized)) {
    return {
      isValid: false,
      message: 'A senha deve conter ao menos 1 número.',
    };
  }

  if (!/[!@#$%]/.test(normalized)) {
    return {
      isValid: false,
      message: 'A senha deve conter ao menos 1 caractere especial (!@#$%).',
    };
  }

  return { isValid: true };
}

function isHashedPassword(stored) {
  return typeof stored === 'string' && stored.startsWith(PASSWORD_PREFIX);
}

function hashPassword(password) {
  const normalized = normalizePassword(password);
  const salt = crypto.randomBytes(SALT_LENGTH).toString('hex');
  const key = crypto.scryptSync(normalized, salt, HASH_KEY_LENGTH).toString('hex');
  return `${PASSWORD_PREFIX}${salt}$${key}`;
}

function verifyPassword(inputPassword, storedPassword) {
  if (!storedPassword) return false;

  // Compatibilidade temporária com usuários legados ainda em texto puro.
  if (!isHashedPassword(storedPassword)) {
    return normalizePassword(inputPassword) === normalizePassword(storedPassword);
  }

  const parts = String(storedPassword).split('$');
  if (parts.length !== 3) return false;

  const [, salt, expectedHex] = parts;
  const actual = crypto.scryptSync(normalizePassword(inputPassword), salt, HASH_KEY_LENGTH);
  const expected = Buffer.from(expectedHex, 'hex');

  if (expected.length !== actual.length) return false;
  return crypto.timingSafeEqual(expected, actual);
}

function loginBlockUntilFromNow() {
  return new Date(Date.now() + LOGIN_BLOCK_DURATION_MINUTES * 60 * 1000).toISOString();
}

module.exports = {
  PASSWORD_PREFIX,
  MIN_PASSWORD_LENGTH,
  MAX_FAILED_LOGIN_ATTEMPTS,
  LOGIN_BLOCK_DURATION_MINUTES,
  normalizePassword,
  validatePasswordStrength,
  isHashedPassword,
  hashPassword,
  verifyPassword,
  loginBlockUntilFromNow,
};
