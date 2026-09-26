const ROLE_OWNER = 'owner';
const ROLE_ADMIN = 'admin';
const ROLE_CONSULTOR_ENTREGAS = 'consultor_entregas';

function normalizeRole(role) {
  return String(role || '').trim().toLowerCase();
}

function isDeliveryConsultant(user) {
  return normalizeRole(user && user.role) === ROLE_CONSULTOR_ENTREGAS;
}

function isFinancialAdmin(user) {
  const role = normalizeRole(user && user.role);
  return role === ROLE_OWNER || role === ROLE_ADMIN;
}

function isDeliveryPath(pathname) {
  const path = String(pathname || '');
  return path === '/entregas'
    || path.startsWith('/entregas/')
    || path === '/api/entregas'
    || path.startsWith('/api/entregas/');
}

function consultantCanAccessPath(pathname) {
  const path = String(pathname || '');
  return isDeliveryPath(path)
    || path === '/change-password'
    || path === '/logout';
}

function authorizationForRequest(user, pathname) {
  if (!user) return { allowed: false, reason: 'unauthenticated' };
  if (!isDeliveryConsultant(user)) return { allowed: true, reason: 'financial-user' };
  return consultantCanAccessPath(pathname)
    ? { allowed: true, reason: 'consultant-deliveries' }
    : { allowed: false, reason: 'consultant-financial-denied' };
}

module.exports = {
  ROLE_OWNER,
  ROLE_ADMIN,
  ROLE_CONSULTOR_ENTREGAS,
  normalizeRole,
  isDeliveryConsultant,
  isFinancialAdmin,
  isDeliveryPath,
  consultantCanAccessPath,
  authorizationForRequest,
};
