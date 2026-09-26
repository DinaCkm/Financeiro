const fs = require('fs');
const path = require('path');
const crypto = require('crypto');

const PASSWORD_PREFIX = 'scrypt$';

function hashPassword(password) {
  const salt = crypto.randomBytes(16).toString('hex');
  const key = crypto.scryptSync(String(password), salt, 64).toString('hex');
  return `${PASSWORD_PREFIX}${salt}$${key}`;
}

function createJsonStorage(dbPath) {
  return {
    async init() {
      if (!fs.existsSync(dbPath)) {
        fs.mkdirSync(path.dirname(dbPath), { recursive: true });
        fs.writeFileSync(dbPath, JSON.stringify({
          users: [], uploads: [], entries: [], issues: [],
          reviewRegistry: [], savedRules: [], manualAdjustments: []
        }, null, 2));
      }
      const db = JSON.parse(fs.readFileSync(dbPath, 'utf8'));
      if (!db.users.length) {
        db.users.push({
          id: 'owner-ckm',
          email: 'owner@ckm.local',
          password: hashPassword('123456'),
          role: 'owner',
          status: 'ativo',
          failedLoginAttempts: 0,
          loginBlockedUntil: null,
          lastLoginAt: null,
          mustChangePassword: false
        });
        fs.writeFileSync(dbPath, JSON.stringify(db, null, 2));
      }
    },
    async loadDb() {
      return JSON.parse(fs.readFileSync(dbPath, 'utf8'));
    },
    async saveDb(db) {
      fs.writeFileSync(dbPath, JSON.stringify(db, null, 2));
    }
  };
}

function createPostgresStorage(databaseUrl) {
  const { Pool } = require('pg');
  const pool = new Pool({ connectionString: databaseUrl });

  async function ensureSchema() {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS users (
        id TEXT PRIMARY KEY,
        email TEXT NOT NULL UNIQUE,
        password TEXT NOT NULL,
        role TEXT NOT NULL,
        name TEXT,
        status TEXT NOT NULL DEFAULT 'ativo',
        failed_login_attempts INTEGER NOT NULL DEFAULT 0,
        login_blocked_until TIMESTAMPTZ,
        last_login_at TIMESTAMPTZ,
        must_change_password BOOLEAN NOT NULL DEFAULT FALSE,
        password_reset_token_hash TEXT,
        password_reset_expires_at TIMESTAMPTZ,
        created_at TIMESTAMPTZ DEFAULT now()
      );
      CREATE TABLE IF NOT EXISTS uploads (
        id TEXT PRIMARY KEY,
        file_name TEXT NOT NULL,
        uploaded_at TIMESTAMPTZ NOT NULL,
        row_count INTEGER NOT NULL,
        payload JSONB DEFAULT '{}'::jsonb
      );
      CREATE TABLE IF NOT EXISTS entries (
        id TEXT PRIMARY KEY,
        upload_id TEXT NOT NULL,
        data JSONB NOT NULL
      );
      CREATE TABLE IF NOT EXISTS issues (
        id TEXT PRIMARY KEY,
        upload_id TEXT,
        data JSONB NOT NULL
      );
      CREATE TABLE IF NOT EXISTS review_registry (
        id TEXT PRIMARY KEY,
        data JSONB NOT NULL
      );
      CREATE TABLE IF NOT EXISTS saved_rules (
        id TEXT PRIMARY KEY,
        data JSONB NOT NULL
      );
      CREATE TABLE IF NOT EXISTS manual_adjustments (
        id TEXT PRIMARY KEY,
        data JSONB NOT NULL,
        created_at TIMESTAMPTZ DEFAULT now()
      );
      CREATE TABLE IF NOT EXISTS sessions (
        id TEXT PRIMARY KEY,
        user_id TEXT NOT NULL,
        expires_at TIMESTAMPTZ NOT NULL,
        created_at TIMESTAMPTZ DEFAULT now()
      );
      CREATE TABLE IF NOT EXISTS app_meta (
        key TEXT PRIMARY KEY,
        value JSONB NOT NULL
      );
      CREATE TABLE IF NOT EXISTS audit_log (
        id TEXT PRIMARY KEY,
        ts TIMESTAMPTZ NOT NULL DEFAULT now(),
        entry_id TEXT,
        usuario TEXT NOT NULL,
        campo TEXT NOT NULL,
        de TEXT,
        para TEXT,
        tipo TEXT DEFAULT 'lancamento',
        created_at TIMESTAMPTZ DEFAULT now()
      );
      CREATE TABLE IF NOT EXISTS portal_consultor_clientes (
        consultant_user_id TEXT NOT NULL,
        cliente_id INTEGER NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (consultant_user_id, cliente_id)
      );
      CREATE TABLE IF NOT EXISTS portal_contatos_validacao (
        id TEXT PRIMARY KEY,
        cliente_id INTEGER NOT NULL,
        nome TEXT NOT NULL,
        email TEXT NOT NULL,
        ativo BOOLEAN NOT NULL DEFAULT TRUE,
        created_by TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (cliente_id, email)
      );
      CREATE INDEX IF NOT EXISTS idx_portal_contatos_cliente
        ON portal_contatos_validacao(cliente_id, ativo);
      CREATE TABLE IF NOT EXISTS portal_entregas (
        id TEXT PRIMARY KEY,
        cliente_id INTEGER NOT NULL,
        projeto_id INTEGER,
        contrato_id INTEGER,
        titulo TEXT NOT NULL,
        descricao TEXT NOT NULL,
        responsavel_user_id TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'aguardando_cliente',
        validation_mode TEXT NOT NULL DEFAULT 'all',
        current_version INTEGER NOT NULL DEFAULT 1,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        sent_at TIMESTAMPTZ
      );
      CREATE TABLE IF NOT EXISTS portal_entrega_versions (
        id TEXT PRIMARY KEY,
        entrega_id TEXT NOT NULL,
        version_number INTEGER NOT NULL,
        file_name TEXT NOT NULL,
        mime_type TEXT NOT NULL,
        file_size BIGINT NOT NULL,
        file_hash TEXT NOT NULL,
        storage_key TEXT,
        file_data BYTEA NOT NULL,
        uploaded_by TEXT NOT NULL,
        uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        status TEXT NOT NULL DEFAULT 'aguardando_cliente',
        UNIQUE (entrega_id, version_number)
      );
      CREATE TABLE IF NOT EXISTS portal_entrega_convidados (
        id TEXT PRIMARY KEY,
        entrega_id TEXT NOT NULL,
        version_id TEXT,
        nome TEXT NOT NULL,
        email TEXT NOT NULL,
        can_comment BOOLEAN NOT NULL DEFAULT TRUE,
        can_request_changes BOOLEAN NOT NULL DEFAULT TRUE,
        can_validate BOOLEAN NOT NULL DEFAULT TRUE,
        token_hash TEXT NOT NULL UNIQUE,
        status TEXT NOT NULL DEFAULT 'convidado',
        invited_at TIMESTAMPTZ NOT NULL DEFAULT now(),
        first_access_at TIMESTAMPTZ,
        last_access_at TIMESTAMPTZ
      );
      CREATE TABLE IF NOT EXISTS portal_entrega_messages (
        id TEXT PRIMARY KEY,
        entrega_id TEXT NOT NULL,
        version_id TEXT NOT NULL,
        actor_type TEXT NOT NULL,
        actor_user_id TEXT,
        convidado_id TEXT,
        actor_name TEXT NOT NULL,
        actor_email TEXT,
        message TEXT NOT NULL,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      CREATE TABLE IF NOT EXISTS portal_entrega_decisions (
        id TEXT PRIMARY KEY,
        entrega_id TEXT NOT NULL,
        version_id TEXT NOT NULL,
        convidado_id TEXT NOT NULL,
        decision TEXT NOT NULL,
        decision_text TEXT,
        created_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
      CREATE TABLE IF NOT EXISTS portal_entrega_validations (
        id TEXT PRIMARY KEY,
        entrega_id TEXT NOT NULL,
        version_id TEXT NOT NULL,
        convidado_id TEXT NOT NULL,
        protocol TEXT NOT NULL UNIQUE,
        file_hash TEXT NOT NULL,
        validator_name TEXT NOT NULL,
        validator_email TEXT NOT NULL,
        validator_cpf TEXT,
        declaration_text TEXT NOT NULL,
        validated_at TIMESTAMPTZ NOT NULL DEFAULT now()
      );
    `);

    await pool.query("ALTER TABLE portal_entregas ADD COLUMN IF NOT EXISTS validation_mode TEXT NOT NULL DEFAULT 'all'");
    await pool.query("ALTER TABLE portal_entrega_convidados ADD COLUMN IF NOT EXISTS version_id TEXT");
    await pool.query("ALTER TABLE portal_entrega_versions ADD COLUMN IF NOT EXISTS storage_key TEXT");

    // Migração aditiva e idempotente para instalações que já possuem a tabela users.
    await pool.query(`
      ALTER TABLE users ADD COLUMN IF NOT EXISTS name TEXT;
      ALTER TABLE users ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'ativo';
      ALTER TABLE users ADD COLUMN IF NOT EXISTS failed_login_attempts INTEGER NOT NULL DEFAULT 0;
      ALTER TABLE users ADD COLUMN IF NOT EXISTS login_blocked_until TIMESTAMPTZ;
      ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login_at TIMESTAMPTZ;
      ALTER TABLE users ADD COLUMN IF NOT EXISTS must_change_password BOOLEAN NOT NULL DEFAULT FALSE;
      ALTER TABLE users ADD COLUMN IF NOT EXISTS password_reset_token_hash TEXT;
      ALTER TABLE users ADD COLUMN IF NOT EXISTS password_reset_expires_at TIMESTAMPTZ;
    `);
  }

  async function seedUser() {
    const count = await pool.query('SELECT COUNT(*)::int AS c FROM users');
    if (!count.rows[0].c) {
      await pool.query(
        'INSERT INTO users (id, email, password, role) VALUES ($1, $2, $3, $4)',
        ['owner-ckm', 'owner@ckm.local', hashPassword('123456'), 'owner']
      );
    }
  }

  async function loadCollection(query) {
    const result = await pool.query(query);
    return result.rows.map((r) => r.data);
  }

  // Inserção em lote via unnest — muito mais rápido que INSERT individual
  // Processa em chunks de 500 para evitar limites de parâmetros do PostgreSQL
  async function batchUpsertEntries(client, entries) {
    if (!entries || !entries.length) return;
    const CHUNK = 500;
    for (let i = 0; i < entries.length; i += CHUNK) {
      const chunk = entries.slice(i, i + CHUNK);
      const ids = chunk.map(e => e.id);
      const uploadIds = chunk.map(e => e.uploadId || '');
      const datas = chunk.map(e => e);
      await client.query(
        `INSERT INTO entries (id, upload_id, data)
         SELECT * FROM unnest($1::text[], $2::text[], $3::jsonb[])
         ON CONFLICT (id) DO UPDATE
         SET upload_id = EXCLUDED.upload_id, data = EXCLUDED.data`,
        [ids, uploadIds, datas]
      );
    }
  }

  async function batchUpsertIssues(client, issues) {
    if (!issues || !issues.length) return;
    const CHUNK = 500;
    for (let i = 0; i < issues.length; i += CHUNK) {
      const chunk = issues.slice(i, i + CHUNK);
      const ids = chunk.map(i => i.id);
      const uploadIds = chunk.map(i => i.uploadId || null);
      const datas = chunk.map(i => i);
      await client.query(
        `INSERT INTO issues (id, upload_id, data)
         SELECT * FROM unnest($1::text[], $2::text[], $3::jsonb[])
         ON CONFLICT (id) DO UPDATE
         SET upload_id = EXCLUDED.upload_id, data = EXCLUDED.data`,
        [ids, uploadIds, datas]
      );
    }
  }

  async function batchUpsertSimple(client, table, items) {
    if (!items || !items.length) return;
    const CHUNK = 500;
    for (let i = 0; i < items.length; i += CHUNK) {
      const chunk = items.slice(i, i + CHUNK);
      const ids = chunk.map(r => r.id);
      const datas = chunk.map(r => r);
      await client.query(
        `INSERT INTO ${table} (id, data)
         SELECT * FROM unnest($1::text[], $2::jsonb[])
         ON CONFLICT (id) DO UPDATE SET data = EXCLUDED.data`,
        [ids, datas]
      );
    }
  }

  async function batchUpsertUploads(client, uploads) {
    if (!uploads || !uploads.length) return;
    const CHUNK = 500;
    for (let i = 0; i < uploads.length; i += CHUNK) {
      const chunk = uploads.slice(i, i + CHUNK);
      const ids = chunk.map(u => u.id);
      const fileNames = chunk.map(u => u.fileName);
      const uploadedAts = chunk.map(u => u.uploadedAt);
      const rowCounts = chunk.map(u => u.rowCount);
      const payloads = chunk.map(u => u);
      await client.query(
        `INSERT INTO uploads (id, file_name, uploaded_at, row_count, payload)
         SELECT * FROM unnest($1::text[], $2::text[], $3::timestamptz[], $4::int[], $5::jsonb[])
         ON CONFLICT (id) DO UPDATE
         SET file_name = EXCLUDED.file_name,
             uploaded_at = EXCLUDED.uploaded_at,
             row_count = EXCLUDED.row_count,
             payload = EXCLUDED.payload`,
        [ids, fileNames, uploadedAts, rowCounts, payloads]
      );
    }
  }

  return {
    async init() {
      await ensureSchema();
      await seedUser();
    },

    async loadDb() {
      const [users, uploads, entries, issues, reviewRegistry, savedRules, manualAdjustments, metaRows] = await Promise.all([
        pool.query(`SELECT id, email, password, role, name, status,
          failed_login_attempts, login_blocked_until, last_login_at, must_change_password,
          password_reset_token_hash, password_reset_expires_at
          FROM users ORDER BY created_at`),
        pool.query('SELECT id, file_name, uploaded_at, row_count FROM uploads ORDER BY uploaded_at'),
        loadCollection('SELECT data FROM entries'),
        loadCollection('SELECT data FROM issues'),
        loadCollection('SELECT data FROM review_registry'),
        loadCollection('SELECT data FROM saved_rules'),
        loadCollection('SELECT data FROM manual_adjustments'),
        pool.query('SELECT key, value FROM app_meta')
      ]);

      // Reconstruir objeto meta a partir das linhas key/value
      const meta = {};
      for (const row of metaRows.rows) {
        meta[row.key] = row.value;
      }

      return {
        users: users.rows.map((r) => ({
          id: r.id,
          email: r.email,
          password: r.password,
          role: r.role,
          name: r.name || null,
          status: r.status || 'ativo',
          failedLoginAttempts: Number(r.failed_login_attempts || 0),
          loginBlockedUntil: r.login_blocked_until || null,
          lastLoginAt: r.last_login_at || null,
          mustChangePassword: !!r.must_change_password,
          passwordResetTokenHash: r.password_reset_token_hash || null,
          passwordResetExpiresAt: r.password_reset_expires_at || null
        })),
        uploads: uploads.rows.map((r) => ({
          id: r.id, fileName: r.file_name, uploadedAt: r.uploaded_at, rowCount: r.row_count
        })),
        entries,
        issues,
        reviewRegistry,
        savedRules,
        manualAdjustments,
        meta
      };
    },

    async saveDb(db) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');

        // Usuários — upsert aditivo. Não remove usuários ausentes do snapshot em memória.
        for (const user of (db.users || []).filter(Boolean)) {
          await client.query(
            `INSERT INTO users (
              id, email, password, role, name, status, failed_login_attempts,
              login_blocked_until, last_login_at, must_change_password,
              password_reset_token_hash, password_reset_expires_at
            ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
            ON CONFLICT (id) DO UPDATE SET
              email = EXCLUDED.email,
              password = EXCLUDED.password,
              role = EXCLUDED.role,
              name = EXCLUDED.name,
              status = EXCLUDED.status,
              failed_login_attempts = EXCLUDED.failed_login_attempts,
              login_blocked_until = EXCLUDED.login_blocked_until,
              last_login_at = EXCLUDED.last_login_at,
              must_change_password = EXCLUDED.must_change_password,
              password_reset_token_hash = EXCLUDED.password_reset_token_hash,
              password_reset_expires_at = EXCLUDED.password_reset_expires_at`,
            [
              user.id,
              user.email,
              user.password,
              user.role || 'owner',
              user.name || null,
              user.status || 'ativo',
              Number(user.failedLoginAttempts || 0),
              user.loginBlockedUntil || null,
              user.lastLoginAt || null,
              !!user.mustChangePassword,
              user.passwordResetTokenHash || null,
              user.passwordResetExpiresAt || null
            ]
          );
        }

        // Uploads
        const uploads = (db.uploads || []).filter(Boolean).map(u => ({
          ...u,
          id: u.id || crypto.randomUUID(),
          fileName: u.fileName || '',
          uploadedAt: u.uploadedAt || new Date().toISOString(),
          rowCount: Number(u.rowCount || 0)
        }));
        if (uploads.length) {
          await client.query('DELETE FROM uploads WHERE id <> ALL($1::text[])', [uploads.map(u => u.id)]);
        } else {
          await client.query('DELETE FROM uploads');
        }
        await batchUpsertUploads(client, uploads);

        // Entries
        const entries = (db.entries || []).filter(Boolean).map(e => ({
          ...e, id: e.id || crypto.randomUUID()
        }));
        if (entries.length) {
          await client.query('DELETE FROM entries WHERE id <> ALL($1::text[])', [entries.map(e => e.id)]);
        } else {
          await client.query('DELETE FROM entries');
        }
        await batchUpsertEntries(client, entries);

        // Issues
        const issues = (db.issues || []).filter(Boolean).map(i => ({
          ...i, id: i.id || crypto.randomUUID()
        }));
        if (issues.length) {
          await client.query('DELETE FROM issues WHERE id <> ALL($1::text[])', [issues.map(i => i.id)]);
        } else {
          await client.query('DELETE FROM issues');
        }
        await batchUpsertIssues(client, issues);

        // Review Registry
        const registry = (db.reviewRegistry || []).filter(Boolean).map(r => ({
          ...r, id: r.id || crypto.randomUUID()
        }));
        if (registry.length) {
          await client.query('DELETE FROM review_registry WHERE id <> ALL($1::text[])', [registry.map(r => r.id)]);
        } else {
          await client.query('DELETE FROM review_registry');
        }
        await batchUpsertSimple(client, 'review_registry', registry);

        // Saved Rules
        const rules = (db.savedRules || []).filter(Boolean).map(r => ({
          ...r, id: r.id || crypto.randomUUID()
        }));
        if (rules.length) {
          await client.query('DELETE FROM saved_rules WHERE id <> ALL($1::text[])', [rules.map(r => r.id)]);
        } else {
          await client.query('DELETE FROM saved_rules');
        }
        await batchUpsertSimple(client, 'saved_rules', rules);

        // Manual Adjustments — sempre trunca e reinserir
        await client.query('TRUNCATE manual_adjustments');
        const adjustments = (db.manualAdjustments || []).filter(Boolean).map(a => ({
          ...a, id: a.id || crypto.randomUUID()
        }));
        if (adjustments.length) {
          const ids = adjustments.map(a => a.id);
          const datas = adjustments.map(a => a);
          const CHUNK = 500;
          for (let i = 0; i < adjustments.length; i += CHUNK) {
            const cIds = ids.slice(i, i + CHUNK);
            const cDatas = datas.slice(i, i + CHUNK);
            await client.query(
              `INSERT INTO manual_adjustments (id, data)
               SELECT * FROM unnest($1::text[], $2::jsonb[])
               ON CONFLICT (id) DO NOTHING`,
              [cIds, cDatas]
            );
          }
        }

        // Meta (ultimoNumLanc e outros valores de controle)
        if (db.meta && typeof db.meta === 'object') {
          for (const [key, value] of Object.entries(db.meta)) {
            await client.query(
              `INSERT INTO app_meta (key, value) VALUES ($1, $2::jsonb)
               ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value`,
              [key, JSON.stringify(value)]
            );
          }
        }

        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }
    },
    getPool() {
      return pool;
    },
    async sessionGet(sid) {
      const r = await pool.query(
        'SELECT user_id, expires_at FROM sessions WHERE id = $1',
        [sid]
      );
      if (!r.rows.length) return null;
      const row = r.rows[0];
      if (new Date(row.expires_at) <= new Date()) {
        await pool.query('DELETE FROM sessions WHERE id = $1', [sid]);
        return null;
      }
      return { userId: row.user_id, expiresAt: new Date(row.expires_at).getTime() };
    },
    async sessionSet(sid, userId, ttlSeconds) {
      const expiresAt = new Date(Date.now() + ttlSeconds * 1000).toISOString();
      await pool.query(
        `INSERT INTO sessions (id, user_id, expires_at)
         VALUES ($1, $2, $3)
         ON CONFLICT (id) DO UPDATE SET user_id = $2, expires_at = $3`,
        [sid, userId, expiresAt]
      );
    },
    async sessionDelete(sid) {
      await pool.query('DELETE FROM sessions WHERE id = $1', [sid]);
    },
    async sessionDeleteByUser(userId) {
      await pool.query('DELETE FROM sessions WHERE user_id = $1', [userId]);
    }
  };
}

function createStorage({ dbPath, databaseUrl }) {
  if (databaseUrl) return createPostgresStorage(databaseUrl);
  return createJsonStorage(dbPath);
}

module.exports = { createStorage };
