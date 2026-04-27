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
        db.users.push({ id: 'owner-ckm', email: 'owner@ckm.local', password: hashPassword('123456'), role: 'owner' });
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
      const [users, uploads, entries, issues, reviewRegistry, savedRules, manualAdjustments] = await Promise.all([
        pool.query('SELECT id, email, password, role FROM users ORDER BY created_at'),
        pool.query('SELECT id, file_name, uploaded_at, row_count FROM uploads ORDER BY uploaded_at'),
        loadCollection('SELECT data FROM entries'),
        loadCollection('SELECT data FROM issues'),
        loadCollection('SELECT data FROM review_registry'),
        loadCollection('SELECT data FROM saved_rules'),
        loadCollection('SELECT data FROM manual_adjustments')
      ]);

      return {
        users: users.rows.map((r) => ({ id: r.id, email: r.email, password: r.password, role: r.role })),
        uploads: uploads.rows.map((r) => ({
          id: r.id, fileName: r.file_name, uploadedAt: r.uploaded_at, rowCount: r.row_count
        })),
        entries,
        issues,
        reviewRegistry,
        savedRules,
        manualAdjustments
      };
    },

    async saveDb(db) {
      const client = await pool.connect();
      try {
        await client.query('BEGIN');

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
    }
  };
}

function createStorage({ dbPath, databaseUrl }) {
  if (databaseUrl) return createPostgresStorage(databaseUrl);
  return createJsonStorage(dbPath);
}

module.exports = { createStorage };
