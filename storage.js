const { Pool } = require('pg');

function createStorage({ databaseUrl }) {
  if (!databaseUrl) {
    throw new Error('DATABASE_URL é obrigatório na Sprint 4 para uso contínuo com PostgreSQL.');
  }

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
    `);
  }

  async function seedUser() {
    const count = await pool.query('SELECT COUNT(*)::int AS c FROM users');
    if (!count.rows[0].c) {
      await pool.query('INSERT INTO users (id, email, password, role) VALUES ($1, $2, $3, $4)', ['owner-ckm', 'owner@ckm.local', '123456', 'owner']);
    }
  }

  async function loadCollection(query) {
    const result = await pool.query(query);
    return result.rows.map((r) => r.data);
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
        uploads: uploads.rows.map((r) => ({ id: r.id, fileName: r.file_name, uploadedAt: r.uploaded_at, rowCount: r.row_count })),
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
        await client.query('TRUNCATE uploads, entries, issues, review_registry, saved_rules, manual_adjustments');

        for (const upload of db.uploads || []) {
          await client.query('INSERT INTO uploads (id, file_name, uploaded_at, row_count, payload) VALUES ($1, $2, $3, $4, $5)', [upload.id, upload.fileName || '', upload.uploadedAt || new Date().toISOString(), upload.rowCount || 0, upload]);
        }

        for (const entry of db.entries || []) {
          await client.query('INSERT INTO entries (id, upload_id, data) VALUES ($1, $2, $3)', [entry.id, entry.uploadId || '', entry]);
        }

        for (const issue of db.issues || []) {
          await client.query('INSERT INTO issues (id, upload_id, data) VALUES ($1, $2, $3)', [issue.id, issue.uploadId || null, issue]);
        }

        for (const item of db.reviewRegistry || []) {
          await client.query('INSERT INTO review_registry (id, data) VALUES ($1, $2)', [item.id, item]);
        }

        for (const rule of db.savedRules || []) {
          await client.query('INSERT INTO saved_rules (id, data) VALUES ($1, $2)', [rule.id, rule]);
        }

        for (const adjustment of db.manualAdjustments || []) {
          await client.query('INSERT INTO manual_adjustments (id, data) VALUES ($1, $2)', [adjustment.id, adjustment]);
        }

        await client.query('COMMIT');
      } catch (error) {
        await client.query('ROLLBACK');
        throw error;
      } finally {
        client.release();
      }
    }
  };
}

module.exports = { createStorage };
