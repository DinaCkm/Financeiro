#!/usr/bin/env node
const fs = require('fs');
const path = require('path');
const { createStorage } = require('../storage');

async function main() {
  const databaseUrl = process.env.DATABASE_URL;
  if (!databaseUrl) throw new Error('Defina DATABASE_URL para executar a migração.');

  const file = process.argv[2] || path.join(__dirname, '..', 'data', 'db.json');
  const raw = JSON.parse(fs.readFileSync(file, 'utf8'));
  const db = {
    users: raw.users || [],
    uploads: raw.uploads || [],
    entries: raw.entries || [],
    issues: raw.issues || [],
    reviewRegistry: raw.reviewRegistry || [],
    savedRules: raw.savedRules || [],
    manualAdjustments: raw.manualAdjustments || []
  };

  const storage = createStorage({ databaseUrl });
  await storage.init();
  await storage.saveDb(db);

  console.log(JSON.stringify({ ok: true, migrated: {
    users: db.users.length,
    uploads: db.uploads.length,
    entries: db.entries.length,
    issues: db.issues.length,
    reviewRegistry: db.reviewRegistry.length,
    savedRules: db.savedRules.length,
    manualAdjustments: db.manualAdjustments.length
  } }, null, 2));
}

main().catch((error) => {
  console.error(error.message);
  process.exit(1);
});
