import { getDb } from './db.js';

const DEFAULT_CONFIGS = {
  'max-retries': '3',
  'backoff-base': '2',
  'default-timeout': '0'
};

export async function getConfig(key) {
  const db = getDb();
  try {
    const row = await db.get('SELECT value FROM config WHERE key = ?', [key]);
    if (row !== undefined && row.value !== null) {
      return row.value;
    }
  } catch (error) {
  }
  return DEFAULT_CONFIGS[key] || null;
}

export async function getConfigInt(key) {
  const val = await getConfig(key);
  return parseInt(val, 10);
}

export async function getConfigFloat(key) {
  const val = await getConfig(key);
  return parseFloat(val);
}

export async function setConfig(key, value) {
  const db = getDb();
  await db.run(
    `INSERT INTO config (key, value)
     VALUES (?, ?)
     ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
    [key, String(value)]
  );
}
