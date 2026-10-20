// Baileys WhatsApp daemon for Muse.
// Persistent connection to Yahel's WhatsApp (+972 55 883 6729) with a local
// HTTP API on 127.0.0.1:3987 for reading chats/messages and sending.
// Auth state lives in ../auth (multi-file). Messages are appended to
// ../data/messages.jsonl; chat metadata in ../data/chats.json.

import fs from 'node:fs';
import path from 'node:path';
import http from 'node:http';
import { fileURLToPath } from 'node:url';
import pino from 'pino';
import hpkg from 'https-proxy-agent';
import makeWASocket, {
  useMultiFileAuthState,
  DisconnectReason,
  fetchLatestBaileysVersion,
  fetchLatestWaWebVersion,
} from 'baileys';

const { HttpsProxyAgent } = hpkg;
// This VM reaches the internet only through an egress proxy. WhatsApp's
// WebSocket must go through it or the TLS handshake fails (EPROTO).
const PROXY_URL = process.env.WA_PROXY || 'http://hatch-egress-proxy:3128';
const agent = new HttpsProxyAgent(PROXY_URL);

const ROOT = path.dirname(path.dirname(fileURLToPath(import.meta.url)));
const AUTH_DIR = process.env.WA_AUTH_DIR || path.join(ROOT, 'auth');
const DATA_DIR = process.env.WA_DATA_DIR || path.join(ROOT, 'data');
const MSG_FILE = path.join(DATA_DIR, 'messages.jsonl');
const CHAT_FILE = path.join(DATA_DIR, 'chats.json');
const PHONE = '972558836729'; // Yahel's WhatsApp number, international, no '+'
const PORT = Number(process.env.WA_PORT || 3987);

fs.mkdirSync(AUTH_DIR, { recursive: true });
fs.mkdirSync(DATA_DIR, { recursive: true });

const logger = pino({ level: 'silent' });
const log = (...a) => console.log(new Date().toISOString(), ...a);

let sock = null;
let connectionState = 'starting';
let me = null;
let pairing = null; // { code, at }
let pairingRequested = false;
let reconnectTimer = null;
let starting = false;

function loadChats() {
  try { return JSON.parse(fs.readFileSync(CHAT_FILE, 'utf8')); }
  catch { return {}; }
}
function saveChats(chats) {
  fs.writeFileSync(CHAT_FILE, JSON.stringify(chats, null, 1));
}
const chats = loadChats();

// Message retry support. When a peer device cannot decrypt one of our
// messages (garbled first delivery), WhatsApp asks the sender to re-encrypt
// and resend it; Baileys serves that retry from getMessage. Without it the
// peer stays stuck at one tick forever. Index our own text messages by id
// (loaded from the store, updated live) so retries can always be served.
const messageIndex = new Map();
function indexOwnMessage(rec) {
  if (rec && rec.fromMe && rec.id && rec.kind === 'text' && typeof rec.text === 'string') {
    messageIndex.set(rec.id, rec.text);
  }
}
try {
  for (const line of fs.readFileSync(MSG_FILE, 'utf8').split('\n')) {
    if (!line.trim()) continue;
    try { indexOwnMessage(JSON.parse(line)); } catch {}
  }
} catch {}

// Persistent retry-counter cache (Baileys CacheStore shape). The in-memory
// default loses the retry counts on every restart, which breaks the
// server's retry accounting mid-conversation.
const RETRY_FILE = path.join(DATA_DIR, 'retry-cache.json');
const retryStore = (() => {
  try { return new Map(Object.entries(JSON.parse(fs.readFileSync(RETRY_FILE, 'utf8')))); }
  catch { return new Map(); }
})();
function saveRetryStore() {
  try { fs.writeFileSync(RETRY_FILE, JSON.stringify(Object.fromEntries(retryStore))); } catch {}
}
const msgRetryCounterCache = {
  get: async (key) => retryStore.get(key),
  set: async (key, value) => { retryStore.set(key, value); saveRetryStore(); },
  del: async (key) => { retryStore.delete(key); saveRetryStore(); },
  flushAll: async () => { retryStore.clear(); saveRetryStore(); },
};

function upsertChats(list) {
  for (const c of list || []) {
    if (!c.id) continue;
    const prev = chats[c.id] || {};
    chats[c.id] = {
      id: c.id,
      name: c.name || prev.name || null,
      unreadCount: c.unreadCount ?? prev.unreadCount ?? 0,
      lastTs: c.conversationTimestamp
        ? Number(c.conversationTimestamp) * 1000
        : (prev.lastTs || null),
    };
  }
  saveChats(chats);
}

function upsertContacts(list) {
  for (const c of list || []) {
    if (!c.id) continue;
    const name = c.name || c.notify || c.verifiedName;
    if (!name) continue;
    const prev = chats[c.id] || { id: c.id, unreadCount: 0, lastTs: null };
    prev.name = prev.name || name;
    chats[c.id] = prev;
  }
  saveChats(chats);
}

function extractText(msg) {
  const m = msg.message || {};
  if (m.conversation) return { text: m.conversation, kind: 'text' };
  if (m.extendedTextMessage?.text) return { text: m.extendedTextMessage.text, kind: 'text' };
  if (m.imageMessage) return { text: m.imageMessage.caption || '[image]', kind: 'image' };
  if (m.videoMessage) return { text: m.videoMessage.caption || '[video]', kind: 'video' };
  if (m.audioMessage) return { text: '[voice note]', kind: 'audio' };
  if (m.documentMessage) return { text: `[document: ${m.documentMessage.fileName || 'file'}]`, kind: 'document' };
  if (m.stickerMessage) return { text: '[sticker]', kind: 'sticker' };
  if (m.reactionMessage) return { text: `[reaction ${m.reactionMessage.text || ''}]`, kind: 'reaction' };
  if (m.locationMessage) return { text: '[location]', kind: 'location' };
  if (m.contactMessage) return { text: `[contact: ${m.contactMessage.displayName || ''}]`, kind: 'contact' };
  const keys = Object.keys(m);
  return { text: keys.length ? `[${keys[0]}]` : '[empty]', kind: 'other' };
}

function recordMessage(msg) {
  if (!msg?.key?.remoteJid || !msg.message) return;
  const { text, kind } = extractText(msg);
  const rec = {
    ts: msg.messageTimestamp ? Number(msg.messageTimestamp) * 1000 : Date.now(),
    id: msg.key.id,
    jid: msg.key.remoteJid,
    fromMe: !!msg.key.fromMe,
    sender: msg.key.participant || msg.key.remoteJid,
    pushName: msg.pushName || null,
    text,
    kind,
  };
  fs.appendFileSync(MSG_FILE, JSON.stringify(rec) + '\n');
  indexOwnMessage(rec);
  const prev = chats[rec.jid] || { id: rec.jid, unreadCount: 0 };
  prev.lastTs = Math.max(prev.lastTs || 0, rec.ts);
  chats[rec.jid] = prev;
  saveChats(chats);
}

async function startSock() {
  if (starting) return;
  starting = true;
  try {
    const { state, saveCreds } = await useMultiFileAuthState(AUTH_DIR);
    // WA's version endpoint returns different values per edge; an older
    // client version gets its pairing socket dropped with a 401. Fetch a
    // few times and take the newest version on offer. The fetch must go
    // through the egress proxy or it silently returns a stale fallback.
    let version;
    const candidates = [];
    const waOpts = { httpsAgent: agent, httpAgent: agent, proxy: false };
    for (let i = 0; i < 3; i++) {
      try { candidates.push((await fetchLatestWaWebVersion(waOpts)).version); } catch {}
    }
    try { candidates.push((await fetchLatestBaileysVersion()).version); } catch {}
    if (candidates.length) {
      version = candidates.reduce((a, b) => (b[2] > a[2] ? b : a));
    }
    log('using WA version:', version ? version.join('.') : 'bundled default');

    sock = makeWASocket({
      version,
      auth: state,
      agent,
      fetchAgent: agent,
      logger,
      printQRInTerminal: false,
      browser: ['Ubuntu', 'Chrome', '22.04.0'],
      syncFullHistory: false,
      markOnlineOnConnect: false,
      msgRetryCounterCache,
      getMessage: async (key) => {
        const text = key?.id ? messageIndex.get(key.id) : undefined;
        return text ? { conversation: text } : undefined;
      },
    });

    sock.ev.on('creds.update', saveCreds);
    sock.ev.on('chats.upsert', upsertChats);
    sock.ev.on('chats.update', upsertChats);
    sock.ev.on('contacts.upsert', upsertContacts);
    sock.ev.on('contacts.update', upsertContacts);
    sock.ev.on('messages.upsert', ({ messages }) => {
      for (const m of messages || []) recordMessage(m);
    });

    sock.ev.on('connection.update', async (update) => {
      const { connection, lastDisconnect } = update;
      if (connection) connectionState = connection;
      if (connection === 'open') {
        me = sock.user || null;
        pairing = null;
        log('connection open as', me?.id || 'unknown');
      }
      if (connection === 'close') {
        const code = lastDisconnect?.error?.output?.statusCode;
        log('connection closed, statusCode:', code);
        // A 401 before registration completes is the pairing handshake
        // restarting the socket, not a real logout. Only stop when a
        // previously registered device was logged out.
        if (code === DisconnectReason.loggedOut && state.creds.registered) {
          connectionState = 'logged_out';
          log('LOGGED OUT. Re-pairing required.');
        } else {
          clearTimeout(reconnectTimer);
          reconnectTimer = setTimeout(() => { starting = false; startSock(); }, 4000);
        }
      }
    });

    // Request the pairing code once per registration. Re-requesting on
    // every reconnect rotates the code and makes WhatsApp drop the socket
    // with 401s, so the phone can never finish pairing.
    if (!state.creds.registered && !pairingRequested) {
      pairingRequested = true;
      setTimeout(async () => {
        try {
          const code = await sock.requestPairingCode(PHONE);
          pairing = { code, at: Date.now() };
          log('PAIRING CODE:', code);
        } catch (e) {
          pairingRequested = false;
          log('pairing code request failed:', e?.message || e);
        }
      }, 3500);
    }
  } finally {
    starting = false;
  }
}

// ---------- local HTTP API ----------
function json(res, code, body) {
  res.writeHead(code, { 'Content-Type': 'application/json' });
  res.end(JSON.stringify(body));
}
function readBody(req) {
  return new Promise((resolve) => {
    let data = '';
    req.on('data', (c) => { data += c; });
    req.on('end', () => { try { resolve(JSON.parse(data || '{}')); } catch { resolve({}); } });
  });
}
function readAllMessages() {
  try {
    return fs.readFileSync(MSG_FILE, 'utf8').split('\n').filter(Boolean).map((l) => JSON.parse(l));
  } catch { return []; }
}

const server = http.createServer(async (req, res) => {
  const url = new URL(req.url, 'http://127.0.0.1');
  try {
    if (url.pathname === '/status') {
      return json(res, 200, {
        connection: connectionState,
        me: me ? { id: me.id, name: me.name || null } : null,
        registered: fs.existsSync(path.join(AUTH_DIR, 'creds.json'))
          ? JSON.parse(fs.readFileSync(path.join(AUTH_DIR, 'creds.json'), 'utf8')).registered ?? null
          : false,
        pairing,
        chats: Object.keys(chats).length,
      });
    }
    if (url.pathname === '/pairing-code' && req.method === 'POST') {
      if (!sock) return json(res, 503, { error: 'socket not started' });
      const code = await sock.requestPairingCode(PHONE);
      pairing = { code, at: Date.now() };
      pairingRequested = true;
      return json(res, 200, pairing);
    }
    if (url.pathname === '/chats') {
      const list = Object.values(chats).sort((a, b) => (b.lastTs || 0) - (a.lastTs || 0));
      return json(res, 200, list);
    }
    if (url.pathname === '/messages') {
      const jid = url.searchParams.get('jid');
      const limit = Math.min(Number(url.searchParams.get('limit') || 50), 500);
      let msgs = readAllMessages();
      if (jid) msgs = msgs.filter((m) => m.jid === jid);
      msgs.sort((a, b) => a.ts - b.ts);
      return json(res, 200, msgs.slice(-limit));
    }
    if (url.pathname === '/search') {
      const q = (url.searchParams.get('q') || '').toLowerCase();
      const msgs = readAllMessages().filter(
        (m) => (m.text || '').toLowerCase().includes(q) || (chats[m.jid]?.name || '').toLowerCase().includes(q),
      );
      return json(res, 200, msgs.slice(-100));
    }
    if (url.pathname === '/send' && req.method === 'POST') {
      const body = await readBody(req);
      if (!sock || connectionState !== 'open') return json(res, 503, { error: 'not connected' });
      let jid = body.jid;
      if (!jid && body.number) {
        const num = String(body.number).replace(/\D/g, '');
        jid = `${num}@s.whatsapp.net`;
      }
      if (!jid || !body.text) return json(res, 400, { error: 'jid or number, and text, are required' });
      const sent = await sock.sendMessage(jid, { text: String(body.text) });
      return json(res, 200, { ok: true, id: sent?.key?.id || null, jid });
    }
    return json(res, 404, { error: 'not found' });
  } catch (e) {
    return json(res, 500, { error: e?.message || String(e) });
  }
});

server.listen(PORT, '127.0.0.1', () => log('API listening on 127.0.0.1:' + PORT));
startSock();
