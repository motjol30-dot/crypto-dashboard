'use strict';

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const axios = require('axios');
const path = require('path');
const fs = require('fs');
const crypto = require('crypto');
const os = require('os');
const { RSI, MACD, BollingerBands, EMA, CCI, ATR } = require('technicalindicators');

// 🛡️ شبكة أمان عامة — Node (نسخة 15 فما فوق) توقف السيرفر بالكامل افتراضيًا لو صار أي خطأ غير معالج
// (مثلاً Binance ما استجاب لثانيتين بنص الليل). بدون هذا، أي خطأ بسيط بأي مكان بالملف ممكن يطفّي البوت بالكامل
// ونلقاه متوقف الصبح. هذا يمنع الإغلاق المفاجئ ويسجّل الخطأ بس يخلي السيرفر شغال.
process.on('uncaughtException', (err) => {
  console.error('⚠️ خطأ غير متوقع (uncaughtException) — السيرفر يستمر بالعمل:', err && err.stack ? err.stack : err);
});
process.on('unhandledRejection', (reason) => {
  console.error('⚠️ خطأ Promise غير معالج (unhandledRejection) — السيرفر يستمر بالعمل:', reason);
});

const PORT = process.env.PORT || 3000;

// قائمة العملات (40 عملة)
const SYMBOLS = [
  'BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','XRPUSDT','ADAUSDT','AVAXUSDT',
  'LINKUSDT','NEARUSDT','DOTUSDT','FETUSDT','GRTUSDT','RENDERUSDT','WLDUSDT',
  'AKTUSDT','ROSEUSDT','SEIUSDT','SUIUSDT','KASUSDT','HBARUSDT','VETUSDT',
  'ALGOUSDT','XLMUSDT','ATOMUSDT','MINAUSDT','XTZUSDT','POLUSDT','ARBUSDT',
  'OPUSDT','STRKUSDT','MANTAUSDT','ZKUSDT','STXUSDT','TIAUSDT','ALTUSDT',
  'JASMYUSDT','IOTXUSDT','ASTRUSDT','CKBUSDT','ACHUSDT'
];

// حوض بحث أوسع (~200 عملة) يفحصه البوت بطلب REST خفيف بدل بث مباشر دائم لكل واحدة — هذا يخلي البحث الواسع
// عمليًا بدون ما نفتح 200 اتصال WebSocket دائم (يثقل السيرفر). أي عملة يختارها البوت فعليًا تترقّى لبث مباشر
// فوري (نفس آلية SYMBOLS العادية) لحظة اختيارها، عشان التنفيذ يبقى بأحدث سعر لحظي.
const SCAN_POOL_EXTRA = [
  'DOGEUSDT','SHIBUSDT','PEPEUSDT','TRXUSDT','LTCUSDT','BCHUSDT','ETCUSDT','FILUSDT',
  'ICPUSDT','APTUSDT','ARUSDT','INJUSDT','RUNEUSDT','KAVAUSDT','FLOWUSDT','EGLDUSDT',
  'THETAUSDT','CHZUSDT','MANAUSDT','SANDUSDT','AXSUSDT','GALAUSDT','ENJUSDT','APEUSDT',
  'IMXUSDT','LDOUSDT','CRVUSDT','SNXUSDT','COMPUSDT','MKRUSDT','AAVEUSDT','UNIUSDT',
  'SUSHIUSDT','1INCHUSDT','YFIUSDT','ZRXUSDT','BALUSDT','RENUSDT','KNCUSDT','LRCUSDT',
  'OMGUSDT','BATUSDT','ZILUSDT','ONTUSDT','QTUMUSDT','ICXUSDT','WAVESUSDT','ZENUSDT',
  'DASHUSDT','DCRUSDT','RVNUSDT','SCUSDT','DGBUSDT','XNOUSDT','SXPUSDT','CELOUSDT',
  'ANKRUSDT','SKLUSDT','STORJUSDT','OCEANUSDT','CTSIUSDT','BANDUSDT','RSRUSDT','COTIUSDT',
  'DENTUSDT','HOTUSDT','WINUSDT','CHRUSDT','ALICEUSDT','TLMUSDT','MTLUSDT','DUSKUSDT',
  'AUDIOUSDT','C98USDT','MASKUSDT','GTCUSDT','GMTUSDT','APEUSDT','JASMYUSDT','SPELLUSDT',
  'PEOPLEUSDT','RAREUSDT','LOOKSUSDT','FLOKIUSDT','BONKUSDT','WIFUSDT','ORDIUSDT','SATSUSDT',
  '1000SATSUSDT','JUPUSDT','PYTHUSDT','DYMUSDT','PORTALUSDT','AXLUSDT','METISUSDT','MOVRUSDT',
  'GLMRUSDT','ONEUSDT','CFXUSDT','KLAYUSDT','IOTAUSDT','XDCUSDT','QNTUSDT','TFUELUSDT',
  'ENSUSDT','ARKMUSDT','SSVUSDT','HFTUSDT','HOOKUSDT','MAGICUSDT','HIGHUSDT','ACHUSDT',
  'STGUSDT','JOEUSDT','WOOUSDT','IDUSDT','EDUUSDT','SUPERUSDT','RADUSDT','PHBUSDT',
  'GNSUSDT','POLYXUSDT','POWRUSDT','CYBERUSDT','VANRYUSDT','AIUSDT','XAIUSDT','MANTAUSDT',
  'PIXELUSDT','AEVOUSDT','ETHFIUSDT','ENAUSDT','WUSDT','TNSRUSDT','SAGAUSDT','OMNIUSDT',
  'REZUSDT','BBUSDT','NOTUSDT','IOUSDT','ZKJUSDT','LISTAUSDT','ZROUSDT','RENDERUSDT',
  'TONUSDT','TAOUSDT','TURBOUSDT','MEWUSDT','POPCATUSDT','BOMEUSDT','DOGSUSDT','CATIUSDT',
  'HMSTRUSDT','NEIROUSDT','EIGENUSDT','SCRUSDT','SAFEUSDT','GUSDT','BANANAUSDT','LUMIAUSDT',
  'ACXUSDT','ORCAUSDT','MOODENGUSDT','ACTUSDT','PNUTUSDT','COOKIEUSDT','ALCHUSDT','SUNDOGUSDT',
  'DEGENUSDT','SLERFUSDT','GOATUSDT','FIDAUSDT','SPXUSDT','APUUSDT','MOGUSDT','PONKEUSDT',
];

// دمج القائمتين + إزالة التكرار — هذا هو حوض البحث الكامل اللي يشوفه البوت
const SCAN_POOL = Array.from(new Set([...SYMBOLS, ...SCAN_POOL_EXTRA]));

const INTERVALS = ['1s','1m','3m','5m','15m','30m','1h','2h','4h'];
const MEXC_WS_INTERVAL = { '1m':'Min1', '3m':'Min3','5m':'Min5','15m':'Min15','30m':'Min30','1h':'Hour1','2h':'Hour2','4h':'Hour4' };
const OKX_BAR = { '1s':'1s', '1m':'1m', '3m':'3m','5m':'5m','15m':'15m','30m':'30m','1h':'1H','2h':'2H','4h':'4H' };

const candleStore = {};
const streamWs = {};
const clientSubs = new Map();

const app = express();
const server = http.createServer(app);

const APP_USER = process.env.APP_USER || 'group';
const APP_PASS = process.env.APP_PASS || 'change-me-1234';
const MAX_USERS = parseInt(process.env.MAX_USERS || '12', 10);
const SESSION_TTL_MS = 15 * 60 * 1000;
const AUTH_ENABLED = process.env.AUTH_ENABLED === 'true';
const activeSessions = new Map();

// إعدادات API — البوت الآن يعمل على Binance فقط (بوت الشبكة الجديد)
const BINANCE_API_KEY = process.env.BINANCE_API_KEY || '';
const BINANCE_API_SECRET = process.env.BINANCE_API_SECRET || '';

/* ============================================================================
 * حسابات المستخدمين المتعددين — كل صديق يقدر يضيف مفتاح Binance API الخاص فيه (إيميل + رمز قصير +
 * المفتاح)، فيتداول بحسابه هو، مو بمفتاحك. المفاتيح تُشفَّر قبل ما تُخزَّن على القرص (AES-256-GCM)
 * بمفتاح تشفير محلي يتولّد أول مرة ويتحفظ بملف منفصل — حتى لو حد فتح ملف الحسابات ما يشوف المفاتيح
 * كنص واضح. الرمز (PIN) إجباري: بدونه أي شخص يعرف إيميل صديقك يقدر ياخذ حسابه ويتحكم بمفتاحه —
 * الرمز أقل حماية ضرورية هنا لأن الموضوع مفاتيح تداول حقيقية.
 * ============================================================================ */
// 📁 مجلد بيانات دائم خارج مجلد التطبيق: الحسابات (إيميل + مفاتيح Binance المشفّرة) ومفتاح التشفير وإعدادات التداول التلقائي.
// قبل كان كل شيء يُحفظ داخل مجلد التطبيق نفسه، فأي رفع/نشر جديد للملفات على الاستضافة كان يمسحه ويطلب منك الإيميل
// والمفاتيح من جديد. الآن يُحفظ في مجلد خاص بالمستخدم (أو المسار الذي تحدده بمتغير البيئة DATA_DIR)، وتُنسخ الملفات
// القديمة إليه تلقائيًا أول مرة.
function resolveDataDir() {
  const want = process.env.DATA_DIR || path.join(os.homedir(), '.crypto-auto-online');
  try { fs.mkdirSync(want, { recursive: true }); fs.accessSync(want, fs.constants.W_OK); }
  catch (e) { console.error('⚠️ تعذّر استخدام مجلد البيانات الدائم', want, '— نستخدم مجلد التطبيق (قد تضيع الحسابات عند رفع ملفات جديدة):', e.message); return __dirname; }
  if (path.resolve(want) !== path.resolve(__dirname)) {
    const names = ['enc.key', 'accounts.json', 'auto-trade.json'];
    if (!names.some(n => fs.existsSync(path.join(want, n)))) {
      for (const n of names) {
        try { const src = path.join(__dirname, n); if (fs.existsSync(src)) { fs.copyFileSync(src, path.join(want, n)); try { fs.chmodSync(path.join(want, n), 0o600); } catch {} console.log('📦 نُقل', n, 'إلى المجلد الدائم'); } } catch (e) { console.error('تعذّر نقل', n, e.message); }
      }
    }
  }
  console.log('📁 مجلد البيانات الدائم:', want);
  return want;
}
const DATA_DIR = resolveDataDir();
const ENC_KEY_FILE = path.join(DATA_DIR, 'enc.key');
const ACCOUNTS_FILE = path.join(DATA_DIR, 'accounts.json');
// 🪞 نسخة مرآة بجانب server.js لكل ملف بيانات (حسابات/مفتاح تشفير/تداول تلقائي): لو ضاع أحد المكانين (رفع ملفات، مسح مجلد،
// نقل التطبيق) يستعيدها السيرفر تلقائيًا من المكان الآخر عند التشغيل. الاستعادة فقط لو الأصلي مفقود أو فاضي.
function mirrorPathOf(file) { return path.join(__dirname, path.basename(file)); }
function mirrorCopy(file) {
  try { const m = mirrorPathOf(file); if (path.resolve(m) !== path.resolve(file) && fs.existsSync(file)) { fs.copyFileSync(file, m); try { fs.chmodSync(m, 0o600); } catch {} } }
  catch (e) { /* المرآة اختيارية — لا نكسر الحفظ الأصلي */ }
}
function mirrorRestore(file) {
  try {
    const m = mirrorPathOf(file);
    if (path.resolve(m) === path.resolve(file) || !fs.existsSync(m)) return;
    const missing = !fs.existsSync(file) || fs.statSync(file).size <= 2;
    if (missing && fs.statSync(m).size > 2) { fs.copyFileSync(file === m ? m : m, file); try { fs.chmodSync(file, 0o600); } catch {} console.log('♻️ استُعيد', path.basename(file), 'من النسخة الاحتياطية بجانب server.js'); }
  } catch (e) { console.error('تعذّرت استعادة', file, e.message); }
}
mirrorRestore(ENC_KEY_FILE); mirrorRestore(ACCOUNTS_FILE);

function loadOrCreateEncKey() {
  try {
    const hex = fs.readFileSync(ENC_KEY_FILE, 'utf8').trim();
    if (hex.length === 64) return Buffer.from(hex, 'hex');
  } catch (err) { /* ما فيه ملف بعد — نولّد وحدة جديدة */ }
  const key = crypto.randomBytes(32);
  try { fs.writeFileSync(ENC_KEY_FILE, key.toString('hex'), { mode: 0o600 }); mirrorCopy(ENC_KEY_FILE); }
  catch (err) { console.error('⚠️ تعذر حفظ ملف مفتاح التشفير — المفاتيح المحفوظة ستضيع عند إعادة التشغيل:', err.message); }
  return key;
}
const ACCOUNTS_ENC_KEY = loadOrCreateEncKey();

function encryptSecret(text) {
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv('aes-256-gcm', ACCOUNTS_ENC_KEY, iv);
  const enc = Buffer.concat([cipher.update(text, 'utf8'), cipher.final()]);
  return Buffer.concat([iv, cipher.getAuthTag(), enc]).toString('base64');
}
function decryptSecret(b64) {
  const buf = Buffer.from(b64, 'base64');
  const iv = buf.subarray(0, 12), tag = buf.subarray(12, 28), enc = buf.subarray(28);
  const decipher = crypto.createDecipheriv('aes-256-gcm', ACCOUNTS_ENC_KEY, iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(enc), decipher.final()]).toString('utf8');
}
function hashPin(pin, saltHex) { return crypto.scryptSync(pin, saltHex, 32).toString('hex'); }

let accounts = {}; // email (lowercase) -> { saltHex, pinHash, apiKeyEnc, apiSecretEnc, createdAt }
function loadAccounts() {
  try { accounts = JSON.parse(fs.readFileSync(ACCOUNTS_FILE, 'utf8')); } catch (err) { accounts = {}; }
}
function saveAccounts() {
  try { fs.writeFileSync(ACCOUNTS_FILE, JSON.stringify(accounts, null, 2), { mode: 0o600 }); mirrorCopy(ACCOUNTS_FILE); }
  catch (err) { console.error('⚠️ فشل حفظ ملف الحسابات:', err.message); }
}
loadAccounts();
mirrorCopy(ENC_KEY_FILE); mirrorCopy(ACCOUNTS_FILE);

const wsAccount = new Map(); // اتصال WebSocket -> إيميل الحساب المرتبط فيه (أو بدون ربط = مفتاحك الافتراضي)

function sweepSessions() {
  const now = Date.now();
  for (const [sid, lastSeen] of activeSessions) {
    if (now - lastSeen > SESSION_TTL_MS) activeSessions.delete(sid);
  }
}
setInterval(sweepSessions, 60 * 1000);

function parseCookies(req) {
  const header = req.headers.cookie;
  const out = {};
  if (!header) return out;
  for (const part of header.split(';')) {
    const idx = part.indexOf('=');
    if (idx === -1) continue;
    out[part.slice(0, idx).trim()] = decodeURIComponent(part.slice(idx + 1).trim());
  }
  return out;
}

function checkBasicAuth(req) {
  const header = req.headers.authorization || '';
  if (!header.startsWith('Basic ')) return false;
  let decoded;
  try { decoded = Buffer.from(header.slice(6), 'base64').toString('utf8'); } catch { return false; }
  const idx = decoded.indexOf(':');
  if (idx === -1) return false;
  const user = decoded.slice(0, idx), pass = decoded.slice(idx + 1);
  const safeEqual = (a, b) => {
    const bufA = Buffer.from(a), bufB = Buffer.from(b);
    if (bufA.length !== bufB.length) return false;
    return crypto.timingSafeEqual(bufA, bufB);
  };
  return safeEqual(user, APP_USER) && safeEqual(pass, APP_PASS);
}

function authMiddleware(req, res, next) {
  if (!checkBasicAuth(req)) {
    res.set('WWW-Authenticate', 'Basic realm="Crypto Dashboard"');
    return res.status(401).send('كلمة المرور مطلوبة للدخول إلى هذه اللوحة.');
  }
  next();
}

function sessionLimitMiddleware(req, res, next) {
  sweepSessions();
  const cookies = parseCookies(req);
  const existingSid = cookies.sid;
  const isKnown = existingSid && activeSessions.has(existingSid);
  if (!isKnown && activeSessions.size >= MAX_USERS) {
    return res.status(503).send(`الموقع ممتلئ حاليًا (الحد الأقصى ${MAX_USERS} مستخدم متزامن).`);
  }
  const sid = isKnown ? existingSid : crypto.randomBytes(16).toString('hex');
  if (!isKnown) res.cookie('sid', sid, { httpOnly: true, sameSite: 'lax', maxAge: SESSION_TTL_MS });
  activeSessions.set(sid, Date.now());
  req.sid = sid;
  next();
}

if (AUTH_ENABLED) {
  app.use(authMiddleware);
  app.use(sessionLimitMiddleware);
}

// REST
app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/favicon.ico', (_req, res) => res.status(204).end());
app.get('/api/symbols', (_req, res) => res.json({ symbols: SYMBOLS, intervals: INTERVALS }));
// كل مربعات البحث الثلاثة تقترح الآن من كامل حوض الـ~211 عملة (مو ثلث ثابت لكل مربع كما كان سابقًا)
app.get('/api/explosion-groups', (_req, res) => res.json({ groups: [SCAN_POOL, SCAN_POOL, SCAN_POOL] }));
// عدد الزوار المتصلين الآن بلوحة التحليل (كل اتصال WebSocket حي = متصفح مفتوح على الصفحة)
app.get('/api/visitor-count', (_req, res) => {
  let count = 0;
  for (const client of wss.clients) if (client.readyState === WebSocket.OPEN) count++;
  res.json({ count });
});

let marketCache = { data: null, ts: 0 };
const MARKET_CACHE_MS = 60 * 1000;

app.get('/api/market-overview', async (_req, res) => {
  const now = Date.now();
  if (marketCache.data && now - marketCache.ts < MARKET_CACHE_MS) return res.json(marketCache.data);

  const result = { fearGreed: null, btcDominance: null, totalMarketCap: null, marketCapChange24h: null, totalVolume24h: null, fundingRate: null, openInterest: null, updatedAt: new Date().toISOString(), errors: [] };
  try { const r = await axios.get('https://api.alternative.me/fng/?limit=1', { timeout: 8000 }); const d = r.data?.data?.[0]; if (d) result.fearGreed = { value: Number(d.value), label: d.value_classification }; } catch (e) { result.errors.push('fearGreed'); }
  try { const r = await axios.get('https://api.coingecko.com/api/v3/global', { timeout: 8000, headers: { 'User-Agent': 'Mozilla/5.0' } }); const d = r.data?.data; if (d) { result.btcDominance = d.market_cap_percentage?.btc ?? null; result.totalMarketCap = d.total_market_cap?.usd ?? null; result.totalVolume24h = d.total_volume?.usd ?? null; result.marketCapChange24h = d.market_cap_change_percentage_24h_usd ?? null; } } catch (e) { result.errors.push('coingecko'); }
  try { const r = await axios.get('https://contract.mexc.com/api/v1/contract/funding_rate/BTC_USDT', { timeout: 8000 }); const fr = r.data?.data?.fundingRate; if (fr != null) result.fundingRate = Number(fr) * 100; } catch (e) { result.errors.push('fundingRate'); }
  try { const r = await axios.get('https://contract.mexc.com/api/v1/contract/open_interest/BTC_USDT', { timeout: 8000 }); const oi = r.data?.data?.holdVol ?? r.data?.data?.amount; if (oi != null) result.openInterest = Number(oi); } catch (e) { result.errors.push('openInterest'); }
  marketCache = { data: result, ts: now };
  res.json(result);
});

const TWELVE_DATA_KEY = process.env.TWELVE_DATA_KEY || '';
let macroCache = { data: null, ts: 0 };
const MACRO_CACHE_MS = 60 * 1000;

async function computeDxyProxy() {
  const pairs = ['EUR/USD','USD/JPY','GBP/USD','USD/CAD','USD/SEK','USD/CHF'];
  const r = await axios.get('https://api.twelvedata.com/price', { params: { symbol: pairs.join(','), apikey: TWELVE_DATA_KEY }, timeout: 8000 });
  const d = r.data;
  const get = (sym) => { const v = pairs.length === 1 ? d : d[sym]; if (!v || !v.price) throw new Error(v?.message || `تعذّر جلب ${sym}`); return parseFloat(v.price); };
  const eurusd = get('EUR/USD'), usdjpy = get('USD/JPY'), gbpusd = get('GBP/USD'), usdcad = get('USD/CAD'), usdsek = get('USD/SEK'), usdchf = get('USD/CHF');
  return 50.14348112 * Math.pow(eurusd, -0.576) * Math.pow(usdjpy, 0.136) * Math.pow(gbpusd, -0.119) * Math.pow(usdcad, 0.091) * Math.pow(usdsek, 0.042) * Math.pow(usdchf, 0.036);
}

app.get('/api/macro-overview', async (_req, res) => {
  const now = Date.now();
  if (macroCache.data && now - macroCache.ts < MACRO_CACHE_MS) return res.json(macroCache.data);
  const result = { dxy: null, news: null, dxyError: null, errors: [] };
  if (!TWELVE_DATA_KEY) { result.errors.push('no_twelvedata_key'); result.dxyError = 'لا يوجد مفتاح TWELVE_DATA_KEY'; }
  else { try { result.dxy = await computeDxyProxy(); } catch (e) { result.errors.push('dxy'); result.dxyError = e.response?.data?.message || e.message; } }
  macroCache = { data: result, ts: now };
  res.json(result);
});

async function fetchBinanceFutures(symbol) {
  const out = {};
  try { const r = await axios.get(`https://fapi.binance.com/fapi/v1/premiumIndex?symbol=${symbol}`, { timeout: 8000 }); if (r.data?.lastFundingRate != null) out.fundingRate = Number(r.data.lastFundingRate) * 100; } catch (e) {}
  try { const r = await axios.get(`https://fapi.binance.com/fapi/v1/openInterest?symbol=${symbol}`, { timeout: 8000 }); if (r.data?.openInterest != null) out.openInterest = Number(r.data.openInterest); } catch (e) {}
  try { const r = await axios.get(`https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=${symbol}&period=5m&limit=1`, { timeout: 8000 }); const d = r.data?.[0]; if (d) { out.longShortRatio = Number(d.longShortRatio); out.longAccountPct = Number(d.longAccount) * 100; out.shortAccountPct = Number(d.shortAccount) * 100; } } catch (e) {}
  return Object.keys(out).length ? out : null;
}

async function fetchOkxFutures(symbol) {
  const base = symbol.replace(/USDT$/, ''); const instId = `${base}-USDT-SWAP`; const out = {};
  try { const r = await axios.get(`https://www.okx.com/api/v5/public/funding-rate?instId=${instId}`, { timeout: 8000 }); const fr = r.data?.data?.[0]?.fundingRate; if (fr != null) out.fundingRate = Number(fr) * 100; } catch (e) {}
  try { const r = await axios.get(`https://www.okx.com/api/v5/public/open-interest?instId=${instId}`, { timeout: 8000 }); const oi = r.data?.data?.[0]?.oi; if (oi != null) out.openInterest = Number(oi); } catch (e) {}
  try { const r = await axios.get(`https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=${base}&period=5m`, { timeout: 8000 }); const d = r.data?.data?.[0]; if (d && d[1] != null) out.longShortRatio = Number(d[1]); } catch (e) {}
  return Object.keys(out).length ? out : null;
}

async function fetchMexcFutures(symbol) {
  const contractSymbol = symbol.replace(/USDT$/, '_USDT'); const out = {};
  try { const r = await axios.get(`https://contract.mexc.com/api/v1/contract/funding_rate/${contractSymbol}`, { timeout: 8000 }); const fr = r.data?.data?.fundingRate; if (fr != null) out.fundingRate = Number(fr) * 100; } catch (e) {}
  try { const r = await axios.get(`https://contract.mexc.com/api/v1/contract/open_interest/${contractSymbol}`, { timeout: 8000 }); const oi = r.data?.data?.holdVol ?? r.data?.data?.amount; if (oi != null) out.openInterest = Number(oi); } catch (e) {}
  return Object.keys(out).length ? out : null;
}

app.get('/api/futures-zone', async (req, res) => {
  const symbol = (req.query.symbol || 'BTCUSDT').toUpperCase();
  const [binance, okx, mexc] = await Promise.all([
    fetchBinanceFutures(symbol).catch(() => null),
    fetchOkxFutures(symbol).catch(() => null),
    fetchMexcFutures(symbol).catch(() => null),
  ]);
  const exchanges = {};
  if (binance) exchanges.binance = binance;
  if (okx) exchanges.okx = okx;
  if (mexc) exchanges.mexc = mexc;
  const fundingRates = Object.values(exchanges).map(e => e.fundingRate).filter(v => v != null);
  const openInterests = Object.values(exchanges).map(e => e.openInterest).filter(v => v != null);
  const longShortRatios = Object.values(exchanges).map(e => e.longShortRatio).filter(v => v != null);
  const aggregate = {
    avgFundingRate: fundingRates.length ? fundingRates.reduce((a,b)=>a+b,0)/fundingRates.length : null,
    fundingRateSpread: fundingRates.length >= 2 ? Math.max(...fundingRates)-Math.min(...fundingRates) : null,
    totalOpenInterest: openInterests.length ? openInterests.reduce((a,b)=>a+b,0) : null,
    avgLongShortRatio: longShortRatios.length ? longShortRatios.reduce((a,b)=>a+b,0)/longShortRatios.length : null,
    exchangeCount: Object.keys(exchanges).length,
  };
  res.json({ fundingRate: aggregate.avgFundingRate, openInterest: aggregate.totalOpenInterest, exchanges, aggregate });
});

// WebSocket Server
const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  if (AUTH_ENABLED && !checkBasicAuth(req)) { ws.close(4001, 'Unauthorized'); return; }
  sweepSessions();
  const cookies = parseCookies(req);
  const sid = cookies.sid;
  const isKnown = sid && activeSessions.has(sid);
  if (AUTH_ENABLED && !isKnown && activeSessions.size >= MAX_USERS) { ws.close(4002, 'SERVER_FULL'); return; }
  if (sid) activeSessions.set(sid, Date.now());

  if (explosionRanking.length) {
    ws.send(JSON.stringify({ type: 'explosion_scan', ranking: explosionRanking }));
  }
  ws.send(botStatusPayload(ws));

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    try { // 🛡️ أي خطأ برسالة وحدة ما يوقف الاتصال ولا السيرفر — بس يتسجّل ونكمل عادي
    // 🔒 حماية: أي أمر يحرّك فلوس أو يتحكّم بالبوت (شراء/بيع يدوي أو تلقائي، تشغيل/إيقاف، تغيير نسب) ممنوع
    // إلا لمن ربط حسابه بالإيميل والرمز ومفتاح Binance API الخاص فيه. هذا يمنع أي شخص ثاني يدخل اللوحة
    // (حتى لو يعرف كلمة مرور الدخول العامة) من لمس حسابك الافتراضي أو يغيّر نسبك بدون ما يربط حسابه هو.
    const ACCOUNT_REQUIRED_TYPES = new Set([
      'bot_toggle', 'bot_set_trade_size', 'bot_set_sell_amount', 'bot_set_take_profit', 'bot_set_max_positions',
      'bot_set_manual_symbol', 'manual_buy_now', 'manual_sell_now', 'auto_trade_set', 'auto_anchor_set', 'auto_problem_clear', 'auto_audit',
      'manual_bot_toggle', 'bot_manual_close', 'bot_cancel_pending',
    ]);
    if (ACCOUNT_REQUIRED_TYPES.has(msg.type) && getAccountKeyForWs(ws) === '__default__') {
      ws.send(JSON.stringify({ type: 'error', message: '🔒 لازم تربط حسابك (إيميل + رمز + مفتاح Binance API الخاص فيك) من أعلى اللوحة أول — قبل ما تقدر تشغّل البوت أو تغيّر أي إعداد. هذا يحمي حسابك من أي شخص ثاني يدخل اللوحة.' }));
      return;
    }
    if (msg.type === 'subscribe') {
      const { symbol, interval } = msg;
      const validSymbol = typeof symbol === 'string' && /^[A-Z0-9]{2,20}USDT$/.test(symbol);
      if (!validSymbol || !INTERVALS.includes(interval)) return ws.send(JSON.stringify({ type: 'error', message: 'رمز أو فترة غير صحيحة' }));
      if (sid) activeSessions.set(sid, Date.now());
      clientSubs.set(ws, { symbol, interval });
      await ensureStream(symbol, interval);
      await ensureStream(symbol, '15m');
      sendSnapshot(ws, symbol, interval);
      for (const iv of INTERVALS) { if (iv !== interval && iv !== '15m') ensureStream(symbol, iv).then(() => broadcastMtfUpdate(symbol, true)); }
      broadcastMtfUpdate(symbol, true);
    }
    else if (msg.type === 'bot_toggle') {
      if (msg.enabled) {
        if (!(BINANCE_API_KEY && BINANCE_API_SECRET)) {
          ws.send(JSON.stringify({ type: 'error', message: 'لا يوجد مفتاح API معرّف لمنصة Binance — أضِف متغيرات البيئة أولاً' }));
          return;
        }
        const symbol = botState.manualSymbol || 'BTCUSDT';
        botState.manualSymbol = symbol;
        try {
          await ensureStream(symbol, SCAN_INTERVAL).catch(() => {});
          await cancelAllOpenOrders(symbol);
          const price = await getCurrentPrice(symbol);
          botState.enabled = true;
          botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'error', message: `🤖 تشغيل بوت الشبكة على ${symbol} — السعر الحالي ${price}` });
          await placeGridOrders(symbol, price);
        } catch (err) {
          botState.enabled = false;
          const detail = err.response?.data?.msg || err.message;
          ws.send(JSON.stringify({ type: 'error', message: 'فشل تشغيل البوت: ' + detail }));
        }
      } else {
        const symbol = botState.manualSymbol || 'BTCUSDT';
        botState.enabled = false;
        try { await cancelAllOpenOrders(symbol); } catch {}
        botState.pendingOrders = {};
        botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'error', message: '⏹️ تم إيقاف البوت وإلغاء كل أوامر الشبكة المفتوحة' });
      }
      botState.tradeLog = botState.tradeLog.slice(0, 50);
      broadcastBotStatus();
    }
    else if (msg.type === 'bot_set_trade_size') {
      // بوت الشبكة: هذه القيمة = مبلغ الصفقة بالـ USDT لكل مستوى في الشبكة
      const v = parseFloat(msg.tradeSizeUsdt);
      if (v > 0 && v <= 100000) { botState.tradeSizeUsdt = v; broadcastBotStatus(); }
    }
    else if (msg.type === 'bot_set_sell_amount') {
      // مبلغ البيع (USDT) — محفوظ حاليًا للعرض فقط؛ زر "بيع الآن" لسه يبيع كامل كمية الصفقة المفتوحة
      const v = parseFloat(msg.sellAmountUsdt);
      if (v > 0 && v <= 100000) { botState.sellAmountUsdt = v; broadcastBotStatus(); }
    }
    else if (msg.type === 'bot_set_take_profit') {
      // بوت الشبكة: هذه القيمة = نسبة المسافة بين كل مستوى شراء/بيع والمستوى الذي يليه (GRID_STEP_PERCENT)
      const v = parseFloat(msg.takeProfitPercent);
      if (v > 0 && v <= 20) { botState.takeProfitPercent = v; broadcastBotStatus(); }
    }
    else if (msg.type === 'bot_set_max_positions') {
      // بوت الشبكة: هذه القيمة = عدد مستويات الشراء (وعدد مستويات البيع، بنفس العدد) في الشبكة (GRID_SIZE)
      const v = parseInt(msg.maxConcurrentPositions, 10);
      if (v >= 1 && v <= 20) { botState.maxConcurrentPositions = v; broadcastBotStatus(); }
    }
    else if (msg.type === 'bot_set_manual_symbol') {
      // اختيار عملة الشبكة — يحصل تلقائيًا لما يضغط المستخدم على أحد مربعات "أقوى 3 عملات" فوق.
      // لتفادي أوامر شبكة يتيمة، يُمنع تغيير العملة والبوت شغّال — لازم إيقافه أولًا.
      if (botState.enabled) {
        ws.send(JSON.stringify({ type: 'error', message: 'أوقف البوت أولًا قبل تغيير عملة الشبكة' }));
        return;
      }
      const raw = (msg.symbol || '').toString().trim().toUpperCase();
      if (!raw) { botState.manualSymbol = null; broadcastBotStatus(); }
      else if (/^[A-Z0-9]{2,20}USDT$/.test(raw)) {
        botState.manualSymbol = raw;
        ensureStream(raw, SCAN_INTERVAL).catch(() => {});
        broadcastBotStatus();
      } else {
        ws.send(JSON.stringify({ type: 'error', message: 'رمز العملة غير صحيح — لازم ينتهي بـ USDT' }));
      }
    }
    else if (msg.type === 'manual_buy_now') {
      // زر "شراء الآن" — شراء Market فوري على الرمز المعروض حاليًا بالواجهة، بمفتاح الحساب المرتبط
      // بهذا الاتصال لو موجود (كل صديق يتداول بمفتاحه هو)، وإلا بمفتاحك الافتراضي
      const accountKey = getAccountKeyForWs(ws);
      const creds = getCredsForAccountKey(accountKey);
      if (!(creds.apiKey && creds.apiSecret)) {
        ws.send(JSON.stringify({ type: 'error', message: 'ما فيه مفتاح Binance API مربوط — أضف حسابك من الإعدادات تحت أولًا' }));
        return;
      }
      const raw = (msg.symbol || '').toString().trim().toUpperCase();
      if (!/^[A-Z0-9]{2,20}USDT$/.test(raw)) {
        ws.send(JSON.stringify({ type: 'error', message: 'رمز العملة غير صحيح' }));
        return;
      }
      executeManualBuy(raw, accountKey, creds).then(() => {
        broadcastBotStatus();
        ws.send(JSON.stringify({ type: 'trade_result', side: 'BUY', symbol: raw }));
      }).catch((err) => {
        const detail = err.response?.data?.msg || err.message;
        ws.send(JSON.stringify({ type: 'error', message: 'فشل الشراء: ' + detail }));
      });
    }
    else if (msg.type === 'manual_sell_now') {
      // زر "بيع الآن" — يبيع فورًا (Market) أقرب صفقة شراء يدوي مفتوحة على هذا الرمز، ملغيًا أمر
      // البيع المحدد المسبق (Limit) لو كان موجود، بدل ما ينتظر يوصل سعره
      const accountKey = getAccountKeyForWs(ws);
      const creds = getCredsForAccountKey(accountKey);
      if (!(creds.apiKey && creds.apiSecret)) {
        ws.send(JSON.stringify({ type: 'error', message: 'ما فيه مفتاح Binance API مربوط — أضف حسابك من الإعدادات تحت أولًا' }));
        return;
      }
      const raw = (msg.symbol || '').toString().trim().toUpperCase();
      if (!/^[A-Z0-9]{2,20}USDT$/.test(raw)) {
        ws.send(JSON.stringify({ type: 'error', message: 'رمز العملة غير صحيح' }));
        return;
      }
      executeManualSell(raw, accountKey, creds).then(() => {
        broadcastBotStatus();
        ws.send(JSON.stringify({ type: 'trade_result', side: 'SELL', symbol: raw }));
      }).catch((err) => {
        const detail = err.response?.data?.msg || err.message;
        ws.send(JSON.stringify({ type: 'error', message: 'فشل البيع: ' + detail }));
      });
    }
    else if (msg.type === 'auto_trade_set') {
      // مربع النسبة: نسبة فارغة = إيقاف التلقائي (يبقى يدوي). نسبة موجودة = شراء عند نزول النسبة من أعلى سعر، وبيع عند ارتفاع نفس النسبة فوق سعر الشراء.
      const accountKey = getAccountKeyForWs(ws);
      const cur = autoTradeByAccount[accountKey];
      if (msg.pct === null || msg.pct === undefined || msg.pct === '') {
        if (cur) { delete autoTradeByAccount[accountKey]; saveAutoTrade(); }
        broadcastBotStatus();
        return;
      }
      const pct = parseFloat(msg.pct);
      if (!(pct >= 0.1 && pct <= 50)) {
        ws.send(JSON.stringify({ type: 'error', message: 'النسبة لازم تكون بين 0.1 و 50' }));
        return;
      }
      const creds = getCredsForAccountKey(accountKey);
      if (!(creds.apiKey && creds.apiSecret)) {
        ws.send(JSON.stringify({ type: 'error', message: 'ما فيه مفتاح Binance API مربوط — أضف حسابك من الإعدادات تحت أولًا' }));
        return;
      }
      const symbol = (msg.symbol || '').toString().trim().toUpperCase();
      if (!/^[A-Z0-9]{2,20}USDT$/.test(symbol)) {
        ws.send(JSON.stringify({ type: 'error', message: 'رمز العملة غير صحيح' }));
        return;
      }
      if (!(botState.tradeSizeUsdt >= 5)) {
        ws.send(JSON.stringify({ type: 'error', message: 'مبلغ الشراء أقل من 5 USDT (الحد الأدنى في Binance)' }));
        return;
      }
      // مربع العدد (بجانب مربع النسبة): كم مرة يشتري بالتدرّج قبل ما ينتظر البيع فقط. فاضي أو 1 = شراء مرة وحدة (نفس القديم تمامًا).
      let levels = 1;
      if (msg.levels !== null && msg.levels !== undefined && msg.levels !== '') {
        levels = parseInt(msg.levels, 10);
        if (!Number.isInteger(levels) || levels < 1 || levels > 10) {
          ws.send(JSON.stringify({ type: 'error', message: 'عدد مرات الشراء لازم يكون رقم صحيح بين 1 و 10' }));
          return;
        }
      }
      // نسبة بيع ثابتة ومنفصلة (اختياري) — لو المستخدم حددها، تُستخدم دائمًا لحساب نقطة البيع فوق متوسط الشراء
      // (مو نسبة آخر مرحلة اشترى فيها). فاضية = يرجع للسلوك التلقائي (نسبة آخر مرحلة اشترى فيها فعليًا).
      let sellPctOverride = null;
      if (msg.sellPct !== null && msg.sellPct !== undefined && msg.sellPct !== '') {
        const sp = parseFloat(msg.sellPct);
        if (!(sp >= 0.1 && sp <= 50)) {
          ws.send(JSON.stringify({ type: 'error', message: 'نسبة البيع لازم تكون بين 0.1 و 50' }));
          return;
        }
        sellPctOverride = sp;
      }
      // نسبة مخصّصة لكل مرحلة شراء (اختياري) — المرحلة 1 دائمًا تستخدم مربع النسبة الرئيسي فقط، وتبدأ القائمة من المرحلة 2.
      // أي مرحلة ما لها نسبة مخصّصة هنا تستخدم النسبة الرئيسية تلقائيًا.
      let levelPct = {};
      if (msg.levelPct && typeof msg.levelPct === 'object') {
        for (const [k, v] of Object.entries(msg.levelPct)) {
          const lvl = parseInt(k, 10);
          if (!Number.isInteger(lvl) || lvl < 2 || lvl > 10) continue; // المرحلة 1 أو أرقام غير صالحة — نتجاهلها بصمت
          if (v === null || v === undefined || v === '') continue; // فاضي = تلقائي، ما نخزّن شي
          const vv = parseFloat(v);
          if (!(vv >= 0.1 && vv <= 50)) {
            ws.send(JSON.stringify({ type: 'error', message: `نسبة المرحلة ${lvl} لازم تكون بين 0.1 و 50` }));
            return;
          }
          levelPct[lvl] = vv;
        }
      }
      // 🛑 وقف الخسارة (مربع واحد): { pct, minutes } — النسبة = النزول تحت متوسط الشراء، الدقائق = مدة مراقبة الارتداد قبل البيع.
      // 🔄 إعادة الضبط بدون بيع (مربع ثاني): { pct, minutes } — أي واحد منهما كافٍ (النسبة أو الوقت، أيهما أولًا).
      // الخانتين فاضيتين = الميزة معطّلة. لو الواجهة ما أرسلت الحقل أصلًا (صفحة قديمة بالكاش) نترك المحفوظ كما هو.
      const isEmptyV = (x) => x === null || x === undefined || x === '';
      const hasShape = (o) => o && typeof o === 'object' && ('pct' in o || 'minutes' in o);
      const anchorTolProvided = 'anchorTol' in msg;
      let anchorTol = 0;
      if (anchorTolProvided && !isEmptyV(msg.anchorTol)) {
        anchorTol = parseFloat(msg.anchorTol);
        if (!(anchorTol >= 0 && anchorTol <= 50)) { ws.send(JSON.stringify({ type: 'error', message: 'سماحية المرساة: لازم بين 0 و 50%' })); return; }
      }
      const stopLossProvided = hasShape(msg.stopLoss);
      const softResetProvided = hasShape(msg.softReset);
      let stopLoss = null, softReset = null;
      if (stopLossProvided && !(isEmptyV(msg.stopLoss.pct) && isEmptyV(msg.stopLoss.minutes))) {
        const sp = parseFloat(msg.stopLoss.pct), sm = parseInt(msg.stopLoss.minutes, 10);
        if (!(sp >= 0.1 && sp <= 90) || !(sm >= 1 && sm <= 720)) {
          ws.send(JSON.stringify({ type: 'error', message: 'وقف الخسارة: النسبة بين 0.1 و 90، والدقائق بين 1 و 720 — وعبّئ الخانتين معًا' }));
          return;
        }
        stopLoss = { pct: sp, minutes: sm };
      }
      if (softResetProvided && !(isEmptyV(msg.softReset.pct) && isEmptyV(msg.softReset.minutes))) {
        let rp = null, rm = null;
        if (!isEmptyV(msg.softReset.pct)) { rp = parseFloat(msg.softReset.pct); if (!(rp >= 0.1 && rp <= 90)) { ws.send(JSON.stringify({ type: 'error', message: 'إعادة الضبط: النسبة لازم تكون بين 0.1 و 90' })); return; } }
        if (!isEmptyV(msg.softReset.minutes)) { rm = parseInt(msg.softReset.minutes, 10); if (!(rm >= 1 && rm <= 1440)) { ws.send(JSON.stringify({ type: 'error', message: 'إعادة الضبط: الدقائق لازم تكون بين 1 و 1440' })); return; } }
        softReset = { pct: rp, minutes: rm };
      }
      if (cur && cur.state === 'holding' && cur.symbol !== symbol) {
        ws.send(JSON.stringify({ type: 'error', message: `عندك صفقة تلقائية مفتوحة على ${cur.symbol} — بعها أولًا (زر بيع) قبل تغيير العملة` }));
        return;
      }
      if (cur && cur.symbol === symbol) {
        cur.pct = pct; cur.amountUsdt = botState.tradeSizeUsdt; cur.levels = levels; cur.levelPct = levelPct; cur.sellPct = sellPctOverride; if (stopLossProvided) cur.stopLoss = stopLoss; if (softResetProvided) cur.softReset = softReset; if (anchorTolProvided) cur.anchorTol = anchorTol; // نفس العملة: نحدّث الإعدادات فقط بدون تصفير الحالة
      } else {
        let price = 0;
        try { price = await getCurrentPrice(symbol); } catch {}
        // عند الحفظ: نسجّل سعر الحفظ كـ"مرساة" — إذا ارتفع السعر بعدها نتجاهل حساب النسبة تماماً حتى يرجع السعر لهذه المرساة
        // (حتى لا يشتري البوت في منتصف صعود قوي)، وإذا نزل مباشرة بعد الحفظ تعمل النسبة عادي من أول لحظة.
        autoTradeByAccount[accountKey] = {
          pct, symbol, amountUsdt: botState.tradeSizeUsdt, levels, levelPct, sellPct: sellPctOverride, state: 'waiting_buy',
          peak: price, anchor: price, armed: false, legs: [], lastBuyPrice: null,
          stopLoss: stopLossProvided ? stopLoss : null, softReset: softResetProvided ? softReset : null, stopLossWatch: null,
          resetBase: null, resetCount: 0, holdSince: null, anchorTol: anchorTolProvided ? anchorTol : 0, overshot: false, buyRetryAt: 0,
          fails: 0, pausedUntil: 0, busy: false,
        };
      }
      saveAutoTrade();
      broadcastBotStatus();
    }
    else if (msg.type === 'auto_audit') {
      const accountKey = getAccountKeyForWs(ws);
      const a = autoTradeByAccount[accountKey];
      if (!a || !(a.legs || []).length || a.auditing) { ws.send(JSON.stringify({ type: 'auto_audit_result', symbol: a ? a.symbol : null, empty: true })); return; }
      a.auditing = true;
      try {
        const credsA = getCredsForAccountKey(accountKey);
        const rep = await auditAutoLegs(accountKey, a, credsA);
        let freeUsdt = null, lockedUsdt = null; try { const bal = await getUsdtBalances(credsA); freeUsdt = bal.free; lockedUsdt = bal.locked; } catch (e) { /* مفتاح بلا صلاحية قراءة، أو خطأ مؤقت */ }
        saveAutoTrade(); broadcastBotStatus();
        ws.send(JSON.stringify({ type: 'auto_audit_result', ...rep, totalCostNow: legsTotalCost(a), freeUsdt, lockedUsdt, nextAmount: a.amountUsdt,
          lastError: a.problem ? { kind: a.problem.kind, raw: a.problem.detail, context: a.problem.context, fails: a.problem.fails } : null }));
      } catch (err) { ws.send(JSON.stringify({ type: 'error', message: 'تعذّر التدقيق: ' + (err && err.message || err) })); }
      finally { a.auditing = false; }
    }
    else if (msg.type === 'auto_problem_clear') {
      // المستخدم مسح التنبيه من اللوحة: نصفّر المشكلة وعدّاد الفشل والانتظار، فيرجع البوت يشتغل ويحاول مباشرة بدون أي تأخير
      const accountKey = getAccountKeyForWs(ws);
      const a = autoTradeByAccount[accountKey];
      if (a) {
        const had = !!a.problem;
        a.problem = null; a.fails = 0; a.pausedUntil = 0; a.buyRetryAt = 0;
        saveAutoTrade();
        if (had) notifyAccount(accountKey, { type: 'auto_problem', symbol: a.symbol, active: false });
        broadcastBotStatus();
      }
    }
    else if (msg.type === 'auto_anchor_set') {
      // تعديل يدوي لسعر المرساة 🎯 (المربع الأصفر فوق): يُسمح فقط قبل أول شراء (انتظار النزول)، لأنه بعد
      // أول شراء المربع يعرض متوسط الشراء مو المرساة. لو ما لمسه المستخدم يضل زي ما كان بدون أي تغيير.
      const accountKey = getAccountKeyForWs(ws);
      const a = autoTradeByAccount[accountKey];
      if (!a) { ws.send(JSON.stringify({ type: 'error', message: 'ما فيه تداول تلقائي شغال حاليًا' })); return; }
      if (msg.symbol && a.symbol !== msg.symbol.toString().trim().toUpperCase()) { ws.send(JSON.stringify({ type: 'error', message: 'العملة تغيّرت — حدّث الصفحة وحاول مرة ثانية' })); return; }
      if (a.state !== 'waiting_buy') { ws.send(JSON.stringify({ type: 'error', message: 'ما تقدر تعدّل المرساة أثناء إمساك صفقة مفتوحة' })); return; }
      const anchor = parseFloat(msg.anchor);
      if (!(anchor > 0)) { ws.send(JSON.stringify({ type: 'error', message: 'سعر المرساة غير صحيح' })); return; }
      a.anchor = anchor;
      let price = anchor;
      try { price = await getCurrentPrice(a.symbol); } catch {}
      const limitM = anchor * (1 + (a.anchorTol > 0 ? a.anchorTol : 0) / 100);
      if (price <= limitM) { a.armed = true; a.peak = Math.max(anchor, price); a.overshot = false; } else { a.armed = false; a.peak = anchor; a.overshot = true; }
      saveAutoTrade();
      broadcastBotStatus();
    }
    else if (msg.type === 'manual_bot_toggle') {
      manualBotState.enabled = !!msg.enabled;
      botState.tradeLog.unshift({ time: Date.now(), symbol: '', type: 'error', message: manualBotState.enabled ? '🟢 تفعيل بوت البيع التلقائي (للشراء اليدوي)' : '⏹️ إيقاف بوت البيع التلقائي (للشراء اليدوي)' });
      botState.tradeLog = botState.tradeLog.slice(0, 50);
      broadcastBotStatus();
    }
    else if (msg.type === 'bot_manual_close' || msg.type === 'bot_cancel_pending') {
      // خاصة ببوت الشبكة فقط — الشبكة تُدار وتُعاد موازنتها تلقائيًا، لا حاجة لإغلاق يدوي
      ws.send(JSON.stringify({ type: 'error', message: 'بوت الشبكة تلقائي بالكامل — لا حاجة لأوامر يدوية عليه.' }));
    }
    else if (msg.type === 'account_connect') {
      const email = (msg.email || '').toString().trim().toLowerCase();
      const pin = (msg.pin || '').toString();
      if (!email || !/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email)) {
        ws.send(JSON.stringify({ type: 'error', message: 'إيميل غير صحيح' })); return;
      }
      if (!pin || pin.length < 4) {
        ws.send(JSON.stringify({ type: 'error', message: 'الرمز لازم يكون 4 خانات على الأقل — يحمي حسابك من أي شخص ثاني يعرف إيميلك بس' })); return;
      }
      const existing = accounts[email];
      if (existing) {
        if (hashPin(pin, existing.saltHex) !== existing.pinHash) {
          ws.send(JSON.stringify({ type: 'error', message: 'الرمز غير صحيح لهذا الإيميل' })); return;
        }
        if (msg.apiKey && msg.apiSecret) {
          existing.apiKeyEnc = encryptSecret(msg.apiKey.toString().trim());
          existing.apiSecretEnc = encryptSecret(msg.apiSecret.toString().trim());
          saveAccounts();
        }
      } else {
        if (!msg.apiKey || !msg.apiSecret) {
          ws.send(JSON.stringify({ type: 'error', message: Object.keys(accounts).length ? 'إيميل جديد — لازم تدخل مفتاح Binance API والـ Secret أول مرة (تأكد أنك كتبت نفس الإيميل المسجّل سابقًا)' : 'السيرفر لا يحتفظ بأي حساب الآن (ملف الحسابات فاضي) — أدخل الإيميل والرمز ومفتاحي Binance مرة واحدة وستُحفظ في مجلد دائم' })); return;
        }
        const saltHex = crypto.randomBytes(16).toString('hex');
        accounts[email] = {
          saltHex, pinHash: hashPin(pin, saltHex),
          apiKeyEnc: encryptSecret(msg.apiKey.toString().trim()),
          apiSecretEnc: encryptSecret(msg.apiSecret.toString().trim()),
          createdAt: Date.now(),
        };
        saveAccounts();
      }
      wsAccount.set(ws, email);
      ws.send(JSON.stringify({ type: 'account_status', email, linked: true }));
    }
    else if (msg.type === 'account_unlink') {
      wsAccount.delete(ws);
      ws.send(JSON.stringify({ type: 'account_status', email: null, linked: false }));
    }
    } catch (err) {
      console.error('⚠️ خطأ أثناء معالجة رسالة من العميل — تجاهلناها واستمرينا:', err && err.stack ? err.stack : err);
      try { ws.send(JSON.stringify({ type: 'error', message: 'صار خطأ بمعالجة الطلب، حاول مرة ثانية' })); } catch {}
    }
  });

  ws.on('close', () => { clientSubs.delete(ws); wsAccount.delete(ws); });
  ws.on('error', () => { clientSubs.delete(ws); wsAccount.delete(ws); });
});

const SCAN_INTERVAL = '15m';
const SCAN_SYMBOLS = SYMBOLS;

let explosionRanking = [];

// حد أدنى لمتوسط السيولة (بالدولار) على فريم 15 دقيقة عشان نستبعد العملات شبه الميتة من الترشيح
const MIN_AVG_QUOTE_VOLUME_15M = 15000;

function computeExplosionScore(candles) {
  if (!candles || candles.length < 80) return null;
  const closes = candles.map(c => c.close);
  const highs = candles.map(c => c.high);
  const lows = candles.map(c => c.low);
  const volumes = candles.map(c => c.volume);
  const n = closes.length;

  const bbArr = BollingerBands.calculate({ period: 20, values: closes, stdDev: 2 });
  if (bbArr.length < 40) return null;
  const bbWidths = bbArr.map(b => b.middle ? (b.upper - b.lower) / b.middle : 0);
  const lastBB = bbArr[bbArr.length - 1];
  const lastWidth = bbWidths[bbWidths.length - 1];
  const widthWindow = bbWidths.slice(-100);
  const minW = Math.min(...widthWindow), maxW = Math.max(...widthWindow);
  const widthPercentile = maxW > minW ? (lastWidth - minW) / (maxW - minW) : 0.5;

  const trArr = [];
  for (let i = 1; i < n; i++) trArr.push(Math.max(highs[i]-lows[i], Math.abs(highs[i]-closes[i-1]), Math.abs(lows[i]-closes[i-1])));
  const atrPeriod = 14;
  if (trArr.length < atrPeriod) return null;
  const atrSeries = [];
  let atrVal = trArr.slice(0, atrPeriod).reduce((a,b)=>a+b,0) / atrPeriod;
  atrSeries.push(atrVal);
  for (let i = atrPeriod; i < trArr.length; i++) { atrVal = (atrVal * (atrPeriod-1) + trArr[i]) / atrPeriod; atrSeries.push(atrVal); }
  const lastAtr = atrSeries[atrSeries.length-1];
  const atrWindow = atrSeries.slice(-60);
  const atrMin = Math.min(...atrWindow), atrMax = Math.max(...atrWindow);
  const atrPercentile = atrMax > atrMin ? (lastAtr - atrMin) / (atrMax - atrMin) : 0.5;

  const ema20Arr = EMA.calculate({ values: closes, period: 20 });
  const lastEma20 = ema20Arr[ema20Arr.length-1];
  const kcUpper = lastEma20 + lastAtr * 1.5;
  const kcLower = lastEma20 - lastAtr * 1.5;
  const squeezeOn = lastBB.upper < kcUpper && lastBB.lower > kcLower;

  const avgVol5 = volumes.slice(-5).reduce((a,b)=>a+b,0) / 5;
  const avgVol50 = volumes.slice(-50).reduce((a,b)=>a+b,0) / 50;
  const volRatio = avgVol50 > 0 ? avgVol5 / avgVol50 : 1;

  let obvVal = 0; const obvArr = [0];
  for (let i = 1; i < n; i++) { if (closes[i] > closes[i-1]) obvVal += volumes[i]; else if (closes[i] < closes[i-1]) obvVal -= volumes[i]; obvArr.push(obvVal); }
  const obvSlice = obvArr.slice(-20);
  const obvSlope = obvSlice[obvSlice.length-1] - obvSlice[0];

  const rsiArr = RSI.calculate({ values: closes, period: 14 });
  const lastRsi = rsiArr.length ? rsiArr[rsiArr.length-1] : null;

  let lastMfi = null;
  { const period = 14; const typicalPrices = closes.map((c,i)=>(highs[i]+lows[i]+c)/3); const rawMF = typicalPrices.map((tp,i)=>tp*volumes[i]); if (n > period) { let posMF=0, negMF=0; for (let i=n-period;i<n;i++){ if (typicalPrices[i] > typicalPrices[i-1]) posMF += rawMF[i]; else if (typicalPrices[i] < typicalPrices[i-1]) negMF += rawMF[i]; } lastMfi = negMF===0 ? 100 : 100 - (100/(1+posMF/negMF)); } }

  let lastCmf = null;
  { const period = 20; const win = candles.slice(-period); let mfvSum=0, volSum=0; for (const c of win) { const range = c.high-c.low; const mfm = range ? ((c.close-c.low)-(c.high-c.close))/range : 0; mfvSum += mfm*c.volume; volSum += c.volume; } lastCmf = volSum>0 ? mfvSum/volSum : 0; }

  const rangeHigh = Math.max(...highs.slice(-50));
  const rangeLow = Math.min(...lows.slice(-50));
  const pricePosition = rangeHigh > rangeLow ? (closes[n-1] - rangeLow) / (rangeHigh - rangeLow) : 0.5;
  const priceThen = closes[n-21] ?? closes[0];
  const recentGainPct = priceThen ? ((closes[n-1] - priceThen) / priceThen) * 100 : 0;

  const obvDir = obvSlope > 0 ? 'up' : obvSlope < 0 ? 'down' : 'neutral';
  const cmfDir = lastCmf > 0.03 ? 'up' : lastCmf < -0.03 ? 'down' : 'neutral';
  let direction = 'neutral';
  if (obvDir !== 'neutral' && obvDir === cmfDir) direction = obvDir;
  else if (obvDir !== 'neutral' && cmfDir === 'neutral') direction = obvDir;
  else if (cmfDir !== 'neutral' && obvDir === 'neutral') direction = cmfDir;

  let score = 0;
  score += (1 - Math.max(0, Math.min(1, widthPercentile))) * 25;
  score += (1 - Math.max(0, Math.min(1, atrPercentile))) * 15;
  score += squeezeOn ? 10 : 0;
  score += Math.max(0, Math.min(1, (volRatio-1)*2)) * 15;
  score += Math.min(1, Math.abs(obvSlope) / (avgVol50*20 || 1)) * 10;
  if (lastMfi != null) { if (lastMfi >= 40 && lastMfi <= 65) score += 10; else if (lastMfi > 85) score -= 15; }
  if (direction !== 'neutral' && cmfDir === direction) score += 8;
  const overextPenalty = pricePosition > 0.8 ? ((pricePosition-0.8)/0.2)*30 : 0;
  score -= overextPenalty;
  const gainPenalty = recentGainPct > 10 ? Math.min(1, (recentGainPct-10)/15)*20 : 0;
  score -= gainPenalty;
  const rsiPenalty = lastRsi != null && lastRsi > 70 ? Math.min(1, (lastRsi-70)/15)*15 : 0;
  score -= rsiPenalty;

  const price = closes[n-1];
  const avgQuoteVol50 = avgVol50 * price;

  // 🚫 فلتر سيولة حقيقي: نرفض أي عملة متوسط تداولها بآخر 50 شمعة (15د) أقل من حد أدنى بالدولار —
  // هذا يمنع ترشيح عملات "ميتة" حجمها ضعيف جدًا وممكن تواجه انزلاق سعر (slippage) كبير عند التنفيذ.
  if (!Number.isFinite(avgQuoteVol50) || avgQuoteVol50 < MIN_AVG_QUOTE_VOLUME_15M) return null;

  const overextended = pricePosition > 0.8 || recentGainPct > 10 || (lastRsi != null && lastRsi > 70);

  return {
    score: Math.round(Math.max(0, Math.min(100, score))),
    direction,
    price,
    overextended,
    pricePosition: Math.round(pricePosition*100),
    recentGainPct: Math.round(recentGainPct*10)/10,
    rsi: lastRsi != null ? Math.round(lastRsi) : null,
    volRatio: Math.round(volRatio*100)/100,
    avgQuoteVol50: Math.round(avgQuoteVol50),
  };
}

// يفحص كامل حوض الـ~211 عملة (SCAN_POOL) ويطلع فعليًا أقوى العملات من كامل السوق (مو أفضل واحدة
// من كل ثلث كما كان سابقًا) — هذا يحل مشكلتين كانتا موجودتين: (1) مربع فاضي بدون عملة لو ثلث معين
// ما فيه ترشيح صالح هذي الدورة، و(2) نفس العملات ما تتغير لأن كل مربع محصور بثلث ثابت من البداية.
// بنفس أسلوب الفحص الخفيف (REST مؤقت الذاكرة، بدون بث WebSocket دائم لكل عملة) حتى ما نثقل
// السيرفر بفتح 211 اتصال دائم ولا تتعطل لوحة التحكم عند فتحها.
//
// مرحلة إضافية: بعد الترتيب الأولي على 15 دقيقة، نجيب تأكيد الساعة (1h) فقط لأفضل 15 مرشّح (مو
// كل الحوض — توفير للشبكة)، ونعدّل النقاط: تأكيد مع الاتجاه = مكافأة، تعارض معه = عقوبة، ثم نعيد
// الترتيب ونطلع أفضل 3 نهائيًا. هذا يمنع ترشيح عملة إشارتها الفنية جيدة على المدى القصير لكنها
// تسبح عكس اتجاه السوق الأعم على الساعة.
const HTF_CONFIRM_POOL_SIZE = 15;
const HTF_AGREE_BONUS = 10;
const HTF_CONFLICT_PENALTY = 18;

async function runExplosionScan() {
  const results = [];
  for (let i = 0; i < SCAN_POOL.length; i += SCAN_BATCH_SIZE) {
    const batch = SCAN_POOL.slice(i, i + SCAN_BATCH_SIZE);
    await Promise.all(batch.map(async (symbol) => {
      if (!candleStore[`${symbol}_${SCAN_INTERVAL}`]) await refreshScanCache(symbol);
      const candles = getCandlesForScan(symbol);
      const res = computeExplosionScore(candles);
      if (res) results.push({ symbol, ...res });
    }));
  }
  results.sort((a, b) => b.score - a.score);

  // تأكيد الساعة لأفضل المرشحين فقط
  const contenders = results.slice(0, HTF_CONFIRM_POOL_SIZE);
  await Promise.all(contenders.map(c => refreshHtfCache(c.symbol)));
  for (const c of contenders) {
    const htfTrend = getHtfTrend(c.symbol);
    c.htfTrend = htfTrend;
    if (htfTrend === 'unknown' || htfTrend === 'neutral' || c.direction === 'neutral') continue;
    if (htfTrend === c.direction) c.score = Math.min(100, c.score + HTF_AGREE_BONUS);
    else c.score = Math.max(0, c.score - HTF_CONFLICT_PENALTY);
  }
  contenders.sort((a, b) => b.score - a.score);
  const rest = results.slice(HTF_CONFIRM_POOL_SIZE);
  const finalPool = [...contenders, ...rest];
  finalPool.sort((a, b) => b.score - a.score); // ترتيب نهائي شامل بعد دمج تعديلات تأكيد الساعة

  // نحتفظ بالقائمة الكاملة (كل عملة اجتازت فلتر السيولة وفيها بيانات كافية) مرتبة من الأقوى للأضعف،
  // عشان الواجهة تقدر تستعرضها بالسحب ثلاثة ثلاثة بدل ما تقتصر على أفضل 3 بس.
  explosionRanking = finalPool.length ? finalPool : explosionRanking;
  broadcastExplosionScan();
}

function broadcastExplosionScan() {
  if (!explosionRanking.length) return;
  const payload = JSON.stringify({ type: 'explosion_scan', ranking: explosionRanking });
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  }
}

(async () => {
  // نحمّل بث مباشر دائم فقط لقائمة الـ40 الأساسية (تُستخدم للرسم البياني الرئيسي واختيار العملة يدويًا)
  const batchSize = 10;
  const symbols = [...SCAN_SYMBOLS];
  for (let i = 0; i < symbols.length; i += batchSize) {
    const batch = symbols.slice(i, i + batchSize);
    await Promise.all(batch.map(symbol => ensureStream(symbol, SCAN_INTERVAL).catch(err => console.error(err))));
  }
  // أول فحص لأقوى 3 عملات على كامل حوض الـ~240 عملة (REST خفيف مؤقت الذاكرة — لا يفتح بث دائم)
  runExplosionScan().catch(err => console.error('[explosion-scan] فشل الفحص الأول:', err.message));
})();

// فحص أقوى 3 عملات على كامل الحوض كل 4 دقائق (حسب طلب المستخدم)
setInterval(() => { runExplosionScan().catch(err => console.error('[explosion-scan] فشل:', err.message)); }, 4 * 60 * 1000);

async function fetchHistoricalBinance(symbol, interval, limit = 300) {
  const url = `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`;
  const { data } = await axios.get(url, { timeout: 8000 });
  return data.map(k => ({
    time: Math.floor(k[0] / 1000),
    open: parseFloat(k[1]), high: parseFloat(k[2]), low: parseFloat(k[3]), close: parseFloat(k[4]),
    volume: parseFloat(k[5]), isClosed: true,
  }));
}
async function fetchHistoricalMexc(symbol, interval, limit = 300) {
  const url = `https://api.mexc.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`;
  const { data } = await axios.get(url, { timeout: 10000 });
  return data.map(k => ({
    time: Math.floor(k[0] / 1000),
    open: parseFloat(k[1]), high: parseFloat(k[2]), low: parseFloat(k[3]), close: parseFloat(k[4]),
    volume: parseFloat(k[5]), isClosed: true,
  }));
}
async function fetchHistoricalOkx(symbol, interval, limit = 300) {
  const instId = symbol.replace(/USDT$/, '-USDT');
  const bar = OKX_BAR[interval] || interval;
  const url = `https://www.okx.com/api/v5/market/candles?instId=${instId}&bar=${bar}&limit=${limit}`;
  const { data } = await axios.get(url, { timeout: 10000 });
  const rows = data.data || [];
  return rows.map(r => ({
    time: Math.floor(Number(r[0]) / 1000),
    open: parseFloat(r[1]), high: parseFloat(r[2]), low: parseFloat(r[3]), close: parseFloat(r[4]),
    volume: parseFloat(r[5]), isClosed: true,
  })).reverse();
}

function updateCandleStore(symbol, interval, candle) {
  const key = `${symbol}_${interval}`;
  if (!candleStore[key]) candleStore[key] = [];
  const candles = candleStore[key];
  const lastC = candles[candles.length - 1];
  if (lastC && lastC.time === candle.time) candles[candles.length - 1] = candle;
  else if (!lastC || candle.time > lastC.time) {
    candles.push(candle);
    if (candles.length > 600) candles.shift();
  }
}

async function fetchHistorical(symbol, interval, limit = 300) {
  try {
    const c = await fetchHistoricalBinance(symbol, interval, limit);
    console.log(`[${symbol}_${interval}] بيانات تاريخية من Binance`);
    return c;
  } catch (err) { console.warn(`[${symbol}_${interval}] فشل Binance (${err.message}) — تجربة MEXC`); }

  try {
    const c = await fetchHistoricalMexc(symbol, interval, limit);
    console.log(`[${symbol}_${interval}] بيانات تاريخية من MEXC`);
    return c;
  } catch (err) { console.warn(`[${symbol}_${interval}] فشل MEXC (${err.message}) — تجربة OKX`); }

  const c = await fetchHistoricalOkx(symbol, interval, limit);
  console.log(`[${symbol}_${interval}] بيانات تاريخية من OKX`);
  return c;
}

/* ============================================================================
 * ذاكرة فحص سريع لحوض البحث الواسع (SCAN_POOL ~200 عملة)
 * ============================================================================
 * ما نفتح بث WebSocket دائم لـ 200 عملة — هذا يثقل السيرفر بلا داعي. بدل ذلك نجيب
 * شموع 15د بطلب REST خفيف (يتحدّث كل شوي) لكل عملات الحوض، ونستخدمها بفحص محلي سريع
 * بدون أي طلبات شبكة إضافية. أي عملة يختارها البوت فعليًا كمرشّح جاد تترقّى فورًا
 * لبث مباشر حقيقي (ensureStream) عشان التنفيذ يعتمد على أحدث سعر لحظي دائمًا.
 * ============================================================================ */
const scanCandleCache = {}; // symbol -> candles[] (15د، ~100 شمعة، من REST دوري)
const scanCacheUpdatedAt = {}; // symbol -> timestamp آخر تحديث
const SCAN_CACHE_TTL_MS = 3 * 60 * 1000; // نعيد جلب أي عملة ما تحدثت من 3 دقايق

async function refreshScanCache(symbol) {
  const now = Date.now();
  if (scanCacheUpdatedAt[symbol] && now - scanCacheUpdatedAt[symbol] < SCAN_CACHE_TTL_MS) return;
  // محاولتان قبل ما نتجاهل العملة — لتقليل فراغ المربعات بسبب فشل عابر بالشبكة/التقييد المؤقت من المنصة
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const candles = await fetchHistorical(symbol, '15m', 100);
      if (candles && candles.length >= 60) {
        scanCandleCache[symbol] = candles;
        scanCacheUpdatedAt[symbol] = now;
        return;
      }
    } catch (err) { /* حاول مرة ثانية، ولو فشلت كمان نتجاهل العملة هذي الدورة */ }
  }
}

// يرجّع أحدث شموع متاحة للعملة: البث المباشر لو موجود (أدق)، وإلا ذاكرة الفحص السريع
function getCandlesForScan(symbol) {
  const live = candleStore[`${symbol}_15m`];
  if (live && live.length >= 60) return live;
  return scanCandleCache[symbol] || null;
}

/* ============================================================================
 * تأكيد الفريم الأعلى (1 ساعة) — نجيب شموع الساعة فقط لأفضل المرشحين (مو كل الحوض) عشان نتأكد
 * إن إشارة الانضغاط على 15 دقيقة ما تكون عكس اتجاه السوق الأعم على الساعة. كاش منفصل بعمر أطول
 * (20 دقيقة) لأن شموع الساعة تتغيّر ببطء أكثر من 15 دقيقة.
 * ============================================================================ */
const htfCandleCache = {};      // symbol -> شموع الساعة (~60 شمعة)
const htfCacheUpdatedAt = {};   // symbol -> وقت آخر تحديث
const HTF_CACHE_TTL_MS = 20 * 60 * 1000;

async function refreshHtfCache(symbol) {
  const now = Date.now();
  if (htfCacheUpdatedAt[symbol] && now - htfCacheUpdatedAt[symbol] < HTF_CACHE_TTL_MS) return;
  try {
    const candles = await fetchHistorical(symbol, '1h', 60);
    if (candles && candles.length >= 55) {
      htfCandleCache[symbol] = candles;
      htfCacheUpdatedAt[symbol] = now;
    }
  } catch (err) { /* نتجاهل — لو ما توفر تأكيد الساعة، الترشيح يكمل بدونه بدل ما يتعطل */ }
}

// اتجاه الساعة: نقارن EMA20 مقابل EMA50 وموقع السعر منهم — تأكيد بسيط وموثوق بدون تعقيد زائد
function getHtfTrend(symbol) {
  const candles = htfCandleCache[symbol];
  if (!candles || candles.length < 55) return 'unknown';
  const closes = candles.map(c => c.close);
  const ema20Arr = EMA.calculate({ values: closes, period: 20 });
  const ema50Arr = EMA.calculate({ values: closes, period: 50 });
  if (!ema20Arr.length || !ema50Arr.length) return 'unknown';
  const ema20 = ema20Arr[ema20Arr.length - 1];
  const ema50 = ema50Arr[ema50Arr.length - 1];
  const price = closes[closes.length - 1];
  if (ema20 > ema50 && price > ema20) return 'up';
  if (ema20 < ema50 && price < ema20) return 'down';
  return 'neutral';
}

// تحقق حقيقي وحيّ من رموز Binance الفعلية (بعض العملات بالقائمة تتغيّر تسميتها أو تُشطب من Binance
// رغم بقائها بمنصات ثانية — قبل هذا كان البوت يختار عملة زي هذي ويعلق عليها بلا نهاية لأن Binance يرفضها بأمر الشراء)
let binanceValidSymbolsCache = null;
let binanceValidSymbolsFetchedAt = 0;
const BINANCE_SYMBOLS_TTL_MS = 60 * 60 * 1000; // نحدّث القائمة كل ساعة

async function getBinanceValidSymbols() {
  const now = Date.now();
  if (binanceValidSymbolsCache && now - binanceValidSymbolsFetchedAt < BINANCE_SYMBOLS_TTL_MS) return binanceValidSymbolsCache;
  try {
    const { data } = await axios.get('https://api.binance.com/api/v3/exchangeInfo', { timeout: 10000 });
    const set = new Set(
      (data.symbols || [])
        .filter(s => s.status === 'TRADING' && s.quoteAsset === 'USDT' && s.isSpotTradingAllowed)
        .map(s => s.symbol)
    );
    binanceValidSymbolsCache = set;
    binanceValidSymbolsFetchedAt = now;
    console.log(`[binance-symbols] تم تحميل ${set.size} رمز متاح فعليًا للتداول الفوري على Binance`);
  } catch (err) {
    console.warn('[binance-symbols] فشل جلب قائمة رموز Binance:', err.message);
  }
  return binanceValidSymbolsCache; // ممكن يرجّع null لو فشلت أول محاولة تحميل — بنتجاهل الفلترة وقتها
}

async function ensureStream(symbol, interval) {
  const key = `${symbol}_${interval}`;
  if (streamWs[key] !== undefined) return;
  streamWs[key] = null;
  try {
    const candles = await fetchHistorical(symbol, interval, 300);
    candleStore[key] = candles;
    console.log(`[${key}] loaded ${candles.length} historical candles`);
  } catch (err) {
    console.error(`[${key}] فشلت كل المصادر التاريخية:`, err.message);
    candleStore[key] = [];
  }
  connectStream(symbol, interval);
}

function connectStream(symbol, interval) {
  connectBinanceStream(symbol, interval, () => connectMexcStream(symbol, interval, () => connectOkxStream(symbol, interval)));
}

function connectBinanceStream(symbol, interval, onFail) {
  const key = `${symbol}_${interval}`;
  const wsUrl = `wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@kline_${interval}`;
  const ws = new WebSocket(wsUrl);
  streamWs[key] = ws;
  let hasReceivedData = false;
  const failTimer = setTimeout(() => { if (!hasReceivedData) { console.warn(`[${key}] Binance لم يستجب خلال 8 ثوانٍ — تجربة MEXC`); try { ws.terminate(); } catch (e) {} } }, 8000);
  ws.on('open', () => console.log(`[${key}] Binance stream connected`));
  ws.on('message', (raw) => {
    hasReceivedData = true;
    clearTimeout(failTimer);
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    const k = msg.k;
    if (!k) return;
    updateCandleStore(symbol, interval, { time: Math.floor(k.t / 1000), open: parseFloat(k.o), high: parseFloat(k.h), low: parseFloat(k.l), close: parseFloat(k.c), volume: parseFloat(k.v), isClosed: k.x === true });
    broadcastUpdate(symbol, interval);
    maybeBroadcastManualPrice(symbol, interval);
  });
  ws.on('error', (err) => console.error(`[${key}] Binance WS error:`, err.message));
  ws.on('close', () => {
    clearTimeout(failTimer);
    delete streamWs[key];
    if (!hasReceivedData) { console.warn(`[${key}] بينانس أغلق بدون بيانات — الانتقال لـ MEXC`); onFail(); }
    else { console.log(`[${key}] Binance stream closed — إعادة الاتصال ببينانس خلال 5 ثوانٍ`); setTimeout(() => connectBinanceStream(symbol, interval, onFail), 5000); }
  });
}

function connectMexcStream(symbol, interval, onFail) {
  const key = `${symbol}_${interval}`;
  const wsInterval = MEXC_WS_INTERVAL[interval];
  const topic = `spot@public.kline.v3.api@${symbol}@${wsInterval}`;
  const ws = new WebSocket('wss://wbs.mexc.com/ws');
  streamWs[key] = ws;
  let hasReceivedData = false;
  const failTimer = setTimeout(() => { if (!hasReceivedData) { console.warn(`[${key}] MEXC لم يستجب خلال 8 ثوانٍ — تجربة OKX`); try { ws.terminate(); } catch (e) {} } }, 8000);
  ws.on('open', () => { console.log(`[${key}] MEXC stream connected`); ws.send(JSON.stringify({ method: 'SUBSCRIPTION', params: [topic] })); });
  const ping = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ method: 'PING' })); }, 20000);
  ws.on('message', (raw) => {
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    if (!msg.d || !msg.d.k) return;
    hasReceivedData = true;
    clearTimeout(failTimer);
    const k = msg.d.k;
    updateCandleStore(symbol, interval, { time: Math.floor(k.t / 1000), open: parseFloat(k.o), high: parseFloat(k.h), low: parseFloat(k.l), close: parseFloat(k.c), volume: parseFloat(k.v), isClosed: k.X === true });
    broadcastUpdate(symbol, interval);
    maybeBroadcastManualPrice(symbol, interval);
  });
  ws.on('error', (err) => console.error(`[${key}] MEXC WS error:`, err.message));
  ws.on('close', () => {
    clearInterval(ping);
    clearTimeout(failTimer);
    delete streamWs[key];
    if (!hasReceivedData && onFail) { console.warn(`[${key}] MEXC أغلق بدون بيانات — الانتقال لـ OKX`); onFail(); }
    else { console.log(`[${key}] MEXC stream closed — reconnecting in 5s`); setTimeout(() => connectMexcStream(symbol, interval, onFail), 5000); }
  });
}

function connectOkxStream(symbol, interval) {
  const key = `${symbol}_${interval}`;
  const instId = symbol.replace(/USDT$/, '-USDT');
  const channel = `candle${OKX_BAR[interval] || interval}`;
  const ws = new WebSocket('wss://ws.okx.com:8443/public');
  streamWs[key] = ws;
  ws.on('open', () => { console.log(`[${key}] OKX stream connected`); ws.send(JSON.stringify({ op: 'subscribe', args: [{ channel, instId }] })); });
  const ping = setInterval(() => { if (ws.readyState === WebSocket.OPEN) ws.send('ping'); }, 20000);
  ws.on('message', (raw) => {
    if (raw.toString() === 'pong') return;
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    const row = msg.data?.[0];
    if (!row) return;
    updateCandleStore(symbol, interval, { time: Math.floor(Number(row[0]) / 1000), open: parseFloat(row[1]), high: parseFloat(row[2]), low: parseFloat(row[3]), close: parseFloat(row[4]), volume: parseFloat(row[5]), isClosed: row[8] === '1' });
    broadcastUpdate(symbol, interval);
    maybeBroadcastManualPrice(symbol, interval);
  });
  ws.on('error', (err) => console.error(`[${key}] OKX WS error:`, err.message));
  ws.on('close', () => {
    clearInterval(ping);
    console.log(`[${key}] OKX stream closed — reconnecting in 5s (آخر مصدر بالتسلسل)`);
    delete streamWs[key];
    setTimeout(() => connectOkxStream(symbol, interval), 5000);
  });
}

function computeIndicatorsFixedReversal(symbol, interval, candles) {
  const indicators = computeIndicators(candles);
  if (!indicators) return indicators;
  if (interval !== '15m') {
    const candles15 = candleStore[`${symbol}_15m`];
    if (candles15 && candles15.length >= 30) {
      const ind15 = computeIndicators(candles15);
      if (ind15) {
        indicators.stochRsi = ind15.stochRsi;
        indicators.bbPercentB = ind15.bbPercentB;
        indicators.rsiDivergence = ind15.rsiDivergence;
        indicators.williamsR = ind15.williamsR;
      }
    }
  }
  indicators.hourlyLayer = computeHourlyLayer(symbol);
  return indicators;
}

function computeHourlyLayer(symbol) {
  const candles1h = candleStore[`${symbol}_1h`];
  if (!candles1h || candles1h.length < 60) return null;
  const ind = computeIndicators(candles1h);
  if (!ind) return null;
  let bull = 0, bear = 0;
  const notes = [];
  if (ind.ema50 != null && ind.ema200 != null) {
    if (ind.ema50 > ind.ema200) { bull++; notes.push('EMA50 فوق EMA200 (فريم الساعة) — انحياز صاعد متوسط المدى'); }
    else { bear++; notes.push('EMA50 تحت EMA200 (فريم الساعة) — انحياز هابط متوسط المدى'); }
  }
  if (ind.adx) {
    if (ind.adx.adx >= 20) {
      if (ind.adx.pdi > ind.adx.mdi) { bull++; notes.push(`ADX قوي وصاعد على فريم الساعة (${ind.adx.adx.toFixed(1)})`); }
      else { bear++; notes.push(`ADX قوي وهابط على فريم الساعة (${ind.adx.adx.toFixed(1)})`); }
    } else {
      notes.push(`ADX ضعيف على فريم الساعة (${ind.adx.adx.toFixed(1)}) — لا اتجاه هيكلي واضح بعد`);
    }
  }
  if (ind.supertrend) {
    if (ind.supertrend.trendUp) { bull++; notes.push('Supertrend صاعد على فريم الساعة'); }
    else { bear++; notes.push('Supertrend هابط على فريم الساعة'); }
  }
  if (ind.ichimoku && ind.currentPrice != null) {
    const cloudTop = Math.max(ind.ichimoku.spanA, ind.ichimoku.spanB);
    const cloudBottom = Math.min(ind.ichimoku.spanA, ind.ichimoku.spanB);
    if (ind.currentPrice > cloudTop) { bull++; notes.push('السعر فوق سحابة إيشيموكو (فريم الساعة)'); }
    else if (ind.currentPrice < cloudBottom) { bear++; notes.push('السعر تحت سحابة إيشيموكو (فريم الساعة)'); }
    else notes.push('السعر داخل سحابة إيشيموكو (فريم الساعة) — منطقة تردد هيكلي');
  }
  const verdict = bull > bear ? 'bull' : bear > bull ? 'bear' : 'neutral';
  return { verdict, bull, bear, notes };
}

// يبث آخر سعر حي لعملة التداول اليدوي المختارة فورًا مع كل تحديث شمعة يوصل من البورصة (عدة مرات
// بالثانية على العملات النشطة) — مستقل عن اشتراك الرسم البياني، عشان مربعي الشراء/البيع فوق يتحدثان لحظيًا.
function maybeBroadcastManualPrice(symbol, interval) {
  if (interval !== SCAN_INTERVAL || symbol !== botState.manualSymbol) return;
  const candles = candleStore[`${symbol}_${interval}`];
  const lastCandle = candles && candles[candles.length - 1];
  if (!lastCandle) return;
  const payload = JSON.stringify({ type: 'manual_price', symbol, price: lastCandle.close, time: Date.now() });
  for (const client of wss.clients) { if (client.readyState === WebSocket.OPEN) client.send(payload); }
}

function broadcastUpdate(symbol, interval) {
  const key = `${symbol}_${interval}`;
  const candles = candleStore[key];
  if (!candles || !candles.length) return;
  const indicators = computeIndicatorsFixedReversal(symbol, interval, candles);
  const uci = computeUCISeries(candles);
  if (indicators) indicators.uci = uci.length ? { composite: uci[uci.length - 1].composite, signal: uci[uci.length - 1].signal } : null;
  const decision = makeDecision(indicators);
  const payload = JSON.stringify({ type: 'update', symbol, interval, candles, indicators, decision, uci });
  for (const [client, sub] of clientSubs) {
    if (client.readyState === WebSocket.OPEN && sub.symbol === symbol && sub.interval === interval) client.send(payload);
  }
  broadcastMtfUpdate(symbol);
}

function sendSnapshot(ws, symbol, interval) {
  const key = `${symbol}_${interval}`;
  const candles = candleStore[key];
  if (!candles || !candles.length) return;
  const indicators = computeIndicatorsFixedReversal(symbol, interval, candles);
  const uci = computeUCISeries(candles);
  if (indicators) indicators.uci = uci.length ? { composite: uci[uci.length - 1].composite, signal: uci[uci.length - 1].signal } : null;
  const decision = makeDecision(indicators);
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'update', symbol, interval, candles, indicators, decision, uci }));
}

function computeMtfSnapshot(symbol) {
  const out = {};
  for (const iv of INTERVALS) {
    const candles = candleStore[`${symbol}_${iv}`];
    if (!candles || candles.length < 30) { out[iv] = null; continue; }
    const indicators = computeIndicatorsFixedReversal(symbol, iv, candles);
    const decision = indicators ? makeDecision(indicators) : null;
    out[iv] = decision ? { action: decision.action, confidence: decision.confidence, trend: decision.trend } : null;
  }
  return out;
}

const lastMtfBroadcast = {};

// ── طبقة الفريمات فائقة القصر (1ث/3ث/5ث/1د/3د/5د) — قرار ثنائي إجباري: مرتفع أو هابط، بدون محايد ──
// Binance ما يدعم شموع 3 و5 ثواني أصلًا كفريم مباشر، فنبنيها احنا محليًا بتجميع شموع الثانية الواحدة
// (كل 3 شموع = شمعة 3 ثواني، كل 5 = شمعة 5 ثواني) بعد ما صار عندنا بث حي لفريم 1s أصلًا.
function aggregateCandles(candles, groupSize) {
  const out = [];
  for (let i = 0; i + groupSize <= candles.length; i += groupSize) {
    const group = candles.slice(i, i + groupSize);
    out.push({
      time: group[0].time,
      high: Math.max(...group.map(c => c.high)),
      low: Math.min(...group.map(c => c.low)),
      close: group[group.length - 1].close,
    });
  }
  return out;
}
// اتجاه ثنائي إجباري لسلسلة إغلاقات: نقارن آخر سعر بمتوسط الفترة السابقة — ما فيه حالة "محايد"، أي
// تعادل يُحسم لصالح الصعود (>=) عشان دايمًا نطلع بقرار واحد واضح.
function seriesDirection(closes) {
  if (!closes || closes.length < 6) return null;
  const lookback = Math.min(10, closes.length - 1);
  const ref = closes[closes.length - 1 - lookback];
  const last = closes[closes.length - 1];
  return last >= ref ? 'up' : 'down';
}
function computeUltraShortTrend(symbol) {
  const c1s = candleStore[`${symbol}_1s`];
  if (!c1s || c1s.length < 20) return null; // لسه ما وصل بث كافي لفريم الثانية

  const c3s = aggregateCandles(c1s, 3);
  const c5s = aggregateCandles(c1s, 5);
  const c1m = candleStore[`${symbol}_1m`];
  const c3m = candleStore[`${symbol}_3m`];
  const c5m = candleStore[`${symbol}_5m`];

  const dirs = [
    seriesDirection(c1s.map(c => c.close)),
    seriesDirection(c3s.map(c => c.close)),
    seriesDirection(c5s.map(c => c.close)),
    c1m && c1m.length >= 6 ? seriesDirection(c1m.map(c => c.close)) : null,
    c3m && c3m.length >= 6 ? seriesDirection(c3m.map(c => c.close)) : null,
    c5m && c5m.length >= 6 ? seriesDirection(c5m.map(c => c.close)) : null,
  ].filter(Boolean);
  if (!dirs.length) return null;

  const upCount = dirs.filter(d => d === 'up').length;
  const downCount = dirs.length - upCount;
  if (upCount === downCount) return dirs[0]; // تعادل: نرجّح أقصر فريم (1 ثانية) كفاصل
  return upCount > downCount ? 'up' : 'down';
}

function broadcastMtfUpdate(symbol, force = false) {
  const now = Date.now();
  if (!force && lastMtfBroadcast[symbol] && now - lastMtfBroadcast[symbol] < 3000) return;
  lastMtfBroadcast[symbol] = now;
  const snapshot = computeMtfSnapshot(symbol);
  const ultraShortTrend = computeUltraShortTrend(symbol);
  const payload = JSON.stringify({ type: 'mtf_update', symbol, snapshot, ultraShortTrend });
  for (const [client, sub] of clientSubs) {
    if (client.readyState === WebSocket.OPEN && sub.symbol === symbol) client.send(payload);
  }
}

// ── Indicators ────────────────────────────────────────────────────────────────

function last(arr) { return arr && arr.length ? arr[arr.length - 1] : undefined; }

function computeWMA(values, period) {
  const w = [];
  for (let i = 0; i < values.length; i++) w.push(i + 1);
  const sumW = w.reduce((a, b) => a + b, 0);
  const sumWV = values.reduce((s, v, i) => s + v * w[i], 0);
  return sumWV / sumW;
}

// ── Universal Composite Indicator (UCI) ────────────────────────────────────
// مستقل تمامًا عن نظام المربعات/الإجماع الحالي — لا يُضاف لـ consensusTracker ولا GROUP_MAP.
// يعيد سلسلة زمنية كاملة {time, composite, signal} لعرضها في لوحة منفصلة تحت الشموع،
// مطابقة لمنطق مؤشر Pine Script (RSI + Stochastic + MACD/ATR + CCI → EMA5 → EMA9).
function computeUCISeries(candles) {
  const n = candles.length;
  if (n < 45) return [];
  const closes = candles.map(c => c.close);
  const highs = candles.map(c => c.high);
  const lows = candles.map(c => c.low);

  const RSI_LEN = 14, STOCH_K = 14, STOCH_SMOOTH = 3;
  const MACD_FAST = 12, MACD_SLOW = 26, MACD_SIGNAL = 9, ATR_LEN = 14;
  const CCI_LEN = 20, COMP_EMA = 5, SIGNAL_EMA = 9;

  const alignEnd = (arr) => { const out = new Array(n).fill(null); const off = n - arr.length; for (let i = 0; i < arr.length; i++) out[off + i] = arr[i]; return out; };

  // RSI
  const rsiAligned = alignEnd(RSI.calculate({ values: closes, period: RSI_LEN }));

  // Stochastic %K (raw) + SMA smoothing — يطابق ta.stoch(...) ثم sma(stochSmooth) في Pine
  const stochRaw = new Array(n).fill(null);
  for (let i = STOCH_K - 1; i < n; i++) {
    let hh = -Infinity, ll = Infinity;
    for (let j = i - STOCH_K + 1; j <= i; j++) { if (highs[j] > hh) hh = highs[j]; if (lows[j] < ll) ll = lows[j]; }
    stochRaw[i] = hh === ll ? 50 : ((closes[i] - ll) / (hh - ll)) * 100;
  }
  const stochSmoothed = new Array(n).fill(null);
  for (let i = 0; i < n; i++) {
    if (i < STOCH_K - 1 + STOCH_SMOOTH - 1) continue;
    let sum = 0, ok = true;
    for (let j = i - STOCH_SMOOTH + 1; j <= i; j++) { if (stochRaw[j] == null) { ok = false; break; } sum += stochRaw[j]; }
    if (ok) stochSmoothed[i] = sum / STOCH_SMOOTH;
  }

  // MACD histogram normalized by ATR(14)
  const macdAligned = alignEnd(MACD.calculate({ values: closes, fastPeriod: MACD_FAST, slowPeriod: MACD_SLOW, signalPeriod: MACD_SIGNAL, SimpleMAOscillator: false, SimpleMASignal: false }));
  const atrAligned = alignEnd(ATR.calculate({ period: ATR_LEN, high: highs, low: lows, close: closes }));
  const macdNorm = new Array(n).fill(null);
  for (let i = 0; i < n; i++) {
    const m = macdAligned[i], atrV = atrAligned[i];
    if (!m || m.MACD == null || m.signal == null || atrV == null || !atrV) continue;
    let v = ((m.MACD - m.signal) / atrV) * 25 + 50;
    macdNorm[i] = Math.max(0, Math.min(100, v));
  }

  // CCI normalized
  const cciAligned = alignEnd(CCI.calculate({ period: CCI_LEN, high: highs, low: lows, close: closes }));
  const cciNorm = new Array(n).fill(null);
  for (let i = 0; i < n; i++) {
    if (cciAligned[i] == null) continue;
    let v = ((cciAligned[i] + 200) / 400) * 100;
    cciNorm[i] = Math.max(0, Math.min(100, v));
  }

  // compositeRaw = متوسط الأربعة، فقط حيث تتوفر كلها
  const compositeRaw = new Array(n).fill(null);
  for (let i = 0; i < n; i++) {
    const a = rsiAligned[i], b = stochSmoothed[i], c = macdNorm[i], d = cciNorm[i];
    if (a == null || b == null || c == null || d == null) continue;
    compositeRaw[i] = (a + b + c + d) / 4;
  }

  const idx1 = [], vals1 = [];
  for (let i = 0; i < n; i++) if (compositeRaw[i] != null) { idx1.push(i); vals1.push(compositeRaw[i]); }
  if (vals1.length < COMP_EMA) return [];
  const compEma = EMA.calculate({ period: COMP_EMA, values: vals1 });
  const compositeLine = new Array(n).fill(null);
  const off1 = vals1.length - compEma.length;
  for (let i = 0; i < compEma.length; i++) compositeLine[idx1[off1 + i]] = compEma[i];

  const idx2 = [], vals2 = [];
  for (let i = 0; i < n; i++) if (compositeLine[i] != null) { idx2.push(i); vals2.push(compositeLine[i]); }
  const signalEma = vals2.length >= SIGNAL_EMA ? EMA.calculate({ period: SIGNAL_EMA, values: vals2 }) : [];
  const signalLine = new Array(n).fill(null);
  const off2 = vals2.length - signalEma.length;
  for (let i = 0; i < signalEma.length; i++) signalLine[idx2[off2 + i]] = signalEma[i];

  const series = [];
  for (let i = 0; i < n; i++) {
    if (compositeLine[i] == null) continue;
    series.push({ time: candles[i].time, composite: Math.round(compositeLine[i] * 100) / 100, signal: signalLine[i] == null ? null : Math.round(signalLine[i] * 100) / 100 });
  }
  return series;
}

function computeIndicators(candles) {
  if (candles.length < 30) return null;
  const closes = candles.map(c => c.close);
  const highs = candles.map(c => c.high);
  const lows = candles.map(c => c.low);
  const volumes = candles.map(c => c.volume);
  const rsiArr = RSI.calculate({ values: closes, period: 14 });
  const rsi = last(rsiArr);
  const macdRaw = last(MACD.calculate({ values: closes, fastPeriod: 12, slowPeriod: 26, signalPeriod: 9, SimpleMAOscillator: false, SimpleMASignal: false }));
  const macd = macdRaw ? { value: macdRaw.MACD, signal: macdRaw.signal, histogram: macdRaw.histogram } : null;
  const bbRaw = last(BollingerBands.calculate({ period: 20, values: closes, stdDev: 2 }));
  const bb = bbRaw ? { upper: bbRaw.upper, middle: bbRaw.middle, lower: bbRaw.lower } : null;
  const ema50 = last(EMA.calculate({ values: closes, period: 50 }));
  const ema200 = closes.length >= 200 ? last(EMA.calculate({ values: closes, period: 200 })) : null;
  const n = closes.length;

  let sma20 = null;
  if (n >= 20) {
    let sum = 0;
    for (let i = n - 20; i < n; i++) sum += closes[i];
    sma20 = sum / 20;
  }

  let vwap = null;
  {
    const win = candles.slice(-100);
    let cumPV = 0, cumVol = 0;
    for (const c of win) {
      const typical = (c.high + c.low + c.close) / 3;
      cumPV += typical * c.volume;
      cumVol += c.volume;
    }
    if (cumVol > 0) vwap = cumPV / cumVol;
  }

  let stochastic = null;
  if (n >= 17) {
    const kValues = [];
    for (let i = 13; i < n; i++) {
      const hh = Math.max(...highs.slice(i - 13, i + 1));
      const ll = Math.min(...lows.slice(i - 13, i + 1));
      kValues.push(hh === ll ? 50 : ((closes[i] - ll) / (hh - ll)) * 100);
    }
    const kLast = kValues[kValues.length - 1];
    const dSlice = kValues.slice(-3);
    const dLast = dSlice.reduce((a, b) => a + b, 0) / dSlice.length;
    stochastic = { k: kLast, d: dLast };
  }

  let adx = null;
  if (n >= 30) {
    const period = 14;
    const trArr = [], plusDM = [], minusDM = [];
    for (let i = 1; i < n; i++) {
      const upMove = highs[i] - highs[i - 1];
      const downMove = lows[i - 1] - lows[i];
      plusDM.push(upMove > downMove && upMove > 0 ? upMove : 0);
      minusDM.push(downMove > upMove && downMove > 0 ? downMove : 0);
      trArr.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i - 1]), Math.abs(lows[i] - closes[i - 1])));
    }
    const wilderSmooth = (arr) => {
      const out = [];
      let s = 0;
      for (let i = 0; i < period; i++) s += arr[i];
      out.push(s);
      for (let i = period; i < arr.length; i++) out.push(out[out.length - 1] - out[out.length - 1] / period + arr[i]);
      return out;
    };
    if (trArr.length >= period) {
      const trS = wilderSmooth(trArr), plusS = wilderSmooth(plusDM), minusS = wilderSmooth(minusDM);
      const pdiArr = plusS.map((v, i) => (trS[i] ? (v / trS[i]) * 100 : 0));
      const mdiArr = minusS.map((v, i) => (trS[i] ? (v / trS[i]) * 100 : 0));
      const dxArr = pdiArr.map((p, i) => { const m = mdiArr[i]; return (p + m) ? (Math.abs(p - m) / (p + m)) * 100 : 0; });
      if (dxArr.length >= period) {
        let sum = 0;
        for (let i = 0; i < period; i++) sum += dxArr[i];
        let adxVal = sum / period;
        for (let i = period; i < dxArr.length; i++) adxVal = (adxVal * (period - 1) + dxArr[i]) / period;
        adx = { adx: adxVal, pdi: pdiArr[pdiArr.length - 1], mdi: mdiArr[mdiArr.length - 1] };
      }
    }
  }

  let obv = null, obvPrev = null, obvTrendRef = null;
  {
    let val = 0;
    const arr = [0];
    for (let i = 1; i < n; i++) {
      if (closes[i] > closes[i - 1]) val += volumes[i];
      else if (closes[i] < closes[i - 1]) val -= volumes[i];
      arr.push(val);
    }
    obv = arr[arr.length - 1];
    obvPrev = arr.length > 1 ? arr[arr.length - 2] : null;
    const lookback = Math.min(14, arr.length - 1);
    obvTrendRef = lookback > 0 ? arr[arr.length - 1 - lookback] : null;
  }

  const supertrend = computeSupertrend(candles);

  let ichimoku = null;
  if (n >= 52) {
    const hl = (period) => {
      const h = Math.max(...highs.slice(-period));
      const l = Math.min(...lows.slice(-period));
      return (h + l) / 2;
    };
    const conversion = hl(9), base = hl(26), spanB = hl(52);
    ichimoku = { conversion, base, spanA: (conversion + base) / 2, spanB };
  }

  const volumeProfile = computeVolumeProfile(candles.slice(-100));

  let pivot = null;
  if (n >= 2) {
    const prev = candles[n - 2];
    const p = (prev.high + prev.low + prev.close) / 3;
    pivot = { p, r1: 2 * p - prev.low, s1: 2 * p - prev.high };
  }

  let candleCompare = null;
  if (n >= 3) {
    const c1 = candles[n - 3], c2 = candles[n - 2], c3 = candles[n - 1];
    const dir = (c) => c.close > c.open ? 1 : c.close < c.open ? -1 : 0;
    const d1 = dir(c1), d2 = dir(c2), d3 = dir(c3);
    const consecutiveUp = d1 > 0 && d2 > 0 && d3 > 0;
    const consecutiveDown = d1 < 0 && d2 < 0 && d3 < 0;
    const bullEngulf = d2 < 0 && d3 > 0 && c3.close > c2.open && c3.open < c2.close;
    const bearEngulf = d2 > 0 && d3 < 0 && c3.close < c2.open && c3.open > c2.close;
    candleCompare = { consecutiveUp, consecutiveDown, bullEngulf, bearEngulf };
  }

  let accDist = null;
  {
    const win = candles.slice(-60);
    let adLine = 0;
    const adArr = [];
    for (const c of win) {
      const range = c.high - c.low;
      const mfm = range === 0 ? 0 : ((c.close - c.low) - (c.high - c.close)) / range;
      adLine += mfm * c.volume;
      adArr.push(adLine);
    }
    if (adArr.length >= 5) {
      const recent = adArr.slice(-5);
      const rising = recent[recent.length - 1] > recent[0];
      const lastMFM = ((win[win.length - 1].close - win[win.length - 1].low) - (win[win.length - 1].high - win[win.length - 1].close)) / ((win[win.length - 1].high - win[win.length - 1].low) || 1);
      let zone = 'متعادل';
      if (rising && lastMFM > 0.2) zone = 'تجميع (Accumulation)';
      else if (!rising && lastMFM < -0.2) zone = 'تصريف (Distribution)';
      accDist = { value: adLine, rising, zone };
    }
  }

  let cvd = null;
  {
    const win = candles.slice(-50);
    let running = 0;
    const cvdArr = [];
    for (const c of win) {
      const range = c.high - c.low;
      const buyVol = range === 0 ? c.volume / 2 : c.volume * ((c.close - c.low) / range);
      const sellVol = c.volume - buyVol;
      running += (buyVol - sellVol);
      cvdArr.push(running);
    }
    if (cvdArr.length >= 10) {
      const lookback = 10;
      const priceNow = win[win.length - 1].close;
      const priceBefore = win[win.length - 1 - lookback].close;
      const cvdNow = cvdArr[cvdArr.length - 1];
      const cvdBefore = cvdArr[cvdArr.length - 1 - lookback];
      const priceDir = priceNow > priceBefore ? 'up' : priceNow < priceBefore ? 'down' : 'flat';
      const cvdDir = cvdNow > cvdBefore ? 'up' : cvdNow < cvdBefore ? 'down' : 'flat';
      let signal = 'confirm';
      if (priceDir === 'down' && cvdDir === 'up') signal = 'bullish_divergence';
      else if (priceDir === 'up' && cvdDir === 'down') signal = 'bearish_divergence';
      cvd = { value: cvdNow, priceDir, cvdDir, signal };
    }
  }

  let stochRsi = null;
  if (rsiArr.length >= 17) {
    const kArr = [];
    for (let i = 13; i < rsiArr.length; i++) {
      const win = rsiArr.slice(i - 13, i + 1);
      const hi = Math.max(...win), lo = Math.min(...win);
      kArr.push(hi === lo ? 50 : ((rsiArr[i] - lo) / (hi - lo)) * 100);
    }
    if (kArr.length >= 4) {
      const dSmooth = (arr, p) => arr.slice(-p).reduce((a, b) => a + b, 0) / p;
      const kNow = dSmooth(kArr, 3), kPrev = dSmooth(kArr.slice(0, -1), 3);
      const dNow = dSmooth(kArr.slice(-5), 3), dPrev = dSmooth(kArr.slice(0, -1).slice(-5), 3);
      stochRsi = { k: kNow, d: dNow, crossUp: kPrev <= dPrev && kNow > dNow, crossDown: kPrev >= dPrev && kNow < dNow, zoneUp: kNow < 25, zoneDown: kNow > 75 };
    }
  }

  let bbPercentB = null;
  if (bb) {
    const closesForBB = closes.slice(-21);
    const prevClose = closesForBB[closesForBB.length - 2];
    const range = bb.upper - bb.lower;
    const nowB = range === 0 ? 0.5 : (closes[n - 1] - bb.lower) / range;
    const prevB = range === 0 || prevClose == null ? nowB : (prevClose - bb.lower) / range;
    bbPercentB = { now: nowB, prev: prevB, crossedUpFromZero: prevB < 0 && nowB >= 0, crossedDownFromOne: prevB > 1 && nowB <= 1, zoneUp: nowB <= 0.05, zoneDown: nowB >= 0.95 };
  }

  let rsiDivergence = null;
  if (rsiArr.length >= 11 && n >= 11) {
    const priceNow = closes[n - 1], priceBefore = closes[n - 11];
    const rsiNow = rsiArr[rsiArr.length - 1], rsiBefore = rsiArr[rsiArr.length - 11];
    let type = 'none';
    if (priceNow < priceBefore && rsiNow > rsiBefore && rsiNow < 40) type = 'bullish';
    else if (priceNow > priceBefore && rsiNow < rsiBefore && rsiNow > 60) type = 'bearish';
    rsiDivergence = { type, priceNow, priceBefore, rsiNow, rsiBefore };
  }

  let williamsR = null;
  if (n >= 15) {
    const calcWR = (idx) => {
      const hh = Math.max(...highs.slice(idx - 13, idx + 1));
      const ll = Math.min(...lows.slice(idx - 13, idx + 1));
      return hh === ll ? -50 : ((hh - closes[idx]) / (hh - ll)) * -100;
    };
    const wrNow = calcWR(n - 1), wrPrev = calcWR(n - 2);
    williamsR = { now: wrNow, prev: wrPrev, crossUpFrom80: wrPrev < -80 && wrNow >= -80, crossDownFrom20: wrPrev > -20 && wrNow <= -20, zoneUp: wrNow <= -80, zoneDown: wrNow >= -20 };
  }

  let chop = null;
  if (n >= 15) {
    const trWin = [];
    for (let i = n - 14; i < n; i++) trWin.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i - 1]), Math.abs(lows[i] - closes[i - 1])));
    const atrSum = trWin.reduce((a, b) => a + b, 0);
    const hh14 = Math.max(...highs.slice(-14)), ll14 = Math.min(...lows.slice(-14));
    const range14 = hh14 - ll14;
    if (range14 > 0 && atrSum > 0) chop = 100 * Math.log10(atrSum / range14) / Math.log10(14);
  }

  let atr = null;
  if (n >= 15) {
    const trArrAtr = [];
    for (let i = 1; i < n; i++) trArrAtr.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i - 1]), Math.abs(lows[i] - closes[i - 1])));
    const atrPeriod = 14;
    if (trArrAtr.length >= atrPeriod) {
      let atrVal = trArrAtr.slice(0, atrPeriod).reduce((a, b) => a + b, 0) / atrPeriod;
      for (let i = atrPeriod; i < trArrAtr.length; i++) atrVal = (atrVal * (atrPeriod - 1) + trArrAtr[i]) / atrPeriod;
      const lastClose = closes[n - 1];
      const atrPercent = lastClose ? (atrVal / lastClose) * 100 : null;
      atr = { value: atrVal, percent: atrPercent };
    }
  }

  // ═══════════ المؤشرات الخمسة عشر الجديدة ═══════════
  let donchian = null;
  if (n >= 20) {
    const period = 20;
    donchian = { upper: Math.max(...highs.slice(-period)), lower: Math.min(...lows.slice(-period)) };
  }

  let hma = null;
  if (n >= 20) {
    const period = 20;
    const half = Math.floor(period / 2);
    const wmaHalf = computeWMA(closes.slice(-half), half);
    const wmaFull = computeWMA(closes.slice(-period), period);
    hma = 2 * wmaHalf - wmaFull;
  }

  let vwma = null;
  if (n >= 20) {
    const period = 20;
    const closes20 = closes.slice(-period);
    const volumes20 = volumes.slice(-period);
    let sumPV = 0, sumV = 0;
    for (let i = 0; i < period; i++) { sumPV += closes20[i] * volumes20[i]; sumV += volumes20[i]; }
    vwma = sumV > 0 ? sumPV / sumV : null;
  }

  let trix = null;
  if (n >= 30) {
    const ema1Arr = EMA.calculate({ values: closes, period: 15 });
    const ema2Arr = EMA.calculate({ values: ema1Arr, period: 15 });
    const ema3Arr = EMA.calculate({ values: ema2Arr, period: 15 });
    if (ema3Arr.length >= 2) {
      const prev = ema3Arr[ema3Arr.length - 2];
      const curr = ema3Arr[ema3Arr.length - 1];
      trix = prev !== 0 ? ((curr - prev) / prev) * 100 : 0;
    }
  }

  // نحتاج keltner لاحقاً لحساب squeeze momentum
  let keltner = null;
  if (n >= 20 && atr) {
    const ema20 = last(EMA.calculate({ values: closes, period: 20 }));
    if (ema20 != null) keltner = { upper: ema20 + 2 * atr.value, lower: ema20 - 2 * atr.value, middle: ema20 };
  }

  let squeezeMomentum = null;
  if (n >= 20 && keltner && bb) {
    const bbWidth = bb.upper - bb.lower;
    const kcWidth = keltner.upper - keltner.lower;
    squeezeMomentum = kcWidth > 0 ? bbWidth / kcWidth : 0;
  }

  let fisherTransform = null;
  if (n >= 10) {
    const prices = closes.slice(-10);
    const maxPrice = Math.max(...prices);
    const minPrice = Math.min(...prices);
    const range = maxPrice - minPrice;
    if (range > 0) {
      const normalized = ((closes[n - 1] - minPrice) / range) * 2 - 1;
      const clamped = Math.max(-0.999, Math.min(0.999, normalized));
      fisherTransform = 0.5 * Math.log((1 + clamped) / (1 - clamped));
    }
  }

  let elderRay = null;
  if (n >= 13) {
    const ema13 = last(EMA.calculate({ values: closes, period: 13 }));
    if (ema13 != null) {
      elderRay = { bullPower: closes[n - 1] - ema13, bearPower: lows[n - 1] - ema13 };
    }
  }

  let vortex = null;
  if (n >= 15) {
    const period = 14;
    let plusVM = 0, minusVM = 0, trSum = 0;
    for (let i = n - period; i < n; i++) {
      const tr = Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i - 1]), Math.abs(lows[i] - closes[i - 1]));
      plusVM += Math.abs(highs[i] - lows[i - 1]);
      minusVM += Math.abs(lows[i] - highs[i - 1]);
      trSum += tr;
    }
    if (trSum > 0) vortex = { plusVI: plusVM / trSum, minusVI: minusVM / trSum };
  }

  let zScore = null;
  if (n >= 20) {
    const closes20 = closes.slice(-20);
    const mean = closes20.reduce((a, b) => a + b, 0) / 20;
    const std = Math.sqrt(closes20.reduce((s, c) => s + Math.pow(c - mean, 2), 0) / 20);
    zScore = std !== 0 ? (closes[n - 1] - mean) / std : 0;
  }

  let zigzag = null;
  if (n >= 5) {
    const lastHigh = Math.max(...highs.slice(-5));
    const lastLow = Math.min(...lows.slice(-5));
    const range = lastHigh - lastLow;
    const price = closes[n - 1];
    if (range > 0) {
      const pos = (price - lastLow) / range;
      if (pos > 0.7) zigzag = { direction: 'up', price };
      else if (pos < 0.3) zigzag = { direction: 'down', price };
    }
  }

  let klinger = null;
  if (n >= 55) {
    const ema34 = last(EMA.calculate({ values: closes, period: 34 }));
    const ema55 = last(EMA.calculate({ values: closes, period: 55 }));
    if (ema34 != null && ema55 != null) klinger = ema34 - ema55;
  }

  let linearRegression = null;
  if (n >= 20 && atr) {
    const period = 20;
    const closes20 = closes.slice(-period);
    const x = Array.from({ length: period }, (_, i) => i + 1);
    const sumX = x.reduce((a, b) => a + b, 0);
    const sumY = closes20.reduce((a, b) => a + b, 0);
    const sumXY = x.reduce((s, xi, i) => s + xi * closes20[i], 0);
    const sumX2 = x.reduce((s, xi) => s + xi * xi, 0);
    const slope = (period * sumXY - sumX * sumY) / (period * sumX2 - sumX * sumX);
    const intercept = (sumY - slope * sumX) / period;
    const mid = intercept + slope * period;
    const halfRange = 2 * atr.value;
    linearRegression = { upper: mid + halfRange, lower: mid - halfRange };
  }

  let relativeStrengthBTC = null;
  {
    const btcCandles = candleStore[`BTCUSDT_${SCAN_INTERVAL}`];
    if (btcCandles && btcCandles.length >= 11 && n >= 11) {
      const btcReturn = (btcCandles[btcCandles.length - 1].close - btcCandles[btcCandles.length - 11].close) / btcCandles[btcCandles.length - 11].close * 100;
      const coinReturn = (closes[n - 1] - closes[n - 10]) / closes[n - 10] * 100;
      relativeStrengthBTC = coinReturn - btcReturn;
    }
  }

  let marketFacilitation = null;
  if (n >= 2) {
    const curVol = volumes[n - 1], prevVol = volumes[n - 2];
    const curRange = highs[n - 1] - lows[n - 1], prevRange = highs[n - 2] - lows[n - 2];
    if (curVol > prevVol && curRange > prevRange) marketFacilitation = 1;
    else if (curVol < prevVol && curRange < prevRange) marketFacilitation = -1;
    else marketFacilitation = 0;
  }

  return {
    rsi, macd, bb, ema50, ema200, sma20, vwap, stochastic, adx, obv, obvPrev, obvTrendRef, supertrend, ichimoku, volumeProfile,
    pivot, candleCompare, accDist, cvd, stochRsi, bbPercentB, rsiDivergence, williamsR, chop, atr,
    donchian, hma, vwma, trix, squeezeMomentum, fisherTransform, elderRay, vortex, zScore, zigzag, klinger, linearRegression, relativeStrengthBTC, marketFacilitation,
    currentPrice: closes[closes.length - 1]
  };
}

function computeSupertrend(candles, period = 10, multiplier = 3) {
  const highs = candles.map(c => c.high);
  const lows = candles.map(c => c.low);
  const closes = candles.map(c => c.close);
  const trArr = [];
  for (let i = 1; i < closes.length; i++) trArr.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i - 1]), Math.abs(lows[i] - closes[i - 1])));
  if (trArr.length < period) return null;
  let atr = trArr.slice(0, period).reduce((a, b) => a + b, 0) / period;
  const atrArr = [atr];
  for (let i = period; i < trArr.length; i++) { atr = (atr * (period - 1) + trArr[i]) / period; atrArr.push(atr); }
  if (!atrArr.length) return null;
  const offset = candles.length - atrArr.length;
  let finalUpper = null, finalLower = null, trendUp = true, st = null;
  for (let i = 0; i < atrArr.length; i++) {
    const idx = i + offset;
    const hl2 = (highs[idx] + lows[idx]) / 2;
    const a = atrArr[i];
    const basicUpper = hl2 + multiplier * a;
    const basicLower = hl2 - multiplier * a;
    const close = closes[idx];
    const prevClose = idx > 0 ? closes[idx - 1] : close;
    if (finalUpper === null) { finalUpper = basicUpper; finalLower = basicLower; }
    else { finalUpper = (basicUpper < finalUpper || prevClose > finalUpper) ? basicUpper : finalUpper; finalLower = (basicLower > finalLower || prevClose < finalLower) ? basicLower : finalLower; }
    if (close > finalUpper) trendUp = true;
    else if (close < finalLower) trendUp = false;
    st = trendUp ? finalLower : finalUpper;
  }
  return st !== null ? { value: st, trendUp } : null;
}

function computeVolumeProfile(candles, buckets = 24) {
  if (!candles.length) return null;
  const highs = candles.map(c => c.high);
  const lows = candles.map(c => c.low);
  const max = Math.max(...highs);
  const min = Math.min(...lows);
  if (max === min) return null;
  const step = (max - min) / buckets;
  const vol = new Array(buckets).fill(0);
  for (const c of candles) {
    const mid = (c.high + c.low) / 2;
    let idx = Math.floor((mid - min) / step);
    if (idx < 0) idx = 0;
    if (idx >= buckets) idx = buckets - 1;
    vol[idx] += c.volume;
  }
  let pocIdx = 0;
  for (let i = 1; i < buckets; i++) if (vol[i] > vol[pocIdx]) pocIdx = i;
  const pocPrice = min + step * (pocIdx + 0.5);
  return { pocPrice, rangeHigh: max, rangeLow: min };
}

// ── Decision Engine ───────────────────────────────────────────────────────────

function computeEarlySignal(indicators) {
  if (!indicators) return null;
  let bull = 0, bear = 0;
  const reasons = [];

  if (indicators.donchian) {
    const mid = (indicators.donchian.upper + indicators.donchian.lower) / 2;
    if (indicators.currentPrice > mid) { bull += 1; reasons.push('فوق منتصف دونتشيان'); }
    else { bear += 1; reasons.push('تحت منتصف دونتشيان'); }
  }
  if (indicators.hma != null) {
    if (indicators.currentPrice > indicators.hma) { bull += 1; reasons.push('فوق HMA'); }
    else { bear += 1; reasons.push('تحت HMA'); }
  }
  if (indicators.vwma != null) {
    if (indicators.currentPrice > indicators.vwma) { bull += 1; reasons.push('فوق VWMA'); }
    else { bear += 1; reasons.push('تحت VWMA'); }
  }
  if (indicators.trix != null) {
    if (indicators.trix > 0) { bull += 1; reasons.push('TRIX موجب'); }
    else { bear += 1; reasons.push('TRIX سالب'); }
  }
  if (indicators.squeezeMomentum != null) {
    if (indicators.squeezeMomentum < 0.5) {
      if (indicators.currentPrice > indicators.hma) { bull += 1; reasons.push('انضغاط صاعد'); }
      else { bear += 1; reasons.push('انضغاط هابط'); }
    }
  }
  if (indicators.fisherTransform != null) {
    if (indicators.fisherTransform > 0) { bull += 1; reasons.push('Fisher موجب'); }
    else { bear += 1; reasons.push('Fisher سالب'); }
  }
  if (indicators.elderRay) {
    if (indicators.elderRay.bullPower > indicators.elderRay.bearPower) { bull += 1; reasons.push('الثيران أقوى'); }
    else { bear += 1; reasons.push('الدببة أقوى'); }
  }
  if (indicators.vortex) {
    if (indicators.vortex.plusVI > indicators.vortex.minusVI) { bull += 1; reasons.push('Vortex صاعد'); }
    else { bear += 1; reasons.push('Vortex هابط'); }
  }
  if (indicators.zScore != null) {
    if (indicators.zScore < -1.5) { bull += 1; reasons.push('Z-Score متطرف أدنى'); }
    else if (indicators.zScore > 1.5) { bear += 1; reasons.push('Z-Score متطرف أعلى'); }
  }
  if (indicators.zigzag) {
    if (indicators.zigzag.direction === 'up') { bull += 1; reasons.push('زجزاج صاعد'); }
    else { bear += 1; reasons.push('زجزاج هابط'); }
  }
  if (indicators.klinger != null) {
    if (indicators.klinger > 0) { bull += 1; reasons.push('Klinger موجب'); }
    else { bear += 1; reasons.push('Klinger سالب'); }
  }
  if (indicators.linearRegression) {
    const mid = (indicators.linearRegression.upper + indicators.linearRegression.lower) / 2;
    if (indicators.currentPrice > mid) { bull += 1; reasons.push('فوق منتصف الانحدار'); }
    else { bear += 1; reasons.push('تحت منتصف الانحدار'); }
  }
  if (indicators.relativeStrengthBTC != null) {
    if (indicators.relativeStrengthBTC > 0) { bull += 1; reasons.push('أقوى من BTC'); }
    else { bear += 1; reasons.push('أضعف من BTC'); }
  }
  if (indicators.marketFacilitation != null) {
    if (indicators.marketFacilitation > 0) { bull += 1; reasons.push('تيسير إيجابي'); }
    else if (indicators.marketFacilitation < 0) { bear += 1; reasons.push('تيسير سلبي'); }
  }

  const total = bull + bear;
  let verdict = 'neutral';
  let strength = 0;
  if (total > 0) {
    strength = Math.max(bull, bear) / total;
    verdict = bull > bear ? 'bull' : bull < bear ? 'bear' : 'neutral';
  }
  return { verdict, bull, bear, total, strength: Math.round(strength * 100), reasons };
}

function makeDecision(indicators) {
  if (!indicators) return null;
  const { rsi, macd, bb, ema50, ema200, currentPrice, stochRsi, williamsR, cvd, accDist, chop, atr } = indicators;
  const notes = [];
  let bull = 0, bear = 0, rawScore = 50;
  if (rsi != null) {
    if (rsi < 30) { bull += 2; rawScore += 15; notes.push(`RSI عند ${rsi.toFixed(1)} — تشبع بيعي قوي، فرصة شراء محتملة`); }
    else if (rsi < 45) { bull += 1; rawScore += 8; notes.push(`RSI عند ${rsi.toFixed(1)} — ضغط بيعي، المشترون يترقبون`); }
    else if (rsi > 70) { bear += 2; rawScore -= 15; notes.push(`RSI عند ${rsi.toFixed(1)} — تشبع شرائي، احتمالية تصحيح مرتفعة`); }
    else if (rsi > 55) { bull += 1; rawScore += 6; notes.push(`RSI عند ${rsi.toFixed(1)} — زخم صعودي معتدل`); }
    else notes.push(`RSI عند ${rsi.toFixed(1)} — محايد`);
  }
  if (macd) {
    if (macd.value > macd.signal) { bull += 1; rawScore += 10; notes.push(`MACD فوق خط الإشارة — إشارة شراء نشطة`); }
    else { bear += 1; rawScore -= 10; notes.push(`MACD تحت خط الإشارة — إشارة بيع نشطة`); }
    if (macd.histogram > 0) { bull += 1; rawScore += 5; notes.push(`هيستوغرام MACD موجب — تصاعد الزخم الصعودي`); }
    else { bear += 1; rawScore -= 5; notes.push(`هيستوغرام MACD سالب — تصاعد الزخم الهبوطي`); }
  }
  if (ema50 != null) {
    if (currentPrice > ema50) { bull += 1; rawScore += 7; notes.push(`السعر فوق EMA50 — دعم متحرك قصير المدى`); }
    else { bear += 1; rawScore -= 7; notes.push(`السعر تحت EMA50 — مقاومة متحركة قصيرة المدى`); }
  }
  if (ema200 != null) {
    if (currentPrice > ema200) { bull += 2; rawScore += 10; notes.push(`السعر فوق EMA200 — الاتجاه العام صعودي`); }
    else { bear += 2; rawScore -= 10; notes.push(`السعر تحت EMA200 — الاتجاه العام هبوطي`); }
  }
  if (ema50 != null && ema200 != null) {
    if (ema50 > ema200) { bull += 1; rawScore += 5; notes.push(`EMA50 فوق EMA200 — التقاطع الذهبي مؤكَّد`); }
    else { bear += 1; rawScore -= 5; notes.push(`EMA50 تحت EMA200 — التقاطع الميت مؤكَّد`); }
  }
  if (bb) {
    if (currentPrice < bb.lower) { bull += 2; rawScore += 12; notes.push(`السعر تحت الحد الأدنى لبولينجر — منطقة شراء قوية`); }
    else if (currentPrice > bb.upper) { bear += 2; rawScore -= 12; notes.push(`السعر فوق الحد الأعلى لبولينجر — منطقة بيع قوية`); }
    else notes.push(currentPrice > (bb.upper + bb.lower) / 2 ? `السعر في النصف العلوي لبولينجر — ميل صعودي` : `السعر في النصف السفلي لبولينجر — ميل هبوطي`);
    if ((bb.upper - bb.lower) / bb.middle < 0.02) notes.push(`نطاق بولينجر ضيق جداً — تذبذب محتمل قريب`);
  }
  if (stochRsi) {
    if (stochRsi.crossUp) { bull += 2; rawScore += 10; notes.push(`StochRSI (15د) عبور صاعد من منطقة التشبع البيعي — إشارة ارتداد صعودي`); }
    else if (stochRsi.crossDown) { bear += 2; rawScore -= 10; notes.push(`StochRSI (15د) عبور هابط من منطقة التشبع الشرائي — إشارة ارتداد هبوطي`); }
    else if (stochRsi.zoneUp) { bull += 1; rawScore += 5; notes.push(`StochRSI (15د) داخل منطقة تشبع بيعي — احتمال ارتداد قريب`); }
    else if (stochRsi.zoneDown) { bear += 1; rawScore -= 5; notes.push(`StochRSI (15د) داخل منطقة تشبع شرائي — احتمال تصحيح قريب`); }
  }
  if (williamsR) {
    if (williamsR.crossUpFrom80) { bull += 2; rawScore += 10; notes.push(`Williams %R (15د) خرج من منطقة التشبع البيعي (-80) — زخم ارتدادي صعودي`); }
    else if (williamsR.crossDownFrom20) { bear += 2; rawScore -= 10; notes.push(`Williams %R (15د) خرج من منطقة التشبع الشرائي (-20) — زخم ارتدادي هبوطي`); }
    else if (williamsR.zoneUp) { bull += 1; rawScore += 4; notes.push(`Williams %R (15د) داخل منطقة تشبع بيعي`); }
    else if (williamsR.zoneDown) { bear += 1; rawScore -= 4; notes.push(`Williams %R (15د) داخل منطقة تشبع شرائي`); }
  }
  if (cvd) {
    if (cvd.signal === 'bullish_divergence') { bull += 2; rawScore += 9; notes.push(`CVD يصعد بينما السعر ينخفض — امتصاص مؤسسي خفي (شراء بصمت)`); }
    else if (cvd.signal === 'bearish_divergence') { bear += 2; rawScore -= 9; notes.push(`CVD ينخفض بينما السعر يصعد — تصريف مؤسسي خفي (بيع بصمت)`); }
  }
  if (accDist) {
    if (accDist.zone === 'تجميع (Accumulation)') { bull += 1; rawScore += 6; notes.push(`A/D Line في منطقة تجميع — تدفق سيولة شرائي حقيقي`); }
    else if (accDist.zone === 'تصريف (Distribution)') { bear += 1; rawScore -= 6; notes.push(`A/D Line في منطقة تصريف — تدفق سيولة بيعي حقيقي`); }
  }
  let trendStable = null;
  if (chop != null) {
    if (chop < 38.2) { trendStable = true; notes.push(`Choppiness Index عند ${chop.toFixed(1)} — السوق في اتجاه ثابت وقوي`); }
    else if (chop > 61.8) { trendStable = false; notes.push(`Choppiness Index عند ${chop.toFixed(1)} — السوق متذبذب وعشوائي`); }
  }
  if (atr && atr.percent != null) {
    if (atr.percent >= 1.5) notes.push(`ATR عند ${atr.percent.toFixed(2)}% — تذبذب مرتفع`);
    else if (atr.percent <= 0.3) notes.push(`ATR عند ${atr.percent.toFixed(2)}% — تذبذب منخفض جداً`);
  }

  const trend = bull > bear + 1 ? 'صعود' : bear > bull + 1 ? 'هبوط' : 'تذبذب';
  const score = Math.max(0, Math.min(100, rawScore));
  let action;
  if (rsi != null && rsi < 35 && macd && macd.value > macd.signal) action = 'buy zone';
  else if (rsi != null && rsi > 65 && macd && macd.value < macd.signal) action = 'sell zone';
  else if (bb && currentPrice < bb.lower) action = 'buy zone';
  else if (bb && currentPrice > bb.upper) action = 'sell zone';
  else if (trend === 'صعود' && score >= 62) action = 'buy zone';
  else if (trend === 'هبوط' && score <= 38) action = 'sell zone';
  else action = 'wait';

  const total = bull + bear;
  const dominance = total > 0 ? Math.max(bull, bear) / total : 0.5;
  if (trendStable === false && action !== 'wait' && dominance < 0.75) {
    action = 'wait';
    notes.push(`تم تحويل القرار إلى "انتظار": التذبذب العشوائي المرتفع يُضعف موثوقية الإشارة`);
  }
  let confidence = Math.round(Math.min(100, score * (0.4 + dominance * 0.6)));
  if (trendStable === true) confidence = Math.min(100, Math.round(confidence * 1.1));
  else if (trendStable === false) confidence = Math.round(confidence * 0.85);

  // منطقة الشراء ومنطقة البيع: مكان واحد فقط لكل منطقة (مش نطاق من رقمين) —
  // الشراء = أفضل نقطة دخول (الحد الأدنى لبولينجر)، والبيع = نفس نقطة الدخول مضروبة
  // بنسبة جني الربح التي يضبطها المستخدم (بوت-take-profit)، فتتحدّث تلقائيًا كل ما غيّر النسبة.
  const fmtZonePrice = (p) => (p == null || !Number.isFinite(p)) ? null : (p >= 1 ? p.toFixed(4) : p.toFixed(8));
  const buyPriceNum = bb ? bb.lower : null;
  const tpPct = (botState && botState.takeProfitPercent > 0) ? botState.takeProfitPercent : 1;
  const sellPriceNum = buyPriceNum != null ? buyPriceNum * (1 + tpPct / 100) : null;
  const buyZone = buyPriceNum != null ? { price: fmtZonePrice(buyPriceNum) } : null;
  const sellZone = sellPriceNum != null ? { price: fmtZonePrice(sellPriceNum) } : null;
  const early = computeEarlySignal(indicators);
  return { trend, action, confidence, notes, buyZone, sellZone, early };
}

// ── بوت الشبكة (Grid Bot) ─────────────────────────────────────────────────────
// امسحنا البوت اليدوي القديم بالكامل. البوت الآن بوت شبكة تلقائي كامل على Binance فقط،
// نفس منطق سكربت Python اللي زوّدنا به المستخدم: عند التشغيل يلغي كل الأوامر المفتوحة على
// الرمز، يجيب السعر الحالي، ثم يضع GRID_SIZE أوامر شراء Limit تحت السعر و GRID_SIZE أوامر بيع
// Limit فوق السعر بمسافة GRID_STEP_PERCENT بين كل مستوى. كل 30 ثانية يفحص الأوامر المفتوحة،
// ولو نقص عددها عن المتوقع (يعني تنفذت صفقة) يلغي الباقي ويعيد بناء الشبكة على السعر الجديد.
//
// ⚠️ ملاحظة مهمة (نفس قيد سكربت Python الأصلي): أوامر البيع Limit على Binance Spot تتطلب
// إنك تملك فعليًا كمية من العملة الأساسية (مثلاً BTC) لتغطية أوامر البيع، وإلا سترفضها Binance
// برسالة "insufficient balance". هذا سلوك طبيعي في أي بوت شبكة على Spot وليس خطأ بالكود.

let botState = {
  enabled: false,
  exchange: 'binance',
  tradeSizeUsdt: 50,        // مبلغ الصفقة بالـ USDT لكل مستوى في الشبكة (AMOUNT_PER_GRID محسوبة ديناميكيًا)
  sellAmountUsdt: 50,       // مبلغ البيع (USDT) — محفوظ للعرض حاليًا
  takeProfitPercent: 1,     // GRID_STEP_PERCENT: نسبة المسافة بين كل مستوى (١٪ افتراضيًا)
  maxConcurrentPositions: 5,// GRID_SIZE: عدد مستويات الشراء (ونفس العدد للبيع)
  manualSymbol: 'BTCUSDT',  // الرمز اللي تعمل عليه الشبكة حاليًا
  positions: {},            // غير مستخدم في بوت الشبكة (موجود فقط توافقًا مع الواجهة القديمة)
  pendingOrders: {},        // أوامر الشبكة المفتوحة حاليًا، كل أمر بمفتاح فريد
  pendingSellOrders: {},    // غير مستخدم في بوت الشبكة (توافقًا مع الواجهة القديمة)
  tradeLog: [],
};

const SCAN_BATCH_SIZE = 15; // كم عملة نفحص بالتوازي بكل دفعة أثناء فحص أقوى 3 عملات (REST خفيف) — يُستخدم في runExplosionScan فوق

// ── توقيع وأدوات Binance ────────────────────────────────────────────────────
function binanceSignedQuery(params, secret = BINANCE_API_SECRET) {
  const qs = new URLSearchParams(params).toString();
  const sig = crypto.createHmac('sha256', secret).update(qs).digest('hex');
  return `${qs}&signature=${sig}`;
}
// ⏱️ مزامنة الوقت مع سيرفر Binance — ساعة السيرفر عندنا ممكن تنحرف شوي عن Binance (خصوصًا على استضافات
// مشتركة)، وأي انحراف فوق recvWindow يخلي كل الأوامر الموقّعة تُرفض بخطأ "Timestamp for this request...".
// نحسب الفرق مرة كل نصف ساعة + فورًا لما نكتشف خطأ توقيت فعلي، ونضيفه لكل Date.now() نرسله لـ Binance.
let binanceTimeOffsetMs = 0;
async function syncBinanceTime() {
  try {
    const { data } = await axios.get('https://api.binance.com/api/v3/time', { timeout: 8000 });
    if (data && data.serverTime) binanceTimeOffsetMs = data.serverTime - Date.now();
  } catch (err) { console.warn('[binance-time] فشل مزامنة الوقت مع Binance:', err.message); }
}
function bTimestamp() { return Date.now() + binanceTimeOffsetMs; }
syncBinanceTime();
setInterval(syncBinanceTime, 30 * 60 * 1000);
function roundQty(qty, price) {
  let decimals;
  if (price >= 1000) decimals = 5;
  else if (price >= 100) decimals = 4;
  else if (price >= 1) decimals = 3;
  else if (price >= 0.01) decimals = 1;
  else decimals = 0;
  return Number(qty.toFixed(decimals));
}
function roundPrice(price) {
  if (price >= 1000) return Number(price.toFixed(2));
  if (price >= 1) return Number(price.toFixed(4));
  if (price >= 0.01) return Number(price.toFixed(6));
  return Number(price.toFixed(8));
}

async function placeLimitOrder(symbol, side, price, quantity, creds = { apiKey: BINANCE_API_KEY, apiSecret: BINANCE_API_SECRET }) {
  const params = { symbol, side, type: 'LIMIT', timeInForce: 'GTC', quantity, price, timestamp: bTimestamp(), recvWindow: 5000 };
  const { data } = await axios.post(`https://api.binance.com/api/v3/order?${binanceSignedQuery(params, creds.apiSecret)}`, null, { headers: { 'X-MBX-APIKEY': creds.apiKey }, timeout: 10000 });
  return data;
}
async function cancelOrder(symbol, orderId, creds = { apiKey: BINANCE_API_KEY, apiSecret: BINANCE_API_SECRET }) {
  const params = { symbol, orderId, timestamp: bTimestamp(), recvWindow: 5000 };
  const { data } = await axios.delete(`https://api.binance.com/api/v3/order?${binanceSignedQuery(params, creds.apiSecret)}`, { headers: { 'X-MBX-APIKEY': creds.apiKey }, timeout: 10000 });
  return data;
}
async function getOpenOrders(symbol, creds = { apiKey: BINANCE_API_KEY, apiSecret: BINANCE_API_SECRET }) {
  const params = { symbol, timestamp: bTimestamp(), recvWindow: 5000 };
  const { data } = await axios.get(`https://api.binance.com/api/v3/openOrders?${binanceSignedQuery(params, creds.apiSecret)}`, { headers: { 'X-MBX-APIKEY': creds.apiKey }, timeout: 10000 });
  return data;
}
async function getCurrentPrice(symbol) {
  const { data } = await axios.get(`https://api.binance.com/api/v3/ticker/price?symbol=${symbol}`, { timeout: 10000 });
  return parseFloat(data.price);
}

function logBotError(symbol, err) {
  const detail = err.response?.data?.msg || err.message;
  console.error(`[GRID-BOT] خطأ في ${symbol}:`, detail);
  botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'error', message: String(detail) });
  botState.tradeLog = botState.tradeLog.slice(0, 50);
}

// 🧹 إلغاء كافة الأوامر المفتوحة على الرمز — نفس cancel_all_orders() في سكربت Python
async function cancelAllOpenOrders(symbol, creds) {
  let orders;
  try { orders = await getOpenOrders(symbol, creds); } catch (err) { logBotError(symbol, err); return; }
  for (const o of orders) {
    try { await cancelOrder(symbol, o.orderId, creds); } catch (err) { logBotError(symbol, err); }
  }
}

// 🛠️ بناء وإرسال شبكة الأوامر الجديدة — نفس place_grid_orders() في سكربت Python
async function placeGridOrders(symbol, currentPrice) {
  const gridSize = botState.maxConcurrentPositions;
  const stepPct = botState.takeProfitPercent / 100;
  const usdtPerLevel = botState.tradeSizeUsdt;
  botState.pendingOrders = {};

  for (let i = 1; i <= gridSize; i++) {
    const buyPrice = roundPrice(currentPrice * (1 - stepPct * i));
    const qty = roundQty(usdtPerLevel / buyPrice, buyPrice);
    if (!qty || qty <= 0) continue;
    try {
      const data = await placeLimitOrder(symbol, 'BUY', buyPrice, qty);
      if (data.orderId) {
        botState.pendingOrders[`${symbol}_buy_${i}`] = { orderId: data.orderId, side: 'BUY', symbol, price: buyPrice, qty, level: i };
        botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'order', side: 'BUY', price: buyPrice, qty, exchange: 'binance', reason: `📥 أمر شراء شبكة #${i} عند ${buyPrice}` });
      }
    } catch (err) { logBotError(symbol, err); }
  }

  for (let i = 1; i <= gridSize; i++) {
    const sellPrice = roundPrice(currentPrice * (1 + stepPct * i));
    const qty = roundQty(usdtPerLevel / sellPrice, sellPrice);
    if (!qty || qty <= 0) continue;
    try {
      const data = await placeLimitOrder(symbol, 'SELL', sellPrice, qty);
      if (data.orderId) {
        botState.pendingOrders[`${symbol}_sell_${i}`] = { orderId: data.orderId, side: 'SELL', symbol, price: sellPrice, qty, level: i };
        botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'order', side: 'SELL', price: sellPrice, qty, exchange: 'binance', reason: `📤 أمر بيع شبكة #${i} عند ${sellPrice}` });
      }
    } catch (err) { logBotError(symbol, err); }
  }

  botState.tradeLog = botState.tradeLog.slice(0, 50);
}

// 🔄 دورة المراقبة الدورية — نفس حلقة while True في سكربت Python: تفحص الأوامر المفتوحة كل 30 ثانية،
// ولو نقص عددها عن المتوقع (GRID_SIZE × 2) فهذا يعني تنفيذ صفقة، فتُعاد موازنة الشبكة على السعر الجديد.
async function runBotCycle() {
  if (!botState.enabled) return;
  const symbol = botState.manualSymbol || 'BTCUSDT';
  try {
    const openOrders = await getOpenOrders(symbol);
    const expectedTotal = botState.maxConcurrentPositions * 2;
    if (openOrders.length < expectedTotal) {
      botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'error', message: '🔔 اكتشاف تنفيذ صفقة في السوق! جاري تحديث وموازنة الشبكة...' });
      await cancelAllOpenOrders(symbol);
      const newPrice = await getCurrentPrice(symbol);
      await placeGridOrders(symbol, newPrice);
      botState.tradeLog = botState.tradeLog.slice(0, 50);
    }
  } catch (err) {
    logBotError(symbol, err);
  }
  broadcastBotStatus();
}

function botStatusPayload(ws) {
  const accountKey = getAccountKeyForWs(ws);
  const trades = manualBotState.tradesByAccount[accountKey] || [];
  return JSON.stringify({
    type: 'bot_status', enabled: botState.enabled, exchange: botState.exchange,
    tradeSizeUsdt: botState.tradeSizeUsdt, sellAmountUsdt: botState.sellAmountUsdt, takeProfitPercent: botState.takeProfitPercent,
    maxConcurrentPositions: botState.maxConcurrentPositions, manualSymbol: botState.manualSymbol,
    positions: botState.positions, pendingOrders: botState.pendingOrders,
    pendingSellOrders: botState.pendingSellOrders, tradeLog: botState.tradeLog.slice(0, 20),
    manualBot: { enabled: manualBotState.enabled, trades: trades.slice(0, 30) },
    linkedAccount: accountKey === '__default__' ? null : accountKey,
    autoTrade: (() => {
      const a = autoTradeByAccount[accountKey];
      if (!a) return null;
      const legs = effectiveLegs(a);
      const totalQty = legs.reduce((s, l) => s + l.qty, 0);
      const avgBuyPrice = totalQty > 0 ? legs.reduce((s, l) => s + l.qty * l.price, 0) / totalQty : null;
      // نسبة البيع الفعلية: نسبة ثابتة مخصّصة لو موجودة، وإلا نسبة آخر مرحلة اشترينا فيها (مو دائمًا الرئيسية) — نفس حساب دورة التداول تمامًا
      const sellPct = (a.sellPct != null) ? a.sellPct : (legs.length <= 1 ? a.pct : ((a.levelPct && a.levelPct[legs.length]) || a.pct));
      return {
        pct: a.state === 'holding' ? sellPct : a.pct, mainPct: a.pct, sellPct: a.sellPct, symbol: a.symbol, state: a.state, peak: a.peak, anchor: a.anchor, armed: a.armed,
        amountUsdt: a.amountUsdt, levels: a.levels || 1, levelPct: a.levelPct || {}, legsDone: legs.length,
        avgBuyPrice, lastBuyPrice: a.lastBuyPrice,
        stopLoss: a.stopLoss || null, softReset: a.softReset || null, stopLossWatch: a.stopLossWatch || null,
        totalCost: a.state === 'holding' ? legsTotalCost(a) : 0, resetCount: a.resetCount || 0, buysCount: (a.legs || []).length,
        anchorTol: a.anchorTol || 0, anchorLimit: a.anchor > 0 ? a.anchor * (1 + (a.anchorTol > 0 ? a.anchorTol : 0) / 100) : null,
        problem: a.problem || null, // مشكلة حالية (إن وجدت) — تبقى هنا لحد ما تُحل، حتى لو المستخدم كان بعيد عن الصفحة وقت حدوثها
      };
    })(),
  });
}
function broadcastBotStatus() {
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(botStatusPayload(client));
  }
}

setInterval(runBotCycle, 30 * 1000); // كل 30 ثانية — فحص الشبكة وإعادة الموازنة عند الحاجة

// ── بوت الشراء اليدوي + بوت البيع التلقائي (طبقة ثانية مستقلة عن بوت الشبكة) ──────────────────
// زر "شراء الآن" ينفّذ Market فوري على الرمز المعروض. بمجرد تسجيل الصفقة، بوت البيع (مستقل، دورة
// كل 10 ثوانٍ لأنه لازم يكون سريع) يضع لها أمر بيع Limit تلقائي = سعر الشراء × (1 + نسبة البيع%)،
// ثم يتابعها لحد ما تُنفذ. يشترك مع بوت الشبكة بنفس القيم (مبلغ الصفقة USDT، نسبة البيع، وأقصى
// عدد صفقات مفتوحة بنفس الوقت) بدل ما نكرر إعدادات منفصلة له.
// يشترك مع بوت الشبكة بنفس القيم (مبلغ الصفقة USDT، نسبة البيع، وأقصى عدد صفقات مفتوحة بنفس الوقت)
// بدل ما نكرر إعدادات منفصلة له. مفعّل دائمًا — ما فيه مفتاح تشغيل/إيقاف بالواجهة (بطلب المستخدم).
// الصفقات مقسّمة حسب الحساب (accountKey = إيميل الحساب المرتبط، أو '__default__' لمفتاحك الأساسي)
// حتى كل صديق يشوف صفقاته هو بس، وتُنفَّذ بمفتاح Binance تبعه هو، مو مفتاحك.
let manualBotState = {
  enabled: true,
  tradesByAccount: {}, // accountKey -> [{ id, symbol, qty, buyPrice, buyOrderId, sellOrderId, sellPrice, status, time }]
};
function getAccountKeyForWs(ws) { return wsAccount.get(ws) || '__default__'; }
function getCredsForAccountKey(accountKey) {
  if (accountKey && accountKey !== '__default__' && accounts[accountKey]) {
    try { return { apiKey: decryptSecret(accounts[accountKey].apiKeyEnc), apiSecret: decryptSecret(accounts[accountKey].apiSecretEnc) }; }
    catch (err) { /* فشل فك التشفير (نادر) — نرجع للمفتاح الافتراضي بدل ما نكسر الطلب */ }
  }
  return { apiKey: BINANCE_API_KEY, apiSecret: BINANCE_API_SECRET };
}

async function placeMarketOrder(symbol, side, quantity, creds = { apiKey: BINANCE_API_KEY, apiSecret: BINANCE_API_SECRET }) {
  const params = { symbol, side, type: 'MARKET', quantity, timestamp: bTimestamp(), recvWindow: 5000 };
  const { data } = await axios.post(`https://api.binance.com/api/v3/order?${binanceSignedQuery(params, creds.apiSecret)}`, null, { headers: { 'X-MBX-APIKEY': creds.apiKey }, timeout: 10000 });
  return data;
}
async function queryOrderStatus(symbol, orderId, creds = { apiKey: BINANCE_API_KEY, apiSecret: BINANCE_API_SECRET }) {
  const params = { symbol, orderId, timestamp: bTimestamp(), recvWindow: 5000 };
  const { data } = await axios.get(`https://api.binance.com/api/v3/order?${binanceSignedQuery(params, creds.apiSecret)}`, { headers: { 'X-MBX-APIKEY': creds.apiKey }, timeout: 10000 });
  return data;
}

// الرصيد الحر من USDT في Spot (للشراء بما تبقّى لو الرصيد أقل بقليل من مبلغ المرحلة)
async function getUsdtBalances(creds) {
  const params = { timestamp: bTimestamp(), recvWindow: 5000 };
  const { data } = await axios.get(`https://api.binance.com/api/v3/account?${binanceSignedQuery(params, creds.apiSecret)}`, { headers: { 'X-MBX-APIKEY': creds.apiKey }, timeout: 10000 });
  const b = (data.balances || []).find(x => x.asset === 'USDT');
  return { free: b ? parseFloat(b.free) : 0, locked: b ? parseFloat(b.locked) : 0 };
}
async function getFreeUsdt(creds) { return (await getUsdtBalances(creds)).free; }

// 🟢 تنفيذ شراء فوري (Market) — يُستدعى عند ضغط المستخدم على زر "شراء الآن". accountKey يحدد صفقات
// مين نسجّل فيها (كل صديق يشوف صفقاته هو بس)، وcreds هي مفتاح Binance الفعلي المستخدم بالتنفيذ.
async function executeManualBuy(symbol, accountKey, creds, amountUsdt, isAuto) {
  const price = await getCurrentPrice(symbol);
  const qty = roundQty((amountUsdt > 0 ? amountUsdt : botState.tradeSizeUsdt) / price, price);
  if (!qty || qty <= 0) throw new Error('الكمية المحسوبة صفر — تأكد من مبلغ الصفقة');
  const data = await placeMarketOrder(symbol, 'BUY', qty, creds);
  // ✅ تحقق من Binance نفسها: لا نسجّل صفقة إلا لو الأمر نُفّذ فعلًا (executedQty > 0). قبل كان ممكن يُسجَّل شراء وهمي
  // بالكمية المطلوبة لو الرد جاء بدون كمية منفّذة، فيزيد "إجمالي التكلفة" ويصير فيه مراحل أكثر من الحقيقي.
  if (!(parseFloat(data.executedQty) > 0) || (data.status && data.status !== 'FILLED')) {
    try {
      const v = await queryOrderStatus(symbol, data.orderId, creds);
      if (v) { data.executedQty = v.executedQty; data.cummulativeQuoteQty = v.cummulativeQuoteQty; data.status = v.status; }
    } catch (e) { /* نكمل بالرد الأصلي */ }
    if (!(parseFloat(data.executedQty) > 0)) throw new Error(`لم يُنفَّذ أمر الشراء على Binance (الحالة: ${data.status || 'غير معروفة'}) — لم يُسجَّل أي شراء`);
  }
  // متوسط سعر التنفيذ الفعلي من fills لو متوفرة، وإلا السعر اللحظي اللي جبناه قبل الإرسال
  let fillPrice = price;
  let filledQty = null;
  if (data.fills && data.fills.length) {
    const totalQty = data.fills.reduce((s, f) => s + parseFloat(f.qty), 0);
    const totalCost = data.fills.reduce((s, f) => s + parseFloat(f.qty) * parseFloat(f.price), 0);
    if (totalQty > 0) fillPrice = totalCost / totalQty;
    if (totalQty > 0) filledQty = totalQty;
  }
  // 🐛 إصلاح مشكلة "البيع ما يشتغل": كنا نسجّل الكمية المطلوبة (qty) بدل الكمية المنفذة فعليًا —
  // أي فرق بسيط (تقريب Binance، سيولة، إلخ) كان يخلي أمر البيع لاحقًا يُرفض لعدم كفاية الرصيد الفعلي.
  // الحل: نستخدم executedQty من Binance نفسها (المصدر الموثوق)، ونطرح هامش أمان بسيط جدًا (0.1%)
  // يحمي من أي فرق تقريب أخير وقت البيع.
  const actualQty = data.executedQty ? parseFloat(data.executedQty) : filledQty;
  const safeQty = actualQty ? roundQty(actualQty * 0.999, price) : qty;
  const trade = {
    id: `${symbol}_${Date.now()}_${crypto.randomBytes(3).toString('hex')}`, symbol, qty: safeQty, buyPrice: roundPrice(fillPrice),
    buyOrderId: data.orderId, sellOrderId: null, sellPrice: null, status: 'open', time: Date.now(), auto: !!isAuto,
    cost: (parseFloat(data.cummulativeQuoteQty) > 0) ? parseFloat(data.cummulativeQuoteQty) : (actualQty ? actualQty * fillPrice : safeQty * fillPrice),
  };
  const trades = manualBotState.tradesByAccount[accountKey] || (manualBotState.tradesByAccount[accountKey] = []);
  trades.unshift(trade);
  manualBotState.tradesByAccount[accountKey] = trades.slice(0, 100);
  botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'order', side: 'BUY', price: trade.buyPrice, qty: safeQty, exchange: 'binance', reason: `${isAuto ? '🤖 شراء تلقائي (نزول)' : '🟢 شراء يدوي فوري'} عند ${trade.buyPrice}` });
  botState.tradeLog = botState.tradeLog.slice(0, 50);
  return trade;
}

// 🔴 تنفيذ بيع فوري (Market) — يُستدعى عند ضغط المستخدم على زر "بيع الآن". يبيع أقرب صفقة مفتوحة
// (أحدث صفقة بحالة open أو pending_sell) على نفس الرمز من صفقات نفس الحساب، ويلغي أمر البيع المحدد
// المسبق لو كان موجود.
async function executeManualSell(symbol, accountKey, creds, tradeId, isAuto) {
  const trades = manualBotState.tradesByAccount[accountKey] || [];
  const trade = trades.find(t => t.symbol === symbol && t.status !== 'sold' && (!tradeId || t.id === tradeId));
  if (!trade) throw new Error(`لا توجد صفقة شراء مفتوحة على ${symbol} حاليًا`);
  if (trade.status === 'pending_sell' && trade.sellOrderId) {
    try { await cancelOrder(symbol, trade.sellOrderId, creds); } catch (err) { /* ممكن يكون اتنفذ قبل ما نلغيه، نكمل عادي */ }
  }
  const data = await placeMarketOrder(symbol, 'SELL', trade.qty, creds);
  let fillPrice = trade.sellPrice || trade.buyPrice;
  if (data.fills && data.fills.length) {
    const totalQty = data.fills.reduce((s, f) => s + parseFloat(f.qty), 0);
    const totalCost = data.fills.reduce((s, f) => s + parseFloat(f.qty) * parseFloat(f.price), 0);
    if (totalQty > 0) fillPrice = totalCost / totalQty;
  }
  trade.status = 'sold';
  trade.sellPrice = roundPrice(fillPrice);
  botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'order', side: 'SELL', price: trade.sellPrice, qty: trade.qty, exchange: 'binance', reason: `${isAuto ? '🤖 بيع تلقائي (ارتفاع)' : '🔴 بيع يدوي فوري'} عند ${trade.sellPrice}` });
  botState.tradeLog = botState.tradeLog.slice(0, 50);
  return trade;
}

// 🔴 بيع كل مراحل الشراء المتدرّج (DCA) بأمر Market واحد — يُستخدم من محرك التداول التلقائي فقط لما
// يكون عنده أكثر من صفقة مفتوحة على نفس العملة (مربع العدد أكبر من 1): يجمع كمياتها كلها ويبيعها دفعة وحدة.
async function executeManualSellAll(symbol, accountKey, creds, tradeIds, isAuto, label) {
  const trades = manualBotState.tradesByAccount[accountKey] || [];
  const legs = tradeIds.map(id => trades.find(t => t.id === id && t.status !== 'sold')).filter(Boolean);
  if (!legs.length) throw new Error(`لا توجد صفقات مفتوحة على ${symbol} حاليًا`);
  const rawQty = legs.reduce((s, t) => s + t.qty, 0);
  const price = await getCurrentPrice(symbol);
  const qty = roundQty(rawQty, price);
  if (!qty || qty <= 0) throw new Error('الكمية المجمّعة للبيع صفر');
  const data = await placeMarketOrder(symbol, 'SELL', qty, creds);
  let fillPrice = price;
  if (data.fills && data.fills.length) {
    const totalQty = data.fills.reduce((s, f) => s + parseFloat(f.qty), 0);
    const totalCost = data.fills.reduce((s, f) => s + parseFloat(f.qty) * parseFloat(f.price), 0);
    if (totalQty > 0) fillPrice = totalCost / totalQty;
  }
  const sellPrice = roundPrice(fillPrice);
  for (const t of legs) { t.status = 'sold'; t.sellPrice = sellPrice; }
  const avgBuy = legs.reduce((s, t) => s + t.qty * t.buyPrice, 0) / legs.reduce((s, t) => s + t.qty, 0);
  botState.tradeLog.unshift({
    time: Date.now(), symbol, type: 'order', side: 'SELL', price: sellPrice, qty,
    reason: `${label || (isAuto ? '🤖 بيع تلقائي (ارتفاع)' : '🔴 بيع')} ${legs.length} مرحلة شراء مجمّعة — متوسط الشراء ${roundPrice(avgBuy)} → بيع عند ${sellPrice}`,
  });
  botState.tradeLog = botState.tradeLog.slice(0, 50);
  return { sellPrice, qty, legsSold: legs.length };
}

// 🔍 تدقيق المراحل المسجّلة عند البوت مقابل أوامر Binance الحقيقية: يصحّح التكلفة والكمية من المصدر، ويحذف أي مرحلة
// أمرها غير موجود/ملغى/غير منفّذ (شراء وهمي). فشل الاتصال ما يغيّر شيئًا (نعتبر المرحلة سليمة ونعيد التدقيق لاحقًا).
async function auditAutoLegs(accountKey, a, creds) {
  const trades = manualBotState.tradesByAccount[accountKey] || [];
  const rep = { symbol: a.symbol, checked: 0, phantom: 0, fixed: 0, unknown: 0, recordedCost: 0, binanceCost: 0, orders: [] };
  for (const leg of (a.legs || [])) {
    const trade = trades.find(t => t.id === leg.id);
    rep.recordedCost += (leg.cost > 0 ? leg.cost : leg.qty * leg.price);
    const orderId = leg.orderId || (trade && trade.buyOrderId);
    if (!trade || !orderId) { rep.unknown++; continue; }
    let o = null, gone = false;
    try { o = await queryOrderStatus(a.symbol, orderId, creds); }
    catch (err) {
      const code = err && err.response && err.response.data && err.response.data.code;
      if (code === -2013) gone = true; else { rep.unknown++; continue; }
    }
    rep.checked++;
    const exec = o ? parseFloat(o.executedQty) : 0;
    if (gone || !(exec > 0)) {
      trade.status = 'sold'; trade.phantom = true; leg.phantom = true; rep.phantom++;
      rep.orders.push({ orderId, status: gone ? 'NOT_FOUND' : o.status, cost: 0 });
      continue;
    }
    const quote = parseFloat(o.cummulativeQuoteQty);
    const realCost = quote > 0 ? quote : exec * leg.price;
    rep.binanceCost += realCost;
    rep.orders.push({ orderId, status: o.status, cost: realCost });
    const safeQty = roundQty(exec * 0.999, leg.price);
    if (Math.abs(realCost - (leg.cost || 0)) > 0.005 || (safeQty > 0 && Math.abs(safeQty - leg.qty) / leg.qty > 0.01)) {
      leg.cost = realCost; trade.cost = realCost;
      if (safeQty > 0 && Math.abs(safeQty - leg.qty) / leg.qty > 0.01) { leg.qty = safeQty; trade.qty = safeQty; }
      rep.fixed++;
    }
  }
  a.lastAuditAt = Date.now();
  return rep;
}

// 🔄 دورة بوت البيع التلقائي — كل 10 ثوانٍ (أسرع من دورة الشبكة، حسب طلب المستخدم بالسرعة):
// 1) أي صفقة "open" بدون أمر بيع بعد → نضع لها أمر بيع Limit فورًا حسب نسبة البيع الحالية.
// 2) أي صفقة "pending_sell" → نتحقق هل أمر البيع نُفذ، ولو نعم نعلّمها "sold".
async function runManualSellCycle() {
  if (!manualBotState.enabled) return;
  const pct = botState.takeProfitPercent / 100;
  for (const [accountKey, trades] of Object.entries(manualBotState.tradesByAccount)) {
    if (!trades.length) continue;
    const creds = getCredsForAccountKey(accountKey);
    for (const trade of trades) {
      if (trade.status === 'sold' || trade.auto) continue; // صفقات التداول التلقائي يبيعها محرّك النسبة بنفسه
      try {
        if (trade.status === 'open') {
          const sellPrice = roundPrice(trade.buyPrice * (1 + pct));
          const data = await placeLimitOrder(trade.symbol, 'SELL', sellPrice, trade.qty, creds);
          if (data.orderId) {
            trade.sellOrderId = data.orderId;
            trade.sellPrice = sellPrice;
            trade.status = 'pending_sell';
            botState.tradeLog.unshift({ time: Date.now(), symbol: trade.symbol, type: 'order', side: 'SELL', price: sellPrice, qty: trade.qty, exchange: 'binance', reason: `📤 أمر بيع تلقائي (${botState.takeProfitPercent}%) عند ${sellPrice}` });
          }
        } else if (trade.status === 'pending_sell' && trade.sellOrderId) {
          const status = await queryOrderStatus(trade.symbol, trade.sellOrderId, creds);
          if (status.status === 'FILLED') {
            trade.status = 'sold';
            botState.tradeLog.unshift({ time: Date.now(), symbol: trade.symbol, type: 'error', message: `✅ تم تنفيذ أمر البيع عند ${trade.sellPrice} — الصفقة اكتملت` });
          }
        }
      } catch (err) { logBotError(trade.symbol, err); }
    }
    // نحتفظ بسجل الصفقات المباعة آخر 6 ساعات بس، بعدها تُحذف من القائمة (تبقى بسجل الأحداث النصي)
    // نفلتر المصفوفة *الحالية* (مو النسخة القديمة اللي التقطناها بأول الدورة): أثناء انتظار Binance قد يشتري التداول
    // التلقائي مرحلة جديدة وتُضاف لمصفوفة جديدة، والكتابة فوقها بالنسخة القديمة كانت تمسح تلك المرحلة من السجل
    // فيظن المحرك أنها اتباعت ويتغيّر العدّاد.
    const liveTrades = manualBotState.tradesByAccount[accountKey] || trades;
    manualBotState.tradesByAccount[accountKey] = liveTrades.filter(t => t.status !== 'sold' || (Date.now() - t.time) < 6 * 60 * 60 * 1000);
  }
  botState.tradeLog = botState.tradeLog.slice(0, 50);
  broadcastBotStatus();
}
setInterval(runManualSellCycle, 10 * 1000);

// ── التداول التلقائي بالنسبة (مربع النسبة بجانب مبلغ الشراء/البيع) ─────────────────────────────
// يعمل على السيرفر (يستمر حتى لو أغلقت الصفحة). الدورة: ينتظر نزول السعر بنسبة pct من أعلى سعر وصله
// منذ التفعيل → شراء Market → ينتظر ارتفاع السعر pct فوق سعر الشراء → بيع Market → يعيد الدورة.
// صفقة واحدة فقط بنفس الوقت لكل حساب. الإعدادات والصفقة المفتوحة تُحفظ بملف auto-trade.json وتُستعاد بعد إعادة التشغيل.
const AUTO_FILE = path.join(DATA_DIR, 'auto-trade.json');
let autoTradeByAccount = {}; // accountKey -> { pct, symbol, amountUsdt, state: 'waiting_buy'|'holding', peak, position, fails, pausedUntil, busy }
function saveAutoTrade() {
  const out = {};
  for (const [k, a] of Object.entries(autoTradeByAccount)) { const { busy, ...rest } = a; out[k] = rest; }
  try { fs.writeFileSync(AUTO_FILE, JSON.stringify(out, null, 2), { mode: 0o600 }); if (typeof mirrorCopy === 'function') mirrorCopy(AUTO_FILE); }
  catch (err) { console.error('⚠️ فشل حفظ auto-trade.json:', err.message); }
}
// 🔄 الصفقة "الفعلية" للحسابات: المراحل الحقيقية (a.legs) تبقى كما هي لأجل البيع الفعلي، لكن بعد "إعادة ضبط بدون بيع"
// نطوي كل المراحل التي كانت موجودة وقتها بمرحلة افتراضية واحدة بسعر إعادة الضبط (resetBase) — فيحسب البوت المتوسط والمرحلة
// ونسبة البيع كأنه اشترى للتو بذلك السعر. أي مراحل جديدة تُشترى بعدها تُضاف فوقها طبيعيًا.
function effectiveLegs(a) {
  const legs = a.legs || [];
  const rb = a.resetBase;
  if (!rb || !Array.isArray(rb.ids) || !(rb.price > 0)) return legs;
  const folded = legs.filter(l => rb.ids.includes(l.id));
  if (!folded.length) return legs;
  const rest = legs.filter(l => !rb.ids.includes(l.id));
  const qty = folded.reduce((s, l) => s + l.qty, 0);
  return [{ id: folded[0].id, qty, price: rb.price, time: rb.time || folded[0].time, virtual: true }, ...rest];
}
// إجمالي التكلفة الحقيقية (USDT) لكل ما اشتراه البوت في الصفقة الحالية — لا يتأثر بإعادة الضبط
function legsTotalCost(a) {
  return (a.legs || []).reduce((s, l) => s + (l.cost > 0 ? l.cost : l.qty * l.price), 0);
}
function loadAutoTrade() {
  if (typeof mirrorRestore === 'function') mirrorRestore(AUTO_FILE);
  try { autoTradeByAccount = JSON.parse(fs.readFileSync(AUTO_FILE, 'utf8')) || {}; } catch (err) { autoTradeByAccount = {}; }
  for (const [k, a] of Object.entries(autoTradeByAccount)) {
    a.busy = false; a.fails = 0; a.pausedUntil = 0; a.problem = null; a.levels = a.levels || 1; a.levelPct = a.levelPct || {};
    if (a.sellPct === undefined) a.sellPct = null;
    // توافق مع التصميم القديم (5 صفوف حسب المرحلة): نأخذ صف مرحلة العدد الحالي أو أول صف موجود كوقف خسارة واحد
    if (a.stopLoss && typeof a.stopLoss === 'object' && a.stopLoss.pct === undefined) {
      const ks = Object.keys(a.stopLoss);
      a.stopLoss = ks.length ? (a.stopLoss[a.levels] || a.stopLoss[ks[0]] || null) : null;
    }
    if (!a.stopLoss || !(a.stopLoss.pct > 0)) a.stopLoss = null;
    if (!a.softReset || typeof a.softReset !== 'object') a.softReset = null;
    if (a.resetCount === undefined) a.resetCount = 0;
    if (a.holdSince === undefined) a.holdSince = null;
    if (!(a.anchorTol > 0)) a.anchorTol = 0;
    a.overshot = !!a.overshot; a.buyRetryAt = 0;
    if (a.resetBase === undefined) a.resetBase = null;
    if (a.stopLossWatch === undefined) a.stopLossWatch = null;
    if (!(a.anchor > 0)) { a.anchor = a.peak; a.armed = true; } else if (a.armed === undefined) a.armed = true;
    // توافق الصفقات القديمة (قبل مرحلة الشراء المتدرّج): كانت تخزّن position مفردة بدل legs — نحوّلها
    if (a.position && !a.legs) { a.legs = [a.position]; a.lastBuyPrice = a.position.buyPrice; delete a.position; }
    if (!a.legs) a.legs = [];
    if (a.legs.length && a.state === 'holding') {
      if (!a.holdSince) a.holdSince = Date.now();
      const trades = manualBotState.tradesByAccount[k] || (manualBotState.tradesByAccount[k] = []);
      for (const leg of a.legs) {
        if (!trades.some(t => t.id === leg.id)) {
          trades.unshift({ id: leg.id, symbol: a.symbol, qty: leg.qty, buyPrice: leg.price, buyOrderId: leg.orderId || null, sellOrderId: null, sellPrice: null, status: 'open', time: leg.time, auto: true, cost: leg.cost });
        }
      }
    } else { a.state = 'waiting_buy'; a.legs = []; a.lastBuyPrice = null; a.stopLossWatch = null; a.resetBase = null; a.resetCount = 0; a.holdSince = null; }
  }
}
function notifyAccount(accountKey, payload) {
  const str = JSON.stringify(payload);
  for (const c of wss.clients) if (c.readyState === WebSocket.OPEN && getAccountKeyForWs(c) === accountKey) c.send(str);
}
// 🩺 تصنيف أخطاء Binance المعروفة أثناء التداول التلقائي — نحدد هل المشكلة مؤقتة تُصلح نفسها
// (شبكة/توقيت/تجاوز حد طلبات) أو تحتاج تدخل المستخدم (رصيد/مفتاح API/كمية أقل من الحد الأدنى) —
// بكل الحالات البوت يستمر يحاول من جديد، بس بفواصل زمنية متدرّجة بدل ما يوقف نفسه نهائيًا.
function classifyTradeError(err) {
  const raw = String(err.response?.data?.msg || err.message || 'خطأ غير معروف');
  const apiCode = err.response?.data?.code;
  const low = raw.toLowerCase();
  if (low.includes('insufficient balance'))
    return { kind: 'balance', needsAction: true, arabic: 'الرصيد غير كافٍ بمحفظة Spot — عبّئ رصيدك أو قلّل مبلغ الصفقة', raw };
  if (low.includes('invalid api-key') || low.includes('ip, or permissions'))
    return { kind: 'api_key', needsAction: true, arabic: 'مفتاح API غير صالح، أو الـ IP غير مسموح، أو صلاحيات المفتاح ناقصة', raw };
  if (low.includes('timestamp') && (low.includes('recvwindow') || low.includes('ahead of') || low.includes('outside')))
    return { kind: 'timestamp', needsAction: false, autoFix: 'resync_time', arabic: 'فرق توقيت مؤقت مع سيرفر Binance — يُصلح تلقائيًا الآن', raw };
  if (apiCode === -1003 || low.includes('too many requests') || low.includes('way too much request weight') || low.includes('banned until'))
    return { kind: 'rate_limit', needsAction: false, autoFix: 'extra_backoff', arabic: 'تجاوزنا الحد المسموح من الطلبات مؤقتًا — ننتظر أكثر ثم نكمل', raw };
  if (low.includes('min_notional') || low.includes('notional') || low.includes('lot_size') || low.includes('filter failure'))
    return { kind: 'filter', needsAction: true, arabic: 'مبلغ/كمية الصفقة أقل من الحد الأدنى المسموح على Binance لهذه العملة — كبّر مبلغ الشراء', raw };
  if (low.includes('invalid symbol'))
    return { kind: 'symbol', needsAction: true, arabic: 'هذا الرمز غير متاح للتداول الفوري على Binance حاليًا', raw };
  if (low.includes('econnreset') || low.includes('etimedout') || low.includes('enotfound') || low.includes('timeout') || low.includes('network'))
    return { kind: 'network', needsAction: false, autoFix: 'retry', arabic: 'مشكلة اتصال مؤقتة بالشبكة — سنعيد المحاولة', raw };
  return { kind: 'unknown', needsAction: false, arabic: 'خطأ غير معروف من Binance', raw };
}
// فاصل الانتظار قبل إعادة المحاولة: يتضاعف مع تكرار نفس المشكلة (حتى لا نكرر نفس الطلب المرفوض كل 3 ثوانٍ
// ونخاطر بحظر مؤقت من Binance)، بسقف أعلى. أخطاء تجاوز حد الطلبات تاخذ انتظار أطول من البداية.
function nextAutoTradeBackoffMs(fails, autoFix) {
  if (autoFix === 'extra_backoff') return Math.min(60000 * fails, 10 * 60 * 1000); // 1 دقيقة × عدد المحاولات، سقف 10 دقائق
  const base = 20000; // يبدأ 20 ثانية
  return Math.min(base * Math.pow(2, Math.max(0, fails - 1)), 5 * 60 * 1000); // يتضاعف، سقف 5 دقائق
}
// يُستدعى عند أي فشل شراء/بيع بالتداول التلقائي — بدل ما يوقف البوت نهائيًا (كان القديم يحذف الإعداد كامل)،
// نسجّل المشكلة بوضوح (تظهر بلوحة التنبيهات الدائمة بالواجهة، وتبقى بـ bot_status لحد ما تنحل حتى لو
// المستخدم رجع بعد ساعات)، ونجدول إعادة محاولة تلقائية بفاصل متدرّج. الإعدادات نفسها (% والمراحل) ما تنمسح أبدًا.
function handleAutoTradeFailure(a, accountKey, err, context) {
  const cls = classifyTradeError(err);
  a.fails = (a.fails || 0) + 1;
  // فشل شراء مرحلة إضافية (مثلًا الرصيد خلص): الصفقة الحالية سليمة، فلا نوقف الدورة — نؤجّل محاولات الشراء فقط،
  // ويستمر العداد وفحص البيع/وقف الخسارة/إعادة الضبط كل ثانية بدون أي تأخير.
  const levelBuy = typeof context === 'string' && context.startsWith('buy_level_');
  let retryAt;
  if (cls.autoFix === 'resync_time') { syncBinanceTime(); retryAt = Date.now() + 5000; }
  else retryAt = Date.now() + nextAutoTradeBackoffMs(a.fails, cls.autoFix);
  if (cls.autoFix !== 'extra_backoff') retryAt = Math.min(retryAt, Date.now() + 60000); // الدورة لا تتوقف، فنعيد محاولة الأمر خلال دقيقة كحد أقصى
  if (levelBuy) { a.buyRetryAt = retryAt; }
  else a.pausedUntil = retryAt;
  const message = (levelBuy && cls.kind === 'balance')
    ? 'لا يوجد رصيد USDT كافٍ لشراء المرحلة التالية — صفقتك الحالية سليمة والبوت يواصل مراقبة البيع، ويعيد محاولة الشراء كل دقيقة'
    : cls.arabic;
  a.problem = {
    kind: cls.kind, needsAction: levelBuy && cls.kind === 'balance' ? false : !!cls.needsAction, message, detail: cls.raw, context,
    fails: a.fails, since: (a.problem && a.problem.since) || Date.now(), lastAt: Date.now(), retryAt,
  };
  saveAutoTrade();
  notifyAccount(accountKey, { type: 'auto_problem', symbol: a.symbol, active: true, ...a.problem });
  logBotError(a.symbol, err);
}
// يُستدعى عند أي نجاح (شراء أو بيع) — يصفّر عدّاد الفشل ويعلم الواجهة إن المشكلة السابقة (إن وجدت) انحلّت.
function clearAutoTradeProblem(a, accountKey) {
  const hadProblem = !!a.problem;
  a.fails = 0; a.problem = null; a.buyRetryAt = 0;
  if (hadProblem) notifyAccount(accountKey, { type: 'auto_problem', symbol: a.symbol, active: false });
}
async function runAutoTradeCycle() {
  const keys = Object.keys(autoTradeByAccount);
  if (!keys.length) return;
  const priceCache = {};
  for (const accountKey of keys) {
    const a = autoTradeByAccount[accountKey];
    if (!a || a.busy) continue;
    // ⏱ فشل سابق (رصيد/اتصال…): ما نوقف الدورة أبدًا — العداد والمتابعة يستمران كل ثانية، وفقط *إرسال أمر جديد* ينتظر مؤقّت إعادة المحاولة
    const paused = Date.now() < (a.pausedUntil || 0);
    a.busy = true; // يمنع تداخل دورتين (أمر Binance قد ياخذ عدة ثوانٍ) وبالتالي شراء مزدوج
    try {
      let price = priceCache[a.symbol];
      if (!(price > 0)) {
        try { price = priceCache[a.symbol] = await getCurrentPrice(a.symbol); } catch { continue; } // خطأ شبكة عابر: نتخطى الدورة
      }
      const creds = getCredsForAccountKey(accountKey);
      if (!a.legs) a.legs = [];
      if (a.state === 'waiting_buy') {
        if (!(a.anchor > 0)) { a.anchor = price; a.armed = true; a.peak = price; } // توافق صفقات قديمة بدون مرساة محفوظة
        else if (!a.armed) {
          // سماحية الصعود فوق المرساة (مربع المستخدم): تحتها يعمل عادي من أول لحظة، وفوقها ينتظر الرجوع لحدّها.
          // السماحية 0 (الافتراضي) = السلوك السابق تمامًا (ينتظر الرجوع للمرساة نفسها).
          const limit = a.anchor * (1 + (a.anchorTol > 0 ? a.anchorTol : 0) / 100);
          if (price <= limit) { a.armed = true; a.peak = a.overshot ? limit : Math.max(a.anchor, price); a.overshot = false; }
          else a.overshot = true; // فوق الحد: ما نحسب شي، فقط ننتظر رجوعه — يمنع الشراء في منتصف صعود قوي (للمرحلة الأولى فقط)
        } else if (price > a.peak) a.peak = price;
      }
      // ── حالة الانتظار قبل أول شراء ──────────────────────────────────────────────
      if (a.state === 'waiting_buy') {
        const move = a.armed ? (a.peak - price) / a.peak * 100 : null;
        notifyAccount(accountKey, { type: 'auto_progress', symbol: a.symbol, state: 'waiting_buy', pct: a.pct, move, price, anchor: a.anchor, anchorLimit: a.anchor * (1 + (a.anchorTol > 0 ? a.anchorTol : 0) / 100), armed: a.armed, legsDone: 0, levels: a.levels });
        if (a.armed) {
          const dropPct = (a.peak - price) / a.peak * 100;
          if (dropPct >= a.pct && !paused) {
            let trade;
            try { trade = await executeManualBuy(a.symbol, accountKey, creds, a.amountUsdt, true); }
            catch (err) {
              // فشل الشراء: لا نوقف التداول التلقائي أبدًا — نسجّل المشكلة ونعيد المحاولة تلقائيًا بفاصل متدرّج
              handleAutoTradeFailure(a, accountKey, err, 'buy');
              broadcastBotStatus();
              continue;
            }
            a.state = 'holding';
            a.legs = [{ id: trade.id, qty: trade.qty, price: trade.buyPrice, cost: trade.cost, orderId: trade.buyOrderId, time: trade.time }];
            a.lastBuyPrice = trade.buyPrice; a.resetBase = null; a.resetCount = 0; a.holdSince = Date.now();
            clearAutoTradeProblem(a, accountKey);
            saveAutoTrade();
            notifyAccount(accountKey, { type: 'trade_result', side: 'BUY', symbol: a.symbol });
            broadcastBotStatus();
          }
        }
        continue;
      }
      // ── حالة الإمساك بصفقة (مرحلة شراء واحدة أو أكثر مفتوحة) ──────────────────────
      // نتأكد أولًا إن كل المراحل المسجّلة ما زالت مفتوحة فعليًا (لو المستخدم باع يدويًا بالزر الأحمر، تنحذف من هنا)
      // 🔍 كل 10 دقائق (وأول مرة بعد التشغيل) نطابق مراحلنا مع أوامر Binance الحقيقية — لو في اختلاف يُصحَّح تلقائيًا
      if (a.legs.length && Date.now() - (a.lastAuditAt || 0) > 10 * 60 * 1000) {
        try {
          const rep = await auditAutoLegs(accountKey, a, creds);
          if (rep.phantom || rep.fixed) {
            saveAutoTrade();
            notifyAccount(accountKey, { type: 'auto_audit_result', auto: true, ...rep });
          }
        } catch (err) { a.lastAuditAt = Date.now() - 9 * 60 * 1000; /* نعيد المحاولة بعد دقيقة */ }
      }
      const openTrades = manualBotState.tradesByAccount[accountKey] || [];
      const legsBefore = a.legs.length;
      a.legs = a.legs.filter(leg => { const t = openTrades.find(x => x.id === leg.id); return t && t.status !== 'sold'; });
      if (a.resetBase && !a.legs.some(l => a.resetBase.ids.includes(l.id))) a.resetBase = null; // كل المراحل المطويّة انباعت يدويًا
      if (a.legs.length && a.legs.length !== legsBefore) {
        // بيعت مرحلة يدويًا (زر بيع): آخر سعر شراء يصير سعر أحدث مرحلة باقية، وإلا تُحسب المرحلة القادمة من سعر صفقة انباعت
        const lastReal = a.legs[a.legs.length - 1];
        a.lastBuyPrice = (a.resetBase && a.resetBase.ids.includes(lastReal.id)) ? a.resetBase.price : lastReal.price;
        saveAutoTrade(); broadcastBotStatus();
      }
      if (!a.legs.length) { // كل المراحل اتباعت يدويًا → نرجع ننتظر النزول من جديد
        a.state = 'waiting_buy'; a.lastBuyPrice = null; a.peak = price; a.anchor = price; a.armed = false;
        a.resetBase = null; a.resetCount = 0; a.holdSince = null; a.stopLossWatch = null;
        saveAutoTrade(); broadcastBotStatus(); continue;
      }
      // المراحل "الفعلية" للحساب: بعد إعادة ضبط بدون بيع تُطوى المراحل القديمة بمرحلة واحدة بسعر إعادة الضبط
      const vLegs = effectiveLegs(a);
      const totalQty = vLegs.reduce((s, l) => s + l.qty, 0);
      const avgBuyPrice = vLegs.reduce((s, l) => s + l.qty * l.price, 0) / totalQty;
      const sellMove = (price - avgBuyPrice) / avgBuyPrice * 100;
      // نسبة البيع: لو المستخدم حدّد نسبة بيع ثابتة بمربعها الخاص تُستخدم هي دائمًا. وإلا تلقائيًا = نسبة *آخر
      // مرحلة اشترينا فيها فعليًا* (المرحلة 1 النسبة الرئيسية، ومن 2 لـ10 نسبتها المخصّصة لها وإلا الرئيسية).
      const lastLevel = vLegs.length;
      const sellPct = (a.sellPct != null) ? a.sellPct : (lastLevel <= 1 ? a.pct : ((a.levelPct && a.levelPct[lastLevel]) || a.pct));
      // نسبة المرحلة القادمة (لو باقي نشتري أكثر): من مربع نسبتها الخاص وإلا الرئيسية تلقائيًا
      const nextLevel = vLegs.length + 1;
      const nextPct = (a.levelPct && a.levelPct[nextLevel]) || a.pct;
      const buyMove = vLegs.length < (a.levels || 1) ? (a.lastBuyPrice - price) / a.lastBuyPrice * 100 : null;
      notifyAccount(accountKey, {
        type: 'auto_progress', symbol: a.symbol, state: 'holding', pct: sellPct, move: sellMove, price,
        anchor: a.anchor, armed: true, legsDone: vLegs.length, levels: a.levels, avgBuyPrice, lastBuyPrice: a.lastBuyPrice,
        nextLevel, nextPct: vLegs.length < (a.levels || 1) ? nextPct : null, buyMove,
        totalCost: legsTotalCost(a), resetCount: a.resetCount || 0, buysCount: a.legs.length,
      });
      // 🛑 وقف الخسارة و🔄 إعادة الضبط يعملان فقط على الصفقة المكتملة (كل مراحل الشراء المطلوبة تمت).
      const full = vLegs.length >= (a.levels || 1);
      const slRow = (full && a.stopLoss && a.stopLoss.pct > 0) ? a.stopLoss : null;
      const rsRow = (full && a.softReset && (a.softReset.pct > 0 || a.softReset.minutes > 0)) ? a.softReset : null;
      if (a.stopLossWatch && !slRow) { // ألغيت المراقبة (حذف الإعداد أو تغيّرت المراحل) — نوقفها
        a.stopLossWatch = null; saveAutoTrade();
        notifyAccount(accountKey, { type: 'stop_loss_watch', symbol: a.symbol, active: false });
      }
      if (price >= avgBuyPrice * (1 + sellPct / 100) && !paused) {
        // وصل للنسبة فوق متوسط سعر كل المراحل المشتراة → يبيعها كلها بأمر واحد
        try {
          const sold = await executeManualSellAll(a.symbol, accountKey, creds, a.legs.map(l => l.id), true);
          a.state = 'waiting_buy'; a.legs = []; a.lastBuyPrice = null;
          a.peak = sold.sellPrice || price; a.anchor = a.peak; a.armed = false;
          a.resetBase = null; a.resetCount = 0; a.holdSince = null;
          if (a.stopLossWatch) { a.stopLossWatch = null; notifyAccount(accountKey, { type: 'stop_loss_watch', symbol: a.symbol, active: false }); }
          clearAutoTradeProblem(a, accountKey);
          saveAutoTrade();
          notifyAccount(accountKey, { type: 'trade_result', side: 'SELL', symbol: a.symbol });
          broadcastBotStatus();
        } catch (err) {
          // فشل البيع: الصفقة تضل مفتوحة (تقدر تبيعها يدويًا بالزر الأحمر بأي وقت) والتداول التلقائي
          // ما يتوقف أبدًا — يسجّل المشكلة ويعيد المحاولة تلقائيًا بفاصل متدرّج لحد ما تُحل
          handleAutoTradeFailure(a, accountKey, err, 'sell');
          broadcastBotStatus();
        }
      } else if (a.stopLossWatch) {
        // مراقبة وقف الخسارة شغّالة: نتتبع القاع والارتداد، ونبيع قرب أفضل ارتداد بدل أسوأ سعر
        const w = a.stopLossWatch;
        if (price < w.low) { w.low = price; w.peak = price; } else if (price > w.peak) w.peak = price; // قاع جديد = نبدأ تتبع الارتداد من جديد
        const rebounded = w.peak >= w.low * 1.003;               // حصل ارتداد فعلي (0.3% فوق القاع على الأقل)
        const pulledBack = rebounded && price <= w.peak * (1 - 0.003); // ثم تراجع 0.3% عن قمة الارتداد = وقت الخروج
        const timeUp = Date.now() >= w.endsAt;                    // انتهت مدة المراقبة
        if ((timeUp || pulledBack) && !paused) {
          try {
            const sold = await executeManualSellAll(a.symbol, accountKey, creds, a.legs.map(l => l.id), true, '🛑 وقف خسارة تلقائي (بعد مراقبة الارتداد)');
            const sellPrice = sold.sellPrice || price;
            const lossPct = (sellPrice - avgBuyPrice) / avgBuyPrice * 100;
            a.state = 'waiting_buy'; a.legs = []; a.lastBuyPrice = null;
            a.peak = sellPrice; a.anchor = a.peak; a.armed = false; a.stopLossWatch = null;
            a.resetBase = null; a.resetCount = 0; a.holdSince = null;
            clearAutoTradeProblem(a, accountKey);
            saveAutoTrade();
            notifyAccount(accountKey, { type: 'stop_loss_watch', symbol: a.symbol, active: false, sold: true, sellPrice, avgBuyPrice, lossPct, reason: pulledBack ? 'rebound' : 'time' });
            notifyAccount(accountKey, { type: 'trade_result', side: 'SELL', symbol: a.symbol, stopLoss: true, lossPct });
            broadcastBotStatus();
          } catch (err) {
            handleAutoTradeFailure(a, accountKey, err, 'stop_loss_sell');
            broadcastBotStatus();
          }
        }
      } else if (slRow || rsRow) {
        const stopPrice = slRow ? avgBuyPrice * (1 - slRow.pct / 100) : null;
        const stopHit = !!slRow && price <= stopPrice;
        const resetByPct = !!(rsRow && rsRow.pct > 0) && price <= avgBuyPrice * (1 - rsRow.pct / 100);
        // الوقت يُحسب من آخر شراء/إعادة ضبط، ولا يُفعَّل إلا والسعر تحت متوسط الشراء (حتى لا نرفع نقطة البيع وإحنا برابحين)
        const resetByTime = !!(rsRow && rsRow.minutes > 0) && price < avgBuyPrice && a.holdSince > 0 && (Date.now() - a.holdSince) >= rsRow.minutes * 60000;
        const resetHit = resetByPct || resetByTime;
        // لو الاثنين انكسرا بنفس الدورة: وقف الخسارة (بيع) يغلب، إلا لو نسبة إعادة الضبط أصغر من نسبة وقف الخسارة (يعني كانت تسبقه)
        const stopWins = stopHit && !(resetByPct && slRow && rsRow.pct < slRow.pct);
        if (stopWins) {
          // السعر كسر وقف الخسارة: ما نبيع فورًا بأسوأ سعر — نبدأ مراقبة لمدة محدّدة بانتظار أفضل ارتداد
          a.stopLossWatch = { pct: slRow.pct, minutes: slRow.minutes, stopPrice, startedAt: Date.now(), endsAt: Date.now() + slRow.minutes * 60000, low: price, peak: price };
          saveAutoTrade();
          notifyAccount(accountKey, { type: 'stop_loss_watch', symbol: a.symbol, active: true, price, ...a.stopLossWatch });
          broadcastBotStatus();
        } else if (resetHit) {
          // 🔄 إعادة ضبط بدون بيع: ما يصير أي أمر على Binance. نطوي كل المراحل الحالية بمرحلة واحدة بالسعر الحالي،
          // فيرجع البوت يشتغل كأنه اشترى للتو بهذا السعر: نسبة البيع الأولى من السعر الحالي، والمراحل تبدأ من 2 من جديد.
          a.resetBase = { price, ids: a.legs.map(l => l.id), time: Date.now() };
          a.lastBuyPrice = price; a.holdSince = Date.now(); a.resetCount = (a.resetCount || 0) + 1;
          saveAutoTrade();
          notifyAccount(accountKey, { type: 'soft_reset', symbol: a.symbol, price, oldAvg: avgBuyPrice, reason: resetByPct ? 'pct' : 'time', resetCount: a.resetCount, totalCost: legsTotalCost(a) });
          broadcastBotStatus();
        }
      } else if (vLegs.length < (a.levels || 1) && Date.now() >= (a.buyRetryAt || 0)) {
        // ما وصلنا عدد مرات الشراء المطلوب بعد → ننتظر نزول نسبة *هذي المرحلة تحديدًا* (مخصّصة لها أو الرئيسية تلقائيًا)
        // من آخر سعر اشترينا فيه عشان نشتري مرحلة جديدة
        if (buyMove >= nextPct) {
          try {
            const trade = await executeManualBuy(a.symbol, accountKey, creds, a.amountUsdt, true);
            a.legs.push({ id: trade.id, qty: trade.qty, price: trade.buyPrice, cost: trade.cost, orderId: trade.buyOrderId, time: trade.time });
            a.lastBuyPrice = trade.buyPrice; a.holdSince = Date.now();
            clearAutoTradeProblem(a, accountKey);
            saveAutoTrade();
            notifyAccount(accountKey, { type: 'trade_result', side: 'BUY', symbol: a.symbol });
            broadcastBotStatus();
          } catch (err) {
            // رصيدك الحر أقل بقليل من مبلغ المرحلة (مثلًا 5.02$ والمبلغ 5.1$): نشتري بما تبقّى لو يكفي الحد الأدنى للأمر
            let bought = false;
            if (classifyTradeError(err).kind === 'balance') {
              try {
                const free = await getFreeUsdt(creds);
                if (free >= 5.01 && free < a.amountUsdt) {
                  const trade = await executeManualBuy(a.symbol, accountKey, creds, Math.floor((free - 0.01) * 100) / 100, true);
                  a.legs.push({ id: trade.id, qty: trade.qty, price: trade.buyPrice, cost: trade.cost, orderId: trade.buyOrderId, time: trade.time });
                  a.lastBuyPrice = trade.buyPrice; a.holdSince = Date.now();
                  clearAutoTradeProblem(a, accountKey);
                  saveAutoTrade();
                  notifyAccount(accountKey, { type: 'trade_result', side: 'BUY', symbol: a.symbol });
                  broadcastBotStatus();
                  bought = true;
                }
              } catch (e2) { /* نرجع للمعالجة العادية بالأسفل */ }
            }
            if (!bought) {
              // فشل شراء مرحلة إضافية: المراحل السابقة تضل مفتوحة (تقدر تبيعها يدويًا لو احتجت)، والتداول
              // التلقائي يستمر يحاول من جديد تلقائيًا بفاصل متدرّج بدل ما يتوقف
              handleAutoTradeFailure(a, accountKey, err, `buy_level_${vLegs.length + 1}`);
              broadcastBotStatus();
            }
          }
        }
      }
    } catch (err) { logBotError(a.symbol, err); }
    finally { a.busy = false; }
  }
}
loadAutoTrade();
setInterval(runAutoTradeCycle, 3000);

server.listen(PORT, () => console.log(`Crypto Dashboard running on port ${PORT}`));
