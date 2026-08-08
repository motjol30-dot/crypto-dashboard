'use strict';

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const axios = require('axios');
const path = require('path');
const crypto = require('crypto');
const { RSI, MACD, BollingerBands, EMA } = require('technicalindicators');

const PORT = process.env.PORT || 3000;

const SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'AVAXUSDT', 'SHIBUSDT', 'PEPEUSDT', 'PAXGUSDT'];
// ملاحظة: طلب المستخدم فريم 20 دقيقة أيضًا، لكنه غير مدعوم أصلًا من أي مصدر بيانات (Binance/MEXC/OKX)
// كشمعة حقيقية — أقرب فريمين متوفرين فعليًا هما 15m و30m، فاستبعدناه بدل تركيب شموع اصطناعية هشة.
const INTERVALS = ['3m', '5m', '15m', '30m', '1h', '2h', '4h'];

// MEXC WebSocket interval codes
const MEXC_WS_INTERVAL = { '3m': 'Min3', '5m': 'Min5', '15m': 'Min15', '30m': 'Min30', '1h': 'Hour1', '2h': 'Hour2', '4h': 'Hour4' };
// OKX يستخدم صيغة مختلفة للساعات وما فوق (حرف H كبير) بعكس باقي المصادر
const OKX_BAR = { '3m': '3m', '5m': '5m', '15m': '15m', '30m': '30m', '1h': '1H', '2h': '2H', '4h': '4H' };

// candleStore[key] = [{ time, open, high, low, close, volume }]
const candleStore = {};
// streamWs[key] = WebSocket | null (null = loading, undefined = not started)
const streamWs = {};
// clientSubs: Map<ws, { symbol, interval }>
const clientSubs = new Map();

const app = express();
const server = http.createServer(app);

/* ============================================================
 * الحماية: كلمة مرور (Basic Auth) + حد أقصى لعدد المستخدمين المتزامنين
 * مصمّمة عشان تشارك رابط التجربة بأمان مع جروب محدود (تليجرام مثلًا)
 * بدون أي مكتبات خارجية إضافية — فقط env vars + جلسة بسيطة عبر كوكي
 * ============================================================ */
const APP_USER = process.env.APP_USER || 'group';
const APP_PASS = process.env.APP_PASS || 'change-me-1234';
if (!process.env.APP_PASS) {
  console.warn('⚠️  تحذير: APP_PASS غير معرّف كمتغير بيئة — يتم استخدام كلمة مرور افتراضية غير آمنة (change-me-1234). عرّف APP_USER و APP_PASS قبل مشاركة الرابط.');
}

const MAX_USERS = parseInt(process.env.MAX_USERS || '12', 10); // أقصى عدد أشخاص متزامنين (كل شخص = جلسة واحدة بغض النظر عن عدد اتصالات WS)
const SESSION_TTL_MS = 15 * 60 * 1000; // 15 دقيقة بدون أي نشاط = تحرّر مكان الجلسة تلقائيًا لشخص جديد

// معطّلة افتراضيًا حاليًا بناءً على طلبك (تأجيل الفكرة). فعّلها لاحقًا بتعيين متغيّر بيئة AUTH_ENABLED=true
// (وحدد وقتها APP_USER و APP_PASS كمان). طالما معطّلة، الموقع مفتوح للجميع بدون كلمة مرور ولا حد للمستخدمين.
const AUTH_ENABLED = process.env.AUTH_ENABLED === 'true';
if (!AUTH_ENABLED) {
  console.warn('⚠️ الحماية بكلمة المرور معطّلة حاليًا (AUTH_ENABLED غير مفعّل) — الموقع مفتوح لأي شخص لديه الرابط.');
}

const activeSessions = new Map(); // sid -> آخر وقت نشاط

/* ============================================================
 * بوت التداول التلقائي: مفاتيح API لكل منصة — تُقرأ من متغيرات البيئة فقط
 * ولا تُرسَل أبدًا للواجهة الأمامية. البوت يبدأ دائمًا معطّلاً (enabled=false)
 * بغض النظر عن وجود المفاتيح — لازم المستخدم يفعّله يدويًا من اللوحة كل مرة.
 * ============================================================ */
const BINANCE_API_KEY = process.env.BINANCE_API_KEY || '';
const BINANCE_API_SECRET = process.env.BINANCE_API_SECRET || '';
const MEXC_API_KEY = process.env.MEXC_API_KEY || '';
const MEXC_API_SECRET = process.env.MEXC_API_SECRET || '';

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
  // مقارنة بزمن ثابت لتفادي هجمات قياس التوقيت (timing attack) على كلمة المرور
  const safeEqual = (a, b) => {
    const bufA = Buffer.from(a), bufB = Buffer.from(b);
    if (bufA.length !== bufB.length) return false;
    return crypto.timingSafeEqual(bufA, bufB);
  };
  return safeEqual(user, APP_USER) && safeEqual(pass, APP_PASS);
}

// طبقة 1: كلمة المرور — تُطبَّق على كل طلبات HTTP (الصفحة + أي مسار /api)
function authMiddleware(req, res, next) {
  if (!checkBasicAuth(req)) {
    res.set('WWW-Authenticate', 'Basic realm="Crypto Dashboard"');
    return res.status(401).send('كلمة المرور مطلوبة للدخول إلى هذه اللوحة.');
  }
  next();
}

// طبقة 2: حد أقصى لعدد المستخدمين المتزامنين — تعمل بعد التأكد من كلمة المرور
function sessionLimitMiddleware(req, res, next) {
  sweepSessions();
  const cookies = parseCookies(req);
  const existingSid = cookies.sid;
  const isKnown = existingSid && activeSessions.has(existingSid);

  if (!isKnown && activeSessions.size >= MAX_USERS) {
    return res.status(503).send(`الموقع ممتلئ حاليًا (الحد الأقصى ${MAX_USERS} مستخدم متزامن). حاول مرة أخرى بعد قليل.`);
  }

  const sid = isKnown ? existingSid : crypto.randomBytes(16).toString('hex');
  if (!isKnown) {
    res.cookie('sid', sid, { httpOnly: true, sameSite: 'lax', maxAge: SESSION_TTL_MS });
  }
  activeSessions.set(sid, Date.now());
  req.sid = sid;
  next();
}

if (AUTH_ENABLED) {
  app.use(authMiddleware);
  app.use(sessionLimitMiddleware);
}

// ── REST ──────────────────────────────────────────────────────────────────────

app.get('/', (_req, res) => res.sendFile(path.join(__dirname, 'index.html')));
app.get('/favicon.ico', (_req, res) => res.status(204).end());
app.get('/api/symbols', (_req, res) => res.json({ symbols: SYMBOLS, intervals: INTERVALS }));

/* ============================================================
 * نظرة عامة على السوق: 5 مؤشرات مجانية، مع تخزين مؤقت (Cache)
 * لتجنب استهلاك حدود الطلبات المجانية لكل مصدر
 * ============================================================ */
let marketCache = { data: null, ts: 0 };
const MARKET_CACHE_MS = 60 * 1000; // تحديث كل دقيقة

app.get('/api/market-overview', async (_req, res) => {
  const now = Date.now();
  if (marketCache.data && now - marketCache.ts < MARKET_CACHE_MS) {
    return res.json(marketCache.data);
  }

  const result = {
    fearGreed: null,
    btcDominance: null,
    totalMarketCap: null,
    marketCapChange24h: null,
    totalVolume24h: null,
    fundingRate: null,
    openInterest: null,
    updatedAt: new Date().toISOString(),
    errors: [],
  };

  // 1) مؤشر الخوف والطمع (مجاني بالكامل، بدون مفتاح)
  try {
    const r = await axios.get('https://api.alternative.me/fng/?limit=1', { timeout: 8000 });
    const d = r.data?.data?.[0];
    if (d) result.fearGreed = { value: Number(d.value), label: d.value_classification };
  } catch (e) { result.errors.push('fearGreed'); }

  // 2) هيمنة البيتكوين + القيمة السوقية الإجمالية + الحجم (CoinGecko مجاني)
  try {
    const r = await axios.get('https://api.coingecko.com/api/v3/global', { timeout: 8000, headers: { 'User-Agent': 'Mozilla/5.0 (compatible; CryptoDashboard/1.0)' } });
    const d = r.data?.data;
    if (d) {
      result.btcDominance = d.market_cap_percentage?.btc ?? null;
      result.totalMarketCap = d.total_market_cap?.usd ?? null;
      result.totalVolume24h = d.total_volume?.usd ?? null;
      result.marketCapChange24h = d.market_cap_change_percentage_24h_usd ?? null;
    }
  } catch (e) { result.errors.push('coingecko'); }

  // 3) معدل التمويل + الفائدة المفتوحة لعقود البيتكوين الآجلة (نفس منصة MEXC)
  try {
    const r = await axios.get('https://contract.mexc.com/api/v1/contract/funding_rate/BTC_USDT', { timeout: 8000 });
    const fr = r.data?.data?.fundingRate;
    if (fr != null) result.fundingRate = Number(fr) * 100; // نسبة مئوية
  } catch (e) { result.errors.push('fundingRate'); }

  try {
    const r = await axios.get('https://contract.mexc.com/api/v1/contract/open_interest/BTC_USDT', { timeout: 8000 });
    const oi = r.data?.data?.holdVol ?? r.data?.data?.amount;
    if (oi != null) result.openInterest = Number(oi);
  } catch (e) { result.errors.push('openInterest'); }

  marketCache = { data: result, ts: now };
  res.json(result);
});

/* ============================================================
 * الطبقة الكلية: الدولار (DXY) + النفط (WTI) + مناطق الفيوتشر + الأخبار
 * DXY: نحسبه بأنفسنا من 6 أزواج عملات (معادلة رسمية) — يشتغل بنفس مفتاح Twelve Data المجاني
 * WTI: يحتاج مصدر ثاني لأن Twelve Data يطلب خطة مدفوعة للسلع — Alpha Vantage (مجاني، اختياري)
 * ============================================================ */
const TWELVE_DATA_KEY = process.env.TWELVE_DATA_KEY || '';

let macroCache = { data: null, ts: 0 };
const MACRO_CACHE_MS = 60 * 1000;

// معادلة مؤشر الدولار الرسمية (ICE US Dollar Index) من 6 أزواج عملات رئيسية
async function computeDxyProxy() {
  const pairs = ['EUR/USD', 'USD/JPY', 'GBP/USD', 'USD/CAD', 'USD/SEK', 'USD/CHF'];
  const r = await axios.get('https://api.twelvedata.com/price', {
    params: { symbol: pairs.join(','), apikey: TWELVE_DATA_KEY }, timeout: 8000,
  });
  const d = r.data;
  // لو زوج واحد بس مطلوب، الرد يكون object مباشر بدل object لكل زوج — نطبّع الشكلين
  const get = (sym) => {
    const v = pairs.length === 1 ? d : d[sym];
    if (!v || !v.price) throw new Error(v?.message || `تعذّر جلب ${sym}`);
    return parseFloat(v.price);
  };
  const eurusd = get('EUR/USD'), usdjpy = get('USD/JPY'), gbpusd = get('GBP/USD'),
        usdcad = get('USD/CAD'), usdsek = get('USD/SEK'), usdchf = get('USD/CHF');

  return 50.14348112 *
    Math.pow(eurusd, -0.576) *
    Math.pow(usdjpy, 0.136) *
    Math.pow(gbpusd, -0.119) *
    Math.pow(usdcad, 0.091) *
    Math.pow(usdsek, 0.042) *
    Math.pow(usdchf, 0.036);
}

app.get('/api/macro-overview', async (_req, res) => {
  const now = Date.now();
  if (macroCache.data && now - macroCache.ts < MACRO_CACHE_MS) {
    return res.json(macroCache.data);
  }

  const result = { dxy: null, news: null, dxyError: null, errors: [] };

  if (!TWELVE_DATA_KEY) {
    result.errors.push('no_twelvedata_key');
    result.dxyError = 'لا يوجد مفتاح TWELVE_DATA_KEY';
  } else {
    try {
      result.dxy = await computeDxyProxy();
    } catch (e) {
      result.errors.push('dxy');
      result.dxyError = e.response?.data?.message || e.message;
    }
  }

  macroCache = { data: result, ts: now };
  res.json(result);
});

// ══════════ طبقة البيانات الخام متعددة المنصات (فيوتشر): بايننس + OKX + MEXC ══════════
// نجيب نفس المؤشرات من 3 منصات مختلفة، ونجمّعها/نفلترها بدل الاعتماد على منصة وحدة —
// أي منصة توقفت أو رجعت رقم شاذ ما تكسر النتيجة، والفرق بين المنصات نفسه مؤشر جودة بيانات.

async function fetchBinanceFutures(symbol) {
  const out = {};
  try {
    const r = await axios.get(`https://fapi.binance.com/fapi/v1/premiumIndex?symbol=${symbol}`, { timeout: 8000 });
    if (r.data?.lastFundingRate != null) out.fundingRate = Number(r.data.lastFundingRate) * 100;
  } catch (e) { /* تجاهل — احتمال حظر جغرافي أو العملة غير مدرجة */ }
  try {
    const r = await axios.get(`https://fapi.binance.com/fapi/v1/openInterest?symbol=${symbol}`, { timeout: 8000 });
    if (r.data?.openInterest != null) out.openInterest = Number(r.data.openInterest);
  } catch (e) { /* تجاهل */ }
  try {
    const r = await axios.get(`https://fapi.binance.com/futures/data/globalLongShortAccountRatio?symbol=${symbol}&period=5m&limit=1`, { timeout: 8000 });
    const d = r.data?.[0];
    if (d) {
      out.longShortRatio = Number(d.longShortRatio);
      out.longAccountPct = Number(d.longAccount) * 100;
      out.shortAccountPct = Number(d.shortAccount) * 100;
    }
  } catch (e) { /* تجاهل */ }
  return Object.keys(out).length ? out : null;
}

async function fetchOkxFutures(symbol) {
  const base = symbol.replace(/USDT$/, '');
  const instId = `${base}-USDT-SWAP`;
  const out = {};
  try {
    const r = await axios.get(`https://www.okx.com/api/v5/public/funding-rate?instId=${instId}`, { timeout: 8000 });
    const fr = r.data?.data?.[0]?.fundingRate;
    if (fr != null) out.fundingRate = Number(fr) * 100;
  } catch (e) { /* تجاهل */ }
  try {
    const r = await axios.get(`https://www.okx.com/api/v5/public/open-interest?instId=${instId}`, { timeout: 8000 });
    const oi = r.data?.data?.[0]?.oi;
    if (oi != null) out.openInterest = Number(oi);
  } catch (e) { /* تجاهل */ }
  try {
    const r = await axios.get(`https://www.okx.com/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy=${base}&period=5m`, { timeout: 8000 });
    const d = r.data?.data?.[0]; // [timestamp, ratio]
    if (d && d[1] != null) out.longShortRatio = Number(d[1]);
  } catch (e) { /* تجاهل */ }
  return Object.keys(out).length ? out : null;
}

async function fetchMexcFutures(symbol) {
  const contractSymbol = symbol.replace(/USDT$/, '_USDT');
  const out = {};
  try {
    const r = await axios.get(`https://contract.mexc.com/api/v1/contract/funding_rate/${contractSymbol}`, { timeout: 8000 });
    const fr = r.data?.data?.fundingRate;
    if (fr != null) out.fundingRate = Number(fr) * 100;
  } catch (e) { /* تجاهل */ }
  try {
    const r = await axios.get(`https://contract.mexc.com/api/v1/contract/open_interest/${contractSymbol}`, { timeout: 8000 });
    const oi = r.data?.data?.holdVol ?? r.data?.data?.amount;
    if (oi != null) out.openInterest = Number(oi);
  } catch (e) { /* تجاهل */ }
  return Object.keys(out).length ? out : null;
}

// مناطق الفيوتشر: معدل التمويل + الفائدة المفتوحة + نسبة الشراء/البيع، مجمّعة ومفلترة من 3 منصات (بايننس/OKX/MEXC)
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

  const fundingRates = Object.values(exchanges).map((e) => e.fundingRate).filter((v) => v != null);
  const openInterests = Object.values(exchanges).map((e) => e.openInterest).filter((v) => v != null);
  const longShortRatios = Object.values(exchanges).map((e) => e.longShortRatio).filter((v) => v != null);

  const aggregate = {
    avgFundingRate: fundingRates.length ? fundingRates.reduce((a, b) => a + b, 0) / fundingRates.length : null,
    fundingRateSpread: fundingRates.length >= 2 ? Math.max(...fundingRates) - Math.min(...fundingRates) : null,
    totalOpenInterest: openInterests.length ? openInterests.reduce((a, b) => a + b, 0) : null,
    avgLongShortRatio: longShortRatios.length ? longShortRatios.reduce((a, b) => a + b, 0) / longShortRatios.length : null,
    exchangeCount: Object.keys(exchanges).length,
  };

  // حقول قديمة نسيبها لأجل التوافق مع أي كود سابق يعتمد عليها مباشرة (fundingRate/openInterest المسطّحة)
  res.json({
    fundingRate: aggregate.avgFundingRate,
    openInterest: aggregate.totalOpenInterest,
    exchanges,
    aggregate,
  });
});

// ── Frontend WebSocket server ─────────────────────────────────────────────────

const wss = new WebSocket.Server({ server });

wss.on('connection', (ws, req) => {
  // نفس حماية كلمة المرور تُطبَّق على اتصال الـ WebSocket (المتصفح يرسل نفس الـ Authorization و Cookie تلقائيًا)
  if (AUTH_ENABLED && !checkBasicAuth(req)) {
    ws.close(4001, 'Unauthorized');
    return;
  }
  sweepSessions();
  const cookies = parseCookies(req);
  const sid = cookies.sid;
  const isKnown = sid && activeSessions.has(sid);
  if (AUTH_ENABLED && !isKnown && activeSessions.size >= MAX_USERS) {
    ws.close(4002, 'SERVER_FULL');
    return;
  }
  if (sid) activeSessions.set(sid, Date.now()); // تجديد نشاط الجلسة (لو ما فيه sid أصلًا نادر جدًا، نسمح مرورًا بلا تتبّع)

  // إرسال آخر نتيجة لماسح "العملات المرشحة للانفجار" فورًا، بدل ما ينتظر العميل دورة الفحص التالية (كل 30 ثانية)
  if (explosionRanking.length) {
    ws.send(JSON.stringify({ type: 'explosion_scan', ranking: explosionRanking.slice(0, 2) }));
  }
  ws.send(botStatusPayload());

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

    if (msg.type === 'subscribe') {
      const { symbol, interval } = msg;
      const validSymbol = typeof symbol === 'string' && /^[A-Z0-9]{2,20}USDT$/.test(symbol);
      if (!validSymbol || !INTERVALS.includes(interval)) {
        ws.send(JSON.stringify({ type: 'error', message: 'رمز أو فترة زمنية غير صحيحة' }));
        return;
      }
      if (sid) activeSessions.set(sid, Date.now());
      clientSubs.set(ws, { symbol, interval });
      await ensureStream(symbol, interval);
      await ensureStream(symbol, '15m'); // فريم ثابت لمؤشرات الارتداد (لا يتأثر بتغيير فريم العرض)
      sendSnapshot(ws, symbol, interval);

      // شريط الفريمات: نشغّل بيانات كل الفريمات السبعة لنفس العملة (بالخلفية) عشان تتلوّن مربعاتها كلها فورًا
      for (const iv of INTERVALS) {
        if (iv !== interval && iv !== '15m') ensureStream(symbol, iv).then(() => broadcastMtfUpdate(symbol, true));
      }
      broadcastMtfUpdate(symbol, true);
    }

    // ══ رسائل التحكم في بوت التداول ══
    else if (msg.type === 'bot_toggle') {
      if (msg.enabled) {
        const hasKeys = botState.exchange === 'binance'
          ? (BINANCE_API_KEY && BINANCE_API_SECRET)
          : (MEXC_API_KEY && MEXC_API_SECRET);
        if (!hasKeys) {
          ws.send(JSON.stringify({ type: 'error', message: `لا يوجد مفتاح API معرّف لمنصة ${botState.exchange === 'binance' ? 'Binance' : 'MEXC'} — أضِف متغيرات البيئة أولاً (BINANCE_API_KEY/SECRET أو MEXC_API_KEY/SECRET)` }));
          return;
        }
      }
      botState.enabled = !!msg.enabled;
      broadcastBotStatus();
    }
    else if (msg.type === 'bot_set_exchange') {
      if (msg.exchange === 'mexc' || msg.exchange === 'binance') {
        botState.exchange = msg.exchange;
        broadcastBotStatus();
      }
    }
    else if (msg.type === 'bot_set_trade_size') {
      const v = parseFloat(msg.tradeSizeUsdt);
      if (v > 0 && v <= 100000) { botState.tradeSizeUsdt = v; broadcastBotStatus(); }
    }
    else if (msg.type === 'bot_set_take_profit') {
      const v = parseFloat(msg.takeProfitPercent);
      if (v > 0 && v <= 100) { botState.takeProfitPercent = v; broadcastBotStatus(); }
    }
    else if (msg.type === 'bot_set_max_positions') {
      const v = parseInt(msg.maxConcurrentPositions, 10);
      if (v >= 1 && v <= SCAN_SYMBOLS.length) { botState.maxConcurrentPositions = v; broadcastBotStatus(); }
    }
    else if (msg.type === 'bot_manual_close') {
      const { symbol } = msg;
      if (botState.positions[symbol]) {
        executeBotSell(symbol, null, 'إغلاق يدوي من المستخدم').catch((err) => {
          const detail = err.response?.data?.msg || err.message;
          ws.send(JSON.stringify({ type: 'error', message: 'فشل إغلاق الصفقة: ' + detail }));
          botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'error', message: String(detail) });
          broadcastBotStatus();
        });
      }
    }
    else if (msg.type === 'bot_cancel_pending') {
      const { symbol } = msg;
      const pending = botState.pendingOrders[symbol];
      if (pending) {
        (async () => {
          try {
            await cancelOrder(pending.exchange, symbol, pending.orderId);
            delete botState.pendingOrders[symbol];
            botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'error', message: 'أُلغي الأمر المعلّق يدويًا من المستخدم' });
            botState.tradeLog = botState.tradeLog.slice(0, 50);
            broadcastBotStatus();
          } catch (err) {
            const detail = err.response?.data?.msg || err.message;
            ws.send(JSON.stringify({ type: 'error', message: 'فشل إلغاء الأمر: ' + detail }));
          }
        })();
      }
    }
  });

  ws.on('close', () => clientSubs.delete(ws));
  ws.on('error', () => clientSubs.delete(ws));
});

/* ================================================================================
 * طبقة "البحث عن العملات المرشحة للانفجار" (Pre-Breakout Explosion Scanner)
 * ================================================================================
 * الفكرة مبنية على مفهوم معروف في التحليل الفني (TTM Squeeze وما شابه): العملة تكون على
 * وشك حركة قوية عندما تجتمع هذه العوامل خلال فترة هدوء واضحة:
 *   1) انضغاط بولينجر (BB width) عند أدنى مستوياته النسبية خلال آخر 100 قيمة — تذبذب مكتوم جدًا
 *   2) انكماش ATR (تقلب حقيقي منخفض جدًا مقارنة بالفترة الأخيرة)
 *   3) شرط "السكويز" الكلاسيكي: نطاق بولينجر بالكامل داخل قناة كيلتنر (EMA20 ± 1.5×ATR)
 *   4) بداية ارتفاع في الحجم مقارنة بمتوسطه الهادئ (نذاق أول إشارة اهتمام مؤسسي/سيولة داخلة)
 *   5) ميل OBV خلال آخر 20 شمعة لتخمين اتجاه الانفجار المحتمل (تجميع صعودي أو تصريف هابط)
 * كل هذا يُحسب دائمًا على فريم 15 دقيقة الثابت لكل العملات، لضمان مقارنة عادلة بينها
 * بغض النظر عن الفريم اللي يشاهده المستخدم حاليًا. عملة الذهب PAXGUSDT مستبعدة من الفحص
 * لأنها مربوطة بسعر الذهب وطبيعتها الهادئة تجعلها تظهر "مضغوطة" دائمًا بدون معنى حقيقي.
 * ================================================================================ */

const SCAN_INTERVAL = '15m';
const SCAN_SYMBOLS = SYMBOLS.filter((s) => s !== 'PAXGUSDT');

function computeExplosionScore(candles) {
  if (!candles || candles.length < 80) return null;
  const closes = candles.map((c) => c.close);
  const highs = candles.map((c) => c.high);
  const lows = candles.map((c) => c.low);
  const volumes = candles.map((c) => c.volume);
  const n = closes.length;

  // ── بولينجر (20، 2) وسلسلة عرضه التاريخية لقياس مدى الانضغاط الحالي نسبيًا ──
  const bbArr = BollingerBands.calculate({ period: 20, values: closes, stdDev: 2 });
  if (bbArr.length < 40) return null;
  const bbWidths = bbArr.map((b) => (b.middle ? (b.upper - b.lower) / b.middle : 0));
  const lastBB = bbArr[bbArr.length - 1];
  const lastWidth = bbWidths[bbWidths.length - 1];
  const widthWindow = bbWidths.slice(-100);
  const minWidth = Math.min(...widthWindow), maxWidth = Math.max(...widthWindow);
  const widthPercentile = maxWidth > minWidth ? (lastWidth - minWidth) / (maxWidth - minWidth) : 0.5; // 0 = أضيق نطاق مؤخرًا

  // ── ATR(14) يدوي + سلسلته التاريخية لقياس انكماش التقلب ──
  const trArr = [];
  for (let i = 1; i < n; i++) {
    trArr.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i - 1]), Math.abs(lows[i] - closes[i - 1])));
  }
  const atrPeriod = 14;
  if (trArr.length < atrPeriod) return null;
  const atrSeries = [];
  let atrVal = trArr.slice(0, atrPeriod).reduce((a, b) => a + b, 0) / atrPeriod;
  atrSeries.push(atrVal);
  for (let i = atrPeriod; i < trArr.length; i++) { atrVal = (atrVal * (atrPeriod - 1) + trArr[i]) / atrPeriod; atrSeries.push(atrVal); }
  const lastAtr = atrSeries[atrSeries.length - 1];
  const atrWindow = atrSeries.slice(-60);
  const atrMin = Math.min(...atrWindow), atrMax = Math.max(...atrWindow);
  const atrPercentile = atrMax > atrMin ? (lastAtr - atrMin) / (atrMax - atrMin) : 0.5;

  // ── قناة كيلتنر (EMA20 ± 1.5×ATR) لشرط السكويز الكلاسيكي ──
  const ema20Arr = EMA.calculate({ values: closes, period: 20 });
  const lastEma20 = ema20Arr[ema20Arr.length - 1];
  const kcUpper = lastEma20 + lastAtr * 1.5;
  const kcLower = lastEma20 - lastAtr * 1.5;
  const squeezeOn = lastBB.upper < kcUpper && lastBB.lower > kcLower;

  // ── نسبة الحجم: متوسط آخر 5 شموع مقابل متوسط آخر 50 (أول نبضة اهتمام بعد هدوء) ──
  const avgVol5 = volumes.slice(-5).reduce((a, b) => a + b, 0) / 5;
  const avgVol50 = volumes.slice(-50).reduce((a, b) => a + b, 0) / 50;
  const volRatio = avgVol50 > 0 ? avgVol5 / avgVol50 : 1;

  // ── OBV وميله خلال آخر 20 شمعة ──
  let obvVal = 0;
  const obvArr = [0];
  for (let i = 1; i < n; i++) {
    if (closes[i] > closes[i - 1]) obvVal += volumes[i];
    else if (closes[i] < closes[i - 1]) obvVal -= volumes[i];
    obvArr.push(obvVal);
  }
  const obvSlice = obvArr.slice(-20);
  const obvSlope = obvSlice[obvSlice.length - 1] - obvSlice[0];

  // ── RSI(14): مؤشر زخم كلاسيكي — نستخدمه هنا كفلتر تشبّع، مو كإشارة دخول ──
  const rsiArr = RSI.calculate({ values: closes, period: 14 });
  const lastRsi = rsiArr.length ? rsiArr[rsiArr.length - 1] : null;

  // ── MFI (Money Flow Index, 14): "RSI موزون بالحجم" — أدق مؤشر سيولة+زخم مع بعض ──
  let lastMfi = null;
  {
    const period = 14;
    const typicalPrices = closes.map((c, i) => (highs[i] + lows[i] + c) / 3);
    const rawMF = typicalPrices.map((tp, i) => tp * volumes[i]);
    if (n > period) {
      let posMF = 0, negMF = 0;
      for (let i = n - period; i < n; i++) {
        if (typicalPrices[i] > typicalPrices[i - 1]) posMF += rawMF[i];
        else if (typicalPrices[i] < typicalPrices[i - 1]) negMF += rawMF[i];
      }
      lastMfi = negMF === 0 ? 100 : 100 - (100 / (1 + posMF / negMF));
    }
  }

  // ── CMF (Chaikin Money Flow, 20): تدفق سيولة حقيقي (ضغط شراء/بيع داخل كل شمعة) ──
  let lastCmf = null;
  {
    const period = 20;
    const win = candles.slice(-period);
    let mfvSum = 0, volSum = 0;
    for (const c of win) {
      const range = c.high - c.low;
      const mfm = range ? ((c.close - c.low) - (c.high - c.close)) / range : 0;
      mfvSum += mfm * c.volume;
      volSum += c.volume;
    }
    lastCmf = volSum > 0 ? mfvSum / volSum : 0;
  }

  // ── فلتر عدم الانتفاخ المسبق: وين موقع السعر داخل نطاق آخر 50 شمعة + كم ارتفع فعليًا خلال آخر 20 شمعة ──
  // الهدف: نستبعد/نخفّض تصنيف عملة صعدت فعلًا وقاربت قمتها — لأن هذي الحالة انفجارها المحتمل يكون ارتداد
  // هبوطي (بيع من القمة) مو تفجير صعودي حقيقي، بالضبط الالتباس اللي نبي نتفاداه
  const rangeHigh = Math.max(...highs.slice(-50));
  const rangeLow = Math.min(...lows.slice(-50));
  const pricePosition = rangeHigh > rangeLow ? (closes[n - 1] - rangeLow) / (rangeHigh - rangeLow) : 0.5; // 1 = عند القمة تمامًا

  const priceThen = closes[n - 21] ?? closes[0];
  const recentGainPct = priceThen ? ((closes[n - 1] - priceThen) / priceThen) * 100 : 0;

  // ── اتجاه الانفجار المرجّح: نتفق فيه OBV مع CMF، لو تعارضوا نخليه "غير واضح" بدل ما نخمّن ──
  const obvDir = obvSlope > 0 ? 'up' : obvSlope < 0 ? 'down' : 'neutral';
  const cmfDir = lastCmf > 0.03 ? 'up' : lastCmf < -0.03 ? 'down' : 'neutral';
  let direction = 'neutral';
  if (obvDir !== 'neutral' && obvDir === cmfDir) direction = obvDir;
  else if (obvDir !== 'neutral' && cmfDir === 'neutral') direction = obvDir;
  else if (cmfDir !== 'neutral' && obvDir === 'neutral') direction = cmfDir;

  // ── الدرجة المركّبة (0-100) ──
  let score = 0;
  score += (1 - Math.max(0, Math.min(1, widthPercentile))) * 25; // انضغاط بولينجر (جوهر الفكرة)
  score += (1 - Math.max(0, Math.min(1, atrPercentile))) * 15;   // انكماش ATR
  score += squeezeOn ? 10 : 0;                                    // شرط السكويز الكلاسيكي محقق فعليًا
  score += Math.max(0, Math.min(1, (volRatio - 1) * 2)) * 15;    // بداية ارتفاع حجم (سيولة داخلة)
  score += Math.min(1, Math.abs(obvSlope) / (avgVol50 * 20 || 1)) * 10; // وضوح ميل OBV

  // مكافأة منطقة MFI الصحية (سيولة تتحرك بدون تشبّع شرائي بعد)
  if (lastMfi != null) {
    if (lastMfi >= 40 && lastMfi <= 65) score += 10;
    else if (lastMfi > 85) score -= 15; // سيولة متشبعة تمامًا — خطر ارتداد قريب
  }
  // مكافأة توافق CMF مع اتجاه OBV (تأكيد سيولة حقيقية للاتجاه المرجّح)
  if (direction !== 'neutral' && cmfDir === direction) score += 8;

  // عقوبة الاقتراب من قمة/قاع آخر 50 شمعة — كل ما اقترب أكثر، قلّت مصداقية "الانفجار الصاعد الحقيقي"
  const overextPenalty = pricePosition > 0.8 ? ((pricePosition - 0.8) / 0.2) * 30 : 0;
  score -= overextPenalty;

  // عقوبة الارتفاع الحاد المسبق (يعني العملة فعلًا انفجرت، مو على وشك الانفجار)
  const gainPenalty = recentGainPct > 10 ? Math.min(1, (recentGainPct - 10) / 15) * 20 : 0;
  score -= gainPenalty;

  // عقوبة تشبّع RSI الكلاسيكي (زخم مفرط = قرب من نهاية الحركة مو بدايتها)
  const rsiPenalty = lastRsi != null && lastRsi > 70 ? Math.min(1, (lastRsi - 70) / 15) * 15 : 0;
  score -= rsiPenalty;

  const overextended = pricePosition > 0.8 || recentGainPct > 10 || (lastRsi != null && lastRsi > 70);

  return {
    score: Math.round(Math.max(0, Math.min(100, score))),
    direction,
    widthPercentile: Math.round(widthPercentile * 100),
    atrPercentile: Math.round(atrPercentile * 100),
    squeezeOn,
    volRatio: Math.round(volRatio * 100) / 100,
    rsi: lastRsi != null ? Math.round(lastRsi) : null,
    mfi: lastMfi != null ? Math.round(lastMfi) : null,
    cmf: Math.round(lastCmf * 1000) / 1000,
    pricePosition: Math.round(pricePosition * 100), // % موقع السعر داخل نطاق آخر 50 شمعة (100 = عند القمة)
    recentGainPct: Math.round(recentGainPct * 10) / 10,
    overextended, // true = العملة صعدت فعلًا وقريبة من قمتها، احتمال ارتداد هبوط أكبر من تفجير حقيقي
    price: closes[n - 1],
  };
}

let explosionRanking = []; // [{ symbol, score, direction, ... }] مرتّبة تنازليًا حسب الدرجة

function runExplosionScan() {
  const results = [];
  for (const symbol of SCAN_SYMBOLS) {
    const candles = candleStore[`${symbol}_${SCAN_INTERVAL}`];
    const res = computeExplosionScore(candles);
    if (res) results.push({ symbol, ...res });
  }
  results.sort((a, b) => b.score - a.score);
  explosionRanking = results;
  broadcastExplosionScan();
}

function broadcastExplosionScan() {
  if (!explosionRanking.length) return;
  const payload = JSON.stringify({ type: 'explosion_scan', ranking: explosionRanking.slice(0, 2) });
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  }
}

// عند إقلاع السيرفر: نشغّل تدفق بيانات دائم لكل العملات (باستثناء الذهب) على فريم 15 دقيقة الثابت،
// بغض النظر عن العملة التي يشاهدها أي مستخدم حاليًا، حتى يبقى الماسح يعمل بدون توقف
(async () => {
  for (const symbol of SCAN_SYMBOLS) {
    await ensureStream(symbol, SCAN_INTERVAL);
  }
  setTimeout(runExplosionScan, 5000); // مهلة بسيطة لضمان اكتمال تحميل البيانات التاريخية أولًا
})();

setInterval(runExplosionScan, 5 * 60 * 1000); // إعادة فحص كل 5 دقائق (بدل 30 ثانية) — العملة على فريم 15د ما تتغيّر كل هذي المدة أصلًا

// ── تسلسل المصادر لكل العملات: Binance أولًا ← MEXC احتياطي ← OKX احتياطي ثالث ──

async function fetchHistoricalBinance(symbol, interval, limit = 300) {
  const url = `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`;
  const { data } = await axios.get(url, { timeout: 8000 });
  return data.map((k) => ({
    time: Math.floor(k[0] / 1000),
    open: parseFloat(k[1]), high: parseFloat(k[2]), low: parseFloat(k[3]), close: parseFloat(k[4]),
    volume: parseFloat(k[5]), isClosed: true,
  }));
}

async function fetchHistoricalMexc(symbol, interval, limit = 300) {
  const url = `https://api.mexc.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`;
  const { data } = await axios.get(url, { timeout: 10000 });
  return data.map((k) => ({
    time: Math.floor(k[0] / 1000),
    open: parseFloat(k[1]), high: parseFloat(k[2]), low: parseFloat(k[3]), close: parseFloat(k[4]),
    volume: parseFloat(k[5]), isClosed: true,
  }));
}

async function fetchHistoricalOkx(symbol, interval, limit = 300) {
  const instId = symbol.replace(/USDT$/, '-USDT'); // BTCUSDT → BTC-USDT
  const bar = OKX_BAR[interval] || interval;
  const url = `https://www.okx.com/api/v5/market/candles?instId=${instId}&bar=${bar}&limit=${limit}`;
  const { data } = await axios.get(url, { timeout: 10000 });
  const rows = data.data || [];
  // OKX يرجّع الأحدث أولًا — نعكس الترتيب عشان يصير الأقدم أولًا مثل باقي المصادر
  return rows.map((r) => ({
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

async function ensureStream(symbol, interval) {
  const key = `${symbol}_${interval}`;
  if (streamWs[key] !== undefined) return; // already running or loading
  streamWs[key] = null; // mark loading

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

// تسلسل البث اللحظي: Binance ← MEXC ← OKX (كل واحد يجرب 8 ثوانٍ، لو ما رد ينتقل للي بعده)
function connectStream(symbol, interval) {
  connectBinanceStream(symbol, interval, () =>
    connectMexcStream(symbol, interval, () =>
      connectOkxStream(symbol, interval)));
}

function connectBinanceStream(symbol, interval, onFail) {
  const key = `${symbol}_${interval}`;
  const wsUrl = `wss://stream.binance.com:9443/ws/${symbol.toLowerCase()}@kline_${interval}`;
  const ws = new WebSocket(wsUrl);
  streamWs[key] = ws;
  let hasReceivedData = false;

  const failTimer = setTimeout(() => {
    if (!hasReceivedData) {
      console.warn(`[${key}] Binance لم يستجب خلال 8 ثوانٍ — تجربة MEXC`);
      try { ws.terminate(); } catch (e) {}
    }
  }, 8000);

  ws.on('open', () => console.log(`[${key}] Binance stream connected`));

  ws.on('message', (raw) => {
    hasReceivedData = true;
    clearTimeout(failTimer);
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    const k = msg.k;
    if (!k) return;
    updateCandleStore(symbol, interval, {
      time: Math.floor(k.t / 1000),
      open: parseFloat(k.o), high: parseFloat(k.h), low: parseFloat(k.l), close: parseFloat(k.c),
      volume: parseFloat(k.v), isClosed: k.x === true,
    });
    broadcastUpdate(symbol, interval);
  });

  ws.on('error', (err) => console.error(`[${key}] Binance WS error:`, err.message));

  ws.on('close', () => {
    clearTimeout(failTimer);
    delete streamWs[key];
    if (!hasReceivedData) {
      console.warn(`[${key}] بينانس أغلق بدون بيانات — الانتقال لـ MEXC`);
      onFail();
    } else {
      console.log(`[${key}] Binance stream closed — إعادة الاتصال ببينانس خلال 5 ثوانٍ`);
      setTimeout(() => connectBinanceStream(symbol, interval, onFail), 5000);
    }
  });
}

function connectMexcStream(symbol, interval, onFail) {
  const key = `${symbol}_${interval}`;
  const wsInterval = MEXC_WS_INTERVAL[interval];
  const topic = `spot@public.kline.v3.api@${symbol}@${wsInterval}`;

  const ws = new WebSocket('wss://wbs.mexc.com/ws');
  streamWs[key] = ws;
  let hasReceivedData = false;

  const failTimer = setTimeout(() => {
    if (!hasReceivedData) {
      console.warn(`[${key}] MEXC لم يستجب خلال 8 ثوانٍ — تجربة OKX`);
      try { ws.terminate(); } catch (e) {}
    }
  }, 8000);

  ws.on('open', () => {
    console.log(`[${key}] MEXC stream connected`);
    ws.send(JSON.stringify({ method: 'SUBSCRIPTION', params: [topic] }));
  });

  // MEXC requires a PING every 30 s
  const ping = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ method: 'PING' }));
  }, 20000);

  ws.on('message', (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }
    if (!msg.d || !msg.d.k) return; // تجاهل PONG وتأكيد الاشتراك

    hasReceivedData = true;
    clearTimeout(failTimer);
    const k = msg.d.k;
    updateCandleStore(symbol, interval, {
      time: Math.floor(k.t / 1000),
      open: parseFloat(k.o), high: parseFloat(k.h), low: parseFloat(k.l), close: parseFloat(k.c),
      volume: parseFloat(k.v), isClosed: k.X === true,
    });
    broadcastUpdate(symbol, interval);
  });

  ws.on('error', (err) => console.error(`[${key}] MEXC WS error:`, err.message));

  ws.on('close', () => {
    clearInterval(ping);
    clearTimeout(failTimer);
    delete streamWs[key];
    if (!hasReceivedData && onFail) {
      console.warn(`[${key}] MEXC أغلق بدون بيانات — الانتقال لـ OKX`);
      onFail();
    } else {
      console.log(`[${key}] MEXC stream closed — reconnecting in 5s`);
      setTimeout(() => connectMexcStream(symbol, interval, onFail), 5000);
    }
  });
}

function connectOkxStream(symbol, interval) {
  const key = `${symbol}_${interval}`;
  const instId = symbol.replace(/USDT$/, '-USDT');
  const channel = `candle${OKX_BAR[interval] || interval}`;
  const ws = new WebSocket('wss://ws.okx.com:8443/public');
  streamWs[key] = ws;

  ws.on('open', () => {
    console.log(`[${key}] OKX stream connected`);
    ws.send(JSON.stringify({ op: 'subscribe', args: [{ channel, instId }] }));
  });

  const ping = setInterval(() => {
    if (ws.readyState === WebSocket.OPEN) ws.send('ping');
  }, 20000);

  ws.on('message', (raw) => {
    if (raw.toString() === 'pong') return;
    let msg; try { msg = JSON.parse(raw); } catch { return; }
    const row = msg.data?.[0];
    if (!row) return;
    updateCandleStore(symbol, interval, {
      time: Math.floor(Number(row[0]) / 1000),
      open: parseFloat(row[1]), high: parseFloat(row[2]), low: parseFloat(row[3]), close: parseFloat(row[4]),
      volume: parseFloat(row[5]), isClosed: row[8] === '1',
    });
    broadcastUpdate(symbol, interval);
  });

  ws.on('error', (err) => console.error(`[${key}] OKX WS error:`, err.message));

  ws.on('close', () => {
    clearInterval(ping);
    console.log(`[${key}] OKX stream closed — reconnecting in 5s (آخر مصدر بالتسلسل)`);
    delete streamWs[key];
    setTimeout(() => connectOkxStream(symbol, interval), 5000);
  });
}

// ── Broadcast ─────────────────────────────────────────────────────────────────

// يحسب كل المؤشرات بالفريم المختار، لكن يثبّت طبقة الارتداد على فريم 15 دقيقة دائمًا
// (هذي المؤشرات تحديدًا أدق على 15 دقيقة، فما نخليها تتغير مع تبديل فريم العرض)
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

  // طبقة الساعة: تحليل مستقل تمامًا (لا علاقة له بالقرار النهائي إطلاقًا) دائمًا من فريم الساعة الثابت
  // بغض النظر عن الفريم المعروض — مصمّمة لإعطاء صورة أعمق قليلًا من الزخم قصير المدى (3د-4س)
  indicators.hourlyLayer = computeHourlyLayer(symbol);

  return indicators;
}

// ── طبقة الساعة (مستقلة تمامًا، لا تدخل في حساب القرار النهائي) ──────────────
// فكرتها: بدل مؤشرات الزخم اللحظي، نستخدم مؤشرات هيكلية أبطأ على فريم الساعة الثابت
// (EMA50/200 + ADX + Supertrend + إيشيموكو) — تعطي انحياز اتجاه متوسط المدى بدل الزخم اللحظي
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

function broadcastUpdate(symbol, interval) {
  const key = `${symbol}_${interval}`;
  const candles = candleStore[key];
  if (!candles || !candles.length) return;

  const indicators = computeIndicatorsFixedReversal(symbol, interval, candles);
  const decision = makeDecision(indicators);
  const payload = JSON.stringify({ type: 'update', symbol, interval, candles, indicators, decision });

  for (const [client, sub] of clientSubs) {
    if (client.readyState === WebSocket.OPEN && sub.symbol === symbol && sub.interval === interval) {
      client.send(payload);
    }
  }

  broadcastMtfUpdate(symbol); // أي فريم من فريمات هذي العملة يتحدّث، نحدّث شريط الفريمات كامل (مع throttle داخلي)
}

function sendSnapshot(ws, symbol, interval) {
  const key = `${symbol}_${interval}`;
  const candles = candleStore[key];
  if (!candles || !candles.length) return;
  const indicators = computeIndicatorsFixedReversal(symbol, interval, candles);
  const decision = makeDecision(indicators);
  if (ws.readyState === WebSocket.OPEN) {
    ws.send(JSON.stringify({ type: 'update', symbol, interval, candles, indicators, decision }));
  }
}

// ── شريط الفريمات: يحسب قرار كل فريم (من الـ 7) لنفس العملة، لتلوين مربعاتها الصغيرة أخضر/أحمر/أصفر ──
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

const lastMtfBroadcast = {}; // symbol -> آخر وقت بُث فيه (throttle بسيط، فريم واحد يبعث ~عدة مرات بالثانية أحيانًا)
function broadcastMtfUpdate(symbol, force = false) {
  const now = Date.now();
  if (!force && lastMtfBroadcast[symbol] && now - lastMtfBroadcast[symbol] < 3000) return; // كل 3 ثوانٍ كحد أقصى
  lastMtfBroadcast[symbol] = now;

  const snapshot = computeMtfSnapshot(symbol);
  const payload = JSON.stringify({ type: 'mtf_update', symbol, snapshot });
  for (const [client, sub] of clientSubs) {
    if (client.readyState === WebSocket.OPEN && sub.symbol === symbol) client.send(payload);
  }
}

// ── Indicators ────────────────────────────────────────────────────────────────

function last(arr) { return arr && arr.length ? arr[arr.length - 1] : undefined; }

function computeIndicators(candles) {
  if (candles.length < 30) return null;
  const closes = candles.map((c) => c.close);
  const highs = candles.map((c) => c.high);
  const lows = candles.map((c) => c.low);
  const volumes = candles.map((c) => c.volume);

  const rsiArr = RSI.calculate({ values: closes, period: 14 });
  const rsi = last(rsiArr);

  const macdRaw = last(MACD.calculate({
    values: closes, fastPeriod: 12, slowPeriod: 26, signalPeriod: 9,
    SimpleMAOscillator: false, SimpleMASignal: false,
  }));
  const macd = macdRaw ? { value: macdRaw.MACD, signal: macdRaw.signal, histogram: macdRaw.histogram } : null;

  const bbRaw = last(BollingerBands.calculate({ period: 20, values: closes, stdDev: 2 }));
  const bb = bbRaw ? { upper: bbRaw.upper, middle: bbRaw.middle, lower: bbRaw.lower } : null;

  const ema50 = last(EMA.calculate({ values: closes, period: 50 }));
  const ema200 = closes.length >= 200 ? last(EMA.calculate({ values: closes, period: 200 })) : null;

  // ── المؤشرات الإضافية الثمانية: معادلات يدوية بحتة (بدون أي اعتماد على مكتبة خارجية) ──
  const n = closes.length;

  // SMA 20
  let sma20 = null;
  if (n >= 20) {
    let sum = 0;
    for (let i = n - 20; i < n; i++) sum += closes[i];
    sma20 = sum / 20;
  }

  // VWAP (على آخر 100 شمعة كنافذة تقريبية)
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

  // Stochastic %K / %D (فترة 14، تنعيم 3)
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

  // ADX(14) — طريقة Wilder اليدوية
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

  // OBV (يدوي) + مرجع ميل أطول (14 شمعة) بدل المقارنة اللحظية شمعة بشمعة (كانت مصدر ضجيج كبير في إجماع الحجم والثبات)
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

  // Supertrend (يدوي، مبني على ATR يدوي)
  const supertrend = computeSupertrend(candles);

  // إيشيموكو (يدوي، نسخة مبسّطة بدون الإزاحة الزمنية)
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

  // نقاط الارتكاز (Pivot Points) — من الشمعة السابقة المكتملة
  let pivot = null;
  if (n >= 2) {
    const prev = candles[n - 2];
    const p = (prev.high + prev.low + prev.close) / 3;
    pivot = { p, r1: 2 * p - prev.low, s1: 2 * p - prev.high };
  }

  // مقارنة الشموع (آخر 3 شموع: اتجاه متتالي + كشف ابتلاع بسيط)
  let candleCompare = null;
  if (n >= 3) {
    const c1 = candles[n - 3], c2 = candles[n - 2], c3 = candles[n - 1];
    const dir = (c) => c.close > c.open ? 1 : c.close < c.open ? -1 : 0;
    const d1 = dir(c1), d2 = dir(c2), d3 = dir(c3);
    const consecutiveUp = d1 > 0 && d2 > 0 && d3 > 0;
    const consecutiveDown = d1 < 0 && d2 < 0 && d3 < 0;
    // ابتلاع صاعد/هابط بسيط بين آخر شمعتين
    const bullEngulf = d2 < 0 && d3 > 0 && c3.close > c2.open && c3.open < c2.close;
    const bearEngulf = d2 > 0 && d3 < 0 && c3.close < c2.open && c3.open > c2.close;
    candleCompare = { consecutiveUp, consecutiveDown, bullEngulf, bearEngulf };
  }

  // التجميع والتصريف (Accumulation/Distribution Line) — قراءة تدفق السيولة الحقيقي
  // Money Flow Multiplier = ((close-low)-(high-close)) / (high-low)  → موجب = تجميع (شراء)، سالب = تصريف (بيع)
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
      const lastMFM = ((win[win.length - 1].close - win[win.length - 1].low) - (win[win.length - 1].high - win[win.length - 1].close)) /
                      ((win[win.length - 1].high - win[win.length - 1].low) || 1);
      let zone = 'متعادل';
      if (rising && lastMFM > 0.2) zone = 'تجميع (Accumulation)';
      else if (!rising && lastMFM < -0.2) zone = 'تصريف (Distribution)';
      accDist = { value: adLine, rising, zone };
    }
  }

  // CVD (Cumulative Volume Delta) — تقدير الشراء/البيع الفعلي داخل كل شمعة + كشف التباعد عن السعر (امتصاص مؤسسي)
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
      if (priceDir === 'down' && cvdDir === 'up') signal = 'bullish_divergence'; // امتصاص مؤسسي صاعد
      else if (priceDir === 'up' && cvdDir === 'down') signal = 'bearish_divergence'; // توزيع/تباعد سلبي
      cvd = { value: cvdNow, priceDir, cvdDir, signal };
    }
  }

  // ══════════ طبقة الارتداد (Reversal) ══════════

  // Stochastic RSI (14,3,3)
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
      stochRsi = {
        k: kNow, d: dNow,
        crossUp: kPrev <= dPrev && kNow > dNow, crossDown: kPrev >= dPrev && kNow < dNow,
        zoneUp: kNow < 25, zoneDown: kNow > 75, // منطقة تشبع مستمرة (حتى بدون عبور مؤكد بعد)
      };
    }
  }

  // Bollinger %B (يحتاج قيمة حالية وسابقة لكشف لحظة العبور + منطقة قرب الحد)
  let bbPercentB = null;
  if (bb) {
    const closesForBB = closes.slice(-21);
    const prevClose = closesForBB[closesForBB.length - 2];
    const range = bb.upper - bb.lower;
    const nowB = range === 0 ? 0.5 : (closes[n - 1] - bb.lower) / range;
    const prevB = range === 0 || prevClose == null ? nowB : (prevClose - bb.lower) / range;
    bbPercentB = {
      now: nowB, prev: prevB,
      crossedUpFromZero: prevB < 0 && nowB >= 0, crossedDownFromOne: prevB > 1 && nowB <= 1,
      zoneUp: nowB <= 0.05, zoneDown: nowB >= 0.95, // قريب جدًا من حافة النطاق
    };
  }

  // دايفرجنز RSI — مقارنة السعر بـ RSI خلال آخر 10 شمعات، بشرط أن يكون RSI فعليًا قرب طرف متشبع
  // (فوق 60 لتوزيع محتمل، تحت 40 لتجميع محتمل). بدون هذا الشرط، أي انخفاض بسيط بـ RSI وسط رالي
  // صحي (RSI بين 55-75 مثلًا) كان يُقرأ خطأً كـ"تباعد هابط" رغم إنه لا يعني انعكاس فعلي غالبًا
  let rsiDivergence = null;
  if (rsiArr.length >= 11 && n >= 11) {
    const priceNow = closes[n - 1], priceBefore = closes[n - 11];
    const rsiNow = rsiArr[rsiArr.length - 1], rsiBefore = rsiArr[rsiArr.length - 11];
    let type = 'none';
    if (priceNow < priceBefore && rsiNow > rsiBefore && rsiNow < 40) type = 'bullish'; // تجميع خفي قرب تشبع بيعي حقيقي
    else if (priceNow > priceBefore && rsiNow < rsiBefore && rsiNow > 60) type = 'bearish'; // توزيع خفي قرب تشبع شرائي حقيقي
    rsiDivergence = { type, priceNow, priceBefore, rsiNow, rsiBefore };
  }

  // Williams %R (14) — يحتاج قيمة حالية وسابقة لكشف لحظة العبور + منطقة تشبع مستمرة
  let williamsR = null;
  if (n >= 15) {
    const calcWR = (idx) => {
      const hh = Math.max(...highs.slice(idx - 13, idx + 1));
      const ll = Math.min(...lows.slice(idx - 13, idx + 1));
      return hh === ll ? -50 : ((hh - closes[idx]) / (hh - ll)) * -100;
    };
    const wrNow = calcWR(n - 1), wrPrev = calcWR(n - 2);
    williamsR = {
      now: wrNow, prev: wrPrev,
      crossUpFrom80: wrPrev < -80 && wrNow >= -80, crossDownFrom20: wrPrev > -20 && wrNow <= -20,
      zoneUp: wrNow <= -80, zoneDown: wrNow >= -20,
    };
  }

  // ══════════ طبقة ثبات الاتجاه (Trend Stability) ══════════

  // Choppiness Index (14) — 0-100: تحت 38.2 = ترند ثابت، فوق 61.8 = تذبذب عشوائي
  let chop = null;
  if (n >= 15) {
    const trWin = [];
    for (let i = n - 14; i < n; i++) {
      trWin.push(Math.max(highs[i] - lows[i], Math.abs(highs[i] - closes[i - 1]), Math.abs(lows[i] - closes[i - 1])));
    }
    const atrSum = trWin.reduce((a, b) => a + b, 0);
    const hh14 = Math.max(...highs.slice(-14)), ll14 = Math.min(...lows.slice(-14));
    const range14 = hh14 - ll14;
    if (range14 > 0 && atrSum > 0) {
      chop = 100 * Math.log10(atrSum / range14) / Math.log10(14);
    }
  }

  // ATR (14) — متوسط المدى الحقيقي بتنعيم وايلدر — يقيس التذبذب والسيولة اللحظية
  let atr = null;
  if (n >= 15) {
    const trArrAtr = [];
    for (let i = 1; i < n; i++) {
      trArrAtr.push(Math.max(
        highs[i] - lows[i],
        Math.abs(highs[i] - closes[i - 1]),
        Math.abs(lows[i] - closes[i - 1]),
      ));
    }
    const atrPeriod = 14;
    if (trArrAtr.length >= atrPeriod) {
      let atrVal = trArrAtr.slice(0, atrPeriod).reduce((a, b) => a + b, 0) / atrPeriod;
      for (let i = atrPeriod; i < trArrAtr.length; i++) {
        atrVal = (atrVal * (atrPeriod - 1) + trArrAtr[i]) / atrPeriod;
      }
      const lastClose = closes[n - 1];
      const atrPercent = lastClose ? (atrVal / lastClose) * 100 : null; // نسبة التذبذب من السعر الحالي
      atr = { value: atrVal, percent: atrPercent };
    }
  }

  return {
    rsi, macd, bb, ema50, ema200,
    sma20, vwap, stochastic, adx, obv, obvPrev, obvTrendRef, supertrend, ichimoku, volumeProfile,
    pivot, candleCompare, accDist, cvd,
    stochRsi, bbPercentB, rsiDivergence, williamsR, chop, atr,
    currentPrice: closes[closes.length - 1],
  };
}

// Supertrend (مبني على ATR يدوي) — معادلة قياسية بالكامل يدوية
function computeSupertrend(candles, period = 10, multiplier = 3) {
  const highs = candles.map((c) => c.high);
  const lows = candles.map((c) => c.low);
  const closes = candles.map((c) => c.close);

  // ATR يدوي (تنعيم Wilder)
  const trArr = [];
  for (let i = 1; i < closes.length; i++) {
    trArr.push(Math.max(
      highs[i] - lows[i],
      Math.abs(highs[i] - closes[i - 1]),
      Math.abs(lows[i] - closes[i - 1]),
    ));
  }
  if (trArr.length < period) return null;

  let atr = trArr.slice(0, period).reduce((a, b) => a + b, 0) / period;
  const atrArr = [atr];
  for (let i = period; i < trArr.length; i++) {
    atr = (atr * (period - 1) + trArr[i]) / period;
    atrArr.push(atr);
  }
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
    else {
      finalUpper = (basicUpper < finalUpper || prevClose > finalUpper) ? basicUpper : finalUpper;
      finalLower = (basicLower > finalLower || prevClose < finalLower) ? basicLower : finalLower;
    }

    if (close > finalUpper) trendUp = true;
    else if (close < finalLower) trendUp = false;

    st = trendUp ? finalLower : finalUpper;
  }
  return st !== null ? { value: st, trendUp } : null;
}

// Volume Profile مبسط: يقسّم مدى السعر لمستويات ويحسب أين تركز الحجم (POC)
function computeVolumeProfile(candles, buckets = 24) {
  if (!candles.length) return null;
  const highs = candles.map((c) => c.high);
  const lows = candles.map((c) => c.low);
  const max = Math.max(...highs);
  const min = Math.min(...lows);
  if (max === min) return null;
  const step = (max - min) / buckets;
  const vol = new Array(buckets).fill(0);

  for (const c of candles) {
    const mid = (c.high + c.low) / 2;
    let idx = Math.floor((mid - min) / step);
    if (idx < 0) idx = 0; if (idx >= buckets) idx = buckets - 1;
    vol[idx] += c.volume;
  }
  let pocIdx = 0;
  for (let i = 1; i < buckets; i++) if (vol[i] > vol[pocIdx]) pocIdx = i;
  const pocPrice = min + step * (pocIdx + 0.5);
  return { pocPrice, rangeHigh: max, rangeLow: min };
}

// ── Decision Engine ───────────────────────────────────────────────────────────

function makeDecision(indicators) {
  if (!indicators) return null;
  const { rsi, macd, bb, ema50, ema200, currentPrice, stochRsi, williamsR, cvd, accDist, chop, atr } = indicators;
  const notes = [];
  let bull = 0, bear = 0, rawScore = 50;

  if (rsi != null) {
    if      (rsi < 30)  { bull += 2; rawScore += 15; notes.push(`RSI عند ${rsi.toFixed(1)} — تشبع بيعي قوي، فرصة شراء محتملة`); }
    else if (rsi < 45)  { bull += 1; rawScore += 8;  notes.push(`RSI عند ${rsi.toFixed(1)} — ضغط بيعي، المشترون يترقبون`); }
    else if (rsi > 70)  { bear += 2; rawScore -= 15; notes.push(`RSI عند ${rsi.toFixed(1)} — تشبع شرائي، احتمالية تصحيح مرتفعة`); }
    else if (rsi > 55)  { bull += 1; rawScore += 6;  notes.push(`RSI عند ${rsi.toFixed(1)} — زخم صعودي معتدل`); }
    else                {                              notes.push(`RSI عند ${rsi.toFixed(1)} — محايد`); }
  }

  if (macd) {
    if (macd.value > macd.signal) { bull += 1; rawScore += 10; notes.push(`MACD فوق خط الإشارة — إشارة شراء نشطة`); }
    else                          { bear += 1; rawScore -= 10; notes.push(`MACD تحت خط الإشارة — إشارة بيع نشطة`); }
    if (macd.histogram > 0)       { bull += 1; rawScore += 5;  notes.push(`هيستوغرام MACD موجب — تصاعد الزخم الصعودي`); }
    else                          { bear += 1; rawScore -= 5;  notes.push(`هيستوغرام MACD سالب — تصاعد الزخم الهبوطي`); }
  }

  if (ema50 != null) {
    if (currentPrice > ema50) { bull += 1; rawScore += 7;  notes.push(`السعر فوق EMA50 — دعم متحرك قصير المدى`); }
    else                      { bear += 1; rawScore -= 7;  notes.push(`السعر تحت EMA50 — مقاومة متحركة قصيرة المدى`); }
  }
  if (ema200 != null) {
    if (currentPrice > ema200) { bull += 2; rawScore += 10; notes.push(`السعر فوق EMA200 — الاتجاه العام صعودي`); }
    else                       { bear += 2; rawScore -= 10; notes.push(`السعر تحت EMA200 — الاتجاه العام هبوطي`); }
  }
  if (ema50 != null && ema200 != null) {
    if (ema50 > ema200) { bull += 1; rawScore += 5; notes.push(`EMA50 فوق EMA200 — التقاطع الذهبي مؤكَّد`); }
    else                { bear += 1; rawScore -= 5; notes.push(`EMA50 تحت EMA200 — التقاطع الميت مؤكَّد`); }
  }

  if (bb) {
    if      (currentPrice < bb.lower) { bull += 2; rawScore += 12; notes.push(`السعر تحت الحد الأدنى لبولينجر — منطقة شراء قوية`); }
    else if (currentPrice > bb.upper) { bear += 2; rawScore -= 12; notes.push(`السعر فوق الحد الأعلى لبولينجر — منطقة بيع قوية`); }
    else {
      notes.push(currentPrice > (bb.upper + bb.lower) / 2
        ? `السعر في النصف العلوي لبولينجر — ميل صعودي`
        : `السعر في النصف السفلي لبولينجر — ميل هبوطي`);
    }
    if ((bb.upper - bb.lower) / bb.middle < 0.02) notes.push(`نطاق بولينجر ضيق جداً — تذبذب محتمل قريب`);
  }

  // ══════════ طبقة الارتداد (StochRSI و Williams %R — دائمًا من فريم 15 دقيقة الثابت) ══════════

  if (stochRsi) {
    if      (stochRsi.crossUp)   { bull += 2; rawScore += 10; notes.push(`StochRSI (15د) عبور صاعد من منطقة التشبع البيعي — إشارة ارتداد صعودي`); }
    else if (stochRsi.crossDown) { bear += 2; rawScore -= 10; notes.push(`StochRSI (15د) عبور هابط من منطقة التشبع الشرائي — إشارة ارتداد هبوطي`); }
    else if (stochRsi.zoneUp)    { bull += 1; rawScore += 5;  notes.push(`StochRSI (15د) داخل منطقة تشبع بيعي — احتمال ارتداد قريب`); }
    else if (stochRsi.zoneDown)  { bear += 1; rawScore -= 5;  notes.push(`StochRSI (15د) داخل منطقة تشبع شرائي — احتمال تصحيح قريب`); }
  }

  if (williamsR) {
    if      (williamsR.crossUpFrom80)   { bull += 2; rawScore += 10; notes.push(`Williams %R (15د) خرج من منطقة التشبع البيعي (-80) — زخم ارتدادي صعودي`); }
    else if (williamsR.crossDownFrom20) { bear += 2; rawScore -= 10; notes.push(`Williams %R (15د) خرج من منطقة التشبع الشرائي (-20) — زخم ارتدادي هبوطي`); }
    else if (williamsR.zoneUp)          { bull += 1; rawScore += 4;  notes.push(`Williams %R (15د) داخل منطقة تشبع بيعي`); }
    else if (williamsR.zoneDown)        { bear += 1; rawScore -= 4;  notes.push(`Williams %R (15د) داخل منطقة تشبع شرائي`); }
  }

  // ══════════ طبقة السيولة (CVD للامتصاص المؤسسي + Accumulation/Distribution) ══════════

  if (cvd) {
    if      (cvd.signal === 'bullish_divergence') { bull += 2; rawScore += 9; notes.push(`CVD يصعد بينما السعر ينخفض — امتصاص مؤسسي خفي (شراء بصمت)`); }
    else if (cvd.signal === 'bearish_divergence') { bear += 2; rawScore -= 9; notes.push(`CVD ينخفض بينما السعر يصعد — تصريف مؤسسي خفي (بيع بصمت)`); }
  }

  if (accDist) {
    if      (accDist.zone === 'تجميع (Accumulation)')  { bull += 1; rawScore += 6; notes.push(`A/D Line في منطقة تجميع — تدفق سيولة شرائي حقيقي`); }
    else if (accDist.zone === 'تصريف (Distribution)')  { bear += 1; rawScore -= 6; notes.push(`A/D Line في منطقة تصريف — تدفق سيولة بيعي حقيقي`); }
  }

  // ══════════ طبقة ثبات الاتجاه (Choppiness Index) ══════════

  let trendStable = null; // true = اتجاه قوي وثابت، false = تذبذب عشوائي
  if (chop != null) {
    if      (chop < 38.2) { trendStable = true;  notes.push(`Choppiness Index عند ${chop.toFixed(1)} — السوق في اتجاه ثابت وقوي`); }
    else if (chop > 61.8) { trendStable = false; notes.push(`Choppiness Index عند ${chop.toFixed(1)} — السوق متذبذب وعشوائي، يستدعي الحذر من الإشارات الحالية`); }
  }

  // ATR — قياس التذبذب والسيولة اللحظية (بدون تأثير مباشر على نقاط الاتجاه، بل تحذير/سياق)
  if (atr && atr.percent != null) {
    if      (atr.percent >= 1.5) notes.push(`ATR عند ${atr.percent.toFixed(2)}% من السعر — تذبذب مرتفع، وسّع وقف الخسارة واحسب المخاطرة جيدًا`);
    else if (atr.percent <= 0.3) notes.push(`ATR عند ${atr.percent.toFixed(2)}% من السعر — تذبذب منخفض جدًا وسيولة لحظية ضعيفة، حركة السعر بطيئة`);
  }

  const trend = bull > bear + 1 ? 'صعود' : bear > bull + 1 ? 'هبوط' : 'تذبذب';
  const score = Math.max(0, Math.min(100, rawScore));

  let action;
  if      (rsi != null && rsi < 35 && macd && macd.value > macd.signal) action = 'buy zone';
  else if (rsi != null && rsi > 65 && macd && macd.value < macd.signal) action = 'sell zone';
  else if (bb && currentPrice < bb.lower)   action = 'buy zone';
  else if (bb && currentPrice > bb.upper)   action = 'sell zone';
  else if (trend === 'صعود' && score >= 62) action = 'buy zone';
  else if (trend === 'هبوط' && score <= 38) action = 'sell zone';
  else                                       action = 'wait';

  const total = bull + bear;
  const dominance = total > 0 ? Math.max(bull, bear) / total : 0.5;

  // سوق متذبذب عشوائيًا (Choppiness مرتفع) وإشارة غير حاسمة بما يكفي ← تحويل القرار إلى انتظار
  if (trendStable === false && action !== 'wait' && dominance < 0.75) {
    action = 'wait';
    notes.push(`تم تحويل القرار إلى "انتظار": التذبذب العشوائي المرتفع (Choppiness) يُضعف موثوقية الإشارة الأولية`);
  }

  let confidence = Math.round(Math.min(100, score * (0.4 + dominance * 0.6)));
  if (trendStable === true)       confidence = Math.min(100, Math.round(confidence * 1.1)); // اتجاه ثابت يعزز الثقة
  else if (trendStable === false) confidence = Math.round(confidence * 0.85);                 // تذبذب عشوائي يقلّل الثقة

  const buyZone  = bb ? { from: bb.lower.toFixed(4),                         to: ((bb.lower + bb.middle) / 2).toFixed(4) } : null;
  const sellZone = bb ? { from: ((bb.upper + bb.middle) / 2).toFixed(4),    to: bb.upper.toFixed(4) } : null;

  return { trend, action, confidence, notes, buyZone, sellZone };
}

/* ================================================================================
 * بوت التداول التلقائي (Auto-Trading Bot)
 * ================================================================================
 * يعمل دائمًا على فريم 15 دقيقة الثابت لكل عملات SCAN_SYMBOLS (نفس بيانات ماسح الانفجار).
 * القرار = 75% من نفس تحليلات اللوحة (المؤشرات/المربعات فقط — بدون الاعتماد على نص
 * التوصية بالأسفل) + 25% من تحليل خاص بالبوت وحده (تسارع RSI + انحراف VWAP + اتجاه ATR
 * + معدل تمويل ونسبة شراء/بيع بايننس الآجلة).
 *
 * لا يستخدم البوت شراء أو بيع مفتوح (Market) إطلاقًا في قراراته التلقائية — يحدد سعر
 * دخول داخل منطقة الشراء (Buy Zone) ويضع أمر Limit ينتظر وصول السعر لها، وبعد التنفيذ
 * يضع فورًا أمر Limit بيع عند هدف الربح المحدد وينتظر ارتفاع السعر له. الشراء اليدوي
 * (من زر اللوحة) يبقى Market للسرعة لأنه قرار المستخدم المباشر، مو قرار آلي للبوت.
 *
 * قبل أي شراء، لازم تتأكد آخر 10 شمعات فيها نزول حقيقي + السعر بأدنى جزء من مدى تلك
 * الشمعات + سيولة كافية (لا يشتري بدون هذا التأكيد). لا يفتح أكثر من مركز واحد بنفس
 * الوقت (قابل للتعديل)، ويفضّل عند وجود أكثر من مرشّح شراء العملة الأرخص سعرًا.
 * ================================================================================ */

let botState = {
  enabled: false,                 // يبدأ دائمًا معطّلاً عند إقلاع السيرفر — تفعيل يدوي مطلوب كل مرة
  exchange: 'mexc',               // 'mexc' | 'binance' — قابل للتبديل من اللوحة
  tradeSizeUsdt: 50,              // مبلغ كل صفقة بالـ USDT — يحدده المستخدم يدويًا، لا حساب تلقائي كنسبة من رأس المال
  takeProfitPercent: 1,           // % ربح بسيط — يوضع كسعر هدف لأمر بيع Limit فور تنفيذ الشراء
  maxConcurrentPositions: 1,      // أقصى عدد صفقات/أوامر معلّقة بنفس الوقت — يمنع محاولة شراء أكثر مما يسمح به الرصيد
  positions: {},                  // symbol -> { qty, entryPrice, entryTime }
  pendingOrders: {},              // symbol -> { orderId, side: 'BUY'|'SELL', price, qty, placedAt, exchange }
  tradeLog: [],                   // آخر الصفقات المنفذة (وأي أخطاء تنفيذ)
  lastSignals: {},                // symbol -> آخر تقييم للإشارة المركّبة
};

const BOT_BUY_THRESHOLD = 0.35;    // نطاق -1..+1 — فوق هذا = مرشّح شراء
const BOT_SELL_THRESHOLD = -0.35;  // تحت هذا = مرشّح بيع (احترازي، البيع الفعلي عبر أمر جني الربح المعلّق)
const MAX_PENDING_BUY_MINUTES = 45; // إلغاء أمر الشراء المعلّق لو ما تنفذ خلال هالمدة والسعر رجع فوق منطقة الشراء

// ── مربع "التحليل": نفس مربع اللوحة — أوزان مخصصة (ثبات 35% + ارتداد 25% + زخم 25% + ساعة 15%)
function computeFourBoxScore(indicators) {
  if (!indicators) return 0;
  const leanOf = { stability: 0, reversal: 0, momentum: 0, hourly: 0 };

  // الثبات: ADX اتجاهي (لو قوي) + Williams%R + موقع السعر من سحابة إيشيموكو + OBV مقابل مرجعه
  {
    let bull = 0, bear = 0;
    if (indicators.adx && indicators.adx.adx >= 20) { if (indicators.adx.pdi > indicators.adx.mdi) bull++; else bear++; }
    if (indicators.williamsR) { if (indicators.williamsR.zoneUp) bull++; if (indicators.williamsR.zoneDown) bear++; }
    if (indicators.ichimoku && indicators.currentPrice != null) {
      const top = Math.max(indicators.ichimoku.spanA, indicators.ichimoku.spanB);
      const bottom = Math.min(indicators.ichimoku.spanA, indicators.ichimoku.spanB);
      if (indicators.currentPrice > top) bull++; else if (indicators.currentPrice < bottom) bear++;
    }
    if (indicators.obv != null && indicators.obvTrendRef != null) { if (indicators.obv > indicators.obvTrendRef) bull++; else bear++; }
    const total = bull + bear;
    leanOf.stability = total > 0 ? (bull - bear) / total : 0;
  }

  // الارتداد: StochRSI + Williams%R + Bollinger %B + RSI Divergence
  {
    let bull = 0, bear = 0;
    if (indicators.stochRsi) { if (indicators.stochRsi.crossUp || indicators.stochRsi.zoneUp) bull++; if (indicators.stochRsi.crossDown || indicators.stochRsi.zoneDown) bear++; }
    if (indicators.williamsR) { if (indicators.williamsR.crossUpFrom80 || indicators.williamsR.zoneUp) bull++; if (indicators.williamsR.crossDownFrom20 || indicators.williamsR.zoneDown) bear++; }
    if (indicators.bbPercentB) { if (indicators.bbPercentB.zoneUp) bull++; if (indicators.bbPercentB.zoneDown) bear++; }
    if (indicators.rsiDivergence) { if (indicators.rsiDivergence.type === 'bullish') bull++; else if (indicators.rsiDivergence.type === 'bearish') bear++; }
    const total = bull + bear;
    leanOf.reversal = total > 0 ? (bull - bear) / total : 0;
  }

  // إجماع الزخم: RSI + MACD + Stochastic + ADX اتجاهي
  {
    let bull = 0, bear = 0;
    if (indicators.rsi != null) { if (indicators.rsi > 55) bull++; else if (indicators.rsi < 45) bear++; }
    if (indicators.macd) { if (indicators.macd.value > indicators.macd.signal) bull++; else bear++; }
    if (indicators.stochastic) { if (indicators.stochastic.k > 55 && indicators.stochastic.k >= indicators.stochastic.d) bull++; else if (indicators.stochastic.k < 45 && indicators.stochastic.k <= indicators.stochastic.d) bear++; }
    if (indicators.adx && indicators.adx.adx >= 20) { if (indicators.adx.pdi > indicators.adx.mdi) bull++; else bear++; }
    const total = bull + bear;
    leanOf.momentum = total > 0 ? (bull - bear) / total : 0;
  }

  // طبقة الساعة (مستقلة، محسوبة أصلًا بـ computeHourlyLayer ومرفقة داخل indicators.hourlyLayer)
  {
    const hl = indicators.hourlyLayer;
    const total = hl ? hl.bull + hl.bear : 0;
    leanOf.hourly = total > 0 ? (hl.bull - hl.bear) / total : 0;
  }

  return leanOf.stability * 0.40 + leanOf.reversal * 0.30 + leanOf.momentum * 0.20 + leanOf.hourly * 0.10; // نطاق -1..+1
}

// ── مربع "القرار": نفس منطق القرار النهائي القديم بأعلى اللوحة (اتجاه+توصية+زخم+حجم+ارتداد وزن 1، الثبات وزن 2)
function computeDecision7Score(indicators, decision) {
  if (!indicators || !decision) return 0;
  const terms = [];
  if (indicators.ema200 != null && indicators.currentPrice != null) terms.push({ sign: indicators.currentPrice > indicators.ema200 ? 1 : -1, w: 1 });
  terms.push({ sign: decision.action === 'buy zone' ? 1 : decision.action === 'sell zone' ? -1 : 0, w: 1 });

  let mBull = 0, mBear = 0;
  if (indicators.rsi != null) { if (indicators.rsi > 55) mBull++; else if (indicators.rsi < 45) mBear++; }
  if (indicators.macd) { if (indicators.macd.value > indicators.macd.signal) mBull++; else mBear++; }
  terms.push({ sign: mBull > mBear ? 1 : mBull < mBear ? -1 : 0, w: 1 });

  let vBull = 0, vBear = 0;
  if (indicators.cvd && indicators.cvd.signal === 'bullish_divergence') vBull++;
  if (indicators.cvd && indicators.cvd.signal === 'bearish_divergence') vBear++;
  if (indicators.accDist && indicators.accDist.zone === 'تجميع (Accumulation)') vBull++;
  if (indicators.accDist && indicators.accDist.zone === 'تصريف (Distribution)') vBear++;
  terms.push({ sign: vBull > vBear ? 1 : vBull < vBear ? -1 : 0, w: 1 });

  let rBull = 0, rBear = 0;
  if (indicators.stochRsi) { if (indicators.stochRsi.crossUp || indicators.stochRsi.zoneUp) rBull++; if (indicators.stochRsi.crossDown || indicators.stochRsi.zoneDown) rBear++; }
  if (indicators.williamsR) { if (indicators.williamsR.crossUpFrom80 || indicators.williamsR.zoneUp) rBull++; if (indicators.williamsR.crossDownFrom20 || indicators.williamsR.zoneDown) rBear++; }
  terms.push({ sign: rBull > rBear ? 1 : rBull < rBear ? -1 : 0, w: 1 });

  if (indicators.chop != null) {
    const majoritySign = Math.sign(terms.reduce((s, t) => s + t.sign * t.w, 0)) || 0;
    if (indicators.chop < 38.2) terms.push({ sign: majoritySign, w: 2 });
    else if (indicators.chop > 61.8) terms.push({ sign: 0, w: 2 });
  }

  const totalWeight = terms.reduce((s, t) => s + t.w, 0);
  const rawScore = terms.reduce((s, t) => s + t.sign * t.w, 0);
  return totalWeight > 0 ? rawScore / totalWeight : 0; // نطاق -1..+1
}

// ── مربع "الفريم": يجمع فريمات 3د+5د+15د+30د بمربع واحد بالنسبة (نفس منطق اللوحة تمامًا)
function computeFrameScore(symbol) {
  const snapshot = computeMtfSnapshot(symbol);
  const watched = ['3m', '5m', '15m', '30m'];
  const leans = [];
  for (const iv of watched) {
    const info = snapshot[iv];
    if (!info) continue;
    const sign = info.action === 'buy zone' ? 1 : info.action === 'sell zone' ? -1 : 0;
    leans.push(sign * (info.confidence / 100));
  }
  return leans.length ? leans.reduce((a, b) => a + b, 0) / leans.length : 0;
}

// ── طبقة البيتكوين: تدخل ضمن "القرار" بعد تحرك 70$ (نفس منطق اللوحة) — تُتابَع مرة وحدة لكل دورة بوت، مو لكل عملة
const BTC_LAYER_STEP_SERVER = 70;   // دولار — نفس عتبة الواجهة
const BTC_LAYER_PCT_CAP_SERVER = 25; // دولار = 100%
let botBtcRefPrice = null;
let botBtcLastDiff = 0;
function updateBotBtcLayer() {
  const btcCandles = candleStore[`BTCUSDT_${SCAN_INTERVAL}`];
  if (!btcCandles || !btcCandles.length) return;
  const price = btcCandles[btcCandles.length - 1].close;
  if (botBtcRefPrice === null) { botBtcRefPrice = price; return; }
  botBtcLastDiff = price - botBtcRefPrice;
}
function computeBtcBoost() {
  const moved70 = Math.abs(botBtcLastDiff) >= BTC_LAYER_STEP_SERVER;
  if (!moved70) return 0;
  const pct = Math.min(100, (Math.abs(botBtcLastDiff) / BTC_LAYER_PCT_CAP_SERVER) * 100) / 100;
  const sign = botBtcLastDiff > 0 ? 1 : -1;
  return sign * pct * 0.3; // نفس وزن التعزيز 30% كحد أقصى بالواجهة
}

// ── مربع "التحليل" الثانوي: اتجاه 20% + توصيات 10% + إجماع الحجم 30% + ثقة 20% + تحليل عام 20% (نفس أوزان اللوحة)
function computeSecondaryScore(indicators, decision) {
  if (!indicators) return 0;

  // إجماع الحجم: CVD + تجميع/تصريف
  let volumeLean = 0;
  {
    let bull = 0, bear = 0;
    if (indicators.cvd && indicators.cvd.signal === 'bullish_divergence') bull++;
    if (indicators.cvd && indicators.cvd.signal === 'bearish_divergence') bear++;
    if (indicators.accDist && indicators.accDist.zone === 'تجميع (Accumulation)') bull++;
    if (indicators.accDist && indicators.accDist.zone === 'تصريف (Distribution)') bear++;
    const total = bull + bear;
    volumeLean = total > 0 ? (bull - bear) / total : 0;
  }

  // الاتجاه العام: موقع السعر من EMA200
  const trendSign = (indicators.ema200 != null && indicators.currentPrice != null)
    ? (indicators.currentPrice > indicators.ema200 ? 1 : -1) : 0;

  // التوصيات + الثقة: من نفس دالة القرار الأصلية
  const actionSign = decision ? (decision.action === 'buy zone' ? 1 : decision.action === 'sell zone' ? -1 : 0) : 0;
  const confWeight = decision ? (decision.confidence || 50) / 100 : 0.5;
  const confidenceLean = actionSign * confWeight;

  const W = { trend: 0.20, action: 0.10, volume: 0.30, confidence: 0.20, analysis: 0.20 };
  return trendSign * W.trend + actionSign * W.action + volumeLean * W.volume
       + confidenceLean * W.confidence + trendSign * W.analysis; // نطاق -1..+1
}

// ── 25%: تحليل خاص بالبوت وحده — مؤشرات ما تدخل في قرار اللوحة الأصلي، بما فيها نسبة بايننس (فيوتشر) ──
async function computeBotOwnSignal(indicators, candles, symbol) {
  if (!indicators || !candles || candles.length < 20) return 0;
  const terms = [];

  // 1) تسارع RSI: هل الزخم نفسه يتسارع صعودًا/هبوطًا خلال آخر 5 شمعات (زخم الزخم، غير موجود في اللوحة)
  const closes = candles.map((c) => c.close);
  const rsiSeries = RSI.calculate({ values: closes, period: 14 });
  if (rsiSeries.length >= 5) {
    const recent = rsiSeries.slice(-5);
    const slope = recent[recent.length - 1] - recent[0];
    terms.push({ sign: Math.abs(slope) > 3 ? (slope > 0 ? 1 : -1) : 0, w: 1 });
  }

  // 2) انحراف السعر عن VWAP: ابتعاد واضح = ضغط اتجاهي حقيقي (مو مستخدم في قرار اللوحة الرئيسي)
  if (indicators.vwap != null && indicators.currentPrice != null) {
    const dev = (indicators.currentPrice - indicators.vwap) / indicators.vwap;
    terms.push({ sign: Math.abs(dev) > 0.004 ? (dev > 0 ? 1 : -1) : 0, w: 1 });
  }

  // 3) اتجاه مصحوب بتقلب حقيقي (ATR مرتفع نسبيًا) = زخم دخول/خروج حقيقي مو ضجيج
  if (indicators.atr && indicators.atr.percent != null && indicators.ema50 != null && indicators.currentPrice != null) {
    const trendDir = indicators.currentPrice > indicators.ema50 ? 1 : -1;
    terms.push({ sign: indicators.atr.percent >= 0.5 ? trendDir : 0, w: 1 });
  }

  // 4) "نسبة بايننس" — معدل التمويل ونسبة حسابات الشراء/البيع من عقود بايننس الآجلة (تفسير عكسي/ازدحام)
  try {
    const bf = await fetchBinanceFutures(symbol);
    if (bf) {
      let sign = 0, count = 0;
      if (bf.fundingRate != null) { sign += bf.fundingRate > 0.02 ? -1 : bf.fundingRate < -0.02 ? 1 : 0; count++; }
      if (bf.longShortRatio != null) { sign += bf.longShortRatio > 2 ? -1 : bf.longShortRatio < 0.5 ? 1 : 0; count++; }
      if (count > 0) terms.push({ sign: Math.sign(sign) || 0, w: 1 });
    }
  } catch (e) { /* بايننس غير متاحة لهذي العملة أو فشل الطلب — نتجاهل بصمت ونكمل بباقي المؤشرات */ }

  const totalWeight = terms.reduce((s, t) => s + t.w, 0);
  const rawScore = terms.reduce((s, t) => s + t.sign * t.w, 0);
  return totalWeight > 0 ? rawScore / totalWeight : 0; // نطاق -1..+1
}

const MIN_LIQUIDITY_USDT = 200000; // متوسط سيولة (حجم × سعر) لآخر 10 شمعات 15د — حد أدنى لضمان تنفيذ آمن

// تأكيد الدخول: يشترط نزول حقيقي خلال آخر 10 شمعات + السعر الحالي بالجزء السفلي من مدى تلك الشمعات + سيولة كافية
// هذا يمنع "الشراء المفتوح" العشوائي — ما نشتري إلا بعد تأكد فعلي إن العملة نزلت وصار عندها فرصة ارتداد بسيولة تكفي
function passesEntryFilters(candles) {
  if (!candles || candles.length < 11) return { ok: false, reason: 'بيانات غير كافية (أقل من 10 شمعات)' };
  const last10 = candles.slice(-11, -1); // آخر 10 شمعات مغلقة، بدون الشمعة الحالية غير المكتملة
  const closes = last10.map((c) => c.close);
  const changePct = ((closes[closes.length - 1] - closes[0]) / closes[0]) * 100;
  const currentPrice = candles[candles.length - 1].close;
  const rangeHigh = Math.max(...last10.map((c) => c.high));
  const rangeLow = Math.min(...last10.map((c) => c.low));
  const posInRange = rangeHigh > rangeLow ? (currentPrice - rangeLow) / (rangeHigh - rangeLow) : 0.5;
  const avgQuoteVolume = last10.reduce((s, c) => s + c.volume * c.close, 0) / last10.length;

  const declineConfirmed = changePct < -0.3;
  const nearLowerZone = posInRange <= 0.4;
  const liquidOk = avgQuoteVolume >= MIN_LIQUIDITY_USDT;

  if (!declineConfirmed) return { ok: false, reason: `ما فيه نزول واضح بآخر 10 شمعات (${changePct.toFixed(2)}%)` };
  if (!nearLowerZone) return { ok: false, reason: `السعر مو بالجزء السفلي من مدى آخر 10 شمعات (${(posInRange * 100).toFixed(0)}%)` };
  if (!liquidOk) return { ok: false, reason: `سيولة ضعيفة (${fmtBig(avgQuoteVolume)} USDT بآخر 10 شمعات)` };
  return { ok: true, reason: `تأكيد نزول ${changePct.toFixed(2)}% + السعر بأدنى ${(posInRange * 100).toFixed(0)}% من المدى + سيولة ${fmtBig(avgQuoteVolume)} USDT` };
}

function fmtBig(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return n.toFixed(0);
}

async function evaluateBotSignal(symbol) {
  const candles = candleStore[`${symbol}_${SCAN_INTERVAL}`];
  if (!candles || candles.length < 60) return null;
  const indicators = computeIndicatorsFixedReversal(symbol, SCAN_INTERVAL, candles);
  if (!indicators) return null;
  const decision = makeDecision(indicators);

  updateBotBtcLayer();
  const decision7Base = computeDecision7Score(indicators, decision);
  const decision7Signal = Math.max(-1, Math.min(1, decision7Base + computeBtcBoost())); // القرار بعد دمج تأثير البيتكوين
  const fourBoxSignal = computeFourBoxScore(indicators);
  const secondarySignal = computeSecondaryScore(indicators, decision);
  const frameSignal = computeFrameScore(symbol);
  // نفس تجميع اللوحة: القرار 25% + التحليل 20% + الارتداد 35% + الفريم 20%
  const dashboardSignal = 0.25 * decision7Signal + 0.20 * fourBoxSignal + 0.35 * secondarySignal + 0.20 * frameSignal;
  const botOwnSignal = await computeBotOwnSignal(indicators, candles, symbol);
  const composite = 0.75 * dashboardSignal + 0.25 * botOwnSignal;
  let action = 'hold';
  if (composite >= BOT_BUY_THRESHOLD) action = 'buy';
  else if (composite <= BOT_SELL_THRESHOLD) action = 'sell';

  const filter = passesEntryFilters(candles);

  return {
    symbol, decision7Signal, fourBoxSignal, secondarySignal, frameSignal, dashboardSignal, botOwnSignal, composite, action,
    price: indicators.currentPrice,
    buyZone: decision.buyZone, sellZone: decision.sellZone,
    passesFilters: filter.ok, filterReason: filter.reason,
  };
}

// ── تنفيذ الأوامر: MEXC و Binance (Spot، توقيع HMAC-SHA256) ──
function mexcSignedQuery(params) {
  const qs = new URLSearchParams(params).toString();
  const sig = crypto.createHmac('sha256', MEXC_API_SECRET).update(qs).digest('hex');
  return `${qs}&signature=${sig}`;
}
function binanceSignedQuery(params) {
  const qs = new URLSearchParams(params).toString();
  const sig = crypto.createHmac('sha256', BINANCE_API_SECRET).update(qs).digest('hex');
  return `${qs}&signature=${sig}`;
}

// دقة الكمية والسعر: تقريب مبسّط حسب حجم السعر (بدون استعلام exchangeInfo لكل عملة — قد يحتاج ضبط يدوي لعملات نادرة)
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

async function placeMexcOrder(symbol, side, quoteOrderQty, quantity) {
  const params = { symbol, side, type: 'MARKET', timestamp: Date.now(), recvWindow: 5000 };
  if (side === 'BUY') params.quoteOrderQty = quoteOrderQty; else params.quantity = quantity;
  const { data } = await axios.post(`https://api.mexc.com/api/v3/order?${mexcSignedQuery(params)}`, null, {
    headers: { 'X-MEXC-APIKEY': MEXC_API_KEY }, timeout: 10000,
  });
  return data;
}
async function placeBinanceOrder(symbol, side, quoteOrderQty, quantity) {
  const params = { symbol, side, type: 'MARKET', timestamp: Date.now(), recvWindow: 5000 };
  if (side === 'BUY') params.quoteOrderQty = quoteOrderQty; else params.quantity = quantity;
  const { data } = await axios.post(`https://api.binance.com/api/v3/order?${binanceSignedQuery(params)}`, null, {
    headers: { 'X-MBX-APIKEY': BINANCE_API_KEY }, timeout: 10000,
  });
  return data;
}
function placeOrder(exchange, symbol, side, quoteOrderQty, quantity) {
  return exchange === 'binance'
    ? placeBinanceOrder(symbol, side, quoteOrderQty, quantity)
    : placeMexcOrder(symbol, side, quoteOrderQty, quantity);
}

// ── أوامر محدّدة السعر (Limit) — هذي اللي يستخدمها البوت تلقائيًا، لا شراء ولا بيع مفتوح إطلاقًا ──
async function placeMexcLimitOrder(symbol, side, price, quantity) {
  const params = { symbol, side, type: 'LIMIT', timeInForce: 'GTC', quantity, price, timestamp: Date.now(), recvWindow: 5000 };
  const { data } = await axios.post(`https://api.mexc.com/api/v3/order?${mexcSignedQuery(params)}`, null, {
    headers: { 'X-MEXC-APIKEY': MEXC_API_KEY }, timeout: 10000,
  });
  return data;
}
async function placeBinanceLimitOrder(symbol, side, price, quantity) {
  const params = { symbol, side, type: 'LIMIT', timeInForce: 'GTC', quantity, price, timestamp: Date.now(), recvWindow: 5000 };
  const { data } = await axios.post(`https://api.binance.com/api/v3/order?${binanceSignedQuery(params)}`, null, {
    headers: { 'X-MBX-APIKEY': BINANCE_API_KEY }, timeout: 10000,
  });
  return data;
}
function placeLimitOrder(exchange, symbol, side, price, quantity) {
  return exchange === 'binance'
    ? placeBinanceLimitOrder(symbol, side, price, quantity)
    : placeMexcLimitOrder(symbol, side, price, quantity);
}

async function queryMexcOrder(symbol, orderId) {
  const params = { symbol, orderId, timestamp: Date.now(), recvWindow: 5000 };
  const { data } = await axios.get(`https://api.mexc.com/api/v3/order?${mexcSignedQuery(params)}`, {
    headers: { 'X-MEXC-APIKEY': MEXC_API_KEY }, timeout: 10000,
  });
  return data;
}
async function queryBinanceOrder(symbol, orderId) {
  const params = { symbol, orderId, timestamp: Date.now(), recvWindow: 5000 };
  const { data } = await axios.get(`https://api.binance.com/api/v3/order?${binanceSignedQuery(params)}`, {
    headers: { 'X-MBX-APIKEY': BINANCE_API_KEY }, timeout: 10000,
  });
  return data;
}
function queryOrder(exchange, symbol, orderId) {
  return exchange === 'binance' ? queryBinanceOrder(symbol, orderId) : queryMexcOrder(symbol, orderId);
}

async function cancelMexcOrder(symbol, orderId) {
  const params = { symbol, orderId, timestamp: Date.now(), recvWindow: 5000 };
  const { data } = await axios.delete(`https://api.mexc.com/api/v3/order?${mexcSignedQuery(params)}`, {
    headers: { 'X-MEXC-APIKEY': MEXC_API_KEY }, timeout: 10000,
  });
  return data;
}
async function cancelBinanceOrder(symbol, orderId) {
  const params = { symbol, orderId, timestamp: Date.now(), recvWindow: 5000 };
  const { data } = await axios.delete(`https://api.binance.com/api/v3/order?${binanceSignedQuery(params)}`, {
    headers: { 'X-MBX-APIKEY': BINANCE_API_KEY }, timeout: 10000,
  });
  return data;
}
function cancelOrder(exchange, symbol, orderId) {
  return exchange === 'binance' ? cancelBinanceOrder(symbol, orderId) : cancelMexcOrder(symbol, orderId);
}

// ملاحظة: بيع فوري (Market) — لزر "إغلاق يدوي" فقط. يلغي أي أمر جني ربح معلّق أولًا إذا وجد.
async function executeBotSell(symbol, sig, reasonOverride) {
  const pos = botState.positions[symbol];
  if (!pos) return;
  if (botState.pendingOrders[symbol] && botState.pendingOrders[symbol].side === 'SELL') {
    try { await cancelOrder(botState.exchange, symbol, botState.pendingOrders[symbol].orderId); } catch (e) { /* تجاهل — ممكن يكون اتنفذ لتوّه */ }
    delete botState.pendingOrders[symbol];
  }
  const data = await placeOrder(botState.exchange, symbol, 'SELL', null, pos.qty);
  const executedQty = parseFloat(data.executedQty || pos.qty);
  const quoteReceived = parseFloat(data.cummulativeQuoteQty || 0);
  const exitPrice = executedQty ? quoteReceived / executedQty : pos.entryPrice;
  const pnl = quoteReceived - (pos.qty * pos.entryPrice);
  const pnlPct = pos.entryPrice ? ((exitPrice - pos.entryPrice) / pos.entryPrice) * 100 : 0;
  delete botState.positions[symbol];
  const reason = reasonOverride || `إغلاق يدوي فوري`;
  botState.tradeLog.unshift({
    time: Date.now(), symbol, side: 'SELL', price: exitPrice, qty: executedQty,
    quoteAmount: quoteReceived, exchange: botState.exchange, reason, pnl, pnlPct,
  });
  botState.tradeLog = botState.tradeLog.slice(0, 50);
  broadcastBotStatus();
}

function logBotError(symbol, err) {
  const detail = err.response?.data?.msg || err.message;
  console.error(`[BOT] خطأ في ${symbol}:`, detail);
  botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'error', message: String(detail) });
  botState.tradeLog = botState.tradeLog.slice(0, 50);
}

// ── دورة البوت التلقائية: أوامر Limit فقط في كل قراراتها — بدون شراء أو بيع مفتوح إطلاقًا ──

// يحدد سعر دخول داخل منطقة الشراء (أو السعر الحالي لو كان أصلًا داخلها) ويضع أمر شراء معلّق ينتظر وصول السعر له
async function placeBotLimitBuy(sig) {
  const { symbol } = sig;
  const zoneTo = sig.buyZone ? parseFloat(sig.buyZone.to) : null;
  const buyPrice = roundPrice(zoneTo != null ? Math.min(sig.price, zoneTo) : sig.price);
  const qty = roundQty(botState.tradeSizeUsdt / buyPrice, buyPrice);
  if (!qty || qty <= 0) return;

  const data = await placeLimitOrder(botState.exchange, symbol, 'BUY', buyPrice, qty);
  if (!data.orderId) return;
  botState.pendingOrders[symbol] = { orderId: data.orderId, side: 'BUY', price: buyPrice, qty, placedAt: Date.now(), exchange: botState.exchange };
  botState.tradeLog.unshift({
    time: Date.now(), symbol, type: 'order', side: 'BUY', price: buyPrice, qty, exchange: botState.exchange,
    reason: `أمر شراء محدّد السعر عند ${buyPrice} — ${sig.filterReason} | قرار ${(sig.decision7Signal * 100).toFixed(0)}% (25%) × تحليل ${(sig.fourBoxSignal * 100).toFixed(0)}% (20%) × ارتداد ${(sig.secondarySignal * 100).toFixed(0)}% (35%) × فريم ${(sig.frameSignal * 100).toFixed(0)}% (20%) [إجمالي 75%] × بوت ${(sig.botOwnSignal * 100).toFixed(0)}% (25%)`,
  });
  botState.tradeLog = botState.tradeLog.slice(0, 50);
}

// فور تنفيذ الشراء، يضع أمر بيع معلّق عند هدف الربح المحدد وينتظر ارتفاع السعر له
async function placeBotTakeProfitSell(symbol, position) {
  const sellPrice = roundPrice(position.entryPrice * (1 + botState.takeProfitPercent / 100));
  const data = await placeLimitOrder(botState.exchange, symbol, 'SELL', sellPrice, position.qty);
  if (!data.orderId) return;
  botState.pendingOrders[symbol] = { orderId: data.orderId, side: 'SELL', price: sellPrice, qty: position.qty, placedAt: Date.now(), exchange: botState.exchange };
  botState.tradeLog.unshift({
    time: Date.now(), symbol, type: 'order', side: 'SELL', price: sellPrice, qty: position.qty, exchange: botState.exchange,
    reason: `أمر بيع محدّد السعر (جني ربح ${botState.takeProfitPercent}%) عند ${sellPrice}`,
  });
  botState.tradeLog = botState.tradeLog.slice(0, 50);
}

async function checkPendingOrder(symbol) {
  const pending = botState.pendingOrders[symbol];
  if (!pending) return;
  let data;
  try { data = await queryOrder(pending.exchange, symbol, pending.orderId); }
  catch (err) { return; } // فشل الاستعلام مؤقتًا — نعيد المحاولة بالدورة الجاية

  const status = data.status;

  if (status === 'FILLED') {
    const executedQty = parseFloat(data.executedQty || pending.qty);
    const quoteAmount = parseFloat(data.cummulativeQuoteQty || executedQty * pending.price);
    const fillPrice = executedQty ? quoteAmount / executedQty : pending.price;

    if (pending.side === 'BUY') {
      botState.positions[symbol] = { qty: executedQty, entryPrice: fillPrice, entryTime: Date.now() };
      delete botState.pendingOrders[symbol];
      botState.tradeLog.unshift({
        time: Date.now(), symbol, side: 'BUY', price: fillPrice, qty: executedQty,
        quoteAmount, exchange: pending.exchange, reason: `تنفيذ أمر الشراء المحدّد عند ${fillPrice.toFixed(6)}`,
      });
      botState.tradeLog = botState.tradeLog.slice(0, 50);
      try { await placeBotTakeProfitSell(symbol, botState.positions[symbol]); } catch (err) { logBotError(symbol, err); }
    } else {
      const pos = botState.positions[symbol];
      const pnl = pos ? quoteAmount - pos.qty * pos.entryPrice : null;
      const pnlPct = pos && pos.entryPrice ? ((fillPrice - pos.entryPrice) / pos.entryPrice) * 100 : null;
      delete botState.positions[symbol];
      delete botState.pendingOrders[symbol];
      botState.tradeLog.unshift({
        time: Date.now(), symbol, side: 'SELL', price: fillPrice, qty: executedQty,
        quoteAmount, exchange: pending.exchange, reason: `تنفيذ أمر جني الربح عند ${fillPrice.toFixed(6)}`, pnl, pnlPct,
      });
      botState.tradeLog = botState.tradeLog.slice(0, 50);
    }
  } else if (status === 'CANCELED' || status === 'EXPIRED' || status === 'REJECTED') {
    delete botState.pendingOrders[symbol];
  } else if (pending.side === 'BUY') {
    // أمر شراء لسا معلّق — نلغيه لو قدم كثير والسعر رجع فوق منطقة الشراء (فاتت الفرصة، ننتظر فرصة جديدة أنظف)
    const ageMinutes = (Date.now() - pending.placedAt) / 60000;
    const sig = botState.lastSignals[symbol];
    if (ageMinutes > MAX_PENDING_BUY_MINUTES && sig && sig.price > pending.price * 1.01) {
      try { await cancelOrder(pending.exchange, symbol, pending.orderId); } catch (err) { /* تجاهل */ }
      delete botState.pendingOrders[symbol];
      botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'error', message: `ألغي أمر الشراء المعلّق (${ageMinutes.toFixed(0)} دقيقة بدون تنفيذ، السعر ابتعد عن منطقة الشراء)` });
      botState.tradeLog = botState.tradeLog.slice(0, 50);
    }
  }
  // أوامر البيع (جني الربح) تُترك معلّقة بصبر بدون إلغاء تلقائي — تنتظر الارتفاع مهما طال، ما فيه بيع مفتوح بديل
}

async function runBotCycle() {
  if (!botState.enabled) return;

  // 1) تابع كل الأوامر المعلّقة أولًا (شراء بانتظار التنفيذ، أو بيع بانتظار الارتفاع لهدف الربح)
  for (const symbol of Object.keys(botState.pendingOrders)) {
    try { await checkPendingOrder(symbol); } catch (err) { logBotError(symbol, err); }
  }

  // 2) قيّم كل العملات المراقبة (نحدّث الإشارات حتى لو ما راح نشتري، عشان تنعرض صح باللوحة)
  const candidates = [];
  for (const symbol of SCAN_SYMBOLS) {
    try {
      const sig = await evaluateBotSignal(symbol);
      if (!sig) continue;
      botState.lastSignals[symbol] = sig;
      const alreadyOpen = botState.positions[symbol] || botState.pendingOrders[symbol];
      if (!alreadyOpen && sig.action === 'buy' && sig.passesFilters) candidates.push(sig);
    } catch (err) { logBotError(symbol, err); }
  }

  // 3) لو تحت الحد الأقصى للمراكز/الأوامر المفتوحة، افتح أفضل مرشح — الأرخص سعرًا أولًا (تفضيل العملات منخفضة السعر)
  const openCount = Object.keys(botState.positions).length + Object.keys(botState.pendingOrders).length;
  if (openCount < botState.maxConcurrentPositions && candidates.length) {
    candidates.sort((a, b) => a.price - b.price);
    const pick = candidates[0];
    try { await placeBotLimitBuy(pick); } catch (err) { logBotError(pick.symbol, err); }
  }

  broadcastBotStatus();
}

function botStatusPayload() {
  return JSON.stringify({
    type: 'bot_status',
    enabled: botState.enabled,
    exchange: botState.exchange,
    tradeSizeUsdt: botState.tradeSizeUsdt,
    takeProfitPercent: botState.takeProfitPercent,
    maxConcurrentPositions: botState.maxConcurrentPositions,
    positions: botState.positions,
    pendingOrders: botState.pendingOrders,
    tradeLog: botState.tradeLog.slice(0, 20),
    lastSignals: botState.lastSignals,
  });
}
function broadcastBotStatus() {
  const payload = botStatusPayload();
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  }
}

setInterval(runBotCycle, 60 * 1000); // تقييم وتنفيذ كل دقيقة لكل عملات SCAN_SYMBOLS

// ── Start ─────────────────────────────────────────────────────────────────────

server.listen(PORT, () => console.log(`Crypto Dashboard running on port ${PORT}`));
