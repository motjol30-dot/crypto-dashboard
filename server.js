'use strict';

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const axios = require('axios');
const path = require('path');
const crypto = require('crypto');
const { RSI, MACD, BollingerBands, EMA } = require('technicalindicators');

const PORT = process.env.PORT || 3000;

const SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'AVAXUSDT', 'PAXGUSDT'];
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

// مناطق الفيوتشر: معدل التمويل + الفائدة المفتوحة لأي عملة مختارة (نفس منصة MEXC)
app.get('/api/futures-zone', async (req, res) => {
  const symbol = (req.query.symbol || 'BTCUSDT').toUpperCase();
  const contractSymbol = symbol.replace(/USDT$/, '_USDT');
  const result = { fundingRate: null, openInterest: null, errors: [] };

  try {
    const r = await axios.get(`https://contract.mexc.com/api/v1/contract/funding_rate/${contractSymbol}`, { timeout: 8000 });
    const fr = r.data?.data?.fundingRate;
    if (fr != null) result.fundingRate = Number(fr) * 100;
  } catch (e) { result.errors.push('fundingRate'); }

  try {
    const r = await axios.get(`https://contract.mexc.com/api/v1/contract/open_interest/${contractSymbol}`, { timeout: 8000 });
    const oi = r.data?.data?.holdVol ?? r.data?.data?.amount;
    if (oi != null) result.openInterest = Number(oi);
  } catch (e) { result.errors.push('openInterest'); }

  res.json(result);
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
  return indicators;
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

  // OBV (يدوي)
  let obv = null, obvPrev = null;
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

  // دايفرجنز RSI — مقارنة قاع السعر بقاع RSI خلال آخر 10 شمعات
  let rsiDivergence = null;
  if (rsiArr.length >= 11 && n >= 11) {
    const priceNow = closes[n - 1], priceBefore = closes[n - 11];
    const rsiNow = rsiArr[rsiArr.length - 1], rsiBefore = rsiArr[rsiArr.length - 11];
    let type = 'none';
    if (priceNow < priceBefore && rsiNow > rsiBefore) type = 'bullish'; // تجميع خفي
    else if (priceNow > priceBefore && rsiNow < rsiBefore) type = 'bearish'; // توزيع خفي
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
    sma20, vwap, stochastic, adx, obv, obvPrev, supertrend, ichimoku, volumeProfile,
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

// ── Start ─────────────────────────────────────────────────────────────────────

server.listen(PORT, () => console.log(`Crypto Dashboard running on port ${PORT}`));
