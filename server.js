'use strict';

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const axios = require('axios');
const path = require('path');
const { RSI, MACD, BollingerBands, EMA } = require('technicalindicators');

const PORT = process.env.PORT || 3000;

const SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'AVAXUSDT', 'PAXGUSDT'];
const INTERVALS = ['5m', '15m', '30m', '2h', '4h'];

// MEXC WebSocket interval codes
const MEXC_WS_INTERVAL = { '5m': 'Min5', '15m': 'Min15', '30m': 'Min30', '2h': 'Hour2', '4h': 'Hour4' };

// candleStore[key] = [{ time, open, high, low, close, volume }]
const candleStore = {};
// streamWs[key] = WebSocket | null (null = loading, undefined = not started)
const streamWs = {};
// clientSubs: Map<ws, { symbol, interval }>
const clientSubs = new Map();

const app = express();
const server = http.createServer(app);

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

wss.on('connection', (ws) => {
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
      clientSubs.set(ws, { symbol, interval });
      await ensureStream(symbol, interval);
      await ensureStream(symbol, '15m'); // فريم ثابت لمؤشرات الارتداد (لا يتأثر بتغيير فريم العرض)
      sendSnapshot(ws, symbol, interval);
    }
  });

  ws.on('close', () => clientSubs.delete(ws));
  ws.on('error', () => clientSubs.delete(ws));
});

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
  const url = `https://www.okx.com/api/v5/market/candles?instId=${instId}&bar=${interval}&limit=${limit}`;
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
  const channel = `candle${interval}`;
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
      const l