'use strict';

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const axios = require('axios');
const path = require('path');
const { RSI, MACD, BollingerBands, EMA } = require('technicalindicators');

const PORT = process.env.PORT || 3000;

const SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'AVAXUSDT', 'PAXGUSDT'];
const INTERVALS = ['5m', '15m', '30m'];

// MEXC WebSocket interval codes
const MEXC_WS_INTERVAL = { '5m': 'Min5', '15m': 'Min15', '30m': 'Min30' };

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

function broadcastUpdate(symbol, interval) {
  const key = `${symbol}_${interval}`;
  const candles = candleStore[key];
  if (!candles || !candles.length) return;

  const indicators = computeIndicators(candles);
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
  const indicators = computeIndicators(candles);
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

  const rsi = last(RSI.calculate({ values: closes, period: 14 }));

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

  return {
    rsi, macd, bb, ema50, ema200,
    sma20, vwap, stochastic, adx, obv, obvPrev, supertrend, ichimoku, volumeProfile,
    pivot, candleCompare, accDist,
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
  const { rsi, macd, bb, ema50, ema200, currentPrice } = indicators;
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
  const confidence = Math.round(Math.min(100, score * (0.4 + dominance * 0.6)));

  const buyZone  = bb ? { from: bb.lower.toFixed(4),                         to: ((bb.lower + bb.middle) / 2).toFixed(4) } : null;
  const sellZone = bb ? { from: ((bb.upper + bb.middle) / 2).toFixed(4),    to: bb.upper.toFixed(4) } : null;

  return { trend, action, confidence, notes, buyZone, sellZone };
}

// ── Start ─────────────────────────────────────────────────────────────────────

server.listen(PORT, () => console.log(`Crypto Dashboard running on port ${PORT}`));
