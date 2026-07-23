'use strict';

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const axios = require('axios');
const path = require('path');
const { RSI, MACD, BollingerBands, EMA } = require('technicalindicators');

const PORT = process.env.PORT || 3000;

const SYMBOLS = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'AVAXUSDT'];
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

// ── MEXC Historical + Stream ──────────────────────────────────────────────────

async function fetchHistorical(symbol, interval, limit = 300) {
  // MEXC klines API is Binance-compatible
  const url = `https://api.mexc.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}`;
  const { data } = await axios.get(url, { timeout: 10000 });
  // Each row: [openTime, open, high, low, close, volume, closeTime, quoteVolume]
  return data.map((k) => ({
    time: Math.floor(k[0] / 1000),
    open: parseFloat(k[1]),
    high: parseFloat(k[2]),
    low: parseFloat(k[3]),
    close: parseFloat(k[4]),
    volume: parseFloat(k[5]),
    isClosed: true,
  }));
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
    console.error(`[${key}] historical fetch failed:`, err.message);
    candleStore[key] = [];
  }

  connectStream(symbol, interval);
}

function connectStream(symbol, interval) {
  const key = `${symbol}_${interval}`;
  const wsInterval = MEXC_WS_INTERVAL[interval];
  const topic = `spot@public.kline.v3.api@${symbol}@${wsInterval}`;

  const ws = new WebSocket('wss://wbs.mexc.com/ws');
  streamWs[key] = ws;

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

    // Ignore PONG / subscription confirmations
    if (!msg.d || !msg.d.k) return;

    const k = msg.d.k;
    const candle = {
      time: Math.floor(k.t / 1000),
      open: parseFloat(k.o),
      high: parseFloat(k.h),
      low: parseFloat(k.l),
      close: parseFloat(k.c),
      volume: parseFloat(k.v),
      isClosed: k.X === true,
    };

    if (!candleStore[key]) candleStore[key] = [];
    const candles = candleStore[key];
    const last = candles[candles.length - 1];

    if (last && last.time === candle.time) {
      candles[candles.length - 1] = candle;
    } else if (!last || candle.time > last.time) {
      candles.push(candle);
      if (candles.length > 600) candles.shift();
    }

    broadcastUpdate(symbol, interval);
  });

  ws.on('error', (err) => console.error(`[${key}] MEXC WS error:`, err.message));

  ws.on('close', () => {
    clearInterval(ping);
    console.log(`[${key}] MEXC stream closed — reconnecting in 5s`);
    delete streamWs[key];
    setTimeout(() => connectStream(symbol, interval), 5000);
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

  return { rsi, macd, bb, ema50, ema200, currentPrice: closes[closes.length - 1] };
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
