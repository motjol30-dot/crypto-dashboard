'use strict';

const express = require('express');
const http = require('http');
const WebSocket = require('ws');
const axios = require('axios');
const path = require('path');
const crypto = require('crypto');
const { RSI, MACD, BollingerBands, EMA } = require('technicalindicators');

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

const INTERVALS = ['3m','5m','15m','30m','1h','2h','4h'];
const MEXC_WS_INTERVAL = { '3m':'Min3','5m':'Min5','15m':'Min15','30m':'Min30','1h':'Hour1','2h':'Hour2','4h':'Hour4' };
const OKX_BAR = { '3m':'3m','5m':'5m','15m':'15m','30m':'30m','1h':'1H','2h':'2H','4h':'4H' };

const candleStore = {};
const streamWs = {};
const clientSubs = new Map();

const app = express();
const server = http.createServer(app);

// الحماية
const APP_USER = process.env.APP_USER || 'group';
const APP_PASS = process.env.APP_PASS || 'change-me-1234';
const MAX_USERS = parseInt(process.env.MAX_USERS || '12', 10);
const SESSION_TTL_MS = 15 * 60 * 1000;
const AUTH_ENABLED = process.env.AUTH_ENABLED === 'true';
const activeSessions = new Map();

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

// نظرة عامة على السوق
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

// الطبقة الكلية
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

// فيوتشر متعدد المنصات
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
    ws.send(JSON.stringify({ type: 'explosion_scan', ranking: explosionRanking.slice(0, 3) }));
  }
  ws.send(botStatusPayload());

  ws.on('message', async (raw) => {
    let msg;
    try { msg = JSON.parse(raw); } catch { return; }

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
        const hasKeys = botState.exchange === 'binance'
          ? (BINANCE_API_KEY && BINANCE_API_SECRET)
          : (MEXC_API_KEY && MEXC_API_SECRET);
        if (!hasKeys) {
          ws.send(JSON.stringify({ type: 'error', message: `لا يوجد مفتاح API معرّف لمنصة ${botState.exchange === 'binance' ? 'Binance' : 'MEXC'} — أضِف متغيرات البيئة أولاً` }));
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

// طبقة أفضل 3 عملات
const SCAN_INTERVAL = '15m';
const SCAN_SYMBOLS = SYMBOLS;

let explosionRanking = [];

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

  // أفضلية السعر المنخفض
  const price = closes[n-1];
  let priceBias = 0;
  if (price < 1) priceBias = 12;
  else if (price < 2) priceBias = 8;
  else if (price < 5) priceBias = 5;
  else if (price <= 50) priceBias = 2;
  else if (price > 200) priceBias = -5;
  score += priceBias;

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
  };
}

function runExplosionScan() {
  const results = [];
  for (const symbol of SCAN_SYMBOLS) {
    const candles = candleStore[`${symbol}_${SCAN_INTERVAL}`];
    const res = computeExplosionScore(candles);
    if (res) results.push({ symbol, ...res });
  }
  results.sort((a, b) => b.score - a.score);
  explosionRanking = results.slice(0, 3);
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
  for (const symbol of SCAN_SYMBOLS) { await ensureStream(symbol, SCAN_INTERVAL); }
  setTimeout(runExplosionScan, 5000);
})();
setInterval(runExplosionScan, 5 * 60 * 1000);

// دوال المصادر التاريخية والبث اللحظي
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
  });
  ws.on('error', (err) => console.error(`[${key}] OKX WS error:`, err.message));
  ws.on('close', () => {
    clearInterval(ping);
    console.log(`[${key}] OKX stream closed — reconnecting in 5s (آخر مصدر بالتسلسل)`);
    delete streamWs[key];
    setTimeout(() => connectOkxStream(symbol, interval), 5000);
  });
}

// Broadcast
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

function broadcastUpdate(symbol, interval) {
  const key = `${symbol}_${interval}`;
  const candles = candleStore[key];
  if (!candles || !candles.length) return;
  const indicators = computeIndicatorsFixedReversal(symbol, interval, candles);
  const decision = makeDecision(indicators);
  const payload = JSON.stringify({ type: 'update', symbol, interval, candles, indicators, decision });
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
  const decision = makeDecision(indicators);
  if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify({ type: 'update', symbol, interval, candles, indicators, decision }));
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
function broadcastMtfUpdate(symbol, force = false) {
  const now = Date.now();
  if (!force && lastMtfBroadcast[symbol] && now - lastMtfBroadcast[symbol] < 3000) return;
  lastMtfBroadcast[symbol] = now;
  const snapshot = computeMtfSnapshot(symbol);
  const payload = JSON.stringify({ type: 'mtf_update', symbol, snapshot });
  for (const [client, sub] of clientSubs) {
    if (client.readyState === WebSocket.OPEN && sub.symbol === symbol) client.send(payload);
  }
}

// Indicators
function last(arr) { return arr && arr.length ? arr[arr.length - 1] : undefined; }

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

  return { rsi, macd, bb, ema50, ema200, sma20, vwap, stochastic, adx, obv, obvPrev, obvTrendRef, supertrend, ichimoku, volumeProfile, pivot, candleCompare, accDist, cvd, stochRsi, bbPercentB, rsiDivergence, williamsR, chop, atr, currentPrice: closes[closes.length - 1] };
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

// Decision Engine
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

  const buyZone = bb ? { from: bb.lower.toFixed(4), to: ((bb.lower + bb.middle) / 2).toFixed(4) } : null;
  const sellZone = bb ? { from: ((bb.upper + bb.middle) / 2).toFixed(4), to: bb.upper.toFixed(4) } : null;
  return { trend, action, confidence, notes, buyZone, sellZone };
}

// بوت التداول
let botState = {
  enabled: false,
  exchange: 'binance',
  tradeSizeUsdt: 50,
  takeProfitPercent: 1,
  maxConcurrentPositions: 1,
  positions: {},
  pendingOrders: {},
  pendingSellOrders: {},
  tradeLog: [],
  lastSignals: {},
};

const BOT_BUY_THRESHOLD = 0.35;
const BOT_SELL_THRESHOLD = -0.35;
const MAX_PENDING_BUY_MINUTES = 45;
const TP_LEVELS = [1, 1.5, 2];

function computeFourBoxScore(indicators) {
  if (!indicators) return 0;
  const leanOf = { stability: 0, reversal: 0, momentum: 0, hourly: 0 };
  { let bull = 0, bear = 0;
    if (indicators.adx && indicators.adx.adx >= 20) { if (indicators.adx.pdi > indicators.adx.mdi) bull++; else bear++; }
    if (indicators.williamsR) { if (indicators.williamsR.zoneUp) bull++; if (indicators.williamsR.zoneDown) bear++; }
    if (indicators.ichimoku && indicators.currentPrice != null) { const top = Math.max(indicators.ichimoku.spanA, indicators.ichimoku.spanB); const bottom = Math.min(indicators.ichimoku.spanA, indicators.ichimoku.spanB); if (indicators.currentPrice > top) bull++; else if (indicators.currentPrice < bottom) bear++; }
    if (indicators.obv != null && indicators.obvTrendRef != null) { if (indicators.obv > indicators.obvTrendRef) bull++; else bear++; }
    const total = bull + bear; leanOf.stability = total > 0 ? (bull - bear) / total : 0; }
  { let bull = 0, bear = 0;
    if (indicators.stochRsi) { if (indicators.stochRsi.crossUp || indicators.stochRsi.zoneUp) bull++; if (indicators.stochRsi.crossDown || indicators.stochRsi.zoneDown) bear++; }
    if (indicators.williamsR) { if (indicators.williamsR.crossUpFrom80 || indicators.williamsR.zoneUp) bull++; if (indicators.williamsR.crossDownFrom20 || indicators.williamsR.zoneDown) bear++; }
    if (indicators.bbPercentB) { if (indicators.bbPercentB.zoneUp) bull++; if (indicators.bbPercentB.zoneDown) bear++; }
    if (indicators.rsiDivergence) { if (indicators.rsiDivergence.type === 'bullish') bull++; else if (indicators.rsiDivergence.type === 'bearish') bear++; }
    const total = bull + bear; leanOf.reversal = total > 0 ? (bull - bear) / total : 0; }
  { let bull = 0, bear = 0;
    if (indicators.rsi != null) { if (indicators.rsi > 55) bull++; else if (indicators.rsi < 45) bear++; }
    if (indicators.macd) { if (indicators.macd.value > indicators.macd.signal) bull++; else bear++; }
    if (indicators.stochastic) { if (indicators.stochastic.k > 55 && indicators.stochastic.k >= indicators.stochastic.d) bull++; else if (indicators.stochastic.k < 45 && indicators.stochastic.k <= indicators.stochastic.d) bear++; }
    if (indicators.adx && indicators.adx.adx >= 20) { if (indicators.adx.pdi > indicators.adx.mdi) bull++; else bear++; }
    const total = bull + bear; leanOf.momentum = total > 0 ? (bull - bear) / total : 0; }
  { const hl = indicators.hourlyLayer; const total = hl ? hl.bull + hl.bear : 0; leanOf.hourly = total > 0 ? (hl.bull - hl.bear) / total : 0; }
  return leanOf.stability * 0.45 + leanOf.reversal * 0.30 + leanOf.momentum * 0.20 + leanOf.hourly * 0.05;
}

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
  return totalWeight > 0 ? rawScore / totalWeight : 0;
}

function computeFrameScore(symbol, indicators) {
  const snapshot = computeMtfSnapshot(symbol);
  const watched = ['5m', '15m', '30m', '1h'];
  let sumLean = 0;
  for (const iv of watched) {
    const info = snapshot[iv];
    if (!info) continue;
    const sign = info.action === 'buy zone' ? 1 : info.action === 'sell zone' ? -1 : 0;
    sumLean += (sign * (info.confidence / 100)) * 0.20;
  }
  if (indicators && indicators.ema200 != null && indicators.currentPrice != null) sumLean += (indicators.currentPrice > indicators.ema200 ? 1 : -1) * 0.20;
  return sumLean;
}

const BTC_LAYER_STEP_SERVER = 70;
const BTC_LAYER_PCT_CAP_SERVER = 25;
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
  return sign * pct * 0.3;
}

function computeSecondaryScore(indicators, decision) {
  if (!indicators) return 0;
  let volumeLean = 0;
  { let bull = 0, bear = 0;
    if (indicators.cvd && indicators.cvd.signal === 'bullish_divergence') bull++;
    if (indicators.cvd && indicators.cvd.signal === 'bearish_divergence') bear++;
    if (indicators.accDist && indicators.accDist.zone === 'تجميع (Accumulation)') bull++;
    if (indicators.accDist && indicators.accDist.zone === 'تصريف (Distribution)') bear++;
    const total = bull + bear; volumeLean = total > 0 ? (bull - bear) / total : 0; }
  const trendSign = (indicators.ema200 != null && indicators.currentPrice != null) ? (indicators.currentPrice > indicators.ema200 ? 1 : -1) : 0;
  const actionSign = decision ? (decision.action === 'buy zone' ? 1 : decision.action === 'sell zone' ? -1 : 0) : 0;
  const confWeight = decision ? (decision.confidence || 50) / 100 : 0.5;
  const confidenceLean = actionSign * confWeight;
  const W = { trend: 0.20, action: 0.10, volume: 0.30, confidence: 0.20, analysis: 0.20 };
  return trendSign * W.trend + actionSign * W.action + volumeLean * W.volume + confidenceLean * W.confidence + trendSign * W.analysis;
}

async function computeBotOwnSignal(indicators, candles, symbol) {
  if (!indicators || !candles || candles.length < 20) return 0;
  const terms = [];
  const closes = candles.map(c => c.close);
  const rsiSeries = RSI.calculate({ values: closes, period: 14 });
  if (rsiSeries.length >= 5) {
    const recent = rsiSeries.slice(-5);
    const slope = recent[recent.length - 1] - recent[0];
    terms.push({ sign: Math.abs(slope) > 3 ? (slope > 0 ? 1 : -1) : 0, w: 1 });
  }
  if (indicators.vwap != null && indicators.currentPrice != null) {
    const dev = (indicators.currentPrice - indicators.vwap) / indicators.vwap;
    terms.push({ sign: Math.abs(dev) > 0.004 ? (dev > 0 ? 1 : -1) : 0, w: 1 });
  }
  if (indicators.atr && indicators.atr.percent != null && indicators.ema50 != null && indicators.currentPrice != null) {
    const trendDir = indicators.currentPrice > indicators.ema50 ? 1 : -1;
    terms.push({ sign: indicators.atr.percent >= 0.5 ? trendDir : 0, w: 1 });
  }
  try {
    const bf = await fetchBinanceFutures(symbol);
    if (bf) {
      let sign = 0, count = 0;
      if (bf.fundingRate != null) { sign += bf.fundingRate > 0.02 ? -1 : bf.fundingRate < -0.02 ? 1 : 0; count++; }
      if (bf.longShortRatio != null) { sign += bf.longShortRatio > 2 ? -1 : bf.longShortRatio < 0.5 ? 1 : 0; count++; }
      if (count > 0) terms.push({ sign: Math.sign(sign) || 0, w: 1 });
    }
  } catch (e) {}
  const totalWeight = terms.reduce((s, t) => s + t.w, 0);
  const rawScore = terms.reduce((s, t) => s + t.sign * t.w, 0);
  return totalWeight > 0 ? rawScore / totalWeight : 0;
}

const MIN_LIQUIDITY_USDT = 100000; // تم تخفيضها من 200000

function passesEntryFilters(candles) {
  if (!candles || candles.length < 11) return { ok: false, reason: 'بيانات غير كافية' };
  const last10 = candles.slice(-11, -1);
  const closes = last10.map(c => c.close);
  const changePct = ((closes[closes.length - 1] - closes[0]) / closes[0]) * 100;
  const currentPrice = candles[candles.length - 1].close;
  const rangeHigh = Math.max(...last10.map(c => c.high));
  const rangeLow = Math.min(...last10.map(c => c.low));
  const posInRange = rangeHigh > rangeLow ? (currentPrice - rangeLow) / (rangeHigh - rangeLow) : 0.5;
  const avgQuoteVolume = last10.reduce((s, c) => s + c.volume * c.close, 0) / last10.length;

  // تخفيف الشروط:
  const declineConfirmed = changePct < -0.15;
  const nearLowerZone = posInRange <= 0.55;
  const liquidOk = avgQuoteVolume >= MIN_LIQUIDITY_USDT;

  if (!declineConfirmed) return { ok: false, reason: `ما فيه نزول كافٍ (${changePct.toFixed(2)}%)` };
  if (!nearLowerZone) return { ok: false, reason: `السعر مو بأدنى المدى (${(posInRange * 100).toFixed(0)}%)` };
  if (!liquidOk) return { ok: false, reason: `سيولة ضعيفة (${fmtBig(avgQuoteVolume)} USDT)` };
  return { ok: true, reason: `نزول ${changePct.toFixed(2)}% + السعر بأدنى ${(posInRange * 100).toFixed(0)}% + سيولة ${fmtBig(avgQuoteVolume)} USDT` };
}

function fmtBig(n) {
  if (n >= 1e9) return (n / 1e9).toFixed(2) + 'B';
  if (n >= 1e6) return (n / 1e6).toFixed(2) + 'M';
  if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
  return n.toFixed(0);
}

function computeBuyPoints(candles, indicators, sig) {
  if (!candles || candles.length < 20 || !indicators) return [];
  const price = indicators.currentPrice;
  const lows = candles.slice(-20).map(c => c.low);
  const recentLow = Math.min(...lows);
  const bbLower = indicators.bb?.lower;
  return [
    { point: 1, label: 'نقطة الهبوط المبكر المكتمل', price: roundPrice(price * 0.995), triggered: sig.action === 'buy' && sig.passesFilters },
    { point: 2, label: 'ارتداد من بولينجر السفلي + StochRSI', price: roundPrice(bbLower || price * 0.99), triggered: sig.action === 'buy' && indicators.stochRsi?.crossUp },
    { point: 3, label: 'كسر قاع 20 شمعة بابتلاع صاعد', price: roundPrice(recentLow * 0.99), triggered: sig.action === 'buy' && indicators.candleCompare?.bullEngulf },
  ];
}

// أوامر API
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
  const { data } = await axios.post(`https://api.mexc.com/api/v3/order?${mexcSignedQuery(params)}`, null, { headers: { 'X-MEXC-APIKEY': MEXC_API_KEY }, timeout: 10000 });
  return data;
}
async function placeBinanceOrder(symbol, side, quoteOrderQty, quantity) {
  const params = { symbol, side, type: 'MARKET', timestamp: Date.now(), recvWindow: 5000 };
  if (side === 'BUY') params.quoteOrderQty = quoteOrderQty; else params.quantity = quantity;
  const { data } = await axios.post(`https://api.binance.com/api/v3/order?${binanceSignedQuery(params)}`, null, { headers: { 'X-MBX-APIKEY': BINANCE_API_KEY }, timeout: 10000 });
  return data;
}
function placeOrder(exchange, symbol, side, quoteOrderQty, quantity) {
  return exchange === 'binance' ? placeBinanceOrder(symbol, side, quoteOrderQty, quantity) : placeMexcOrder(symbol, side, quoteOrderQty, quantity);
}

async function placeMexcLimitOrder(symbol, side, price, quantity) {
  const params = { symbol, side, type: 'LIMIT', timeInForce: 'GTC', quantity, price, timestamp: Date.now(), recvWindow: 5000 };
  const { data } = await axios.post(`https://api.mexc.com/api/v3/order?${mexcSignedQuery(params)}`, null, { headers: { 'X-MEXC-APIKEY': MEXC_API_KEY }, timeout: 10000 });
  return data;
}
async function placeBinanceLimitOrder(symbol, side, price, quantity) {
  const params = { symbol, side, type: 'LIMIT', timeInForce: 'GTC', quantity, price, timestamp: Date.now(), recvWindow: 5000 };
  const { data } = await axios.post(`https://api.binance.com/api/v3/order?${binanceSignedQuery(params)}`, null, { headers: { 'X-MBX-APIKEY': BINANCE_API_KEY }, timeout: 10000 });
  return data;
}
function placeLimitOrder(exchange, symbol, side, price, quantity) {
  return exchange === 'binance' ? placeBinanceLimitOrder(symbol, side, price, quantity) : placeMexcLimitOrder(symbol, side, price, quantity);
}

async function queryMexcOrder(symbol, orderId) {
  const params = { symbol, orderId, timestamp: Date.now(), recvWindow: 5000 };
  const { data } = await axios.get(`https://api.mexc.com/api/v3/order?${mexcSignedQuery(params)}`, { headers: { 'X-MEXC-APIKEY': MEXC_API_KEY }, timeout: 10000 });
  return data;
}
async function queryBinanceOrder(symbol, orderId) {
  const params = { symbol, orderId, timestamp: Date.now(), recvWindow: 5000 };
  const { data } = await axios.get(`https://api.binance.com/api/v3/order?${binanceSignedQuery(params)}`, { headers: { 'X-MBX-APIKEY': BINANCE_API_KEY }, timeout: 10000 });
  return data;
}
function queryOrder(exchange, symbol, orderId) {
  return exchange === 'binance' ? queryBinanceOrder(symbol, orderId) : queryMexcOrder(symbol, orderId);
}

async function cancelMexcOrder(symbol, orderId) {
  const params = { symbol, orderId, timestamp: Date.now(), recvWindow: 5000 };
  const { data } = await axios.delete(`https://api.mexc.com/api/v3/order?${mexcSignedQuery(params)}`, { headers: { 'X-MEXC-APIKEY': MEXC_API_KEY }, timeout: 10000 });
  return data;
}
async function cancelBinanceOrder(symbol, orderId) {
  const params = { symbol, orderId, timestamp: Date.now(), recvWindow: 5000 };
  const { data } = await axios.delete(`https://api.binance.com/api/v3/order?${binanceSignedQuery(params)}`, { headers: { 'X-MBX-APIKEY': BINANCE_API_KEY }, timeout: 10000 });
  return data;
}
function cancelOrder(exchange, symbol, orderId) {
  return exchange === 'binance' ? cancelBinanceOrder(symbol, orderId) : cancelMexcOrder(symbol, orderId);
}

async function executeBotSell(symbol, sig, reasonOverride) {
  const pos = botState.positions[symbol];
  if (!pos) return;
  if (botState.pendingSellOrders[symbol]) {
    for (const o of botState.pendingSellOrders[symbol]) { try { await cancelOrder(o.exchange, symbol, o.orderId); } catch {} }
    delete botState.pendingSellOrders[symbol];
  }
  if (botState.pendingOrders[symbol] && botState.pendingOrders[symbol].side === 'SELL') {
    try { await cancelOrder(botState.exchange, symbol, botState.pendingOrders[symbol].orderId); } catch {}
    delete botState.pendingOrders[symbol];
  }
  const data = await placeOrder(botState.exchange, symbol, 'SELL', null, pos.qty);
  const executedQty = parseFloat(data.executedQty || pos.qty);
  const quoteReceived = parseFloat(data.cummulativeQuoteQty || 0);
  const exitPrice = executedQty ? quoteReceived / executedQty : pos.entryPrice;
  const pnl = quoteReceived - (pos.qty * pos.entryPrice);
  const pnlPct = pos.entryPrice ? ((exitPrice - pos.entryPrice) / pos.entryPrice) * 100 : 0;
  delete botState.positions[symbol];
  const reason = reasonOverride || 'إغلاق يدوي فوري';
  botState.tradeLog.unshift({ time: Date.now(), symbol, side: 'SELL', price: exitPrice, qty: executedQty, quoteAmount: quoteReceived, exchange: botState.exchange, reason, pnl, pnlPct });
  botState.tradeLog = botState.tradeLog.slice(0, 50);
  broadcastBotStatus();
}

function logBotError(symbol, err) {
  const detail = err.response?.data?.msg || err.message;
  console.error(`[BOT] خطأ في ${symbol}:`, detail);
  botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'error', message: String(detail) });
  botState.tradeLog = botState.tradeLog.slice(0, 50);
}

async function placeBotTakeProfitSells(symbol, position) {
  const qtyPerOrder = roundQty(position.qty / 3, position.entryPrice);
  if (!qtyPerOrder || qtyPerOrder <= 0) return;
  const tpBase = botState.takeProfitPercent / 100;
  botState.pendingSellOrders[symbol] = [];
  for (const [i, mult] of TP_LEVELS.entries()) {
    const sellPrice = roundPrice(position.entryPrice * (1 + tpBase * mult));
    const pointName = `TP${i + 1} (${(botState.takeProfitPercent * mult).toFixed(1)}%)`;
    try {
      const data = await placeLimitOrder(botState.exchange, symbol, 'SELL', sellPrice, qtyPerOrder);
      if (data.orderId) {
        botState.pendingSellOrders[symbol].push({ orderId: data.orderId, side: 'SELL', price: sellPrice, qty: qtyPerOrder, placedAt: Date.now(), exchange: botState.exchange, point: pointName });
        botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'order', side: 'SELL', price: sellPrice, qty: qtyPerOrder, exchange: botState.exchange, reason: `أمر بيع ${pointName} معلّق عند ${sellPrice}` });
        botState.tradeLog = botState.tradeLog.slice(0, 50);
      }
    } catch (err) { logBotError(symbol, err); }
  }
}

async function placeBotLimitBuy(sig) {
  const { symbol } = sig;
  const buyPoint = sig.buyPoints?.find(p => p.triggered) || sig.buyPoints?.[0];
  const buyPrice = roundPrice(buyPoint?.price || sig.price);
  const qty = roundQty(botState.tradeSizeUsdt / buyPrice, buyPrice);
  if (!qty || qty <= 0) return;

  try {
    let data;
    // إذا كانت إشارة ارتداد قوية نستخدم Market لضمان التنفيذ الفوري
    if (sig.strongReversalUp) {
      data = await placeOrder(botState.exchange, symbol, 'BUY', botState.tradeSizeUsdt, null);
      const executedQty = parseFloat(data.executedQty || qty);
      const quoteAmount = parseFloat(data.cummulativeQuoteQty || botState.tradeSizeUsdt);
      const fillPrice = executedQty > 0 ? quoteAmount / executedQty : buyPrice;
      botState.positions[symbol] = { qty: executedQty, entryPrice: fillPrice, entryTime: Date.now() };
      botState.tradeLog.unshift({
        time: Date.now(), symbol, side: 'BUY', price: fillPrice, qty: executedQty,
        quoteAmount, exchange: botState.exchange,
        reason: `شراء فوري (Market) بسبب إشارة ارتداد قوية — ${sig.filterReason}`,
      });
      await placeBotTakeProfitSells(symbol, botState.positions[symbol]);
    } else {
      data = await placeLimitOrder(botState.exchange, symbol, 'BUY', buyPrice, qty);
      if (!data.orderId) return;
      botState.pendingOrders[symbol] = {
        orderId: data.orderId, side: 'BUY', price: buyPrice, qty,
        placedAt: Date.now(), exchange: botState.exchange,
      };
      botState.tradeLog.unshift({
        time: Date.now(), symbol, type: 'order', side: 'BUY',
        price: buyPrice, qty, exchange: botState.exchange,
        reason: `أمر شراء Limit (${buyPoint?.label || 'نقطة عامة'}) عند ${buyPrice} — ${sig.filterReason}`,
      });
    }
    botState.tradeLog = botState.tradeLog.slice(0, 50);
  } catch (err) {
    logBotError(symbol, err);
  }
}

async function checkPendingOrders(symbol) {
  const buyOrder = botState.pendingOrders[symbol];
  if (buyOrder) {
    let data;
    try { data = await queryOrder(buyOrder.exchange, symbol, buyOrder.orderId); } catch { return; }
    const status = data.status;
    if (status === 'FILLED') {
      const executedQty = parseFloat(data.executedQty || buyOrder.qty);
      const quoteAmount = parseFloat(data.cummulativeQuoteQty || executedQty * buyOrder.price);
      const fillPrice = executedQty ? quoteAmount / executedQty : buyOrder.price;
      botState.positions[symbol] = { qty: executedQty, entryPrice: fillPrice, entryTime: Date.now() };
      delete botState.pendingOrders[symbol];
      botState.tradeLog.unshift({ time: Date.now(), symbol, side: 'BUY', price: fillPrice, qty: executedQty, quoteAmount, exchange: buyOrder.exchange, reason: `نفذ أمر الشراء عند ${fillPrice.toFixed(6)}` });
      botState.tradeLog = botState.tradeLog.slice(0, 50);
      await placeBotTakeProfitSells(symbol, botState.positions[symbol]);
    } else if (status === 'CANCELED' || status === 'EXPIRED' || status === 'REJECTED') {
      delete botState.pendingOrders[symbol];
    } else {
      const ageMinutes = (Date.now() - buyOrder.placedAt) / 60000;
      const sig = botState.lastSignals[symbol];
      if (ageMinutes > MAX_PENDING_BUY_MINUTES && sig && sig.price > buyOrder.price * 1.01) {
        try { await cancelOrder(buyOrder.exchange, symbol, buyOrder.orderId); } catch {}
        delete botState.pendingOrders[symbol];
        botState.tradeLog.unshift({ time: Date.now(), symbol, type: 'error', message: 'أُلغي أمر الشراء المعلق لابتعاد السعر' });
        botState.tradeLog = botState.tradeLog.slice(0, 50);
      }
    }
  }
  const sellOrders = botState.pendingSellOrders[symbol];
  if (sellOrders && sellOrders.length) {
    for (const order of [...sellOrders]) {
      let data;
      try { data = await queryOrder(order.exchange, symbol, order.orderId); } catch { continue; }
      if (data.status === 'FILLED') {
        const executedQty = parseFloat(data.executedQty || order.qty);
        const quoteAmount = parseFloat(data.cummulativeQuoteQty || executedQty * order.price);
        const fillPrice = executedQty ? quoteAmount / executedQty : order.price;
        const pos = botState.positions[symbol];
        const pnl = pos ? quoteAmount - executedQty * pos.entryPrice : null;
        const pnlPct = pos && pos.entryPrice ? ((fillPrice - pos.entryPrice) / pos.entryPrice) * 100 : null;
        botState.pendingSellOrders[symbol] = botState.pendingSellOrders[symbol].filter(o => o.orderId !== order.orderId);
        botState.tradeLog.unshift({ time: Date.now(), symbol, side: 'SELL', price: fillPrice, qty: executedQty, quoteAmount, exchange: order.exchange, reason: `نفذ بيع ${order.point} عند ${fillPrice.toFixed(6)}`, pnl, pnlPct });
        botState.tradeLog = botState.tradeLog.slice(0, 50);
      } else if (['CANCELED', 'EXPIRED', 'REJECTED'].includes(data.status)) {
        botState.pendingSellOrders[symbol] = botState.pendingSellOrders[symbol].filter(o => o.orderId !== order.orderId);
      }
    }
    if (botState.pendingSellOrders[symbol].length === 0) delete botState.pendingSellOrders[symbol];
  }
}

async function runBotCycle() {
  if (!botState.enabled) return;

  const symbolsToCheck = new Set([...Object.keys(botState.pendingOrders), ...Object.keys(botState.pendingSellOrders)]);
  for (const symbol of symbolsToCheck) { try { await checkPendingOrders(symbol); } catch (err) { logBotError(symbol, err); } }

  // نبدأ بأفضل 3 عملات مرشحة ثم باقي العملات
  const scanSymbols = explosionRanking.length ? explosionRanking.map(p => p.symbol) : SCAN_SYMBOLS;
  const candidates = [];
  for (const symbol of scanSymbols) {
    try {
      const sig = await evaluateBotSignal(symbol);
      if (!sig) continue;
      botState.lastSignals[symbol] = sig;
      const alreadyOpen = botState.positions[symbol] || botState.pendingOrders[symbol];
      if (!alreadyOpen && sig.action === 'buy' && sig.passesFilters) candidates.push(sig);
    } catch (err) { logBotError(symbol, err); }
  }

  const openCount = Object.keys(botState.positions).length + Object.keys(botState.pendingOrders).length;
  if (openCount < botState.maxConcurrentPositions && candidates.length) {
    candidates.sort((a, b) => a.price - b.price);
    const pick = candidates[0];
    await placeBotLimitBuy(pick);
  }

  broadcastBotStatus();
}

async function evaluateBotSignal(symbol) {
  const candles = candleStore[`${symbol}_${SCAN_INTERVAL}`];
  if (!candles || candles.length < 60) return null;
  const indicators = computeIndicatorsFixedReversal(symbol, SCAN_INTERVAL, candles);
  if (!indicators) return null;
  const decision = makeDecision(indicators);
  updateBotBtcLayer();
  const decision7Base = computeDecision7Score(indicators, decision);
  const decision7Signal = Math.max(-1, Math.min(1, decision7Base + computeBtcBoost()));
  const fourBoxSignal = computeFourBoxScore(indicators);
  const secondarySignal = computeSecondaryScore(indicators, decision);
  const frameSignal = computeFrameScore(symbol, indicators);
  const dashboardSignal = 0.30 * decision7Signal + 0.20 * secondarySignal + 0.45 * fourBoxSignal + 0.05 * frameSignal;
  const botOwnSignal = await computeBotOwnSignal(indicators, candles, symbol);
  const composite = 0.75 * dashboardSignal + 0.25 * botOwnSignal;
  let action = 'hold';
  if (composite >= BOT_BUY_THRESHOLD) action = 'buy';
  else if (composite <= BOT_SELL_THRESHOLD) action = 'sell';

  // ⭐ إشارة ارتداد قوية تفرض الشراء أو البيع
  let strongReversalUp = false;
  let strongReversalDown = false;
  if (indicators.stochRsi?.crossUp || indicators.williamsR?.crossUpFrom80 ||
      indicators.bbPercentB?.crossedUpFromZero || indicators.candleCompare?.bullEngulf ||
      indicators.rsiDivergence?.type === 'bullish') {
    strongReversalUp = true;
  }
  if (indicators.stochRsi?.crossDown || indicators.williamsR?.crossDownFrom20 ||
      indicators.bbPercentB?.crossedDownFromOne || indicators.candleCompare?.bearEngulf ||
      indicators.rsiDivergence?.type === 'bearish') {
    strongReversalDown = true;
  }

  if (strongReversalUp && action !== 'sell') {
    action = 'buy';
  } else if (strongReversalDown && action !== 'buy') {
    action = 'sell';
  }

  // فلتر الدخول: السماح بالشراء عند إشارة ارتداد قوية حتى لو لم تتحقق كل الشروط
  let filter = passesEntryFilters(candles);
  if (strongReversalUp && !filter.ok) {
    filter = { ok: true, reason: 'إشارة ارتداد قوية (تجاوز الفلاتر)' };
  } else if (strongReversalDown && !filter.ok) {
    filter = { ok: true, reason: 'إشارة انعكاس هبوطي قوية' };
  }

  const buyPoints = computeBuyPoints(candles, indicators, { action, passesFilters: filter.ok, price: indicators.currentPrice });

  return {
    symbol, decision7Signal, fourBoxSignal, secondarySignal, frameSignal, dashboardSignal, botOwnSignal, composite, action,
    price: indicators.currentPrice,
    buyZone: decision.buyZone, sellZone: decision.sellZone,
    passesFilters: filter.ok, filterReason: filter.reason,
    buyPoints,
    strongReversalUp,
    strongReversalDown,
  };
}

function botStatusPayload() {
  return JSON.stringify({
    type: 'bot_status', enabled: botState.enabled, exchange: botState.exchange,
    tradeSizeUsdt: botState.tradeSizeUsdt, takeProfitPercent: botState.takeProfitPercent,
    maxConcurrentPositions: botState.maxConcurrentPositions,
    positions: botState.positions, pendingOrders: botState.pendingOrders,
    pendingSellOrders: botState.pendingSellOrders, tradeLog: botState.tradeLog.slice(0, 20), lastSignals: botState.lastSignals,
  });
}
function broadcastBotStatus() {
  const payload = botStatusPayload();
  for (const client of wss.clients) {
    if (client.readyState === WebSocket.OPEN) client.send(payload);
  }
}

setInterval(runBotCycle, 30 * 1000); // كل 30 ثانية بدلاً من دقيقة

server.listen(PORT, () => console.log(`Crypto Dashboard running on port ${PORT}`));