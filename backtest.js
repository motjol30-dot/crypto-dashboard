'use strict';
/**
 * backtest.js — يختبر صيغة تسجيل "أقوى 3 عملات" على بيانات تاريخية حقيقية من Binance، عشان نتأكد
 * إن الدرجة العالية فعلاً بتتبعها حركة سعرية أقوى بعدين — بدل ما نثق بالأوزان اليدوية بدون دليل.
 *
 * ⚠️ لازم يشتغل على سيرفرك (Hostinger) أو جهازك، مو بيئة Claude — لأن بيئة Claude ما فيها اتصال
 * إنترنت. شغّله بـ:  node backtest.js
 * يحتاج نفس الحزم الموجودة عندك أصلاً بمشروع البوت: axios, technicalindicators
 *
 * وش يسوي:
 * 1. يجيب شموع 15 دقيقة تاريخية (افتراضيًا آخر 45 يوم) لعينة من العملات.
 * 2. يمشي على البيانات شمعة بشمعة (بخطوة كل ساعة تقريبًا لتقليل وقت التشغيل) ويحسب نفس صيغة
 *    التسجيل + تأكيد الساعة، بالضبط متل اللي بالسيرفر الحي.
 * 3. لكل نقطة، يشوف وش صار بالسعر بعدها بساعتين (8 شمعات) — أقصى حركة بنفس اتجاه الإشارة.
 * 4. يجمع النتائج بمجموعات حسب الدرجة (0-20 / 20-40 / ... / 80-100) ويطبع: عدد العينات، متوسط
 *    أقصى حركة، ونسبة "النجاح" (كم مرة تحرك السعر ≥2% بنفس الاتجاه خلال الساعتين).
 * 5. يقارن نفس الشي بس مقسوم حسب تأكيد الساعة (متوافق / متعارض / غير معروف) للتحقق من فايدة هذي
 *    الخطوة تحديدًا.
 *
 * كيف تقرأ النتيجة: لو الدرجات العالية (80-100) عندها متوسط حركة ونسبة نجاح أعلى بوضوح من الدرجات
 * الواطية (0-20)، فهذا دليل إن الصيغة شغالة فعلاً. لو الفرق بسيط أو معدوم، هذا دليل إن الأوزان
 * محتاجة تعديل — ابعت لي النتيجة وأساعدك تضبطها.
 */

const axios = require('axios');
const { RSI, BollingerBands, EMA } = require('technicalindicators');

// ── إعدادات قابلة للتعديل ────────────────────────────────────────────────────
const SAMPLE_SYMBOLS = [
  'BTCUSDT','ETHUSDT','SOLUSDT','XRPUSDT','ADAUSDT','AVAXUSDT','LINKUSDT','DOGEUSDT',
  'NEARUSDT','SUIUSDT','ARBUSDT','OPUSDT','INJUSDT','APTUSDT','TIAUSDT','SEIUSDT',
  'DOTUSDT','ATOMUSDT','LTCUSDT','ETCUSDT','FILUSDT','RUNEUSDT','WLDUSDT','JASMYUSDT','PEPEUSDT',
]; // عيّنة 25 عملة — كبّرها لو تبي تغطية أوسع، بس بياخذ وقت أطول
const LOOKBACK_DAYS = 45;
const STEP_CANDLES = 4;          // نقيّم كل 4 شمعات (ساعة) بدل كل شمعة — لتسريع التشغيل
const FORWARD_HORIZON = 8;       // نشوف الحركة خلال 8 شمعات قادمة (ساعتين)
const SUCCESS_MOVE_PCT = 2.0;    // "نجاح" = تحرك السعر بنفس الاتجاه ≥2% خلال الأفق الزمني
const MIN_AVG_QUOTE_VOLUME_15M = 15000; // نفس فلتر السيولة المستخدم بالسيرفر الحي
const HTF_AGREE_BONUS = 10;
const HTF_CONFLICT_PENALTY = 18;

// ── جلب البيانات التاريخية (مع تقسيم تلقائي لأن Binance يحدد 1000 شمعة بالطلب الواحد) ────────
async function fetchAllKlines(symbol, interval, days) {
  const msPerCandle = interval === '1h' ? 3600000 : 900000;
  const totalCandles = Math.ceil((days * 24 * 60 * 60 * 1000) / msPerCandle);
  const out = [];
  let endTime = Date.now();
  while (out.length < totalCandles) {
    const limit = Math.min(1000, totalCandles - out.length);
    const url = `https://api.binance.com/api/v3/klines?symbol=${symbol}&interval=${interval}&limit=${limit}&endTime=${endTime}`;
    const { data } = await axios.get(url, { timeout: 15000 });
    if (!data || !data.length) break;
    const batch = data.map(k => ({
      time: k[0], open: +k[1], high: +k[2], low: +k[3], close: +k[4], volume: +k[5],
    }));
    out.unshift(...batch);
    endTime = batch[0].time - 1;
    await new Promise(r => setTimeout(r, 150)); // تهدئة بسيطة لتفادي حد الطلبات
    if (data.length < limit) break;
  }
  return out;
}

// ── نفس صيغة التسجيل المستخدمة بالسيرفر الحي (منسوخة حرفيًا عشان الاختبار يعكس الواقع فعلًا) ──
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
  if (!Number.isFinite(avgQuoteVol50) || avgQuoteVol50 < MIN_AVG_QUOTE_VOLUME_15M) return null;

  return { score: Math.round(Math.max(0, Math.min(100, score))), direction, price };
}

function getHtfTrend(hourlyCandles, atIndex) {
  // atIndex = آخر فهرس شمعة ساعة متاحة حتى لحظة الإشارة (بدون تسريب بيانات مستقبلية)
  const closes = hourlyCandles.slice(0, atIndex + 1).map(c => c.close);
  if (closes.length < 55) return 'unknown';
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

function bucketLabel(score) {
  if (score < 20) return '0-20';
  if (score < 40) return '20-40';
  if (score < 60) return '40-60';
  if (score < 80) return '60-80';
  return '80-100';
}

async function backtestSymbol(symbol, stats) {
  const [candles15, candles1h] = await Promise.all([
    fetchAllKlines(symbol, '15m', LOOKBACK_DAYS),
    fetchAllKlines(symbol, '1h', LOOKBACK_DAYS + 5),
  ]);
  if (candles15.length < 200) { console.log(`  ⚠️ ${symbol}: بيانات غير كافية، تخطّي`); return; }

  for (let i = 100; i < candles15.length - FORWARD_HORIZON; i += STEP_CANDLES) {
    const window = candles15.slice(Math.max(0, i - 150), i + 1); // نفس منطق النافذة بالسيرفر
    const res = computeExplosionScore(window);
    if (!res || res.direction === 'neutral') continue;

    // نلقى فهرس أقرب شمعة ساعة ما تتعدى لحظة الإشارة (بدون تسريب مستقبل)
    const signalTime = candles15[i].time;
    let htfIdx = -1;
    for (let h = 0; h < candles1h.length; h++) { if (candles1h[h].time <= signalTime) htfIdx = h; else break; }
    const htfTrend = htfIdx >= 0 ? getHtfTrend(candles1h, htfIdx) : 'unknown';

    let adjScore = res.score;
    if (htfTrend !== 'unknown' && htfTrend !== 'neutral') {
      adjScore = htfTrend === res.direction
        ? Math.min(100, adjScore + HTF_AGREE_BONUS)
        : Math.max(0, adjScore - HTF_CONFLICT_PENALTY);
    }

    // الحركة القادمة خلال الأفق الزمني، بنفس اتجاه الإشارة
    const entryPrice = candles15[i].close;
    const future = candles15.slice(i + 1, i + 1 + FORWARD_HORIZON);
    if (future.length < FORWARD_HORIZON) continue;
    let maxMovePct;
    if (res.direction === 'up') {
      const maxHigh = Math.max(...future.map(c => c.high));
      maxMovePct = ((maxHigh - entryPrice) / entryPrice) * 100;
    } else {
      const minLow = Math.min(...future.map(c => c.low));
      maxMovePct = ((entryPrice - minLow) / entryPrice) * 100;
    }
    const success = maxMovePct >= SUCCESS_MOVE_PCT;

    const bucket = bucketLabel(adjScore);
    stats.byBucket[bucket] = stats.byBucket[bucket] || { count: 0, moveSum: 0, successCount: 0 };
    stats.byBucket[bucket].count++;
    stats.byBucket[bucket].moveSum += maxMovePct;
    if (success) stats.byBucket[bucket].successCount++;

    const htfKey = htfTrend === 'unknown' ? 'unknown' : (htfTrend === res.direction ? 'agree' : (htfTrend === 'neutral' ? 'neutral' : 'conflict'));
    stats.byHtf[htfKey] = stats.byHtf[htfKey] || { count: 0, moveSum: 0, successCount: 0 };
    stats.byHtf[htfKey].count++;
    stats.byHtf[htfKey].moveSum += maxMovePct;
    if (success) stats.byHtf[htfKey].successCount++;
  }
}

function printTable(title, groups, order) {
  console.log(`\n${title}`);
  console.log('─'.repeat(70));
  console.log(`${'المجموعة'.padEnd(14)}${'عدد العينات'.padEnd(14)}${'متوسط أقصى حركة%'.padEnd(20)}${'نسبة النجاح (≥' + SUCCESS_MOVE_PCT + '%)'}`);
  for (const key of order) {
    const g = groups[key];
    if (!g || !g.count) { console.log(`${key.padEnd(14)}لا توجد عينات`); continue; }
    const avgMove = (g.moveSum / g.count).toFixed(2);
    const successRate = ((g.successCount / g.count) * 100).toFixed(1);
    console.log(`${key.padEnd(14)}${String(g.count).padEnd(14)}${(avgMove + '%').padEnd(20)}${successRate}%`);
  }
}

(async () => {
  console.log(`🔬 بدء الاختبار التاريخي — ${SAMPLE_SYMBOLS.length} عملة، آخر ${LOOKBACK_DAYS} يوم...`);
  const stats = { byBucket: {}, byHtf: {} };
  for (const symbol of SAMPLE_SYMBOLS) {
    console.log(`  فحص ${symbol}...`);
    try { await backtestSymbol(symbol, stats); } catch (err) { console.log(`  ⚠️ ${symbol}: فشل (${err.message})`); }
  }
  printTable('📊 النتيجة حسب فئة الدرجة (بعد تعديل تأكيد الساعة)', stats.byBucket, ['0-20', '20-40', '40-60', '60-80', '80-100']);
  printTable('📊 النتيجة حسب تأكيد الساعة', stats.byHtf, ['agree', 'neutral', 'conflict', 'unknown']);
  console.log('\n✅ خلص. لو فئة 80-100 عندها متوسط حركة ونسبة نجاح أعلى بوضوح من 0-20، الصيغة شغالة.');
  console.log('   لو "agree" أفضل بوضوح من "conflict"، تأكيد الساعة فعلاً مفيد ويستاهل يضل مفعّل.');
})();
