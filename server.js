// ── Decision Engine ───────────────────────────────────────────────────────────

function makeDecision(indicators) {
  if (!indicators) return null;
  const { 
    rsi, macd, bb, ema50, ema200, currentPrice,
    stochRsi, bbPercentB, rsiDivergence, williamsR, // مؤشرات الارتداد (ثابتة على 15 دقيقة)
    chop, adx, supertrend,                         // مؤشرات ثبات الاتجاه
    vwap, accDist, cvd                             // مؤشرات السيولة والحجم
  } = indicators;
  
  const notes = [];
  let bull = 0, bear = 0;

  // 1. طبقة الزخم والاتجاه (Momentum & Trend)
  let momentumScore = 0;
  if (rsi != null) {
    if (rsi < 35) { bull += 2; momentumScore += 1; notes.push(`RSI تشبع بيعي (فرصة صعود)`); }
    else if (rsi > 65) { bear += 2; momentumScore -= 1; notes.push(`RSI تشبع شرائي (فرصة هبوط)`); }
  }
  if (macd) {
    if (macd.value > macd.signal) { bull += 1.5; momentumScore += 1; }
    else { bear += 1.5; momentumScore -= 1; }
  }
  if (ema50 && ema200) {
    if (currentPrice > ema50) { bull += 1; } else { bear += 1; }
    if (ema50 > ema200) { bull += 1; momentumScore += 1; } 
    else { bear += 1; momentumScore -= 1; }
  }
  
  // 2. طبقة الارتداد (Reversal - ثابتة دائماً على 15 دقيقة كما طلبت)
  let reversalScore = 0;
  if (stochRsi) {
    if (stochRsi.crossUp || stochRsi.zoneUp) { bull += 2; reversalScore += 1; notes.push(`StochRSI إيجابي (15د)`); }
    else if (stochRsi.crossDown || stochRsi.zoneDown) { bear += 2; reversalScore -= 1; notes.push(`StochRSI سلبي (15د)`); }
  }
  if (bbPercentB) {
    if (bbPercentB.crossedUpFromZero || bbPercentB.zoneUp) { bull += 1.5; reversalScore += 1; }
    else if (bbPercentB.crossedDownFromOne || bbPercentB.zoneDown) { bear += 1.5; reversalScore -= 1; }
  }
  if (rsiDivergence) {
    if (rsiDivergence.type === 'bullish') { bull += 2.5; reversalScore += 2; notes.push(`دايفرجنز إيجابي (15د) - امتصاص شرائي`); }
    else if (rsiDivergence.type === 'bearish') { bear += 2.5; reversalScore -= 2; notes.push(`دايفرجنز سلبي (15د) - تصريف`); }
  }
  if (williamsR) {
    if (williamsR.crossUpFrom80 || williamsR.zoneUp) { bull += 1; reversalScore += 1; }
    else if (williamsR.crossDownFrom20 || williamsR.zoneDown) { bear += 1; reversalScore -= 1; }
  }

  // 3. طبقة ثبات الاتجاه (Stability)
  let stabilityScore = 0;
  if (chop != null) {
    if (chop < 38.2) { 
        // الاتجاه قوي وغير متذبذب، نضيف وزن للاتجاه الحالي
        if (momentumScore > 0) { bull += 1.5; stabilityScore += 1; }
        else if (momentumScore < 0) { bear += 1.5; stabilityScore -= 1; }
    }
  }
  if (adx && adx.adx > 25) {
    if (adx.pdi > adx.mdi) { bull += 1.5; stabilityScore += 1; }
    else { bear += 1.5; stabilityScore -= 1; }
  }
  if (supertrend) {
    if (supertrend.trendUp) { bull += 1; stabilityScore += 1; }
    else { bear += 1; stabilityScore -= 1; }
  }

  // 4. طبقة السيولة والحجم (Volume)
  let volumeScore = 0;
  if (vwap && currentPrice > vwap) { bull += 1; volumeScore += 1; }
  else if (vwap) { bear += 1; volumeScore -= 1; }
  
  if (accDist) {
    if (accDist.rising) { bull += 1.5; volumeScore += 1; }
    else { bear += 1.5; volumeScore -= 1; }
  }
  if (cvd) {
    if (cvd.signal === 'bullish_divergence' || cvd.cvdDir === 'up') { bull += 2; volumeScore += 1; notes.push(`CVD إيجابي - سيولة شراء حقيقية تدخل`);}
    else if (cvd.signal === 'bearish_divergence' || cvd.cvdDir === 'down') { bear += 2; volumeScore -= 1; notes.push(`CVD سلبي - سيولة بيع حقيقية`); }
  }

  // حساب التقييم النهائي
  const totalWeight = bull + bear;
  const rawScore = totalWeight > 0 ? (bull / totalWeight) * 100 : 50;
  const confidence = Math.round(rawScore);

  // تحديد الاتجاه العام بناءً على توافق الزخم والثبات
  let trend = 'تذبذب';
  if (momentumScore >= 1 && stabilityScore >= 0) trend = 'صعود';
  else if (momentumScore <= -1 && stabilityScore <= 0) trend = 'هبوط';

  // 🎯 قرار نهائي مترابط (يجب أن تتفق الطبقات)
  let action = 'wait';
  
  // شروط الشراء القوية (توافق الزخم + تأكيد من الارتداد الثابت + سيولة داعمة)
  if (confidence >= 65 && reversalScore > 0 && volumeScore >= 0 && momentumScore > 0) {
      action = 'buy zone';
  } 
  // شروط البيع القوية (سلبية الزخم + تأكيد سلبية الارتداد + خروج سيولة)
  else if (confidence <= 35 && reversalScore < 0 && volumeScore <= 0 && momentumScore < 0) {
      action = 'sell zone';
  }
  // إشارات قصوى (حتى لو اختلفت طبقة، قوة الإجماع تجبر القرار)
  else if (confidence >= 80) { action = 'buy zone'; } 
  else if (confidence <= 20) { action = 'sell zone'; }

  // حساب مناطق الدخول/الخروج المقترحة بناءً على البولينجر
  const buyZone  = bb ? { from: bb.lower.toFixed(4), to: ((bb.lower + bb.middle) / 2).toFixed(4) } : null;
  const sellZone = bb ? { from: ((bb.upper + bb.middle) / 2).toFixed(4), to: bb.upper.toFixed(4) } : null;

  return { 
    trend, 
    action, 
    confidence, 
    notes, 
    buyZone, 
    sellZone,
    // إرسال نتيجة إجماع كل طبقة للواجهة لتحديث المربعات (Pills)
    consensus: {
      momentum: momentumScore,
      reversal: reversalScore,
      stability: stabilityScore,
      volume: volumeScore
    }
  };
}

module.exports = { makeDecision }; // Adjust exports as per your setup
