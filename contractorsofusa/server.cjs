require('dotenv').config();
const express = require('express');
const axios = require('axios');
const qs = require('qs');
const fs = require('fs');
const path = require('path');
const cron = require('node-cron');
const nodemailer = require('nodemailer');
const app = express();
const PORT = process.env.PORT || 3018;

// Parse JSON form data
app.use(express.json());

const BUCKETS_DIR = path.join(__dirname, '_buckets');
if (!fs.existsSync(BUCKETS_DIR)) fs.mkdirSync(BUCKETS_DIR);

const BLOCKED_LEADS_DIR = path.join(__dirname, '_blocked_leads');
if (!fs.existsSync(BLOCKED_LEADS_DIR)) fs.mkdirSync(BLOCKED_LEADS_DIR);

function parseAiScore(str) {
  if (str == null || str === '') return NaN;
  const s = String(str).trim();
  const match = s.match(/^(\d+)(?:\s*\/\s*10)?$/);
  if (match) return parseInt(match[1], 10);
  const n = parseInt(s, 10);
  return isNaN(n) ? NaN : n;
}

function extractTrustedFormId(urlValue) {
  if (!urlValue) return '';
  const raw = String(urlValue).trim();

  if (/^[a-f0-9]{40}$/i.test(raw)) return raw;

  try {
    const u = new URL(raw);
    const parts = u.pathname.split('/').filter(Boolean);
    const last = parts[parts.length - 1] || '';
    if (/^[a-f0-9]{40}$/i.test(last)) return last;
  } catch (e) {
    // ignore URL parse errors; fallback regex handles raw strings
  }

  const m = raw.match(/([a-f0-9]{40})(?:[/?#]|$)/i);
  return m ? m[1] : '';
}

// --- BUCKET SYSTEM FUNCTIONS ---
function loadBuckets(formType) {
  const filePath = path.join(BUCKETS_DIR, `${formType}.json`);
  if (!fs.existsSync(filePath)) return {};
  try {
    const data = fs.readFileSync(filePath, 'utf8');
    return JSON.parse(data);
  } catch {
    const data = fs.readFileSync(filePath, 'utf8');
    const lines = data.split('\n').filter(Boolean);
    const buckets = {};
    for (const line of lines) {
      try {
        const obj = JSON.parse(line);
        if (obj.aff_id) buckets[obj.aff_id] = obj;
      } catch {}
    }
    fs.writeFileSync(filePath, JSON.stringify(buckets, null, 2));
    return buckets;
  }
}

function saveBuckets(formType, buckets) {
  const filePath = path.join(BUCKETS_DIR, `${formType}.json`);
  fs.writeFileSync(filePath, JSON.stringify(buckets, null, 2));
}

function getThresholds() {
  const thresholdsPath = path.join(BUCKETS_DIR, 'thresholds.json');
  try {
    const data = fs.readFileSync(thresholdsPath, 'utf8');
    return JSON.parse(data);
  } catch {
    return { default: 80 };
  }
}

/** Per-form bucket thresholds for affiliate 759220 (overrides thresholds.json for these formTypes only). */
const SPECIAL_AFFILIATE_BUCKET_THRESHOLDS = {
  '759220': {
    bathroom: 100,
    flooring: 120,
    windows: 84,
    window: 84,
    roofing: 120,
  },
};

function resolveBucketThreshold(aff_id, formType) {
  const thresholds = getThresholds();
  const affKey = String(aff_id ?? '');
  const byForm = SPECIAL_AFFILIATE_BUCKET_THRESHOLDS[affKey];
  if (byForm) {
    const ft = String(formType || '').toLowerCase();
    if (Object.prototype.hasOwnProperty.call(byForm, ft)) {
      return byForm[ft];
    }
  }
  return thresholds[aff_id] || thresholds[affKey] || thresholds.default || 80;
}

function addToBucket(aff_id, formType, price, postbackCallback) {
  const buckets = loadBuckets(formType);
  if (!buckets[aff_id]) buckets[aff_id] = { aff_id, total: 0 };
  buckets[aff_id].total += price;
  const threshold = resolveBucketThreshold(aff_id, formType);
  while (buckets[aff_id].total >= threshold) {
    postbackCallback(aff_id, formType, threshold);
    buckets[aff_id].total -= threshold;
  }
  saveBuckets(formType, buckets);
}

function resetAllBuckets() {
  const files = fs.readdirSync(BUCKETS_DIR).filter(f => f.endsWith('.json'));
  for (const file of files) {
    const filePath = path.join(BUCKETS_DIR, file);
    try {
      const buckets = JSON.parse(fs.readFileSync(filePath, 'utf8'));
      for (const key in buckets) {
        buckets[key].total = 0;
      }
      fs.writeFileSync(filePath, JSON.stringify(buckets, null, 2));
    } catch {
      // Handle old format or error
    }
  }
}

cron.schedule('0 0 1 * *', () => {
  console.log('[CRON] Starting monthly bucket reset...');
  resetAllBuckets();
  console.log('[CRON] Monthly bucket reset completed.');
}, {
  timezone: 'UTC'
});

// --- CORS CONFIGURATION ---
const allowedOrigins = [
  'https://getcontractorpros.com',
  'https://www.getcontractorpros.com',
  'https://contractorsofusa.com',
  'https://www.contractorsofusa.com',
  'http://localhost:4200',
  'http://127.0.0.1:4200',
  'http://localhost:5173',
  'http://127.0.0.1:5173',
];

app.use((req, res, next) => {
  const origin = req.headers.origin;
  if (!origin || allowedOrigins.includes(origin)) {
    res.header('Access-Control-Allow-Origin', origin || '*');
  }
  res.header('Access-Control-Allow-Headers', 'Origin, X-Requested-With, Content-Type, Accept');
  res.header('Access-Control-Allow-Methods', 'GET, POST, PUT, DELETE, OPTIONS');

  if (req.method === 'OPTIONS') {
    return res.sendStatus(200);
  }
  next();
});

// --- ROUTES ---
app.get('/server', (req, res) => {
  res.send('Express server is running 🚀');
});

const UNSUB_DIR = path.join(__dirname, '_unsubscribes');
if (!fs.existsSync(UNSUB_DIR)) fs.mkdirSync(UNSUB_DIR);
const UNSUB_EMAILS_PATH = path.join(UNSUB_DIR, 'emails.txt');
if (!fs.existsSync(UNSUB_EMAILS_PATH)) fs.writeFileSync(UNSUB_EMAILS_PATH, '', 'utf8');
const unsubscribedEmailSet = new Set(
  fs
    .readFileSync(UNSUB_EMAILS_PATH, 'utf8')
    .split(/\r?\n/)
    .map((s) => s.trim().toLowerCase())
    .filter(Boolean)
);

function normalizeEmail(email) {
  return (email || '').toString().trim().toLowerCase();
}

function isValidEmail(email) {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email);
}

app.post('/server/unsubscribe', (req, res) => {
  try {
    const rawEmail = req.body?.email;
    const email = normalizeEmail(rawEmail);

    if (!email || !isValidEmail(email)) {
      return res.status(400).json({ ok: false, error: 'Valid email is required.' });
    }

    const now = new Date();
    const yearMonth = now.toISOString().slice(0, 7);
    const day = now.toISOString().slice(0, 10);
    const logDir = path.join(UNSUB_DIR, yearMonth);
    if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });

    const wasAlreadyUnsubscribed = unsubscribedEmailSet.has(email);
    if (!wasAlreadyUnsubscribed) {
      fs.appendFileSync(UNSUB_EMAILS_PATH, email + '\n');
      unsubscribedEmailSet.add(email);
    }

    const ip = req.headers['x-forwarded-for']?.split(',')[0]?.trim() || req.socket.remoteAddress || '';
    const entry = {
      ts: now.toISOString(),
      email,
      status: wasAlreadyUnsubscribed ? 'already_exists' : 'added',
      ip,
      userAgent: req.headers['user-agent'] || '',
      sourceUrl: req.body?.url || '',
    };
    fs.appendFileSync(path.join(logDir, `${day}.jsonl`), JSON.stringify(entry) + '\n');

    return res.json({ ok: true, status: entry.status });
  } catch (err) {
    console.error('Unsubscribe error:', err.message);
    return res.status(500).json({ ok: false, error: 'Failed to save unsubscribe request.' });
  }
});

const FORM_IDS = {
  bathroom: { vid: '52', leadtype: '2', category: '2' },
  roofing: { vid: '47', leadtype: '2', category: '15' },
  windows: { vid: '48', leadtype: '2', category: '18' },
  solar: { vid: '49', leadtype: '2', category: '20' },
  hvac: { vid: '50', leadtype: '2', category: '19' },
  painting: { vid: '53', leadtype: '2', category: '13' },
  gutters: { vid: '57', leadtype: '2', category: '22' },
  plumbing: { vid: '58', leadtype: '2', category: '14' },
  kitchen: { vid: '51', leadtype: '2', category: '11' },
  flooring: { vid: '56', leadtype: '2', category: '9' },
  home_security: { vid: '60', leadtype: '2', category: '23' },
  siding: { vid: '55', leadtype: '2', category: '16' },
  fencing: { vid: '54', leadtype: '2', category: '8' },
  movers: { vid: '59', leadtype: '2', category: '21' },
  auto_insurance: { vid: '61', leadtype: '2', category: '25' },
};

// Replace these with live URLs when fraud-api, CRM, and Offer18 are available.
const FRAUD_API_URL = process.env.FRAUD_API_URL || 'https://example.com/evaluate-fraud';
const CRM_URL = process.env.CRM_URL || 'https://example.com/crm';
const OFFER18_POSTBACK_URL = process.env.OFFER18_POSTBACK_URL || 'https://example.com/offer18/p?tid={tid}&payout={payout}';
const TEST_LEAD_EMAIL = process.env.TEST_LEAD_EMAIL || 'theleadgrip@gmail.com';
const GMAIL_USER = process.env.GMAIL_USER || TEST_LEAD_EMAIL;
const GMAIL_APP_PASSWORD = process.env.GMAIL_APP_PASSWORD || '';

function isPlaceholderUrl(url) {
  return !url || /example\.com/i.test(String(url));
}

function getMailer() {
  if (!GMAIL_APP_PASSWORD) return null;
  return nodemailer.createTransport({
    service: 'gmail',
    auth: { user: GMAIL_USER, pass: GMAIL_APP_PASSWORD },
  });
}

function escapeHtml(value) {
  return String(value ?? '')
    .replace(/&/g, '&amp;')
    .replace(/</g, '&lt;')
    .replace(/>/g, '&gt;')
    .replace(/"/g, '&quot;');
}

async function sendLeadToGmail(formFields) {
  const rows = Object.keys(formFields)
    .filter((k) => formFields[k] !== '' && formFields[k] != null && typeof formFields[k] !== 'object')
    .map((k) => `<tr><td style="padding:6px 10px;border-bottom:1px solid #eee;font-weight:600">${escapeHtml(k)}</td><td style="padding:6px 10px;border-bottom:1px solid #eee">${escapeHtml(formFields[k])}</td></tr>`)
    .join('');

  const html = `
    <h2>New GetContractorPros lead</h2>
    <p>Form: <b>${escapeHtml(formFields.formType)}</b> &nbsp;|&nbsp; Email: <b>${escapeHtml(formFields.email)}</b></p>
    <table cellpadding="0" cellspacing="0" style="border-collapse:collapse;font-family:Arial,sans-serif;font-size:14px">${rows}</table>
    <p style="color:#888;font-size:12px">Test delivery (CRM placeholder). Swap CRM_URL when the live CRM is ready.</p>
  `;

  const mailer = getMailer();
  if (!mailer) {
    throw new Error('GMAIL_APP_PASSWORD is not set in .env — cannot send test lead email.');
  }

  await mailer.sendMail({
    from: `"GetContractorPros Leads" <${GMAIL_USER}>`,
    to: TEST_LEAD_EMAIL,
    subject: `New ${formFields.formType || 'home'} lead — ${formFields.firstName || ''} ${formFields.lastName || ''}`.trim(),
    html,
    text: JSON.stringify(formFields, null, 2),
  });
}

function fireOffer18Postback(formFields, formType, accepted, price) {
  const { transaction_id, aff_id } = formFields;
  if (!transaction_id || !aff_id) {
    console.log('[lead-async] No transaction_id/aff_id — skipping Offer18 bucket/postback.');
    return;
  }

  addToBucket(aff_id, formType, accepted ? price : 0, (id, type, total) => {
    const pbUrl = OFFER18_POSTBACK_URL
      .replace(/{tid}/g, transaction_id)
      .replace(/{payout}/g, total);

    if (isPlaceholderUrl(OFFER18_POSTBACK_URL)) {
      console.log('[lead-async] Offer18 placeholder postback (not fired):', pbUrl);
      return;
    }
    axios.get(pbUrl).catch((e) => console.error('[lead-async] Postback Error', e.message));
  });
}

/** Geo → (fraud later) → Gmail test CRM / live CRM. Runs after HTTP ack. */
async function processLeadInBackground(rawBody, ip, userAgent, remoteAddress) {
  const body = { ...rawBody };
  const token = 'b4fdc3a23aa22b';
  const formType = body.formType;
  const ids = FORM_IDS[formType];
  if (!ids) {
    console.error('[lead-async] Unknown formType:', formType);
    return;
  }

  let geoData = {};
  try {
    const geoRes = await axios.get(`https://ipinfo.io/${ip}?token=${token}`, { timeout: 10000 });
    geoData = geoRes.data;

    const now = new Date();
    const yearMonth = now.toISOString().slice(0, 7);
    const day = now.toISOString().slice(0, 10);
    const logDir = path.join(__dirname, '_ipcheck', yearMonth);
    if (!fs.existsSync(logDir)) fs.mkdirSync(logDir, { recursive: true });

    const logEntry = { email: body.email, phone: body.phone, zip: body.zip, ip, aff_id: body.aff_id, ipinfo: geoData };
    fs.appendFileSync(path.join(logDir, `${day}.jsonl`), JSON.stringify(logEntry) + '\n');

    const isLocal = ['::1', '127.0.0.1', '::ffff:127.0.0.1'].includes((remoteAddress || '').trim());
    if (geoData.country && geoData.country !== 'US' && !isLocal) {
      console.log('[lead-async] Non-US lead skipped for CRM:', body.email, geoData.country);
      return;
    }

    body.ip_city = geoData.city || '';
    body.ip_region = geoData.region || '';
    body.ip_country = geoData.country || '';
    body.ip_zip = geoData.postal || '';
  } catch (geoErr) {
    console.error('[lead-async] Geo error:', geoErr.message);
  }

  let fraudScore = 0;
  let fraudFlags = {};
  let fraudRes;

  if (isPlaceholderUrl(FRAUD_API_URL)) {
    console.log('[lead-async] Fraud/Gemini API not configured yet — skipping. Placeholder:', FRAUD_API_URL);
  } else {
    try {
      fraudRes = await axios.post(FRAUD_API_URL, {
        ip,
        geoData,
        body,
        userAgent: userAgent || ''
      }, { timeout: 150000, validateStatus: () => true });
      if (fraudRes.data && typeof fraudRes.data.score === 'number') fraudScore = fraudRes.data.score;
      if (fraudRes.data && fraudRes.data.flags) fraudFlags = fraudRes.data.flags;
    } catch (e) {
      console.error('[lead-async] Fraud API error:', e.message);
    }
  }
  const fraudRiskLevel = fraudScore >= 100 ? 'Critical' : fraudScore >= 70 ? 'High' : fraudScore >= 30 ? 'Medium' : 'Low';

  const formFields = { ...body };
  try {
    const raw = formFields.fbclid_present;
    formFields.fbclid_present =
      raw === 1 || raw === '1' || raw === true || raw === 'true' ? 1 : 0;
    delete formFields.fbclid;
  } catch (_) {
    formFields.fbclid_present = 0;
  }

  const tfUrl =
    formFields.xxTrustedFormCertUrl ||
    formFields.trusted_form_url ||
    formFields.trustedFormUrl ||
    formFields.TrustedForm ||
    '';
  const tfId = extractTrustedFormId(tfUrl);
  formFields.trusted_form_cert_id = tfId;
  formFields.trusted_form_id = tfId;

  delete formFields.sessionDurationMs;
  delete formFields.mouseStats;
  delete formFields.typingStats;
  delete formFields.canvasId;
  delete formFields.deviceFp;
  delete formFields.hardwareInfo;
  if (!formFields.ipaddress) formFields.ipaddress = ip;
  formFields.fraud_score = fraudScore;
  formFields.fraud_risk_level = fraudRiskLevel;
  formFields.fraud_tor_exit = fraudFlags.tor_exit ? 1 : 0;
  formFields.fraud_datacenter_ip = fraudFlags.datacenter_ip ? 1 : 0;
  formFields.fraud_ua_mismatch = fraudFlags.ua_hw_mismatch ? 1 : 0;
  formFields.fraud_instant_typing = fraudFlags.instant_typing ? 1 : 0;
  formFields.fraud_canvas_reuse = fraudFlags.canvas_reuse ? 1 : 0;
  formFields.fraud_session_too_short = fraudFlags.session_too_short ? 1 : 0;
  formFields.fraud_low_quality_description = fraudFlags.low_quality_description ? 1 : 0;

  const ai = fraudRes && fraudRes.data && fraudRes.data.ai ? fraudRes.data.ai : {};
  Object.keys(ai).forEach((k) => {
    if (k.startsWith('ai_')) formFields[k] = ai[k] ?? '';
  });

  Object.assign(formFields, {
    vid: ids.vid,
    leadtype: ids.leadtype,
    category: ids.category,
    exclusive: '2',
    tcpaConsent: '1'
  });

  let price = 0;
  let accepted = false;

  if (isPlaceholderUrl(CRM_URL)) {
    try {
      await sendLeadToGmail(formFields);
      accepted = true;
      price = 0;
      console.log('[lead-async] Test lead emailed to', TEST_LEAD_EMAIL, 'for', body.email);
    } catch (mailErr) {
      console.error('[lead-async] Gmail send failed:', mailErr.message);
    }
  } else {
    const response = await axios.post(CRM_URL, qs.stringify(formFields), {
      headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
      timeout: 150000,
      validateStatus: () => true
    });

    const text = response.data;

    try {
      const priceMatch = String(text).match(/<([\w:]+)?Price>(.*?)<\/(?:[\w:]+)?Price>/i);
      if (priceMatch && priceMatch[2]) price = parseFloat(priceMatch[2]);

      const acceptedMatch = String(text).match(/<([\w:]+)?Response>(.*?)<\/(?:[\w:]+)?Response>/i);
      if (acceptedMatch && acceptedMatch[2] && acceptedMatch[2].toLowerCase().includes('accepted')) {
        accepted = true;
      }
    } catch (e) {
      console.warn('[lead-async] XML Parse Error');
    }
  }

  fireOffer18Postback(formFields, formType, accepted, price);
  console.log('[lead-async] Lead complete:', body.email, accepted ? 'accepted' : 'not accepted');
}

app.get('/server/page-session', (req, res) => {
  return res.status(200).json({ ok: false, error: 'unavailable' });
});

app.post('/server/forward-lead', (req, res) => {
  console.log('Lead POST received!');

  try {
    const formType = req.body && req.body.formType;
    if (!formType || !FORM_IDS[formType]) {
      return res.status(400).json({ error: 'Unknown formType' });
    }

    const ip = req.body.ipaddress || req.headers['x-forwarded-for']?.split(',')[0] || req.socket.remoteAddress;
    const payload = { ...req.body };
    const userAgent = req.headers['user-agent'] || '';
    const remoteAddress = req.socket.remoteAddress || '';

    res.status(200).json({ message: 'Thank you!', received: true });

    processLeadInBackground(payload, ip, userAgent, remoteAddress).catch((err) => {
      console.error('[lead-async] processing failed:', err.message);
    });
  } catch (err) {
    console.error('Forward-lead handler error:', err.message);
    res.status(500).json({ error: 'Failed to accept lead.' });
  }
});

app.get('/', (req, res) => res.send('Express server is running 🚀'));

app.listen(PORT, '0.0.0.0', () => {
  console.log(`Backend proxy listening on :${PORT}`);
  if (!GMAIL_APP_PASSWORD) {
    console.warn('[lead] Set GMAIL_APP_PASSWORD in .env (Google App Password) so test leads can be emailed.');
  } else {
    console.log('[lead] Test CRM = Gmail →', TEST_LEAD_EMAIL);
  }
});
