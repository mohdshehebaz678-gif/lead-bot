require('dotenv').config();
const express = require('express');
const axios = require('axios');
const { MongoClient, ObjectId } = require('mongodb');
const { google } = require('googleapis');

const app = express();
app.use(express.json());

// ==================== CONFIG ====================
const CONFIG = {
  TOKEN: process.env.BOT_TOKEN,
  SHEET_ID: process.env.GOOGLE_SHEET_ID,
  LEADS_SHEET_NAME: 'Sheet1',
  STAFF_SHEET_NAME: 'STAFF NAME',
  DAILING_COUNT_SHEET: 'DAILING COUNT',
  LEAD_COLS: {
    NAME: 0, MOBILE: 1, REG_NO: 2, EXPIRED: 3, MAKE: 4, REMARK: 5,
    STAFF_NAME: 6, STATUS: 7, REVIEW: 8, DATE: 9,
    SENT_TIME: 10, DONE_TIME: 11, COUNT_DIALER: 12, BOT_RESPONSE: 13
  },
  STAFF_COLS: {
    USER_NAME: 0, STAFF_NAME: 1, STAFF_NO: 2, ACTIVE_STATUS: 3,
    EMAIL: 4, GENDER: 5, ID_CREATE: 6, CHAT_ID: 7, MAIL: 8
  }
};

// ==================== IST TIMEZONE ====================
const IST_OFFSET_MS = 5.5 * 60 * 60 * 1000;

function toIST(d = new Date()) {
  return new Date(d.getTime() + IST_OFFSET_MS);
}

// ==================== SPEED CACHE ====================
const speedCache = {
  staffByChatId: new Map(),
  staffByUserName: new Map(),
  leads: new Map(),
  rowMap: new Map(),
  sheetData: null,
  sheetDataTime: 0,
  locksCache: null,
  locksCacheTime: 0,

  getStaffByChatId(chatId) {
    return this.staffByChatId.get(chatId);
  },

  getStaffByUserName(userName) {
    return this.staffByUserName.get(userName.toLowerCase());
  },

  setStaff(staff) {
    if (staff.chatId) this.staffByChatId.set(staff.chatId, staff);
    if (staff.userName) this.staffByUserName.set(staff.userName.toLowerCase(), staff);
  },

  clearStaffCache() {
    this.staffByChatId.clear();
    this.staffByUserName.clear();
  },

  invalidateLeads() {
    this.leads.clear();
    this.rowMap.clear();
    this.sheetData = null;
    this.sheetDataTime = 0;
  },

  getSheetData() {
    const now = Date.now();
    if (this.sheetData && (now - this.sheetDataTime) < 3000) {
      return this.sheetData;
    }
    return null;
  },

  setSheetData(data) {
    this.sheetData = data;
    this.sheetDataTime = Date.now();
  },

  getLocks() {
    const now = Date.now();
    if (this.locksCache && (now - this.locksCacheTime) < 3000) {
      return this.locksCache;
    }
    return null;
  },

  setLocks(locks) {
    this.locksCache = locks;
    this.locksCacheTime = Date.now();
  }
};

// ==================== MONGODB ====================
const mongoUri = process.env.MONGODB_URI;
let db, staffsCollection, remindersCollection, tempLocksCollection, statsCollection, activeAssignmentsCollection, pendingSheetUpdatesCollection;

async function connectMongo() {
  const client = new MongoClient(mongoUri, { 
    maxPoolSize: 20,
    serverSelectionTimeoutMS: 5000
  });
  await client.connect();
  db = client.db(process.env.DB_NAME || 'leadbot_db');
  staffsCollection = db.collection('staffs');
  remindersCollection = db.collection('reminders');
  tempLocksCollection = db.collection('tempLocks');
  statsCollection = db.collection('staff_stats');
  activeAssignmentsCollection = db.collection('activeAssignments');
  pendingSheetUpdatesCollection = db.collection('pendingSheetUpdates');

  // Indexes
  await remindersCollection.createIndex({ fireAt: 1 }).catch(() => {});
  await tempLocksCollection.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }).catch(() => {});
  await tempLocksCollection.createIndex({ regNo: 1 }).catch(() => {});
  await staffsCollection.createIndex({ userName: 1 }).catch(() => {});
  await staffsCollection.createIndex({ chatId: 1 }).catch(() => {});
  await statsCollection.createIndex({ staffName: 1, date: 1 }).catch(() => {});
  await activeAssignmentsCollection.createIndex({ regNo: 1 }, { unique: true }).catch(() => {});
  await activeAssignmentsCollection.createIndex({ chatId: 1 }).catch(() => {});
  await activeAssignmentsCollection.createIndex({ staffName: 1 }).catch(() => {});
  await pendingSheetUpdatesCollection.createIndex({ createdAt: 1 }).catch(() => {});
  await pendingSheetUpdatesCollection.createIndex({ regNo: 1 }).catch(() => {});

  console.log('✅ MongoDB connected');
  await syncStaffFromSheet();
}

// ==================== STATS ====================
async function incrementStat(staffName, field) {
  const today = formatDate(new Date());
  try {
    await statsCollection.updateOne(
      { staffName, date: today },
      { $inc: { [field]: 1 }, $setOnInsert: { createdAt: new Date() } },
      { upsert: true }
    );
  } catch (e) {
    console.error('Stats error:', e.message);
  }
}

// ==================== GOOGLE SHEETS ====================
const auth = new google.auth.GoogleAuth({
  credentials: JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT_JSON),
  scopes: ['https://www.googleapis.com/auth/spreadsheets']
});
const sheets = google.sheets({ version: 'v4', auth });

// ==================== FORMAT HELPERS (IST) ====================
function safeStr(val) {
  return val == null ? '' : String(val).trim();
}

function formatDate(d = new Date()) {
  const ist = toIST(d);
  const months = ['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'];
  const day = String(ist.getUTCDate()).padStart(2, '0');
  const month = months[ist.getUTCMonth()];
  const year = ist.getUTCFullYear();
  return `${day}-${month}-${year}`;
}

function formatTime(d = new Date()) {
  const ist = toIST(d);
  let hours = ist.getUTCHours();
  const minutes = String(ist.getUTCMinutes()).padStart(2, '0');
  const seconds = String(ist.getUTCSeconds()).padStart(2, '0');
  const ampm = hours >= 12 ? 'PM' : 'AM';
  hours = hours % 12;
  hours = hours ? hours : 12;
  hours = String(hours).padStart(2, '0');
  return `${hours}:${minutes}:${seconds} ${ampm}`;
}

function formatDateTime(d = new Date()) {
  return `${formatDate(d)} ${formatTime(d)}`;
}

function formatDuration(ms) {
  if (ms <= 0) return '00:00:00';
  const totalSeconds = Math.floor(ms / 1000);
  const hrs = String(Math.floor(totalSeconds / 3600)).padStart(2, '0');
  const mins = String(Math.floor((totalSeconds % 3600) / 60)).padStart(2, '0');
  const secs = String(totalSeconds % 60).padStart(2, '0');
  return `${hrs}:${mins}:${secs}`;
}

function parseSheetDate(val) {
  if (!val) return new Date(9999, 0, 1);
  const months = { jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5, jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11 };
  const m = String(val).match(/(\d{1,2})-([a-zA-Z]{3})-(\d{4})/);
  if (m) {
    const d = new Date(parseInt(m[3]), months[m[2].toLowerCase()], parseInt(m[1]));
    return isNaN(d) ? new Date(9999, 0, 1) : d;
  }
  const d = new Date(val);
  return isNaN(d) ? new Date(9999, 0, 1) : d;
}

// ==================== TELEGRAM API (INSTANT) ====================
async function sendMessage(chatId, text, replyMarkup = null, removeKeyboard = false) {
  const payload = {
    chat_id: chatId,
    text: text,
    parse_mode: 'Markdown',
    disable_web_page_preview: true
  };
  if (removeKeyboard) payload.reply_markup = JSON.stringify({ remove_keyboard: true });
  else if (replyMarkup) payload.reply_markup = JSON.stringify(replyMarkup);

  try {
    return await axios.post(`https://api.telegram.org/bot${CONFIG.TOKEN}/sendMessage`, payload, { timeout: 10000 });
  } catch (e) {
    console.error('sendMessage error:', e.message);
    throw e;
  }
}

async function editMessage(chatId, messageId, text, replyMarkup = null) {
  const payload = { chat_id: chatId, message_id: messageId, text: text, parse_mode: 'Markdown' };
  if (replyMarkup) payload.reply_markup = JSON.stringify(replyMarkup);
  try {
    return await axios.post(`https://api.telegram.org/bot${CONFIG.TOKEN}/editMessageText`, payload, { timeout: 10000 });
  } catch (e) {
    console.error('editMessage error:', e.message);
    throw e;
  }
}

async function answerCallbackQuery(queryId) {
  try {
    await axios.post(`https://api.telegram.org/bot${CONFIG.TOKEN}/answerCallbackQuery`, { callback_query_id: queryId }, { timeout: 5000 });
  } catch (e) {
    console.error('answerCallbackQuery error:', e.message);
  }
}

async function deleteMessage(chatId, messageId) {
  try {
    await axios.post(`https://api.telegram.org/bot${CONFIG.TOKEN}/deleteMessage`, { chat_id: chatId, message_id: messageId }, { timeout: 5000 });
  } catch (e) {
    console.error('deleteMessage error:', e.message);
  }
}

// ==================== BUTTONS & MESSAGES ====================
function getMainButtons() {
  return { keyboard: [[{ text: '▶️ START LEAD' }], [{ text: '📊 MY STATUS' }]], resize_keyboard: true, one_time_keyboard: false };
}

function getLeadButtons(regNo, showSkip) {
  const b = [
    [{ text: '📞 CALL', callback_data: `CALL_${regNo}` }, { text: '💬 WHATSAPP', callback_data: `WHATSAPP_${regNo}` }],
    [{ text: '🔍 REVIEW', callback_data: `REVIEW_${regNo}` }, { text: '✅ DONE', callback_data: `DONE_${regNo}` }]
  ];
  if (showSkip) b.push([{ text: '⏭️ SKIP', callback_data: `SKIP_${regNo}` }]);
  return { inline_keyboard: b };
}

function getReviewButtons(regNo) {
  return {
    inline_keyboard: [
      [{ text: '📞 RINGING', callback_data: `RINGING_${regNo}` }, { text: '❌ NOT CONNECTED', callback_data: `NOTCONN_${regNo}` }],
      [{ text: '📍 OUT OF AREA', callback_data: `OUTAREA_${regNo}` }, { text: '🔴 BUSY', callback_data: `BUSY_${regNo}` }],
      [{ text: '✏️ OTHER', callback_data: `OTHER_${regNo}` }]
    ]
  };
}

function getLeadMsg(rowData) {
  const nm = rowData[CONFIG.LEAD_COLS.NAME];
  const mb = rowData[CONFIG.LEAD_COLS.MOBILE];
  const rn = rowData[CONFIG.LEAD_COLS.REG_NO];
  const d2 = rowData[CONFIG.LEAD_COLS.EXPIRED] || rowData[CONFIG.LEAD_COLS.DATE];
  const rm = rowData[CONFIG.LEAD_COLS.REMARK];
  const mk = rowData[CONFIG.LEAD_COLS.MAKE];
  const sa = safeStr(rowData[CONFIG.LEAD_COLS.STAFF_NAME]);
  const st = safeStr(rowData[CONFIG.LEAD_COLS.STATUS]);
  const botResp = safeStr(rowData[CONFIG.LEAD_COLS.BOT_RESPONSE]);

  let ds = safeStr(d2);
  const re = safeStr(rm).toUpperCase() === 'EXPIRE' ? '🔴 EXPIRE' : '🟢 NEW';

  let msg = '\u2705 NEW LEAD' + '\n\n';
  msg += '👤 *Name:* ' + (nm || '') + '\n';
  msg += '📱 *Mobile:* ' + (mb || '') + '\n';
  msg += '🚗 *Reg:* ' + (rn || '') + '\n';
  msg += '📅 *Date:* ' + ds + '\n';
  msg += re + '\n';
  msg += '🏭 *Make:* ' + (mk || '') + '\n';
  if (sa) msg += '👨\u200d💼 *Staff:* ' + sa + '\n';
  if (st) msg += '📊 *Status:* ' + st + '\n';
  if (botResp) msg += '🤖 *Bot:* ' + botResp + '\n';
  msg += '\nChoose action:';
  return msg;
}

// ==================== SMART REVIEW CLASSIFIER (NEW) ====================
const REVIEW_CATEGORIES = {
  RINGING: {
    keywords: ['ringing', 'ring', 'baj raha', 'baj rhi', 'baj rahi', 'utha nahi', 'pick nahi', 
               'receive nahi', 'phone baj', 'रिंगिंग', 'बज रहा', 'उठा नहीं', 'baj rhi hai',
               'ring ho rahi', 'ringing ja rahi', 'phone baj raha'],
    action: 'RINGING',
    cooling: 2
  },
  NOT_CONNECTED: {
    keywords: ['not connected', 'nahi lag raha', 'switch off', 'band hai', 'unreachable',
               'network nahi', 'out of coverage', 'kat gaya', 'नहीं लग रहा', 'स्विच ऑफ',
               'बंद है', 'नेटवर्क नहीं', 'dead number', 'invalid number', 'wrong number',
               'phone band', 'mobile band', 'network problem', 'no signal', 'switched off'],
    action: 'NOT CONNECTED',
    cooling: 2
  },
  BUSY: {
    keywords: ['busy', 'occupied', 'doosre call pe', 'baat kar rahe', 'line busy',
               'call waiting', 'बिजी', 'दूसरे कॉल पे', 'engaged', 'call pe hai',
               'baad mein batayenge', 'abhi time nahi', 'busy hai'],
    action: 'BUSY',
    cooling: 2
  },
  OUT_AREA: {
    keywords: ['out of area', 'out of station', 'city se bahar', 'gaon gaya', 'village gaya',
               'native gaya', 'dusre shehar', 'out of town', 'travel kar raha', 'trip pe hai',
               'दूसरे शहर', 'गांव गया', 'बाहर है'],
    action: 'OUT OF AREA',
    cooling: 2
  },
  FINAL: {
    keywords: ['already done', 'ho gaya', 'ho chuka', 'le li', 'kar li', 'policy le li',
               'insurance ho gaya', 'renewed', 'not interested', 'do not call', 'call mat karna',
               'phone mat karna', 'block karo', 'gadi bech di', 'car sold', 'sold out',
               'nahi chahiye', 'mana kar diya', 'refuse kar diya', 'wrong number',
               'already renewed', 'policy done', 'insurance done', 'ho gaya hai', 'ho chuka hai',
               'le liya', 'kar liya', 'bech di', 'sale ho gayi', 'pahle se', 'pehle se'],
    action: 'FINAL',
    cooling: 0
  }
};

const TIME_WORDS = ['baad mein', 'bad me', 'kal', 'tomorrow', 'parso', 'sunday', 'monday',
  'tuesday', 'wednesday', 'thursday', 'friday', 'saturday', 'subah', 'sham', 'raat',
  'din', 'ghante', 'ghanta', 'minute', 'min', 'hour', 'hr', 'baje', 'baj', 'o\'clock',
  'am', 'pm', '10', '11', '12', '1', '2', '3', '4', '5', '6', '7', '8', '9',
  'call back', 'callback', 'phir call', 'dobara call', 'remind', 'next time',
  'thodi der', 'baad me call', 'bad me call', 'kal call', 'tomorrow call'];

function normalizeText(text) {
  let t = text.toLowerCase().trim();
  // Fix common misspellings
  const fixes = {
    'nhi': 'nahi', 'nhi ': 'nahi ', ' nhi': ' nahi',
    'bzy': 'busy', 'bzy ': 'busy ', ' bzy': ' busy',
    'cll': 'call', 'cll ': 'call ', ' cll': ' call',
    'phn': 'phone', 'phn ': 'phone ', ' phn': ' phone',
    'utha': 'utha', 'utha ': 'utha ',
    'krna': 'karna', 'krna ': 'karna ',
    'kro': 'karo', 'kro ': 'karo ',
    'kr': 'kar', 'kr ': 'kar ',
    'mein': 'mein', 'me ': 'me ',
    'hai': 'hai', 'h ': 'hai ',
    'tho': 'toh', 'tho ': 'toh ',
    'n': 'nahi', ' n ': ' nahi ',
    'nhi': 'nahi', 'nai': 'nahi',
    'nhi ': 'nahi ', ' nhi': ' nahi'
  };
  for (const [wrong, right] of Object.entries(fixes)) {
    t = t.split(wrong).join(right);
  }
  // Remove extra spaces and punctuation
  t = t.replace(/[.,!?;:'"()-]/g, ' ').replace(/\s+/g, ' ').trim();
  return t;
}

function classifyReview(text) {
  const normalized = normalizeText(text);
  const words = normalized.split(' ');

  let scores = { RINGING: 0, NOT_CONNECTED: 0, BUSY: 0, OUT_AREA: 0, FINAL: 0 };

  // Score each category
  for (const [cat, data] of Object.entries(REVIEW_CATEGORIES)) {
    for (const keyword of data.keywords) {
      if (normalized.includes(keyword)) {
        scores[cat] += keyword.split(' ').length; // Longer match = higher score
      }
    }
  }

  // Check for time words (indicates callback/reminder)
  const hasTimeWord = TIME_WORDS.some(w => normalized.includes(w));

  // Check for negation + action (indicates final)
  const hasNegation = ['nahi', 'na', 'nai', 'not', 'no', 'dont', 'don\'t', 'mat'].some(w => normalized.includes(w));
  const hasAction = ['chahiye', 'lena', 'leni', 'karwana', 'karna', 'karo', 'karein'].some(w => normalized.includes(w));

  // Context rules
  if (hasNegation && hasAction) {
    scores.FINAL += 5;
  }

  // "ho gaya" / "le li" patterns = final
  if (normalized.includes('ho gaya') || normalized.includes('ho chuka') || 
      normalized.includes('le li') || normalized.includes('le liya') ||
      normalized.includes('kar li') || normalized.includes('kar liya') ||
      normalized.includes('bech di') || normalized.includes('bech diya')) {
    scores.FINAL += 5;
  }

  // "already" = final
  if (normalized.includes('already') || normalized.includes('pahle se') || normalized.includes('pehle se')) {
    scores.FINAL += 4;
  }

  // Find winner
  let winner = null;
  let maxScore = 0;
  for (const [cat, score] of Object.entries(scores)) {
    if (score > maxScore) {
      maxScore = score;
      winner = cat;
    }
  }

  // If no clear winner but has time words → CALLBACK
  if (!winner || maxScore < 2) {
    if (hasTimeWord) {
      return { type: 'CALLBACK', category: 'CALLBACK', confidence: 'medium' };
    }
    // Unclear but staff typed something = follow up needed
    return { type: 'RE_DIAL', category: 'UNCLEAR', confidence: 'low' };
  }

  // If winner is FINAL but has time words → CALLBACK (time overrides final)
  if (winner === 'FINAL' && hasTimeWord && maxScore < 5) {
    return { type: 'CALLBACK', category: 'CALLBACK', confidence: 'medium' };
  }

  const catData = REVIEW_CATEGORIES[winner];
  if (winner === 'FINAL') {
    return { type: 'FINAL', category: 'FINAL', confidence: maxScore >= 3 ? 'high' : 'medium' };
  }

  // If has time words with non-final → CALLBACK (explicit time given)
  if (hasTimeWord && winner !== 'FINAL') {
    return { type: 'CALLBACK', category: winner, confidence: 'high' };
  }

  // Default: RE_DIAL (2Hr auto)
  return { type: 'RE_DIAL', category: winner, confidence: 'high' };
}

// ==================== STAFF SYNC ====================
async function syncStaffFromSheet() {
  try {
    speedCache.clearStaffCache();

    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: CONFIG.SHEET_ID,
      range: `${CONFIG.STAFF_SHEET_NAME}!A2:I1000`
    });
    const rows = res.data.values || [];

    for (const row of rows) {
      const userName = safeStr(row[CONFIG.STAFF_COLS.USER_NAME]);
      if (!userName) continue;

      const staffData = {
        userName: userName,
        name: safeStr(row[CONFIG.STAFF_COLS.STAFF_NAME]),
        staffNo: safeStr(row[CONFIG.STAFF_COLS.STAFF_NO]),
        activeStatus: safeStr(row[CONFIG.STAFF_COLS.ACTIVE_STATUS]),
        email: safeStr(row[CONFIG.STAFF_COLS.EMAIL]),
        gender: safeStr(row[CONFIG.STAFF_COLS.GENDER]),
        idCreate: safeStr(row[CONFIG.STAFF_COLS.ID_CREATE]),
        chatId: safeStr(row[CONFIG.STAFF_COLS.CHAT_ID]),
        mail: safeStr(row[CONFIG.STAFF_COLS.MAIL]),
        updatedAt: new Date()
      };

      await staffsCollection.updateOne(
        { userName: { $regex: `^${userName}$`, $options: 'i' } },
        { $set: staffData },
        { upsert: true }
      );

      const dbStaff = await staffsCollection.findOne({ userName: { $regex: `^${userName}$`, $options: 'i' } });
      if (dbStaff) speedCache.setStaff(dbStaff);
    }
    console.log(`✅ Synced ${rows.length} staff members`);
  } catch (e) {
    console.error('Staff sync error:', e.message);
  }
}

// ==================== WRITE CHAT ID TO SHEET ====================
async function updateStaffChatIdInSheet(userName, chatId) {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: CONFIG.SHEET_ID,
      range: `${CONFIG.STAFF_SHEET_NAME}!A2:A1000`
    });
    const rows = res.data.values || [];
    let rowNum = null;
    for (let i = 0; i < rows.length; i++) {
      if (safeStr(rows[i][0]).toUpperCase() === userName.toUpperCase()) {
        rowNum = i + 2;
        break;
      }
    }
    if (rowNum) {
      await sheets.spreadsheets.values.update({
        spreadsheetId: CONFIG.SHEET_ID,
        range: `${CONFIG.STAFF_SHEET_NAME}!H${rowNum}`,
        valueInputOption: 'USER_ENTERED',
        resource: { values: [[chatId]] }
      });
      console.log(`📝 ChatID ${chatId} written to sheet for ${userName} at row ${rowNum}`);
    }
  } catch (e) {
    console.error('Write ChatID error:', e.message);
  }
}

// ==================== SHEET DATA (CACHED) ====================
async function getSheetData() {
  const cached = speedCache.getSheetData();
  if (cached) return cached;

  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: CONFIG.SHEET_ID,
    range: `${CONFIG.LEADS_SHEET_NAME}!A1:N5000`
  });
  const data = res.data.values || [];
  speedCache.setSheetData(data);
  return data;
}

async function getRowMap() {
  if (speedCache.rowMap.size > 0) {
    return Object.fromEntries(speedCache.rowMap);
  }

  const data = await getSheetData();
  const map = {};

  for (let i = 1; i < data.length; i++) {
    const rn = safeStr(data[i][CONFIG.LEAD_COLS.REG_NO]);
    if (rn) {
      map[rn] = i + 1;
    }
  }

  speedCache.rowMap = new Map(Object.entries(map));
  return map;
}

async function getLeadRowData(rowNum) {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: CONFIG.SHEET_ID,
    range: `${CONFIG.LEADS_SHEET_NAME}!A${rowNum}:N${rowNum}`
  });
  return (res.data.values?.[0] || []).map(safeStr);
}

// ==================== BACKGROUND SHEET SYNC (NEW) ====================
async function queueSheetUpdate(regNo, updates) {
  try {
    await pendingSheetUpdatesCollection.updateOne(
      { regNo },
      { 
        $set: { regNo, updates, createdAt: new Date(), retryCount: 0 },
        $setOnInsert: { createdAt: new Date() }
      },
      { upsert: true }
    );
  } catch (e) {
    console.error('Queue sheet update error:', e.message);
  }
}

async function processSheetBatch() {
  try {
    const pending = await pendingSheetUpdatesCollection.find({ retryCount: { $lt: 3 } }).limit(50).toArray();
    if (pending.length === 0) return;

    const rowMap = await getRowMap();

    const data = pending.map(p => ({
      range: `${CONFIG.LEADS_SHEET_NAME}!${String.fromCharCode(65 + p.updates.col)}${p.rowNum || rowMap[p.regNo]}`,
      values: [[p.updates.value]]
    }));

    if (data.length === 0) return;

    await sheets.spreadsheets.values.batchUpdate({
      spreadsheetId: CONFIG.SHEET_ID,
      resource: { valueInputOption: 'USER_ENTERED', data }
    });

    await pendingSheetUpdatesCollection.deleteMany({ _id: { $in: pending.map(p => p._id) } });
  } catch (e) {
    console.error('Sheet batch error:', e.message);
    // Increment retry count
    for (const p of pending) {
      await pendingSheetUpdatesCollection.updateOne(
        { _id: p._id },
        { $inc: { retryCount: 1 } }
      );
    }
  }
}

async function updateLeadCells(rowNum, updates) {
  // Queue for background sync
  for (const u of updates) {
    const regNo = await getRegNoFromRow(rowNum);
    if (regNo) {
      await queueSheetUpdate(regNo, { ...u, rowNum });
    }
  }
}

async function getRegNoFromRow(rowNum) {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: CONFIG.SHEET_ID,
      range: `${CONFIG.LEADS_SHEET_NAME}!C${rowNum}`
    });
    return safeStr(res.data.values?.[0]?.[0]);
  } catch (e) {
    return null;
  }
}

// ==================== DAILING COUNT COPY ====================
async function copyToDailingCount(rowData) {
  try {
    const res = await sheets.spreadsheets.values.get({
      spreadsheetId: CONFIG.SHEET_ID,
      range: `${CONFIG.DAILING_COUNT_SHEET}!A:A`
    });
    const rows = res.data.values || [];
    const nextRow = rows.length + 1;

    const copyData = [
      rowData[CONFIG.LEAD_COLS.NAME] || '',
      rowData[CONFIG.LEAD_COLS.MOBILE] || '',
      rowData[CONFIG.LEAD_COLS.REG_NO] || '',
      rowData[CONFIG.LEAD_COLS.EXPIRED] || '',
      rowData[CONFIG.LEAD_COLS.MAKE] || '',
      rowData[CONFIG.LEAD_COLS.REMARK] || '',
      rowData[CONFIG.LEAD_COLS.STAFF_NAME] || '',
      rowData[CONFIG.LEAD_COLS.STATUS] || '',
      rowData[CONFIG.LEAD_COLS.REVIEW] || '',
      rowData[CONFIG.LEAD_COLS.DATE] || '',
      rowData[CONFIG.LEAD_COLS.SENT_TIME] || '',
      rowData[CONFIG.LEAD_COLS.DONE_TIME] || '',
      rowData[CONFIG.LEAD_COLS.COUNT_DIALER] || '',
      rowData[CONFIG.LEAD_COLS.BOT_RESPONSE] || '',
      formatDateTime() // COPIED_AT in column O
    ];

    await sheets.spreadsheets.values.update({
      spreadsheetId: CONFIG.SHEET_ID,
      range: `${CONFIG.DAILING_COUNT_SHEET}!A${nextRow}:O${nextRow}`,
      valueInputOption: 'USER_ENTERED',
      resource: { values: [copyData] }
    });

    console.log(`✅ Copied to DAILING COUNT row ${nextRow}: ${rowData[CONFIG.LEAD_COLS.REG_NO]}`);
  } catch (e) {
    console.error('Copy to DAILING COUNT error:', e.message);
  }
}

// ==================== COOLING (MONGODB MASTER) ====================
async function isInCooling(regNo) {
  const lock = await tempLocksCollection.findOne({ regNo });
  if (!lock) return false;
  if (lock.expiresAt < new Date()) { 
    await tempLocksCollection.deleteOne({ regNo }); 
    return false; 
  }
  return true;
}

async function setCooling(regNo, hours = 2) {
  const expiresAt = new Date(Date.now() + hours * 3600000);
  await tempLocksCollection.updateOne(
    { regNo },
    { $set: { regNo, expiresAt, createdAt: new Date() } },
    { upsert: true }
  );
  console.log(`🔒 Cooling set for ${regNo}, expires at ${formatTime(expiresAt)}`);
}

async function clearCooling(regNo) {
  await tempLocksCollection.deleteOne({ regNo });
}

// ==================== BOT RESPONSE FORMATTER (NEW) ====================
function getBotResponse(type, data = {}) {
  switch(type) {
    case 'COOLING':
      return `⏱️ ${data.reviewType || 'Active'} | 2Hr cooling | Resets @ ${data.resetTime || formatTime(new Date(Date.now() + 2 * 3600000))}`;
    case 'FINAL':
      return '🏁 FINAL — No callback needed';
    case 'CALLBACK':
      return `⏰ Callback: ${data.reminderTime || 'Pending'}`;
    case 'RE_DIAL':
      return `🔄 Re-dial: 2Hr auto | Resets @ ${data.resetTime || formatTime(new Date(Date.now() + 2 * 3600000))}`;
    case 'DONE':
      return '✅ DONE';
    case 'AVAILABLE':
      return '✅ AVAILABLE — Ready for re-dial';
    default:
      return '';
  }
}

// ==================== DEDUPLICATION & RATE LIMITING ====================
const processedMessages = new Map();
function isDuplicate(key) {
  const now = Date.now();
  if (processedMessages.has(key)) {
    if (now - processedMessages.get(key) < 1000) return true; // Reduced from 5s to 1s
  }
  processedMessages.set(key, now);
  for (const [k, v] of processedMessages) {
    if (now - v > 60000) processedMessages.delete(k);
  }
  return false;
}

const rateLimits = new Map();
function isRateLimited(userId) {
  const now = Date.now();
  const last = rateLimits.get(userId);
  if (last && now - last < 300) return true;
  rateLimits.set(userId, now);
  return false;
}

// In-memory state
const pendingReviews = new Map();
const userLeads = new Map();
const leadUsers = new Map();

// ==================== INSTANT HANDLERS (NEW ARCHITECTURE) ====================
async function processUpdate(update) {
  if (!update.message && !update.callback_query) return;
  const chatId = safeStr(update.message?.chat?.id || update.callback_query?.message?.chat?.id);
  const userId = safeStr(update.message?.from?.id || update.callback_query?.from?.id);
  if (!chatId || !userId) return;

  if (update.callback_query) {
    answerCallbackQuery(update.callback_query.id); // Instant answer
  }

  let dupKey;
  if (update.callback_query) dupKey = `d_${userId}_${update.callback_query.message.message_id}_${update.callback_query.data}`;
  else dupKey = `d_${userId}_${update.message.message_id}`;
  if (isDuplicate(dupKey)) return;
  if (isRateLimited(userId)) return;

  processUpdateAsync(update, chatId, userId).catch(console.error);
}

async function processUpdateAsync(update, chatId, userId) {
  try {
    if (update.callback_query) await handleCallback(update.callback_query, chatId, userId);
    else if (update.message?.text) await handleText(update.message.text.trim(), chatId, userId);
  } catch (err) {
    console.error('processUpdate ERROR:', err);
    sendMessage(chatId, '⚠️ Error: ' + err.message, getMainButtons()).catch(() => {});
  }
}

// ==================== SMART REMINDER (ENHANCED) ====================
function isFinalResponse(text) {
  const t = ' ' + text.toLowerCase().replace(/[.,!?;:'"()-]/g, ' ').replace(/\s+/g, ' ') + ' ';
  const cb = ['baad me','baadme','bad me','badme','baad mein','bad mein','baad m','bad m','baadmein','baad mai','bad mai','baad me kro','bad me kro'];
  if (cb.some(x => t.includes(x))) return false;

  if (/(?:already|alredy)/.test(t) && /(?:renew|renewed|done|taken|purchase|bought)/.test(t)) return true;
  if (/(?:renew|policy|insurance)/.test(t) && /(?:ho\s*(?:gaya|gya|chuka)|done|complete|le\s*li|mil\s*gaya|karwa\s*liya)/.test(t)) return true;
  if (/(?:karwa?\s*(?:chuke|chuka|diye|diya|liye|li|liya|rakha))/.test(t)) return true;
  if (/(?:ho\s*(?:gaya|gya|chuka))/.test(t) && /(?:hai|h)/.test(t)) return true;
  if (/(?:le\s*(?:liya|liye|li))/.test(t) && /(?:hai|h)/.test(t)) return true;
  if (/(?:pahle\s*se|pehle\s*se)/.test(t)) return true;
  if (/(?:dont|don't|do not|never)/.test(t) && /(?:record|call|disturb|phone)/.test(t)) return true;
  if (/(?:do\s*not\s*call|call\s*mat\s*karo|phone\s*mat\s*karna|call\s*na\s*karein)/.test(t)) return true;
  if (/(?:sell|sold|sale|bech|beche|becha)/.test(t) && /(?:car|gadi|gaadi|vehicle)/.test(t)) return true;
  if (/(?:gadi|car|gaadi)/.test(t) && /(?:bech|sell|sale|beche|becha)/.test(t) && /(?:di|diya|de|kar|chuka|gayi)/.test(t)) return true;
  if (/(?:sold|sale)/.test(t) && /(?:long\s*time|1\s*year|2\s*year|months?\s*ago)/.test(t)) return true;
  if (/(?:gadi|car|gaadi)/.test(t) && /(?:nahi|nhi|na|nai)/.test(t) && /(?:hai|h|rahi|available)/.test(t)) return true;
  if (/(?:nahi|nhi|na|nahin|nai)/.test(t) && /(?:chahiye|lena|leni|jarurat|zarurat)/.test(t)) return true;
  if (/(?:not|no)/.test(t) && /(?:interested|need|want|require|required)/.test(t)) return true;
  if (/(?:mana|manaa)/.test(t) && /(?:kar|kar\s*di|diya|kiya)/.test(t)) return true;
  if (/(?:dont|don't|do not)/.test(t) && /(?:want|need|require)/.test(t)) return true;
  if (/(?:block|blacklist)/.test(t) && /(?:kar|karo|kro|kardo)/.test(t)) return true;
  if (/(?:aage|aagey|age|aagay)/.test(t) && /(?:se|say)/.test(t) && /(?:mat|na)/.test(t) && /(?:karna|karo|karein)/.test(t)) return true;
  if (/(?:band|bandh|bnd)/.test(t) && /(?:kardo|kar\s*do|kar\s*de)/.test(t)) return true;
  if (/(?:invalid|wrong|fake|not\s*exist|dead|band|bandh)/.test(t) && /(?:number|no|mobile|phone)/.test(t)) return true;
  if (/(?:number|mobile|phone)/.test(t) && /(?:invalid|wrong|fake|not\s*working)/.test(t)) return true;
  if (/(?:switch\s*off|switched\s*off)/.test(t) && /(?:permanent|always|forever|hai|h)/.test(t)) return true;
  if (/(?:does\s*not|don't)/.test(t) && /(?:exist|live|work)/.test(t)) return true;
  if (/(?:not\s*in\s*use|band|bandh)/.test(t)) return true;
  return false;
}

function detectReminder(text, chatId, staffName, regNo, leadData) {
  const now = new Date();
  const lt = text.toLowerCase().trim();
  if (isFinalResponse(text)) return { time: null, type: '🏁 FINAL RESPONSE — No callback needed', isFinal: true };

  let rt = null, type = '';

  const minM = lt.match(/(?:call\s*back\s*|callback\s*|call\s*)(\d{1,2})\s*(?:min|minute|minutes|minat|mnt|minut|minuts)/);
  if (minM && !rt) { rt = new Date(now.getTime() + parseInt(minM[1]) * 60000); type = `⏰ Call back in ${minM[1]} minute(s)`; }

  const kalM = lt.match(/(?:kal|kl|कल)\s*(?:subah|morning|sham|evening|raat|night|din|दिन)?\s*(\d{1,2})\s*(?:baje|baj|baje|o'?clock|am|pm)?/);
  if (kalM && !rt) { rt = new Date(now.getTime() + 86400000); rt.setHours(parseInt(kalM[1]), 0, 0, 0); type = `📅 Tomorrow at ${kalM[1]}:00 AM`; }

  if (!rt && /(?:next\s*week|nest\s*week|agla\s*hafta|agla\s*week|agale\s*hafte)/.test(lt)) { rt = new Date(now.getTime() + 7 * 86400000); rt.setHours(10, 0, 0, 0); type = '📅 Next week at 10:00 AM'; }
  if (!rt && /(?:next\s*month|agla\s*mahina|agla\s*month|agale\s*mahine|agla\s*mhina)/.test(lt)) { rt = new Date(now.getTime() + 30 * 86400000); rt.setHours(10, 0, 0, 0); type = '📅 Next month at 10:00 AM'; }

  const afterM = lt.match(/(?:after|baad|ke\s*baad|ke\s*bad)\s*(\d{1,2})\s*(?:jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/);
  if (afterM && !rt) {
    const mn = ['jan','feb','mar','apr','may','jun','jul','aug','sep','oct','nov','dec'];
    const ms = lt.match(/(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)/)[1];
    rt = new Date(now.getFullYear(), mn.indexOf(ms), parseInt(afterM[1]) + 1, 10, 0, 0);
    if (rt < now) rt.setFullYear(rt.getFullYear() + 1);
    type = `📅 After ${afterM[1]} ${ms} at 10:00 AM`;
  }

  const dateM = lt.match(/(\d{1,2})\s*(?:को|ko|co)/);
  if (dateM && !rt) {
    const td = parseInt(dateM[1]), tday = now.getDate();
    let tm = now.getMonth(), ty = now.getFullYear();
    if (td < tday) { tm++; if (tm > 11) { tm = 0; ty++; } }
    rt = new Date(ty, tm, td, 10, 0, 0);
    if (rt.getDate() !== td) rt = new Date(ty, tm + 1, 0, 10, 0, 0);
    if (rt < now) { tm++; if (tm > 11) { tm = 0; ty++; } rt = new Date(ty, tm, td, 10, 0, 0); }
    type = `📅 Date: ${td}-${tm + 1}-${ty}`;
  }

  if (!rt) {
    const rel = [{ w: ['aaj','aj','आज'], o: 0 }, { w: ['kal','kl','कल'], o: 1 }, { w: ['parso','paraso','perso','परसों','परसो'], o: 2 }];
    for (const r of rel) {
      if (r.w.some(x => lt.includes(x))) { rt = new Date(now.getTime() + r.o * 86400000); rt.setHours(10, 0, 0, 0); type = `📅 ${r.w[0]} at 10:00 AM`; break; }
    }
  }

  if (!rt) {
    const wd = [
      { n: ['sunday','sundy','sun','san','संडे','sanday'], c: 0 }, { n: ['monday','mondy','mon','mn','मंडे','manday'], c: 1 },
      { n: ['tuesday','tues','tue','tyu','ट्यूसडे','tusday'], c: 2 }, { n: ['wednesday','wed','wd','वेडनेसडे','wednsday'], c: 3 },
      { n: ['thursday','thurs','thu','thur','थर्सडे','thrusday'], c: 4 }, { n: ['friday','fri','fr','फ्राइडे','fridy'], c: 5 },
      { n: ['saturday','saterday','sat','sta','सैटरडे','saturdy'], c: 6 }
    ];
    for (const w of wd) {
      if (w.n.some(n => lt.includes(n))) {
        let du = w.c - now.getDay();
        if (du <= 0) du += 7;
        rt = new Date(now.getTime() + du * 86400000); rt.setHours(10, 0, 0, 0);
        type = `📅 Next ${w.n[0]} at 10:00 AM`; break;
      }
    }
  }

  if (!rt) {
    const nm = lt.match(/(\d{1,2})/);
    if (nm) {
      const n = parseInt(nm[1]);
      const hw = ['hour','hours','hr','hrs','ghanta','ghante','ghnte','ghnta','घंटे','घंटा','ghnt','ghante'];
      const mw = ['minute','minutes','min','mins','minat','मिनट','mint','mnt','minuts'];
      const dw = ['day','days','din','dino','दिन','dinn','deenn'];
      const ww = ['week','weeks','hafta','hafte','हफ्ता','हफ्ते','wek','weaks','haftey','hafte'];
      if (hw.some(w => lt.includes(w))) { rt = new Date(now.getTime() + n * 3600000); type = `⏰ After ${n} hour(s)`; }
      else if (mw.some(w => lt.includes(w))) { rt = new Date(now.getTime() + n * 60000); type = `⏰ After ${n} minute(s)`; }
      else if (dw.some(w => lt.includes(w))) { rt = new Date(now.getTime() + n * 86400000); rt.setHours(10, 0, 0, 0); type = `📅 After ${n} day(s)`; }
      else if (ww.some(w => lt.includes(w))) { rt = new Date(now.getTime() + n * 7 * 86400000); rt.setHours(10, 0, 0, 0); type = `📅 After ${n} week(s)`; }
    }
  }

  if (!rt) {
    const rw = ['disconnect','disconect','cut','not reachable','unreachable','switch off','band','बंद','नेटवर्क','कट गया','kat gaya','network nahi','no network'];
    if (rw.some(w => lt.includes(w))) { rt = new Date(now.getTime() + 1800000); type = '🔄 Retry after 30 minutes'; }
  }

  if (!rt) {
    const gen = ['call back','callback','baad mein','बाद में','baadme','badme','baad me','bad me','badmein'];
    if (gen.some(w => lt.includes(w))) { rt = new Date(now.getTime() + 7200000); type = '⏰ Default: 2 hours'; }
  }

  if (rt) {
    if (rt < now) rt = new Date(rt.getTime() + 86400000);
    return {
      time: rt, type, isFinal: false,
      data: {
        chatId, regNo, staffName, reviewText: text,
        customerName: safeStr(leadData[CONFIG.LEAD_COLS.NAME]),
        customerMobile: safeStr(leadData[CONFIG.LEAD_COLS.MOBILE]),
        reminderType: type, fireAt: rt, fired: false,
        activeLead: true,
        completed: false,
        createdAt: new Date()
      }
    };
  }
  return null;
}

// ==================== ATOMIC LEAD ASSIGNMENT (NEW) ====================
async function atomicAssignLead(regNo, staffName, chatId) {
  try {
    await activeAssignmentsCollection.insertOne({
      regNo,
      staffName,
      chatId,
      assignedAt: new Date(),
      status: 'SENT',
      expiresAt: new Date(Date.now() + 4 * 3600000) // 4 hour TTL
    });
    return true;
  } catch (e) {
    if (e.code === 11000) {
      // Duplicate key - already assigned
      return false;
    }
    throw e;
  }
}

async function getActiveAssignment(chatId) {
  return await activeAssignmentsCollection.findOne({ chatId, status: { $ne: 'DONE' } });
}

async function getActiveAssignmentByRegNo(regNo) {
  return await activeAssignmentsCollection.findOne({ regNo, status: { $ne: 'DONE' } });
}

async function clearAssignment(regNo) {
  await activeAssignmentsCollection.deleteOne({ regNo });
}

async function updateAssignmentStatus(regNo, status, review = null) {
  const update = { $set: { status, updatedAt: new Date() } };
  if (review) update.$set.review = review;
  await activeAssignmentsCollection.updateOne({ regNo }, update);
}

// ==================== HANDLE TEXT ====================
async function handleText(text, chatId, userId) {
  const pending = pendingReviews.get(chatId);

  if (pending) {
    if (text === '/cancel') {
      pendingReviews.delete(chatId);
      await sendMessage(chatId, '❌ Review cancelled.\n🔒 Lead locked.', getMainButtons());
      return;
    }
    if (text.startsWith('/') || text === '▶️ START LEAD' || text === '📊 MY STATUS') {
      pendingReviews.delete(chatId);
    } else {
      const { regNo, messageId } = pending;

      let staff = speedCache.getStaffByChatId(chatId);
      if (!staff) staff = await staffsCollection.findOne({ chatId });
      const sn = staff ? staff.name : '';

      // SMART CLASSIFY (NEW)
      const classification = classifyReview(text);
      console.log(`Smart classify: "${text}" → ${classification.type} (${classification.confidence})`);

      // Get fresh row data
      const rowMap = await getRowMap();
      const rowNum = rowMap[regNo];
      if (!rowNum) {
        pendingReviews.delete(chatId);
        await sendMessage(chatId, '❌ Lead not found in sheet.', getMainButtons());
        return;
      }

      const freshRow = await getLeadRowData(rowNum);

      // Handle based on classification
      if (classification.type === 'FINAL') {
        // Final response - no callback
        await updateAssignmentStatus(regNo, 'REVIEWED', text);
        await updateLeadCells(rowNum, [
          { col: CONFIG.LEAD_COLS.REVIEW, value: text },
          { col: CONFIG.LEAD_COLS.STAFF_NAME, value: sn },
          { col: CONFIG.LEAD_COLS.BOT_RESPONSE, value: getBotResponse('FINAL') }
        ]);

        if (messageId) {
          await editMessage(chatId, messageId, getLeadMsg(freshRow) + '\n\n🏁 *FINAL — No callback needed*', getLeadButtons(regNo, false));
        }
        await sendMessage(chatId, `✅ Review: ${text}\n🔒 LOCKED: ${sn}\n\n🏁 *Final response detected.*\n⚠️ *Click DONE to complete!*`, getMainButtons());
        await incrementStat(sn, 'otherReview');
        pendingReviews.delete(chatId);
        return;
      }

      if (classification.type === 'CALLBACK') {
        // Has explicit time - set reminder
        const reminder = detectReminder(text, chatId, sn, regNo, freshRow);

        await updateAssignmentStatus(regNo, 'REVIEWED', text);
        await updateLeadCells(rowNum, [
          { col: CONFIG.LEAD_COLS.REVIEW, value: text },
          { col: CONFIG.LEAD_COLS.STAFF_NAME, value: sn },
          { col: CONFIG.LEAD_COLS.BOT_RESPONSE, value: getBotResponse('CALLBACK', { reminderTime: reminder?.type }) }
        ]);

        if (messageId) {
          await editMessage(chatId, messageId, getLeadMsg(freshRow) + '\n\n✏️ *Review:* ' + text, getLeadButtons(regNo, false));
        }

        if (reminder) {
          const tStr = formatTime(reminder.time);
          await sendMessage(chatId, `✅ Review: ${text}\n🔒 LOCKED: ${sn}\n\n${reminder.type}\n⏰ *Reminder: ${tStr}*\n🔒 *Lead blocked until reminder complete!*`, getMainButtons());
          await remindersCollection.insertOne(reminder.data);
          await incrementStat(sn, 'reminders');
          await incrementStat(sn, 'otherReview');
        } else {
          await sendMessage(chatId, `✅ Review: ${text}\n🔒 LOCKED: ${sn}\n\n⚠️ Click DONE to complete!`, getMainButtons());
          await incrementStat(sn, 'otherReview');
        }
        pendingReviews.delete(chatId);
        return;
      }

      // RE_DIAL or UNCLEAR - auto 2Hr reminder
      const resetTime = formatTime(new Date(Date.now() + 2 * 3600000));
      await updateAssignmentStatus(regNo, 'REVIEWED', text);
      await setCooling(regNo, 2);

      await updateLeadCells(rowNum, [
        { col: CONFIG.LEAD_COLS.REVIEW, value: text },
        { col: CONFIG.LEAD_COLS.STAFF_NAME, value: sn },
        { col: CONFIG.LEAD_COLS.BOT_RESPONSE, value: getBotResponse('RE_DIAL', { resetTime }) }
      ]);

      if (messageId) {
        await editMessage(chatId, messageId, getLeadMsg(freshRow) + '\n\n✏️ *Review:* ' + text, getLeadButtons(regNo, false));
      }

      await sendMessage(chatId, `✅ Review: ${text}\n🔒 LOCKED: ${sn}\n\n🔄 *Auto re-dial: 2 hours*\n⏱️ *Resets @ ${resetTime}*\n⚠️ Click DONE when complete!`, getMainButtons());
      await incrementStat(sn, 'otherReview');
      pendingReviews.delete(chatId);
      return;
    }
  }

  let staff = speedCache.getStaffByChatId(chatId);
  if (!staff) {
    staff = await staffsCollection.findOne({ chatId });
    if (staff) speedCache.setStaff(staff);
  }

  if (text === '/start') {
    if (staff) await sendWelcome(chatId, staff);
    else await sendMessage(chatId, '👋 Welcome!\n\nSend USER NAME to login.\nExample: MIS-SHAIK-1843', null, true);
    return;
  }

  if (text === '/refresh') {
    await syncStaffFromSheet();
    await sendMessage(chatId, '🔄 Staff data refreshed!', getMainButtons());
    return;
  }

  const loginStaff = speedCache.getStaffByUserName(text);
  if (loginStaff) {
    if (safeStr(loginStaff.activeStatus).toUpperCase() !== 'ACTIVE') {
      await sendMessage(chatId, '❌ NOT ACTIVE. Contact admin.', null, true);
      return;
    }
    await staffsCollection.updateOne({ chatId }, { $set: { chatId: '' } });
    await staffsCollection.updateOne(
      { userName: { $regex: `^${text}$`, $options: 'i' } },
      { $set: { chatId } }
    );
    const updated = await staffsCollection.findOne({ userName: { $regex: `^${text}$`, $options: 'i' } });
    const saved = updated && updated.chatId === chatId;

    await updateStaffChatIdInSheet(text, chatId);

    await sendMessage(chatId, `✅ Switched to: ${loginStaff.name}\n🆔 ID: ${text}\n💾 ChatID Saved: ${saved ? 'YES ✅' : 'NO ❌'}\n\nClick ▶️ START LEAD`, getMainButtons());
    speedCache.setStaff({ ...loginStaff, chatId });
    return;
  }

  if (!staff) {
    await sendMessage(chatId, '❌ USER NAME not found!', null, true);
    return;
  }
  if (safeStr(staff.activeStatus).toUpperCase() !== 'ACTIVE') {
    await sendMessage(chatId, '❌ NOT ACTIVE. Contact admin.', null, true);
    return;
  }

  const sName = staff.name;
  if (text === '/next' || text === '▶️ START LEAD') await sendNext(chatId, sName);
  else if (text === '/status' || text === '📊 MY STATUS') await sendReport(chatId, sName);
  else await sendWelcome(chatId, staff);
}

async function sendWelcome(chatId, staff) {
  const sn = safeStr(staff.name);
  const un = safeStr(staff.userName);
  const st = safeStr(staff.activeStatus);
  const em = st.toUpperCase() === 'ACTIVE' ? '🟢' : '🔴';
  let msg = '👋 *' + sn + '*\n';
  msg += '🆔 `' + un + '`\n';
  msg += em + ' *' + st + '*\n\n';
  msg += '━━━━━━━━━━━━━━\n';
  msg += '📌 *RULES*\n';
  msg += '━━━━━━━━━━━━━━\n';
  msg += '✅ Call / WhatsApp mandatory\n';
  msg += '✅ Review before Done\n';
  msg += '🔒 Locked until DONE\n';
  msg += '⏱️ 2Hr expiry (RINGING / BUSY / NOT CON / OUT AREA)\n';
  msg += '🔐 OTHER = Bot auto-classifies\n';
  msg += '⏰ Smart Reminder: "1 ghante baad", "kal", "28 ko"\n\n';
  msg += '💡 *STEPS*\n';
  msg += '1️⃣ START LEAD → 2️⃣ Call → 3️⃣ REVIEW → 4️⃣ DONE';
  await sendMessage(chatId, msg, getMainButtons());
}

// ==================== SEND NEXT (ATOMIC + INSTANT) ====================
async function sendNext(chatId, sName) {
  if (isDuplicate(`nextlock_${chatId}`)) {
    await sendMessage(chatId, '⏳ Wait... already processing.', getMainButtons());
    return;
  }
  pendingReviews.delete(chatId);

  let staff = speedCache.getStaffByChatId(chatId);
  if (!staff) staff = await staffsCollection.findOne({ chatId });
  if (!staff) return;
  const ns = staff.name;

  // ===== PRIORITY 1: Check active reminder (MongoDB) =====
  const activeRem = await remindersCollection.findOne({ 
    staffName: ns, 
    activeLead: true,
    completed: false
  });

  if (activeRem) {
    const rowMap = await getRowMap();
    const rowNum = rowMap[activeRem.regNo];
    if (rowNum) {
      const rowData = await getLeadRowData(rowNum);
      userLeads.set(chatId, activeRem.regNo);
      leadUsers.set(activeRem.regNo, chatId);

      const header = activeRem.fired 
        ? `⏰ *REMINDER TIME!*\n\n` 
        : `🔒 *BLOCKED — Pending Reminder*\n⏰ ${activeRem.reminderType}\n⏱️ Fire at: ${formatTime(activeRem.fireAt)}\n\n`;

      await sendLead(chatId, header + getLeadMsg(rowData), getLeadButtons(activeRem.regNo, false));
      return;
    }
  }

  // ===== PRIORITY 2: Check active assignment (MongoDB) =====
  const activeAssign = await getActiveAssignment(chatId);
  if (activeAssign && activeAssign.status !== 'DONE') {
    const rowMap = await getRowMap();
    const rowNum = rowMap[activeAssign.regNo];
    if (rowNum) {
      const rowData = await getLeadRowData(rowNum);
      const rv = safeStr(rowData[CONFIG.LEAD_COLS.REVIEW]);
      await sendLead(chatId, getLeadMsg(rowData), getLeadButtons(activeAssign.regNo, rv ? false : true));
      return;
    }
  }

  // ===== PRIORITY 3: Get fresh lead (Atomic Lock) =====
  const allData = await getSheetData();
  if (allData.length <= 1) {
    await sendMessage(chatId, '🎉 No leads available right now. 🏆', getMainButtons());
    return;
  }

  // Get cooling locks from cache
  let locks = speedCache.getLocks();
  if (!locks) {
    locks = await tempLocksCollection.find({ expiresAt: { $gt: new Date() } }).toArray();
    speedCache.setLocks(locks);
  }
  const coolingSet = new Set(locks.map(l => l.regNo));

  // Get existing assignments
  const existingAssignments = await activeAssignmentsCollection.find({ status: { $ne: 'DONE' } }).toArray();
  const assignedSet = new Set(existingAssignments.map(a => a.regNo));

  let pends = [];
  for (let i = 1; i < allData.length; i++) {
    const row = allData[i];
    const st = safeStr(row[CONFIG.LEAD_COLS.STATUS]).toUpperCase();
    const as = safeStr(row[CONFIG.LEAD_COLS.STAFF_NAME]);
    const rv = safeStr(row[CONFIG.LEAD_COLS.REVIEW]);
    const reg = safeStr(row[CONFIG.LEAD_COLS.REG_NO]);

    if (st !== 'DONE' && as === '' && rv === '' && !coolingSet.has(reg) && !assignedSet.has(reg)) {
      pends.push({ row: i + 1, data: row });
    }
  }

  pends.sort((a, b) => {
    const ra = safeStr(a.data[CONFIG.LEAD_COLS.REMARK]).toUpperCase();
    const rb = safeStr(b.data[CONFIG.LEAD_COLS.REMARK]).toUpperCase();
    if (ra === 'EXPIRE' && rb !== 'EXPIRE') return -1;
    if (ra !== 'EXPIRE' && rb === 'EXPIRE') return 1;
    return parseSheetDate(a.data[CONFIG.LEAD_COLS.EXPIRED]) - parseSheetDate(b.data[CONFIG.LEAD_COLS.EXPIRED]);
  });

  if (pends.length === 0) {
    await sendMessage(chatId, '🎉 No leads available right now. 🏆', getMainButtons());
    return;
  }

  const currentDate = formatDate();
  const currentTime = formatTime();

  for (const lead of pends) {
    const rn = safeStr(lead.data[CONFIG.LEAD_COLS.REG_NO]);

    // ATOMIC LOCK ATTEMPT
    const locked = await atomicAssignLead(rn, ns, chatId);
    if (!locked) {
      console.log(`⚠️ Lead ${rn} already assigned, trying next...`);
      continue;
    }

    // INSTANT: Send message immediately
    userLeads.set(chatId, rn);
    leadUsers.set(rn, chatId);

    const freshData = lead.data.map(safeStr);
    await sendLead(chatId, getLeadMsg(freshData), getLeadButtons(rn, true));

    // BACKGROUND: Update sheet
    setImmediate(async () => {
      try {
        await updateLeadCells(lead.row, [
          { col: CONFIG.LEAD_COLS.STAFF_NAME, value: ns },
          { col: CONFIG.LEAD_COLS.STATUS, value: 'SENT' },
          { col: CONFIG.LEAD_COLS.SENT_TIME, value: currentTime },
          { col: CONFIG.LEAD_COLS.DATE, value: currentDate }
        ]);
      } catch (e) {
        console.error('Background sheet update error:', e.message);
      }
    });

    return;
  }

  await sendMessage(chatId, '⏳ All leads just taken by others!\nClick ▶️ START LEAD again.', getMainButtons());
}

async function sendLead(chatId, text, buttons) {
  return sendMessage(chatId, text, buttons);
}

async function sendReport(chatId, sName) {
  const today = formatDate();
  const sn = safeStr(sName);

  const todayStats = await statsCollection.findOne({ staffName: sName, date: today }) || {};

  const allData = await getSheetData();
  let totDone = 0, tLeads = 0, tCalls = 0, tWhatsApp = 0;
  let tRinging = 0, tNotCon = 0, tOutArea = 0, tBusy = 0;
  let tOther = 0, tReminders = 0, tSkips = 0;

  for (let i = 1; i < allData.length; i++) {
    const row = allData[i];
    const staff = safeStr(row[CONFIG.LEAD_COLS.STAFF_NAME]);
    if (staff !== sn) continue;

    const st = safeStr(row[CONFIG.LEAD_COLS.STATUS]).toUpperCase();
    const dt = safeStr(row[CONFIG.LEAD_COLS.DATE]);
    const rv = safeStr(row[CONFIG.LEAD_COLS.REVIEW]).toUpperCase();
    const calls = parseInt(row[CONFIG.LEAD_COLS.COUNT_DIALER] || 0);

    if (st === 'DONE') totDone++;

    if (dt === today) {
      tLeads++;
      tCalls += calls;

      if (rv === 'RINGING') tRinging++;
      else if (rv === 'NOT CONNECTED') tNotCon++;
      else if (rv === 'OUT OF AREA') tOutArea++;
      else if (rv === 'BUSY') tBusy++;
      else if (rv && !['RINGING','NOT CONNECTED','OUT OF AREA','BUSY'].includes(rv)) tOther++;
    }
  }

  tWhatsApp = todayStats.whatsapp || 0;
  tReminders = todayStats.reminders || 0;
  tSkips = todayStats.skips || 0;

  const mixCount = tRinging + tNotCon + tOutArea + tBusy;

  const msg = `📊 *DAILY STATUS REPORT*\n👤 *Name:* ${sn}\n📅 *Date:* ${today}\n\n` +
    `📞 *Total Call Count:* ${tCalls}\n` +
    `💬 *Total WhatsApp Count:* ${tWhatsApp}\n` +
    `✅ *Total Done Count:* ${totDone}\n` +
    `📋 *Total Lead Count:* ${tLeads}\n` +
    `⏭️ *Total Skip Count:* ${tSkips}\n` +
    `🔄 *RINGING/NOT CON/OUT AREA/BUSY:* ${mixCount}\n` +
    `📝 *Total Other Review Count:* ${tOther}\n` +
    `⏰ *Total Reminder Set Count:* ${tReminders}\n` +
    `🏆 *All Over Done Count:* ${totDone}\n\n` +
    `📊 *Breakdown:*\n` +
    `• RINGING: ${tRinging}\n` +
    `• NOT CONNECTED: ${tNotCon}\n` +
    `• OUT OF AREA: ${tOutArea}\n` +
    `• BUSY: ${tBusy}`;

  await sendMessage(chatId, msg, getMainButtons());
}

// ==================== HANDLE CALLBACK (INSTANT + BACKGROUND) ====================
async function handleCallback(cq, chatId, userId) {
  try {
    const data = cq.data;
    const messageId = cq.message.message_id;
    const act = data.split('_')[0];
    if (isDuplicate(`actlock_${chatId}_${act}`)) return;

    let staff = speedCache.getStaffByChatId(chatId);
    if (!staff) {
      staff = await staffsCollection.findOne({ chatId });
      if (staff) speedCache.setStaff(staff);
    }

    if (!staff) { await sendMessage(chatId, '❌ Session expired. Send /start', getMainButtons()); return; }
    if (safeStr(staff.activeStatus).toUpperCase() !== 'ACTIVE') { await sendMessage(chatId, '❌ NOT ACTIVE. Contact admin.', getMainButtons()); return; }

    const sName = staff.name;
    const regNo = data.substring(data.indexOf('_') + 1);

    // Get row data
    const rowMap = await getRowMap();
    const rowNum = rowMap[regNo];
    if (!rowNum) { await sendMessage(chatId, '❌ Lead not found in sheet.', getMainButtons()); return; }

    const rowData = await getLeadRowData(rowNum);
    const lsn = safeStr(rowData[CONFIG.LEAD_COLS.STAFF_NAME]);

    // Verify ownership
    if (lsn && lsn.toUpperCase() !== sName.toUpperCase()) {
      await updateLeadCells(rowNum, [{ col: CONFIG.LEAD_COLS.STAFF_NAME, value: sName }]);
    }

    const currentDate = formatDate();
    const currentTime = formatTime();

    switch (act) {
      case 'CALL': {
        // INSTANT
        let c = parseInt(rowData[CONFIG.LEAD_COLS.COUNT_DIALER] || 0) + 1;
        let mDig = safeStr(rowData[CONFIG.LEAD_COLS.MOBILE]).replace(/\D/g, '');
        if (mDig.startsWith('91') && mDig.length > 10) mDig = mDig.substring(2);
        await sendMessage(chatId, `📞 *Tap to Call*\n\n👤 ${safeStr(rowData[CONFIG.LEAD_COLS.NAME])}\n📱 +91${mDig}\n🔄 Count: ${c}\n\n👆 Tap number to dial`, getMainButtons());

        // BACKGROUND
        setImmediate(async () => {
          try {
            await updateLeadCells(rowNum, [{ col: CONFIG.LEAD_COLS.COUNT_DIALER, value: c }]);
            await incrementStat(sName, 'calls');
          } catch (e) { console.error('CALL background error:', e.message); }
        });
        break;
      }

      case 'WHATSAPP': {
        // INSTANT
        let wDig = safeStr(rowData[CONFIG.LEAD_COLS.MOBILE]).replace(/\D/g, '');
        if (wDig.startsWith('91') && wDig.length > 10) wDig = wDig.substring(2);
        const wName = safeStr(rowData[CONFIG.LEAD_COLS.NAME]);
        const wReg = safeStr(rowData[CONFIG.LEAD_COLS.REG_NO]);
        const wDs = safeStr(rowData[CONFIG.LEAD_COLS.EXPIRED] || rowData[CONFIG.LEAD_COLS.DATE]);
        const wMsg = `🚗 Hello ${wName}!\n\n(*My Insurance Saathi*)\n\nAapki gaadi *${wReg}* ka insurance *${wDs}* ko expire ho raha hai / ho chuka hai.\n\n👉 Kya aap renewal karwana chahenge best price me?\n\n✅ Zero Dep\n✅ Cashless Claim\n✅ Best Company Options\n\nReply karein:\n✔ YES – Quote ke liye\n✔ CALL – Direct baat karne ke liye`;
        const wLink = 'https://wa.me/91' + wDig + '?text=' + encodeURIComponent(wMsg);
        await sendMessage(chatId, `📱 *WhatsApp Ready*\n\n👤 ${wName}\n📱 +${wDig}\n🚗 ${wReg}\n\n👇 Tap button:`, { inline_keyboard: [[{ text: '📱 Open WhatsApp Chat', url: wLink }]] });

        // BACKGROUND
        setImmediate(async () => {
          try { await incrementStat(sName, 'whatsapp'); } catch (e) {}
        });
        break;
      }

      case 'REVIEW':
        // INSTANT
        await editMessage(chatId, messageId, getLeadMsg(rowData) + '\n\n📝 *Select Review:*', getReviewButtons(regNo));
        break;

      case 'RINGING': {
        // INSTANT
        await editMessage(chatId, messageId, getLeadMsg(rowData) + '\n\n⚠️ RINGING', getLeadButtons(regNo, false));
        await sendMessage(chatId, `✅ RINGING\n🔒 ${sName}\n⏱️ 2 HOURS to DONE!`, getMainButtons());

        // BACKGROUND
        setImmediate(async () => {
          try {
            await setCooling(regNo, 2);
            await updateAssignmentStatus(regNo, 'REVIEWED', 'RINGING');
            pendingReviews.delete(chatId); 
            userLeads.set(chatId, regNo); 
            leadUsers.set(regNo, chatId);

            const resetTime = formatTime(new Date(Date.now() + 2 * 3600000));
            await updateLeadCells(rowNum, [
              { col: CONFIG.LEAD_COLS.REVIEW, value: 'RINGING' },
              { col: CONFIG.LEAD_COLS.STAFF_NAME, value: sName },
              { col: CONFIG.LEAD_COLS.BOT_RESPONSE, value: getBotResponse('COOLING', { reviewType: 'RINGING', resetTime }) }
            ]);
            await incrementStat(sName, 'ringing');
          } catch (e) { console.error('RINGING background error:', e.message); }
        });
        break;
      }

      case 'NOTCONN': {
        // INSTANT
        await editMessage(chatId, messageId, getLeadMsg(rowData) + '\n\n⚠️ NOT CONNECTED', getLeadButtons(regNo, false));
        await sendMessage(chatId, `✅ NOT CONNECTED\n🔒 ${sName}\n⏱️ 2 HOURS to DONE!`, getMainButtons());

        // BACKGROUND
        setImmediate(async () => {
          try {
            await setCooling(regNo, 2);
            await updateAssignmentStatus(regNo, 'REVIEWED', 'NOT CONNECTED');
            pendingReviews.delete(chatId); 
            userLeads.set(chatId, regNo); 
            leadUsers.set(regNo, chatId);

            const resetTime = formatTime(new Date(Date.now() + 2 * 3600000));
            await updateLeadCells(rowNum, [
              { col: CONFIG.LEAD_COLS.REVIEW, value: 'NOT CONNECTED' },
              { col: CONFIG.LEAD_COLS.STAFF_NAME, value: sName },
              { col: CONFIG.LEAD_COLS.BOT_RESPONSE, value: getBotResponse('COOLING', { reviewType: 'NOT CONNECTED', resetTime }) }
            ]);
            await incrementStat(sName, 'notConnected');
          } catch (e) { console.error('NOTCONN background error:', e.message); }
        });
        break;
      }

      case 'OUTAREA': {
        // INSTANT
        await editMessage(chatId, messageId, getLeadMsg(rowData) + '\n\n⚠️ OUT OF AREA', getLeadButtons(regNo, false));
        await sendMessage(chatId, `✅ OUT OF AREA\n🔒 ${sName}\n⏱️ 2 HOURS to DONE!`, getMainButtons());

        // BACKGROUND
        setImmediate(async () => {
          try {
            await setCooling(regNo, 2);
            await updateAssignmentStatus(regNo, 'REVIEWED', 'OUT OF AREA');
            pendingReviews.delete(chatId); 
            userLeads.set(chatId, regNo); 
            leadUsers.set(regNo, chatId);

            const resetTime = formatTime(new Date(Date.now() + 2 * 3600000));
            await updateLeadCells(rowNum, [
              { col: CONFIG.LEAD_COLS.REVIEW, value: 'OUT OF AREA' },
              { col: CONFIG.LEAD_COLS.STAFF_NAME, value: sName },
              { col: CONFIG.LEAD_COLS.BOT_RESPONSE, value: getBotResponse('COOLING', { reviewType: 'OUT OF AREA', resetTime }) }
            ]);
            await incrementStat(sName, 'outOfArea');
          } catch (e) { console.error('OUTAREA background error:', e.message); }
        });
        break;
      }

      case 'BUSY': {
        // INSTANT
        await editMessage(chatId, messageId, getLeadMsg(rowData) + '\n\n⚠️ BUSY', getLeadButtons(regNo, false));
        await sendMessage(chatId, `✅ BUSY\n🔒 ${sName}\n⏱️ 2 HOURS to DONE!`, getMainButtons());

        // BACKGROUND
        setImmediate(async () => {
          try {
            await setCooling(regNo, 2);
            await updateAssignmentStatus(regNo, 'REVIEWED', 'BUSY');
            pendingReviews.delete(chatId); 
            userLeads.set(chatId, regNo); 
            leadUsers.set(regNo, chatId);

            const resetTime = formatTime(new Date(Date.now() + 2 * 3600000));
            await updateLeadCells(rowNum, [
              { col: CONFIG.LEAD_COLS.REVIEW, value: 'BUSY' },
              { col: CONFIG.LEAD_COLS.STAFF_NAME, value: sName },
              { col: CONFIG.LEAD_COLS.BOT_RESPONSE, value: getBotResponse('COOLING', { reviewType: 'BUSY', resetTime }) }
            ]);
            await incrementStat(sName, 'busy');
          } catch (e) { console.error('BUSY background error:', e.message); }
        });
        break;
      }

      case 'OTHER': {
        // INSTANT
        await updateAssignmentStatus(regNo, 'REVIEWING', null);
        pendingReviews.set(chatId, { regNo, messageId });
        userLeads.set(chatId, regNo); 
        leadUsers.set(regNo, chatId);

        await editMessage(chatId, messageId, getLeadMsg(rowData) + '\n\n✏️ *Type review & send*\n\n💡 Examples:\n• 1 ghante baad call kro\n• kal call kro\n• 28 ko call kro\n• sunday ko call kro\n• call disconnect', null);
        await sendMessage(chatId, `✏️ Type review & send\n🔐 PERMANENT LOCK: ${sName}\n/cancel to cancel`, null, true);
        break;
      }

      case 'DONE': {
        const rv = safeStr(rowData[CONFIG.LEAD_COLS.REVIEW]);
        if (!rv) { 
          await sendMessage(chatId, '❌ REVIEW mandatory before DONE!', getMainButtons()); 
          return; 
        }

        // Check if active reminder exists
        const activeRem = await remindersCollection.findOne({ 
          regNo, 
          staffName: sName, 
          activeLead: true 
        });

        if (activeRem && activeRem.fired) {
          // Reminder time came, staff clicked DONE → COMPLETE
          // INSTANT
          await sendMessage(chatId, `✅ Reminder completed!\n🔓 Unblocked.\n\nClick ▶️ START LEAD for new lead.`, getMainButtons());
          await deleteMessage(chatId, messageId);

          // BACKGROUND
          setImmediate(async () => {
            try {
              await remindersCollection.updateOne(
                { _id: activeRem._id },
                { $set: { activeLead: false, completed: true, completedAt: new Date() } }
              );
              await clearAssignment(regNo);

              await updateLeadCells(rowNum, [
                { col: CONFIG.LEAD_COLS.STATUS, value: 'DONE' },
                { col: CONFIG.LEAD_COLS.DONE_TIME, value: currentTime },
                { col: CONFIG.LEAD_COLS.BOT_RESPONSE, value: getBotResponse('DONE') }
              ]);

              const finalRowData = await getLeadRowData(rowNum);
              await copyToDailingCount(finalRowData);
              await incrementStat(sName, 'done');
            } catch (e) { console.error('DONE reminder background error:', e.message); }
          });

          pendingReviews.delete(chatId); 
          userLeads.delete(chatId); 
          leadUsers.delete(regNo);
          return;
        }

        const tempReviews = ['RINGING', 'NOT CONNECTED', 'OUT OF AREA', 'BUSY'];
        const rvUpper = rv.toUpperCase();

        if (tempReviews.includes(rvUpper)) {
          // Temp review
          // INSTANT
          await sendMessage(chatId, `✅ ${rvUpper} done!\n🔄 Lead reset.\n⏱️ 2 HOURS cooling period.\n\nClick ▶️ START LEAD`, getMainButtons());
          await deleteMessage(chatId, messageId);

          // BACKGROUND
          setImmediate(async () => {
            try {
              await clearAssignment(regNo);
              await setCooling(regNo, 2);

              const resetTime = formatTime(new Date(Date.now() + 2 * 3600000));
              await updateLeadCells(rowNum, [
                { col: CONFIG.LEAD_COLS.STATUS, value: 'DONE' },
                { col: CONFIG.LEAD_COLS.DONE_TIME, value: currentTime },
                { col: CONFIG.LEAD_COLS.BOT_RESPONSE, value: getBotResponse('COOLING', { reviewType: rvUpper, resetTime }) }
              ]);

              const finalRowData = await getLeadRowData(rowNum);
              await copyToDailingCount(finalRowData);
              await incrementStat(sName, 'done');
            } catch (e) { console.error('DONE temp background error:', e.message); }
          });

          pendingReviews.delete(chatId); 
          userLeads.delete(chatId); 
          leadUsers.delete(regNo);
          return;
        }

        // OTHER review done
        // INSTANT
        await sendMessage(chatId, '✅ Done!\nClick ▶️ START LEAD for next.', getMainButtons());

        // BACKGROUND
        setImmediate(async () => {
          try {
            await clearAssignment(regNo);

            // Check if there's a reminder to mark complete
            const rem = await remindersCollection.findOne({ regNo, staffName: sName, activeLead: true });
            if (rem) {
              await remindersCollection.updateOne(
                { _id: rem._id },
                { $set: { activeLead: false, completed: true, completedAt: new Date() } }
              );
            }

            await updateLeadCells(rowNum, [
              { col: CONFIG.LEAD_COLS.STATUS, value: 'DONE' },
              { col: CONFIG.LEAD_COLS.DONE_TIME, value: currentTime },
              { col: CONFIG.LEAD_COLS.BOT_RESPONSE, value: getBotResponse('DONE') }
            ]);

            const finalRowData = await getLeadRowData(rowNum);
            await copyToDailingCount(finalRowData);
            await incrementStat(sName, 'done');
          } catch (e) { console.error('DONE other background error:', e.message); }
        });

        pendingReviews.delete(chatId); 
        userLeads.delete(chatId); 
        leadUsers.delete(regNo);

        const updatedRow = await getLeadRowData(rowNum);
        await editMessage(chatId, messageId, getLeadMsg(updatedRow) + `\n\n✅ COMPLETED by ${sName} at ${currentTime}`, null);
        break;
      }

      case 'SKIP': {
        // Check if active reminder exists
        const activeRem = await remindersCollection.findOne({ regNo, staffName: sName, activeLead: true });
        if (activeRem) {
          await sendMessage(chatId, `❌ SKIP blocked! Active reminder pending.\n⏰ ${activeRem.reminderType}\n\nComplete the reminder first.`, getMainButtons());
          return;
        }

        const cr = safeStr(rowData[CONFIG.LEAD_COLS.REVIEW]);
        if (cr) {
          await sendMessage(chatId, `❌ SKIP blocked! Review done: ${cr}\nClick DONE.`, getMainButtons()); 
          return;
        }

        // INSTANT
        await sendMessage(chatId, '⏭️ Skipped.\nClick ▶️ START LEAD', getMainButtons());

        // BACKGROUND
        setImmediate(async () => {
          try {
            await clearAssignment(regNo);
            await clearCooling(regNo);

            await updateLeadCells(rowNum, [
              { col: CONFIG.LEAD_COLS.STAFF_NAME, value: '' },
              { col: CONFIG.LEAD_COLS.STATUS, value: '' },
              { col: CONFIG.LEAD_COLS.REVIEW, value: '' },
              { col: CONFIG.LEAD_COLS.SENT_TIME, value: '' },
              { col: CONFIG.LEAD_COLS.DATE, value: '' },
              { col: CONFIG.LEAD_COLS.BOT_RESPONSE, value: '' }
            ]);
            await incrementStat(sName, 'skips');
          } catch (e) { console.error('SKIP background error:', e.message); }
        });

        pendingReviews.delete(chatId);
        userLeads.delete(chatId);
        leadUsers.delete(regNo);
        break;
      }
    }
  } catch (err) {
    console.error('handleCallback ERROR:', err);
    await sendMessage(chatId, '⚠️ Button error: ' + err.message, getMainButtons());
  }
}

// ==================== IS ROW EXPIRED (MONGODB BASED) ====================
async function isRowExpired(rowNum, regNo) {
  const lock = await tempLocksCollection.findOne({ regNo });
  if (!lock) return false;

  const now = new Date();
  if (lock.expiresAt < now) {
    console.log(`⏱️ Cooling expired for ${regNo}, clearing row ${rowNum}`);
    // Clear assignment
    await clearAssignment(regNo);
    // Clear row in sheet (background)
    setImmediate(async () => {
      try {
        await updateLeadCells(rowNum, [
          { col: CONFIG.LEAD_COLS.STAFF_NAME, value: '' },
          { col: CONFIG.LEAD_COLS.STATUS, value: '' },
          { col: CONFIG.LEAD_COLS.REVIEW, value: '' },
          { col: CONFIG.LEAD_COLS.SENT_TIME, value: '' },
          { col: CONFIG.LEAD_COLS.DATE, value: '' },
          { col: CONFIG.LEAD_COLS.BOT_RESPONSE, value: getBotResponse('AVAILABLE') }
        ]);
      } catch (e) { console.error('Expired clear error:', e.message); }
    });

    await tempLocksCollection.deleteOne({ regNo });
    const uc = leadUsers.get(regNo);
    if (uc) { userLeads.delete(uc); leadUsers.delete(regNo); }
    return true;
  }
  return false;
}

// ==================== REMINDER SYSTEM (ENHANCED) ====================
async function checkReminders() {
  await checkExpiredLocks();
  const now = new Date();
  const due = await remindersCollection.find({ fireAt: { $lte: now }, fired: false, activeLead: true }).toArray();

  for (const rem of due) {
    try {
      // Mark as fired BUT keep activeLead: true (block until DONE)
      await remindersCollection.updateOne(
        { _id: rem._id },
        { $set: { fired: true } }
      );

      const mob = safeStr(rem.customerMobile).replace(/\D/g, '');
      const cleanMob = mob.startsWith('91') && mob.length > 10 ? mob.substring(2) : mob;
      const msg = `⏰ *CALLBACK REMINDER*\n\n👤 *Customer:* ${rem.customerName}\n📱 *Mobile:* +${cleanMob}\n🚗 *Reg No:* ${rem.regNo}\n📝 *Note:* ${rem.reviewText}\n⏱️ *Type:* ${rem.reminderType}\n\n👉 *Call back now!*`;

      const rowMap = await getRowMap();
      const rowNum = rowMap[rem.regNo];

      // Set as active lead for the staff
      userLeads.set(rem.chatId, rem.regNo);
      leadUsers.set(rem.regNo, rem.chatId);

      if (rowNum) {
        const rowData = await getLeadRowData(rowNum);

        if (safeStr(rowData[CONFIG.LEAD_COLS.STATUS]).toUpperCase() === 'DONE') {
          // Lead pehle se DONE tha, phir bhi reminder complete karna hai
          await sendMessage(rem.chatId, msg + '\n\n⚠️ This lead was marked DONE earlier. Complete the reminder now.', getMainButtons());
          await sendLead(rem.chatId, `⏰ *REMINDER TIME!*\n\n` + getLeadMsg(rowData), getLeadButtons(rem.regNo, false));
          continue;
        }

        const currentDate = formatDate();
        const currentTime = formatTime();

        // Update assignment
        await activeAssignmentsCollection.updateOne(
          { regNo: rem.regNo },
          { $set: { status: 'SENT', updatedAt: new Date() } },
          { upsert: true }
        );

        // Background sheet update
        setImmediate(async () => {
          try {
            await updateLeadCells(rowNum, [
              { col: CONFIG.LEAD_COLS.STAFF_NAME, value: rem.staffName },
              { col: CONFIG.LEAD_COLS.STATUS, value: 'SENT' },
              { col: CONFIG.LEAD_COLS.SENT_TIME, value: currentTime },
              { col: CONFIG.LEAD_COLS.DATE, value: currentDate }
            ]);
          } catch (e) {}
        });

        await sendLead(rem.chatId, msg + '\n\n' + getLeadMsg(rowData), getLeadButtons(rem.regNo, true));
      } else {
        await sendMessage(rem.chatId, msg, getMainButtons());
      }
    } catch (e) {
      console.error('Reminder error:', e);
    }
  }
}

async function checkExpiredLocks() {
  const expired = await tempLocksCollection.find({ expiresAt: { $lt: new Date() } }).toArray();
  for (const lock of expired) {
    const rowMap = await getRowMap();
    const rowNum = rowMap[lock.regNo];
    if (rowNum) {
      // Background clear
      setImmediate(async () => {
        try {
          await updateLeadCells(rowNum, [
            { col: CONFIG.LEAD_COLS.STAFF_NAME, value: '' },
            { col: CONFIG.LEAD_COLS.STATUS, value: '' },
            { col: CONFIG.LEAD_COLS.REVIEW, value: '' },
            { col: CONFIG.LEAD_COLS.SENT_TIME, value: '' },
            { col: CONFIG.LEAD_COLS.DATE, value: '' },
            { col: CONFIG.LEAD_COLS.BOT_RESPONSE, value: getBotResponse('AVAILABLE') }
          ]);
        } catch (e) {}
      });
    }
    await tempLocksCollection.deleteOne({ _id: lock._id });

    // Clear assignment if exists
    await clearAssignment(lock.regNo);

    const uc = leadUsers.get(lock.regNo);
    if (uc) { userLeads.delete(uc); leadUsers.delete(lock.regNo); }
  }
}

// ==================== LIVE COOLING UPDATER (REAL-TIME) ====================
async function updateBotResponseLive() {
  try {
    const now = new Date();
    const locks = await tempLocksCollection.find({ expiresAt: { $gt: now } }).toArray();
    if (locks.length === 0) return;

    const rowMap = await getRowMap();
    const updates = [];

    for (const lock of locks) {
      const rowNum = rowMap[lock.regNo];
      if (!rowNum) continue;

      const remaining = lock.expiresAt - now;
      const timerStr = formatDuration(remaining);
      const expiryTime = formatTime(lock.expiresAt);

      // Get assignment to know review type
      const assign = await activeAssignmentsCollection.findOne({ regNo: lock.regNo });
      const reviewType = assign?.review || 'Active';

      const botMsg = `⏱️ ${reviewType} | ${timerStr} left | Resets @ ${expiryTime}`;

      updates.push({
        range: `${CONFIG.LEADS_SHEET_NAME}!N${rowNum}`,
        values: [[botMsg]]
      });
    }

    if (updates.length > 0) {
      await sheets.spreadsheets.values.batchUpdate({
        spreadsheetId: CONFIG.SHEET_ID,
        resource: { valueInputOption: 'USER_ENTERED', data: updates }
      });
    }
  } catch (e) {
    console.error('Live cooling update error:', e.message);
  }
}

// ==================== ROUTES ====================
app.post('/webhook', async (req, res) => {
  res.sendStatus(200);
  processUpdate(req.body);
});

app.get('/', (req, res) => res.send('✅ Lead Bot Running on Node.js - INSTANT & SMART'));

// ==================== STARTUP ====================
async function setWebhook() {
  const url = process.env.WEBHOOK_URL;
  if (!url) { console.log('WEBHOOK_URL not set'); return; }
  try {
    await axios.post(`https://api.telegram.org/bot${CONFIG.TOKEN}/setWebhook`, { url });
    console.log('✅ Webhook set:', url);
  } catch (e) {
    console.error('Webhook error:', e.response?.data || e.message);
  }
}

async function start() {
  await connectMongo();

  // Rebuild memory state from MongoDB on startup
  const activeAssigns = await activeAssignmentsCollection.find({ status: { $ne: 'DONE' } }).toArray();
  for (const a of activeAssigns) {
    if (a.chatId && a.regNo) {
      userLeads.set(a.chatId, a.regNo);
      leadUsers.set(a.regNo, a.chatId);
    }
  }
  console.log(`✅ Rebuilt ${activeAssigns.length} active assignments from MongoDB`);

  const PORT = process.env.PORT || 3000;
  app.listen(PORT, async () => {
    console.log(`🚀 Server running on port ${PORT}`);
    await setWebhook();
  });

  // Intervals
  setInterval(() => checkReminders().catch(console.error), 60000);
  setInterval(() => syncStaffFromSheet().catch(console.error), 120000);
  setInterval(() => updateBotResponseLive().catch(console.error), 30000);
  setInterval(() => processSheetBatch().catch(console.error), 5000); // NEW: Background sheet sync
}

start().catch(console.error);
