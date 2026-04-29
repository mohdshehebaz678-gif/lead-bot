require('dotenv').config();
const express = require('express');
const axios = require('axios');
const { MongoClient } = require('mongodb');
const { google } = require('googleapis');

const app = express();
app.use(express.json());

// ==================== CONFIG ====================
const CONFIG = {
  TOKEN: process.env.BOT_TOKEN,
  SHEET_ID: process.env.GOOGLE_SHEET_ID,
  LEADS_SHEET_NAME: 'Sheet1',
  STAFF_SHEET_NAME: 'STAFF NAME',
  LEAD_COLS: {
    NAME: 0, MOBILE: 1, REG_NO: 2, EXPIRED: 3, MAKE: 4, REMARK: 5,
    STAFF_NAME: 6, STATUS: 7, REVIEW: 8, DATE: 9,
    SENT_TIME: 10, DONE_TIME: 11, COUNT_DIALER: 12
  },
  STAFF_COLS: {
    USER_NAME: 0, STAFF_NAME: 1, STAFF_NO: 2, ACTIVE_STATUS: 3,
    EMAIL: 4, GENDER: 5, ID_CREATE: 6, CHAT_ID: 7, MAIL: 8
  }
};

// ==================== SPEED CACHE ====================
const speedCache = {
  staffByChatId: new Map(),
  staffByUserName: new Map(),
  leads: new Map(),
  rowMap: new Map(),
  lastSync: 0,
  
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
  
  getLead(regNo) {
    return this.leads.get(regNo);
  },
  
  setLead(regNo, data) {
    this.leads.set(regNo, data);
  },
  
  invalidateLeads() {
    this.leads.clear();
    this.rowMap.clear();
  }
};

// ==================== MONGODB ====================
const mongoUri = process.env.MONGODB_URI;
let db, staffsCollection, remindersCollection, tempLocksCollection, statsCollection;

async function connectMongo() {
  const client = new MongoClient(mongoUri, { 
    maxPoolSize: 10,
    serverSelectionTimeoutMS: 5000
  });
  await client.connect();
  db = client.db(process.env.DB_NAME || 'leadbot_db');
  staffsCollection = db.collection('staffs');
  remindersCollection = db.collection('reminders');
  tempLocksCollection = db.collection('tempLocks');
  statsCollection = db.collection('staff_stats'); // NEW: daily stats

  await remindersCollection.createIndex({ fireAt: 1 }).catch(() => {});
  await tempLocksCollection.createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 }).catch(() => {});
  await staffsCollection.createIndex({ userName: 1 }).catch(() => {});
  await staffsCollection.createIndex({ chatId: 1 }).catch(() => {});
  await statsCollection.createIndex({ staffName: 1, date: 1 }).catch(() => {}); // NEW
  console.log('✅ MongoDB connected');
  
  await syncStaffFromSheet();
}

// ==================== STATS TRACKING ====================
async function incrementStat(staffName, field) {
  const today = new Date().toLocaleDateString('en-GB');
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

// ==================== HELPERS ====================
function safeStr(val) {
  return val == null ? '' : String(val).trim();
}

function formatDateTime(d = new Date()) {
  const pad = (n) => String(n).padStart(2, '0');
  return `${pad(d.getDate())}-${pad(d.getMonth() + 1)}-${d.getFullYear()} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
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

// ==================== INSTANT TELEGRAM API ====================
const tgQueue = [];
let tgProcessing = false;

async function processTgQueue() {
  if (tgProcessing || tgQueue.length === 0) return;
  tgProcessing = true;
  
  while (tgQueue.length > 0) {
    const { url, payload, resolve, reject } = tgQueue.shift();
    try {
      const res = await axios.post(url, payload, { timeout: 10000 });
      resolve(res);
    } catch (e) {
      reject(e);
    }
    if (tgQueue.length > 0) await new Promise(r => setTimeout(r, 50));
  }
  
  tgProcessing = false;
}

function tgPost(url, payload) {
  return new Promise((resolve, reject) => {
    tgQueue.push({ url, payload, resolve, reject });
    processTgQueue();
  });
}

async function sendMessage(chatId, text, replyMarkup = null, removeKeyboard = false) {
  const payload = {
    chat_id: chatId,
    text: text,
    parse_mode: 'Markdown',
    disable_web_page_preview: true
  };
  if (removeKeyboard) payload.reply_markup = JSON.stringify({ remove_keyboard: true });
  else if (replyMarkup) payload.reply_markup = JSON.stringify(replyMarkup);
  
  return tgPost(`https://api.telegram.org/bot${CONFIG.TOKEN}/sendMessage`, payload);
}

async function editMessage(chatId, messageId, text, replyMarkup = null) {
  const payload = { chat_id: chatId, message_id: messageId, text: text, parse_mode: 'Markdown' };
  if (replyMarkup) payload.reply_markup = JSON.stringify(replyMarkup);
  return tgPost(`https://api.telegram.org/bot${CONFIG.TOKEN}/editMessageText`, payload);
}

async function answerCallbackQuery(queryId) {
  return tgPost(`https://api.telegram.org/bot${CONFIG.TOKEN}/answerCallbackQuery`, { callback_query_id: queryId });
}

async function deleteMessage(chatId, messageId) {
  return tgPost(`https://api.telegram.org/bot${CONFIG.TOKEN}/deleteMessage`, { chat_id: chatId, message_id: messageId });
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

  let ds = safeStr(d2);
  const re = safeStr(rm).toUpperCase() === 'EXPIRE' ? '🔴 EXPIRE' : '🟢 NEW';

  let msg = `📋 *NEW LEAD*\n\n👤 *Name:* ${nm || ''}\n📱 *Mobile:* ${mb || ''}\n🚗 *Reg:* ${rn || ''}\n📅 *Date:* ${ds}\n${re}\n🏭 *Make:* ${mk || ''}\n`;
  if (sa) msg += `👨‍💼 *Staff:* ${sa}\n`;
  if (st) msg += `📊 *Status:* ${st}\n`;
  msg += '\nChoose action:';
  return msg;
}

// ==================== STAFF SYNC ====================
async function syncStaffFromSheet() {
  try {
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
      
      // FIX: Fetch from DB to get _id before caching
      const dbStaff = await staffsCollection.findOne({ userName: { $regex: `^${userName}$`, $options: 'i' } });
      if (dbStaff) speedCache.setStaff(dbStaff);
    }
    console.log(`✅ Synced ${rows.length} staff members`);
  } catch (e) {
    console.error('Staff sync error:', e.message);
  }
}

// ==================== SHEET DATA ====================
async function getSheetData() {
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: CONFIG.SHEET_ID,
    range: `${CONFIG.LEADS_SHEET_NAME}!A1:M10000`
  });
  return res.data.values || [];
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
      speedCache.setLead(rn, data[i]);
    }
  }
  
  speedCache.rowMap = new Map(Object.entries(map));
  return map;
}

async function getLeadRowData(rowNum) {
  // FIX: Removed broken cache check (rowNum keys don't match regNo keys)
  const res = await sheets.spreadsheets.values.get({
    spreadsheetId: CONFIG.SHEET_ID,
    range: `${CONFIG.LEADS_SHEET_NAME}!A${rowNum}:M${rowNum}`,
    valueRenderOption: 'UNFORMATTED_VALUE'
  });
  return (res.data.values?.[0] || []).map(safeStr);
}

async function updateLeadCells(rowNum, updates) {
  const data = updates.map(u => ({
    range: `${CONFIG.LEADS_SHEET_NAME}!${String.fromCharCode(65 + u.col)}${rowNum}`,
    values: [[u.value]]
  }));
  
  await sheets.spreadsheets.values.batchUpdate({
    spreadsheetId: CONFIG.SHEET_ID,
    resource: { valueInputOption: 'USER_ENTERED', data }
  });
}

// ==================== DEDUPLICATION & RATE LIMITING ====================
const processedMessages = new Map();
function isDuplicate(key) {
  const now = Date.now();
  if (processedMessages.has(key)) {
    if (now - processedMessages.get(key) < 30000) return true;
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
  if (last && now - last < 500) return true;
  rateLimits.set(userId, now);
  return false;
}

// In-memory state
const pendingReviews = new Map();
const userLeads = new Map();
const leadUsers = new Map();

// ==================== SMART REMINDER ====================
function isFinalResponse(text) {
  const t = ' ' + text.toLowerCase().replace(/[.,!?;:'"()-]/g, ' ').replace(/\s+/g, ' ') + ' ';
  const cb = ['baad me','baadme','bad me','badme','baad mein','bad mein','baad m','bad m','baadmein','baad mai','bad mai','baad me kro','bad me kro'];
  if (cb.some(x => t.includes(x))) return false;

  if (/\b(?:already|alredy)\b/.test(t) && /\b(?:renew|renewed|done|taken|purchase|bought)\b/.test(t)) return true;
  if (/\b(?:renew|policy|insurance)\b/.test(t) && /\b(?:ho\s*(?:gaya|gya|chuka)|done|complete|le\s*li|mil\s*gaya|karwa\s*liya)\b/.test(t)) return true;
  if (/\b(?:karwa?\s*(?:chuke|chuka|diye|diya|liye|li|liya|rakha))\b/.test(t)) return true;
  if (/\b(?:ho\s*(?:gaya|gya|chuka))\b/.test(t) && /\b(?:hai|h)\b/.test(t)) return true;
  if (/\b(?:le\s*(?:liya|liye|li))\b/.test(t) && /\b(?:hai|h)\b/.test(t)) return true;
  if (/\b(?:pahle\s*se|pehle\s*se)\b/.test(t)) return true;
  if (/\b(?:dont|don't|do not|never)\b/.test(t) && /\b(?:record|call|disturb|phone)\b/.test(t)) return true;
  if (/\b(?:do\s*not\s*call|call\s*mat\s*karo|phone\s*mat\s*karna|call\s*na\s*karein)\b/.test(t)) return true;
  if (/\b(?:sell|sold|sale|bech|beche|becha)\b/.test(t) && /\b(?:car|gadi|gaadi|vehicle)\b/.test(t)) return true;
  if (/\b(?:gadi|car|gaadi)\b/.test(t) && /\b(?:bech|sell|sale|beche|becha)\b/.test(t) && /\b(?:di|diya|de|kar|chuka|gayi)\b/.test(t)) return true;
  if (/\b(?:sold|sale)\b/.test(t) && /\b(?:long\s*time|1\s*year|2\s*year|months?\s*ago)\b/.test(t)) return true;
  if (/\b(?:gadi|car|gaadi)\b/.test(t) && /\b(?:nahi|nhi|na|nai)\b/.test(t) && /\b(?:hai|h|rahi|available)\b/.test(t)) return true;
  if (/\b(?:nahi|nhi|na|nahin|nai)\b/.test(t) && /\b(?:chahiye|lena|leni|jarurat|zarurat)\b/.test(t)) return true;
  if (/\b(?:not|no)\b/.test(t) && /\b(?:interested|need|want|require|required)\b/.test(t)) return true;
  if (/\b(?:mana|manaa)\b/.test(t) && /\b(?:kar|kar\s*di|diya|kiya)\b/.test(t)) return true;
  if (/\b(?:dont|don't|do not)\b/.test(t) && /\b(?:want|need|require)\b/.test(t)) return true;
  if (/\b(?:block|blacklist)\b/.test(t) && /\b(?:kar|karo|kro|kardo)\b/.test(t)) return true;
  if (/\b(?:aage|aagey|age|aagay)\b/.test(t) && /\b(?:se|say)\b/.test(t) && /\b(?:mat|na)\b/.test(t) && /\b(?:karna|karo|karein)\b/.test(t)) return true;
  if (/\b(?:band|bandh|bnd)\b/.test(t) && /\b(?:kardo|kar\s*do|kar\s*de)\b/.test(t)) return true;
  if (/\b(?:invalid|wrong|fake|not\s*exist|dead|band|bandh)\b/.test(t) && /\b(?:number|no|mobile|phone)\b/.test(t)) return true;
  if (/\b(?:number|mobile|phone)\b/.test(t) && /\b(?:invalid|wrong|fake|not\s*working)\b/.test(t)) return true;
  if (/\b(?:switch\s*off|switched\s*off)\b/.test(t) && /\b(?:permanent|always|forever|hai|h)\b/.test(t)) return true;
  if (/\b(?:does\s*not|don't)\b/.test(t) && /\b(?:exist|live|work)\b/.test(t)) return true;
  if (/\b(?:not\s*in\s*use|band|bandh)\b/.test(t)) return true;
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
        reminderType: type, fireAt: rt, fired: false
      }
    };
  }
  return null;
}

// ==================== COOLING ====================
async function isInCooling(regNo) {
  const lock = await tempLocksCollection.findOne({ regNo });
  if (!lock) return false;
  if (lock.expiresAt < new Date()) { await tempLocksCollection.deleteOne({ regNo }); return false; }
  return true;
}

async function setCooling(regNo, hours = 3) {
  await tempLocksCollection.updateOne({ regNo }, { $set: { regNo, expiresAt: new Date(Date.now() + hours * 3600000) } }, { upsert: true });
}

async function clearCooling(regNo) {
  await tempLocksCollection.deleteOne({ regNo });
}

// ==================== INSTANT HANDLERS ====================
async function processUpdate(update) {
  if (!update.message && !update.callback_query) return;
  const chatId = safeStr(update.message?.chat?.id || update.callback_query?.message?.chat?.id);
  const userId = safeStr(update.message?.from?.id || update.callback_query?.from?.id);
  if (!chatId || !userId) return;

  if (update.callback_query) {
    answerCallbackQuery(update.callback_query.id).catch(() => {});
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
      const rowMap = await getRowMap();
      const rowNum = rowMap[regNo];
      if (!rowNum) {
        pendingReviews.delete(chatId);
        await sendMessage(chatId, '❌ Lead not found in sheet.', getMainButtons());
        return;
      }
      
      let staff = speedCache.getStaffByChatId(chatId);
      if (!staff) staff = await staffsCollection.findOne({ chatId });
      const sn = staff ? staff.name : '';

      await updateLeadCells(rowNum, [
        { col: CONFIG.LEAD_COLS.REVIEW, value: text },
        { col: CONFIG.LEAD_COLS.STAFF_NAME, value: sn }
      ]);

      const freshRow = await getLeadRowData(rowNum);
      const reminder = detectReminder(text, chatId, sn, regNo, freshRow);

      if (messageId) {
        await editMessage(chatId, messageId, getLeadMsg(freshRow) + '\n\n✏️ *Review:* ' + text, getLeadButtons(regNo, false));
      }

      if (reminder && reminder.isFinal) {
        await sendMessage(chatId, `✅ Review: ${text}\n🔒 LOCKED: ${sn}\n\n${reminder.type}\n\n⚠️ *Click DONE to complete this lead!*`, getMainButtons());
        await incrementStat(sn, 'otherReview'); // NEW
      } else if (reminder) {
        const tStr = reminder.time.toLocaleString('en-GB', { day: '2-digit', month: '2-digit', year: 'numeric', hour: '2-digit', minute: '2-digit' });
        await sendMessage(chatId, `✅ Review: ${text}\n🔒 LOCKED: ${sn}\n\n${reminder.type}\n⏰ *Reminder: ${tStr}*\n⚠️ Click DONE when complete!`, getMainButtons());
        await remindersCollection.insertOne(reminder.data);
        await incrementStat(sn, 'reminders'); // NEW
        await incrementStat(sn, 'otherReview'); // NEW
      } else {
        await sendMessage(chatId, `✅ Review: ${text}\n🔒 LOCKED: ${sn}\n\n⚠️ Click DONE to complete!`, getMainButtons());
        await incrementStat(sn, 'otherReview'); // NEW
      }
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

  const loginStaff = speedCache.getStaffByUserName(text);
  if (loginStaff) {
    if (safeStr(loginStaff.activeStatus).toUpperCase() !== 'ACTIVE') {
      await sendMessage(chatId, '❌ NOT ACTIVE. Contact admin.', null, true);
      return;
    }
    // FIX: Use userName instead of _id to avoid null crash for new users
    await staffsCollection.updateOne({ chatId }, { $set: { chatId: '' } });
    await staffsCollection.updateOne(
      { userName: { $regex: `^${text}$`, $options: 'i' } },
      { $set: { chatId } }
    );
    const updated = await staffsCollection.findOne({ userName: { $regex: `^${text}$`, $options: 'i' } });
    const saved = updated && updated.chatId === chatId;
    await sendMessage(chatId, `✅ Switched to: ${loginStaff.name}\n🆔 ID: ${text}\n💾 ChatID Saved: ${saved ? 'YES ✅' : 'NO ❌'}\n\nClick ▶️ START LEAD`, getMainButtons());
    // Update cache with new chatId so bot works immediately
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
  const msg = `👋 *${sn}*\n🆔 \`${un}\`\n${em} *${st}*\n\n━━━━━━━━━━━━━━\n📌 *RULES*\n━━━━━━━━━━━━━━\n✅ Call / WhatsApp mandatory\n✅ Review before Done\n🔒 Locked until DONE\n⏱️ 3Hr expiry (RINGING / BUSY / NOT CON / OUT AREA)\n🔐 OTHER = Permanent Lock\n⏰ Smart Reminder: "1 ghante baad", "kal", "28 ko"\n\n💡 *STEPS*\n1️⃣ START LEAD → 2️⃣ Call → 3️⃣ REVIEW → 4️⃣ DONE`;
  await sendMessage(chatId, msg, getMainButtons());
}

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
    const rowMap = await getRowMap();
    const rowNum = rowMap[regNo];
    if (!rowNum) { await sendMessage(chatId, '❌ Lead not found in sheet.', getMainButtons()); return; }

    if (await isRowExpired(rowNum, regNo)) {
      await sendMessage(chatId, '⏱️ This lead expired (3 hours passed).\nClick ▶️ START LEAD for new.', getMainButtons());
      return;
    }

    const rowData = await getLeadRowData(rowNum);
    const lsn = safeStr(rowData[CONFIG.LEAD_COLS.STAFF_NAME]);
    if (lsn && lsn.toUpperCase() !== sName.toUpperCase()) {
      await updateLeadCells(rowNum, [{ col: CONFIG.LEAD_COLS.STAFF_NAME, value: sName }]);
    }

    const tStr = formatDateTime();

    switch (act) {
      case 'CALL': {
        let c = parseInt(rowData[CONFIG.LEAD_COLS.COUNT_DIALER] || 0) + 1;
        await updateLeadCells(rowNum, [{ col: CONFIG.LEAD_COLS.COUNT_DIALER, value: c }]);
        let mDig = safeStr(rowData[CONFIG.LEAD_COLS.MOBILE]).replace(/\D/g, '');
        if (mDig.startsWith('91') && mDig.length > 10) mDig = mDig.substring(2);
        await sendMessage(chatId, `📞 *Tap to Call*\n\n👤 ${safeStr(rowData[CONFIG.LEAD_COLS.NAME])}\n📱 +91${mDig}\n🔄 Count: ${c}\n\n👆 Tap number to dial`, getMainButtons());
        await incrementStat(sName, 'calls'); // NEW
        break;
      }
      case 'WHATSAPP': {
        let wDig = safeStr(rowData[CONFIG.LEAD_COLS.MOBILE]).replace(/\D/g, '');
        if (wDig.startsWith('91') && wDig.length > 10) wDig = wDig.substring(2);
        const wName = safeStr(rowData[CONFIG.LEAD_COLS.NAME]);
        const wReg = safeStr(rowData[CONFIG.LEAD_COLS.REG_NO]);
        const wDs = safeStr(rowData[CONFIG.LEAD_COLS.EXPIRED] || rowData[CONFIG.LEAD_COLS.DATE]);
        const wMsg = `🚗 Hello ${wName}!\n\n(*My Insurance Saathi*)\n\nAapki gaadi *${wReg}* ka insurance *${wDs}* ko expire ho raha hai / ho chuka hai.\n\n👉 Kya aap renewal karwana chahenge best price me?\n\n✅ Zero Dep\n✅ Cashless Claim\n✅ Best Company Options\n\nReply karein:\n✔ YES – Quote ke liye\n✔ CALL – Direct baat karne ke liye`;
        // FIX: Removed space after 91
        const wLink = 'https://wa.me/91' + wDig + '?text=' + encodeURIComponent(wMsg);
        await sendMessage(chatId, `📱 *WhatsApp Ready*\n\n👤 ${wName}\n📱 +${wDig}\n🚗 ${wReg}\n\n👇 Tap button:`, { inline_keyboard: [[{ text: '📱 Open WhatsApp Chat', url: wLink }]] });
        await incrementStat(sName, 'whatsapp'); // NEW
        break;
      }
      case 'REVIEW':
        await editMessage(chatId, messageId, getLeadMsg(rowData) + '\n\n📝 *Select Review:*', getReviewButtons(regNo));
        break;
      case 'RINGING':
        await updateLeadCells(rowNum, [{ col: CONFIG.LEAD_COLS.REVIEW, value: 'RINGING' }, { col: CONFIG.LEAD_COLS.STAFF_NAME, value: sName }]);
        await setCooling(regNo, 3);
        pendingReviews.delete(chatId); userLeads.set(chatId, regNo); leadUsers.set(regNo, chatId);
        await editMessage(chatId, messageId, getLeadMsg(rowData) + '\n\n⚠️ RINGING', getLeadButtons(regNo, false));
        await sendMessage(chatId, `✅ RINGING\n🔒 ${sName}\n⏱️ 3 HOURS to DONE!`, getMainButtons());
        await incrementStat(sName, 'ringing'); // NEW
        break;
      case 'NOTCONN':
        await updateLeadCells(rowNum, [{ col: CONFIG.LEAD_COLS.REVIEW, value: 'NOT CONNECTED' }, { col: CONFIG.LEAD_COLS.STAFF_NAME, value: sName }]);
        await setCooling(regNo, 3);
        pendingReviews.delete(chatId); userLeads.set(chatId, regNo); leadUsers.set(regNo, chatId);
        await editMessage(chatId, messageId, getLeadMsg(rowData) + '\n\n⚠️ NOT CONNECTED', getLeadButtons(regNo, false));
        await sendMessage(chatId, `✅ NOT CONNECTED\n🔒 ${sName}\n⏱️ 3 HOURS to DONE!`, getMainButtons());
        await incrementStat(sName, 'notConnected'); // NEW
        break;
      case 'OUTAREA':
        await updateLeadCells(rowNum, [{ col: CONFIG.LEAD_COLS.REVIEW, value: 'OUT OF AREA' }, { col: CONFIG.LEAD_COLS.STAFF_NAME, value: sName }]);
        await setCooling(regNo, 3);
        pendingReviews.delete(chatId); userLeads.set(chatId, regNo); leadUsers.set(regNo, chatId);
        await editMessage(chatId, messageId, getLeadMsg(rowData) + '\n\n⚠️ OUT OF AREA', getLeadButtons(regNo, false));
        await sendMessage(chatId, `✅ OUT OF AREA\n🔒 ${sName}\n⏱️ 3 HOURS to DONE!`, getMainButtons());
        await incrementStat(sName, 'outOfArea'); // NEW
        break;
      case 'BUSY':
        await updateLeadCells(rowNum, [{ col: CONFIG.LEAD_COLS.REVIEW, value: 'BUSY' }, { col: CONFIG.LEAD_COLS.STAFF_NAME, value: sName }]);
        await setCooling(regNo, 3);
        pendingReviews.delete(chatId); userLeads.set(chatId, regNo); leadUsers.set(regNo, chatId);
        await editMessage(chatId, messageId, getLeadMsg(rowData) + '\n\n⚠️ BUSY', getLeadButtons(regNo, false));
        await sendMessage(chatId, `✅ BUSY\n🔒 ${sName}\n⏱️ 3 HOURS to DONE!`, getMainButtons());
        await incrementStat(sName, 'busy'); // NEW
        break;
      case 'OTHER':
        await updateLeadCells(rowNum, [{ col: CONFIG.LEAD_COLS.STAFF_NAME, value: sName }]);
        pendingReviews.set(chatId, { regNo, messageId });
        userLeads.set(chatId, regNo); leadUsers.set(regNo, chatId);
        await editMessage(chatId, messageId, getLeadMsg(rowData) + '\n\n✏️ *Type review & send*\n\n💡 Examples:\n• 1 ghante baad call kro\n• kal call kro\n• 28 ko call kro\n• sunday ko call kro\n• call disconnect', null);
        await sendMessage(chatId, `✏️ Type review & send\n🔐 PERMANENT LOCK: ${sName}\n/cancel to cancel`, null, true);
        break;
      case 'DONE': {
        const rv = safeStr(rowData[CONFIG.LEAD_COLS.REVIEW]);
        if (!rv) { await sendMessage(chatId, '❌ REVIEW mandatory before DONE!', getMainButtons()); return; }
        // FIX: Prevent double-counting DONE
        const currentStatus = safeStr(rowData[CONFIG.LEAD_COLS.STATUS]).toUpperCase();
        if (currentStatus === 'DONE') {
          await sendMessage(chatId, '⚠️ Already marked DONE!', getMainButtons());
          return;
        }
        const tempReviews = ['RINGING', 'NOT CONNECTED', 'OUT OF AREA', 'BUSY'];
        const rvUpper = rv.toUpperCase();
        if (tempReviews.includes(rvUpper)) {
          await updateLeadCells(rowNum, [
            { col: CONFIG.LEAD_COLS.REVIEW, value: '' },
            { col: CONFIG.LEAD_COLS.STAFF_NAME, value: '' },
            { col: CONFIG.LEAD_COLS.STATUS, value: '' },
            { col: CONFIG.LEAD_COLS.SENT_TIME, value: '' }
          ]);
          pendingReviews.delete(chatId); userLeads.delete(chatId); leadUsers.delete(regNo);
          await deleteMessage(chatId, messageId);
          await sendMessage(chatId, `✅ ${rvUpper} done!\n🔄 Lead reset.\n⏱️ 3 HOURS cooling period.\n\nClick ▶️ START LEAD`, getMainButtons());
          return;
        }
        pendingReviews.delete(chatId); userLeads.delete(chatId); leadUsers.delete(regNo);
        await updateLeadCells(rowNum, [{ col: CONFIG.LEAD_COLS.STATUS, value: 'DONE' }, { col: CONFIG.LEAD_COLS.DONE_TIME, value: tStr }]);
        const updatedRow = await getLeadRowData(rowNum);
        await editMessage(chatId, messageId, getLeadMsg(updatedRow) + `\n\n✅ COMPLETED by ${sName} at ${tStr}`, null);
        await sendMessage(chatId, '✅ Done!\nClick ▶️ START LEAD for next.', getMainButtons());
        await incrementStat(sName, 'done'); // NEW
        break;
      }
      case 'SKIP': {
        const cr = safeStr(rowData[CONFIG.LEAD_COLS.REVIEW]);
        if (cr) { await sendMessage(chatId, `❌ SKIP blocked! Review done: ${cr}\nClick DONE.`, getMainButtons()); return; }
        await updateLeadCells(rowNum, [
          { col: CONFIG.LEAD_COLS.STAFF_NAME, value: '' },
          { col: CONFIG.LEAD_COLS.STATUS, value: '' },
          { col: CONFIG.LEAD_COLS.REVIEW, value: '' },
          { col: CONFIG.LEAD_COLS.SENT_TIME, value: '' }
        ]);
        pendingReviews.delete(chatId); await clearCooling(regNo); userLeads.delete(chatId); leadUsers.delete(regNo);
        await sendMessage(chatId, '⏭️ Skipped.\nClick ▶️ START LEAD', getMainButtons());
        break;
      }
    }
  } catch (err) {
    console.error('handleCallback ERROR:', err);
    await sendMessage(chatId, '⚠️ Button error: ' + err.message, getMainButtons());
  }
}

async function isRowExpired(rowNum, regNo) {
  const lock = await tempLocksCollection.findOne({ regNo });
  if (!lock) return false;
  if (lock.expiresAt < new Date()) {
    const rowData = await getLeadRowData(rowNum);
    const rv = safeStr(rowData[CONFIG.LEAD_COLS.REVIEW]).toUpperCase();
    const tmp = ['RINGING', 'NOT CONNECTED', 'OUT OF AREA', 'BUSY'];
    if (tmp.includes(rv)) {
      await updateLeadCells(rowNum, [
        { col: CONFIG.LEAD_COLS.STAFF_NAME, value: '' },
        { col: CONFIG.LEAD_COLS.STATUS, value: '' },
        { col: CONFIG.LEAD_COLS.REVIEW, value: '' },
        { col: CONFIG.LEAD_COLS.SENT_TIME, value: '' }
      ]);
      await tempLocksCollection.deleteOne({ regNo });
      const uc = leadUsers.get(regNo);
      if (uc) { userLeads.delete(uc); leadUsers.delete(regNo); }
      return true;
    }
  }
  return false;
}

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

  const userLeadReg = userLeads.get(chatId);
  if (userLeadReg) {
    const rowMap = await getRowMap();
    const rowNum = rowMap[userLeadReg];
    if (rowNum) {
      const rowData = await getLeadRowData(rowNum);
      const status = safeStr(rowData[CONFIG.LEAD_COLS.STATUS]).toUpperCase();
      if (status !== 'DONE') {
        if (await isRowExpired(rowNum, userLeadReg)) {
          userLeads.delete(chatId); leadUsers.delete(userLeadReg);
        } else {
          const rv = safeStr(rowData[CONFIG.LEAD_COLS.REVIEW]);
          await sendMessage(chatId, rv ? '⚠️ DONE MANDATORY! Click DONE:' : '⚠️ Active lead! Complete it:', getMainButtons());
          await sendLead(chatId, getLeadMsg(rowData), getLeadButtons(userLeadReg, rv ? false : true));
          return;
        }
      } else {
        userLeads.delete(chatId);
      }
    } else {
      userLeads.delete(chatId);
    }
  }

  const allData = await getSheetData();
  if (allData.length <= 1) {
    await sendMessage(chatId, '🎉 No leads available right now. 🏆', getMainButtons());
    return;
  }

  const locks = await tempLocksCollection.find({ expiresAt: { $gt: new Date() } }).toArray();
  const coolingSet = new Set(locks.map(l => l.regNo));

  let pends = [];
  for (let i = 1; i < allData.length; i++) {
    const row = allData[i];
    const st = safeStr(row[CONFIG.LEAD_COLS.STATUS]).toUpperCase();
    const as = safeStr(row[CONFIG.LEAD_COLS.STAFF_NAME]);
    const rv = safeStr(row[CONFIG.LEAD_COLS.REVIEW]);
    const reg = safeStr(row[CONFIG.LEAD_COLS.REG_NO]);
    if (st !== 'DONE' && st !== 'SENT' && as === '' && !coolingSet.has(reg) && rv === '') {
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

  const ts = formatDateTime();
  for (const lead of pends) {
    const rn = safeStr(lead.data[CONFIG.LEAD_COLS.REG_NO]);
    if (coolingSet.has(rn)) continue;

    const fresh = await getLeadRowData(lead.row);
    if (safeStr(fresh[CONFIG.LEAD_COLS.STATUS]).toUpperCase() === 'DONE' ||
        safeStr(fresh[CONFIG.LEAD_COLS.STATUS]).toUpperCase() === 'SENT' ||
        safeStr(fresh[CONFIG.LEAD_COLS.STAFF_NAME]) !== '' ||
        safeStr(fresh[CONFIG.LEAD_COLS.REVIEW]) !== '') {
      continue;
    }

    await updateLeadCells(lead.row, [
      { col: CONFIG.LEAD_COLS.STAFF_NAME, value: ns },
      { col: CONFIG.LEAD_COLS.STATUS, value: 'SENT' },
      { col: CONFIG.LEAD_COLS.SENT_TIME, value: ts }
    ]);

    const verify = await getLeadRowData(lead.row);
    if (safeStr(verify[CONFIG.LEAD_COLS.STAFF_NAME]).toUpperCase() === ns.toUpperCase() &&
        safeStr(verify[CONFIG.LEAD_COLS.STATUS]).toUpperCase() === 'SENT') {
      userLeads.set(chatId, rn);
      leadUsers.set(rn, chatId);
      await sendLead(chatId, getLeadMsg(verify), getLeadButtons(rn, true));
      return;
    }
  }

  await sendMessage(chatId, '⏳ All leads just taken by others!\nClick ▶️ START LEAD again.', getMainButtons());
}

async function sendLead(chatId, text, buttons) {
  return sendMessage(chatId, text, buttons);
}

// ==================== ENHANCED STATUS REPORT ====================
async function sendReport(chatId, sName) {
  const today = new Date().toLocaleDateString('en-GB');
  const sn = safeStr(sName).toUpperCase();
  
  // Get today's stats from MongoDB
  const todayStats = await statsCollection.findOne({ staffName: sName, date: today }) || {};
  
  // Get sheet data for all-time and current status counts
  const allData = await getSheetData();
  let totDone = 0, tLeads = 0, pend = 0, rev = 0;
  
  for (let i = 1; i < allData.length; i++) {
    const row = allData[i];
    const staff = safeStr(row[CONFIG.LEAD_COLS.STAFF_NAME]).toUpperCase();
    if (staff !== sn) continue;
    
    const st = safeStr(row[CONFIG.LEAD_COLS.STATUS]).toUpperCase();
    const dt = row[CONFIG.LEAD_COLS.DONE_TIME];
    const sentTime = row[CONFIG.LEAD_COLS.SENT_TIME];
    const rv = safeStr(row[CONFIG.LEAD_COLS.REVIEW]).toUpperCase();
    
    if (st === 'DONE') {
      totDone++;
    }
    
    // Count unique leads worked today (sent today or done today)
    let workedToday = false;
    if (sentTime) {
      const sds = sentTime instanceof Date ? sentTime.toLocaleDateString('en-GB') : safeStr(sentTime).split(' ')[0];
      if (sds === today) workedToday = true;
    }
    if (!workedToday && dt) {
      const dds = dt instanceof Date ? dt.toLocaleDateString('en-GB') : safeStr(dt).split(' ')[0];
      if (dds === today) workedToday = true;
    }
    if (workedToday) tLeads++;
    
    // Current status counts
    if (st !== 'DONE') {
      if (['RINGING', 'NOT CONNECTED', 'OUT OF AREA', 'BUSY'].includes(rv) || rv.length > 0) rev++;
      else pend++;
    }
  }
  
  const calls = todayStats.calls || 0;
  const whatsapp = todayStats.whatsapp || 0;
  const done = todayStats.done || 0;
  const ringing = todayStats.ringing || 0;
  const notConnected = todayStats.notConnected || 0;
  const outOfArea = todayStats.outOfArea || 0;
  const busy = todayStats.busy || 0;
  const otherReview = todayStats.otherReview || 0;
  const reminders = todayStats.reminders || 0;
  const mixCount = ringing + notConnected + outOfArea + busy;
  
  const msg = `📊 *DAILY STATUS REPORT*\n👤 *Name:* ${sName}\n📅 *Date:* ${today}\n\n` +
    `📞 *Total Call Count:* ${calls}\n` +
    `💬 *Total WhatsApp Count:* ${whatsapp}\n` +
    `✅ *Total Done Count:* ${done}\n` +
    `📋 *Total Lead Count:* ${tLeads}\n` +
    `🔄 *RINGING / NOT CON / OUT AREA / BUSY:* ${mixCount}\n` +
    `📝 *Total Other Review Count:* ${otherReview}\n` +
    `⏰ *Total Reminder Set Count:* ${reminders}\n` +
    `🏆 *All Over Done Count:* ${totDone}\n\n` +
    `⏳ *Current Pending:* ${pend}\n` +
    `📝 *Current Review:* ${rev}`;
    
  await sendMessage(chatId, msg, getMainButtons());
}

// ==================== REMINDER SYSTEM ====================
async function checkReminders() {
  await checkExpiredLocks();
  const now = new Date();
  const due = await remindersCollection.find({ fireAt: { $lte: now }, fired: false }).toArray();

  for (const rem of due) {
    try {
      const mob = safeStr(rem.customerMobile).replace(/\D/g, '');
      const cleanMob = mob.startsWith('91') && mob.length > 10 ? mob.substring(2) : mob;
      const msg = `⏰ *CALLBACK REMINDER*\n\n👤 *Customer:* ${rem.customerName}\n📱 *Mobile:* +${cleanMob}\n🚗 *Reg No:* ${rem.regNo}\n📝 *Note:* ${rem.reviewText}\n⏱️ *Type:* ${rem.reminderType}\n\n👉 *Call back now!*`;

      const rowMap = await getRowMap();
      const rowNum = rowMap[rem.regNo];
      if (rowNum) {
        const rowData = await getLeadRowData(rowNum);
        if (safeStr(rowData[CONFIG.LEAD_COLS.STATUS]).toUpperCase() === 'DONE') {
          await sendMessage(rem.chatId, msg + '\n\n⚠️ This lead is already marked DONE.', getMainButtons());
          await remindersCollection.updateOne({ _id: rem._id }, { $set: { fired: true } });
          continue;
        }
        const ts = formatDateTime();
        await updateLeadCells(rowNum, [
          { col: CONFIG.LEAD_COLS.STAFF_NAME, value: rem.staffName },
          { col: CONFIG.LEAD_COLS.STATUS, value: 'SENT' },
          { col: CONFIG.LEAD_COLS.SENT_TIME, value: ts }
        ]);
        userLeads.set(rem.chatId, rem.regNo);
        leadUsers.set(rem.regNo, rem.chatId);
        await sendLead(rem.chatId, msg + '\n\n' + getLeadMsg(rowData), getLeadButtons(rem.regNo, true));
      } else {
        await sendMessage(rem.chatId, msg, getMainButtons());
      }
      await remindersCollection.updateOne({ _id: rem._id }, { $set: { fired: true } });
    } catch (e) {
      console.error('Reminder error:', e);
    }
  }
}

async function checkExpiredLocks() {
  const expired = await tempLocksCollection.find({ expiresAt: { $lt: new Date() } }).toArray();
  const rowMap = await getRowMap();
  for (const lock of expired) {
    const rowNum = rowMap[lock.regNo];
    if (rowNum) {
      const rowData = await getLeadRowData(rowNum);
      const rv = safeStr(rowData[CONFIG.LEAD_COLS.REVIEW]).toUpperCase();
      if (['RINGING', 'NOT CONNECTED', 'OUT OF AREA', 'BUSY'].includes(rv)) {
        await updateLeadCells(rowNum, [
          { col: CONFIG.LEAD_COLS.STAFF_NAME, value: '' },
          { col: CONFIG.LEAD_COLS.STATUS, value: '' },
          { col: CONFIG.LEAD_COLS.REVIEW, value: '' },
          { col: CONFIG.LEAD_COLS.SENT_TIME, value: '' }
        ]);
      }
    }
    await tempLocksCollection.deleteOne({ _id: lock._id });
    const uc = leadUsers.get(lock.regNo);
    if (uc) { userLeads.delete(uc); leadUsers.delete(lock.regNo); }
  }
}

// ==================== ROUTES ====================
app.post('/webhook', async (req, res) => {
  res.sendStatus(200);
  processUpdate(req.body);
});

app.get('/', (req, res) => res.send('✅ Lead Bot Running on Node.js - SPEED OPTIMIZED'));

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
  const PORT = process.env.PORT || 3000;
  app.listen(PORT, async () => {
    console.log(`🚀 Server running on port ${PORT}`);
    await setWebhook();
  });
  setInterval(() => checkReminders().catch(console.error), 60000);
}

start().catch(console.error);
