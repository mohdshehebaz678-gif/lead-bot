require('dotenv').config();
const express = require('express');
const mongoose = require('mongoose');
const axios = require('axios');
const cron = require('node-cron');

const app = express();
app.use(express.json());

// ============================================
// CONFIG
// ============================================
const TOKEN = process.env.BOT_TOKEN;
const API_URL = `https://api.telegram.org/bot${TOKEN}`;
const COOLING_MS = 2 * 60 * 60 * 1000;

// ============================================
// MONGODB SCHEMAS
// ============================================
const StaffSchema = new mongoose.Schema({
  userName: { type: String, required: true, unique: true, index: true },
  name: String,
  staffNo: String,
  activeStatus: { type: String, default: 'ACTIVE' },
  email: String,
  gender: String,
  chatId: { type: String, index: true },
  tLeads: { type: Number, default: 0 },
  tCalls: { type: Number, default: 0 },
  tWa: { type: Number, default: 0 },
  tSkip: { type: Number, default: 0 },
  oLeads: { type: Number, default: 0 },
  oCalls: { type: Number, default: 0 },
  oWa: { type: Number, default: 0 },
  oSkip: { type: Number, default: 0 },
  lastDate: { type: String, default: '' }
});

const LeadSchema = new mongoose.Schema({
  name: String,
  mobile: String,
  regNo: { type: String, required: true, unique: true, index: true },
  expired: Date,
  make: String,
  remark: String,
  staffName: { type: String, default: '', index: true },
  status: { type: String, default: '', index: true },
  review: { type: String, default: '' },
  date: Date,
  sentTime: Date,
  doneTime: Date,
  countDialer: { type: Number, default: 0 },
  coolingUntil: { type: Date, default: null },
  lockedBy: { type: String, default: '' },
  lockedAt: { type: Date, default: null },
  lastMessageId: { type: Number, default: null }
});

const ReminderSchema = new mongoose.Schema({
  chatId: String,
  regNo: String,
  staffName: String,
  reviewText: String,
  reminderType: String,
  fireAt: { type: Date, index: true },
  fired: { type: Boolean, default: false }
});

const Staff = mongoose.model('Staff', StaffSchema);
const Lead = mongoose.model('Lead', LeadSchema);
const Reminder = mongoose.model('Reminder', ReminderSchema);

// ============================================
// TELEGRAM HELPERS
// ============================================
async function sendMessage(chatId, text, buttons = null, removeKb = false) {
  const payload = {
    chat_id: chatId,
    text: text,
    parse_mode: 'Markdown',
    disable_web_page_preview: true
  };
  if (removeKb) {
    payload.reply_markup = JSON.stringify({ remove_keyboard: true });
  } else if (buttons) {
    payload.reply_markup = JSON.stringify(buttons);
  }
  try {
    await axios.post(`${API_URL}/sendMessage`, payload, { timeout: 5000 });
  } catch (e) {}
}

async function editMessage(chatId, messageId, text, buttons = null) {
  const payload = {
    chat_id: chatId,
    message_id: messageId,
    text: text,
    parse_mode: 'Markdown'
  };
  if (buttons) payload.reply_markup = JSON.stringify(buttons);
  try {
    await axios.post(`${API_URL}/editMessageText`, payload, { timeout: 5000 });
  } catch (e) {}
}

async function answerCallback(queryId) {
  try {
    await axios.post(`${API_URL}/answerCallbackQuery`, { callback_query_id: queryId }, { timeout: 3000 });
  } catch (e) {}
}

async function deleteMessage(chatId, messageId) {
  try {
    await axios.post(`${API_URL}/deleteMessage`, { chat_id: chatId, message_id: messageId }, { timeout: 3000 });
  } catch (e) {}
}

// ============================================
// BUTTONS
// ============================================
const mainButtons = {
  keyboard: [[{ text: '▶️ START LEAD' }], [{ text: '📊 MY STATUS' }]],
  resize_keyboard: true,
  one_time_keyboard: false
};

function leadButtons(regNo, showSkip) {
  const btns = [
    [{ text: '📞 CALL', callback_data: `CALL_${regNo}` }, { text: '💬 WHATSAPP', callback_data: `WHATSAPP_${regNo}` }],
    [{ text: '🔍 REVIEW', callback_data: `REVIEW_${regNo}` }, { text: '✅ DONE', callback_data: `DONE_${regNo}` }]
  ];
  if (showSkip) btns.push([{ text: '⏭️ SKIP', callback_data: `SKIP_${regNo}` }]);
  return { inline_keyboard: btns };
}

function reviewButtons(regNo) {
  return {
    inline_keyboard: [
      [{ text: '📞 RINGING', callback_data: `RINGING_${regNo}` }, { text: '❌ NOT CONNECTED', callback_data: `NOTCONN_${regNo}` }],
      [{ text: '📍 OUT OF AREA', callback_data: `OUTAREA_${regNo}` }, { text: '🔴 BUSY', callback_data: `BUSY_${regNo}` }],
      [{ text: '✏️ OTHER', callback_data: `OTHER_${regNo}` }]
    ]
  };
}

// ============================================
// MESSAGE BUILDERS
// ============================================
function buildLeadMsg(lead) {
  const ds = lead.expired ? lead.expired.toLocaleDateString('en-GB') : '';
  const re = (lead.remark || '').toUpperCase() === 'EXPIRE' ? '🔴 EXPIRE' : '🟢 NEW';
  let msg = `📋 *NEW LEAD*\n\n👤 ${lead.name || ''}\n📱 ${lead.mobile || ''}\n🚗 ${lead.regNo || ''}\n📅 ${ds}\n${re}\n🏭 ${lead.make || ''}\n`;
  if (lead.staffName) msg += `👨‍💼 ${lead.staffName}\n`;
  if (lead.status) msg += `📊 ${lead.status}\n`;
  if (lead.review && lead.review !== 'PENDING_OTHER') msg += `📝 ${lead.review}\n`;
  if (lead.countDialer > 0) msg += `📞 Count: ${lead.countDialer}\n`;
  msg += '\nChoose action:';
  return msg;
}

function buildWelcome(staff) {
  const em = staff.activeStatus === 'ACTIVE' ? '🟢' : '🔴';
  return `👋 *${staff.name}*\n🆔 \`${staff.userName}\`\n${em} *${staff.activeStatus}*\n\n━━━━━━━━━━━━━━\n📌 *RULES*\n━━━━━━━━━━━━━━\n✅ Call / WhatsApp mandatory\n✅ Review before Done\n🔒 Locked until DONE\n⏱️ 2Hr expiry (RINGING / BUSY / NOT CON / OUT AREA)\n🔐 OTHER = Permanent Lock\n⏰ Smart Reminder: "1 ghante baad", "kal", "28 ko"\n\n💡 *STEPS*\n1️⃣ START LEAD → 2️⃣ Call → 3️⃣ REVIEW → 4️⃣ DONE`;
}

// ============================================
// SMART REMINDER
// ============================================
function detectReminder(text) {
  const now = new Date();
  const lower = text.toLowerCase();
  
  const callbackWords = ['baad me','baadme','bad me','badme','baad mein','bad mein'];
  if (callbackWords.some(w => lower.includes(w))) return null;
  
  const finalRegex = [
    /\b(?:already|alredy|alrady)\b.*\b(?:renew|renewal|done|purchased)\b/i,
    /\b(?:renew|renewal)\b.*\b(?:ho\s*(?:gaya|gya|chuka)|done|complete)\b/i,
    /\b(?:dont|don't|do not|never)\b.*\b(?:call|disturb|record)\b/i,
    /\b(?:nahi|nhi|na|nahin)\b.*\b(?:lenaa|lena|chahiye)\b/i,
    /\b(?:sold|sale)\b.*\b(?:long\s*time|months?\s*ago)\b/i,
    /\b(?:block|blacklist)\b/i,
    /\b(?:not\s*in\s*use|band|bandh)\b/i
  ];
  if (finalRegex.some(rx => rx.test(lower))) {
    return { type: '🏁 FINAL RESPONSE — No callback needed', isFinal: true };
  }

  let time = null, type = '';
  
  const minMatch = lower.match(/(?:call\s*back\s*|callback\s*|call\s*)(\d{1,2})\s*(?:min|minute|minutes|minat|mnt|minut|minuts)/i);
  if (minMatch) { time = new Date(now.getTime() + parseInt(minMatch[1])*60000); type = `⏰ Call back in ${minMatch[1]} min(s)`; }
  
  if (!time && /(?:next\s*week|nest\s*week|agla\s*hafta|agla\s*week|agale\s*hafte)/i.test(lower)) {
    time = new Date(now.getTime() + 7*24*60*60*1000); time.setHours(10,0,0,0); type = '📅 Next week at 10:00 AM';
  }
  
  if (!time && /(?:next\s*month|agla\s*mahina|agla\s*month|agale\s*mahine)/i.test(lower)) {
    time = new Date(now.getTime() + 30*24*60*60*1000); time.setHours(10,0,0,0); type = '📅 Next month at 10:00 AM';
  }
  
  const kalMatch = lower.match(/(?:kal|kl|कल)\s*(?:subah|morning|sham|evening|raat|night|din|दिन)?\s*(\d{1,2})/i);
  if (!time && kalMatch) {
    time = new Date(now.getTime() + 24*60*60*1000); time.setHours(parseInt(kalMatch[1]),0,0,0); type = `📅 Tomorrow at ${kalMatch[1]}:00`;
  }
  
  const weekDays = [
    {names:['sunday','sun'],code:0},{names:['monday','mon'],code:1},{names:['tuesday','tue','tues'],code:2},
    {names:['wednesday','wed'],code:3},{names:['thursday','thu','thurs'],code:4},{names:['friday','fri'],code:5},{names:['saturday','sat'],code:6}
  ];
  if (!time) {
    for (const wd of weekDays) {
      if (wd.names.some(n => lower.includes(n))) {
        let daysUntil = wd.code - now.getDay();
        if (daysUntil <= 0) daysUntil += 7;
        time = new Date(now.getTime() + daysUntil*24*60*60*1000); time.setHours(10,0,0,0);
        type = `📅 Next ${wd.names[0]}`; break;
      }
    }
  }
  
  if (!time) {
    const numMatch = lower.match(/(\d{1,2})/);
    if (numMatch) {
      const num = parseInt(numMatch[1]);
      if (/(?:hour|hr|ghanta|घंटे)/.test(lower)) { time = new Date(now.getTime() + num*60*60*1000); type = `⏰ After ${num} hour(s)`; }
      else if (/(?:minute|min|minat)/.test(lower)) { time = new Date(now.getTime() + num*60*1000); type = `⏰ After ${num} min(s)`; }
      else if (/(?:day|din|दिन)/.test(lower)) { time = new Date(now.getTime() + num*24*60*60*1000); time.setHours(10,0,0,0); type = `📅 After ${num} day(s)`; }
    }
  }
  
  if (time) {
    if (time < now) time = new Date(time.getTime() + 24*60*60*1000);
    return { time, type, isFinal: false };
  }
  return null;
}

// ============================================
// COUNTER
// ============================================
async function incrementCounter(staff, type) {
  const today = new Date().toLocaleDateString('en-GB');
  if (staff.lastDate !== today) {
    staff.tLeads = staff.tCalls = staff.tWa = staff.tSkip = 0;
    staff.lastDate = today;
  }
  if (type === 'LEADS') { staff.tLeads++; staff.oLeads++; }
  else if (type === 'CALLS') { staff.tCalls++; staff.oCalls++; }
  else if (type === 'WA') { staff.tWa++; staff.oWa++; }
  else if (type === 'SKIP') { staff.tSkip++; staff.oSkip++; }
  await staff.save();
}

// ============================================
// WEBHOOK HANDLER
// ============================================
app.post('/webhook', async (req, res) => {
  res.sendStatus(200);
  
  const update = req.body;
  if (!update.message && !update.callback_query) return;
  
  const chatId = String(update.message?.chat?.id || update.callback_query?.message?.chat?.id);
  const userId = String(update.message?.from?.id || update.callback_query?.from?.id);
  if (!chatId || !userId) return;
  
  try {
    if (update.callback_query) {
      await handleCallback(update.callback_query, chatId, userId);
    } else if (update.message?.text) {
      await handleText(update.message.text.trim(), chatId, userId);
    }
  } catch (err) {
    console.error('Handler error:', err.message);
  }
});

async function handleText(text, chatId, userId) {
  if (/^MIS-/i.test(text)) {
    const staff = await Staff.findOne({ userName: text.toUpperCase() });
    if (!staff) { await sendMessage(chatId, '❌ USER NAME not found!', null, true); return; }
    if (staff.activeStatus !== 'ACTIVE') { await sendMessage(chatId, '❌ NOT ACTIVE. Contact admin.', null, true); return; }
    staff.chatId = chatId;
    await staff.save();
    await sendMessage(chatId, `✅ Switched to: ${staff.name}\n\n🆔 ID: ${text}\n\nClick ▶️ START LEAD`, mainButtons);
    return;
  }

  const staff = await Staff.findOne({ chatId });
  
  if (text === '/start') {
    if (staff) await sendMessage(chatId, buildWelcome(staff), mainButtons);
    else await sendMessage(chatId, '👋 Welcome!\n\nSend USER NAME to login.\nExample: MIS-SHAIK-1843', null, true);
    return;
  }

  if (!staff) {
    await sendMessage(chatId, '❌ Send your USER NAME first.\nExample: MIS-SHAIK-1843', null, true);
    return;
  }
  
  if (staff.activeStatus !== 'ACTIVE') {
    await sendMessage(chatId, '❌ NOT ACTIVE. Contact admin.', null, true);
    return;
  }

  const pending = await Lead.findOne({ lockedBy: chatId, review: 'PENDING_OTHER' });
  if (pending && !text.startsWith('/') && text !== '▶️ START LEAD' && text !== '📊 MY STATUS') {
    if (text === '/cancel') {
      pending.review = '';
      await pending.save();
      await sendMessage(chatId, '❌ Review cancelled.\n🔒 Lead locked.', mainButtons);
      return;
    }
    
    const reminder = detectReminder(text);
    pending.review = text;
    pending.staffName = staff.name;
    await pending.save();
    
    if (pending.lastMessageId) {
      await editMessage(chatId, pending.lastMessageId, buildLeadMsg(pending) + '\n\n✏️ *Review:* ' + text, leadButtons(pending.regNo, false));
    }
    
    if (reminder && !reminder.isFinal) {
      await Reminder.create({
        chatId, regNo: pending.regNo, staffName: staff.name,
        reviewText: text, reminderType: reminder.type, fireAt: reminder.time
      });
      const tStr = reminder.time.toLocaleString('en-GB');
      await sendMessage(chatId, `✅ Review: ${text}\n🔒 LOCKED: ${staff.name}\n\n${reminder.type}\n⏰ *${tStr}*\n⚠️ Click DONE!`, mainButtons);
    } else if (reminder && reminder.isFinal) {
      await sendMessage(chatId, `✅ Review: ${text}\n🔒 LOCKED: ${staff.name}\n\n${reminder.type}\n\n⚠️ *Click DONE!*`, mainButtons);
    } else {
      await sendMessage(chatId, `✅ Review: ${text}\n🔒 LOCKED: ${staff.name}\n\n⚠️ Click DONE!`, mainButtons);
    }
    return;
  }

  if (text === '/next' || text === '▶️ START LEAD') {
    await sendNext(chatId, staff);
  } else if (text === '/status' || text === '📊 MY STATUS') {
    await sendReport(chatId, staff);
  } else {
    await sendMessage(chatId, buildWelcome(staff), mainButtons);
  }
}

async function handleCallback(cq, chatId, userId) {
  await answerCallback(cq.id);
  const data = cq.data;
  const mid = cq.message.message_id;
  const act = data.split('_')[0];
  const regNo = data.substring(data.indexOf('_') + 1);
  
  const staff = await Staff.findOne({ chatId });
  if (!staff) { await sendMessage(chatId, '❌ Session expired. Send /start', mainButtons); return; }
  if (staff.activeStatus !== 'ACTIVE') { await sendMessage(chatId, '❌ NOT ACTIVE. Contact admin.', mainButtons); return; }
  
  const lead = await Lead.findOne({ regNo });
  if (!lead) { await sendMessage(chatId, '❌ Lead not found.', mainButtons); return; }
  
  if (lead.coolingUntil && lead.coolingUntil > new Date() && lead.status !== 'DONE') {
    await sendMessage(chatId, '⏱️ Lead expired (2 hours).\nClick ▶️ START LEAD for new.', mainButtons);
    return;
  }
  
  if (lead.staffName && lead.staffName.toUpperCase() !== staff.name.toUpperCase()) {
    lead.staffName = staff.name;
  }
  
  const now = new Date();
  const tStr = now.toLocaleString('en-GB');

  switch(act) {
    case 'CALL': {
      lead.countDialer++;
      await lead.save();
      await incrementCounter(staff, 'CALLS');
      let mDig = (lead.mobile || '').replace(/\D/g, '');
      if (mDig.startsWith('91') && mDig.length > 10) mDig = mDig.slice(2);
      await sendMessage(chatId, `📞 *Tap to Call*\n\n👤 ${lead.name || ''}\n📱 +91${mDig}\n🔄 Count: ${lead.countDialer}\n\n👆 Tap to dial`, mainButtons);
      break;
    }
    
    case 'WHATSAPP': {
      await incrementCounter(staff, 'WA');
      let wDig = (lead.mobile || '').replace(/\D/g, '');
      if (wDig.startsWith('91') && wDig.length > 10) wDig = wDig.slice(2);
      const wDs = lead.expired ? lead.expired.toLocaleDateString('en-GB') : '';
      const wMsg = `🚗 Hello ${lead.name || ''}!\n\n(*My Insurance Saathi*)\n\nAapki gaadi *${lead.regNo}* ka insurance *${wDs}* ko expire ho raha hai.\n\n👉 Best price me renew?\n\n✅ Zero Dep\n✅ Cashless Claim\n✅ Best Companies\n\nReply: YES / CALL`;
      const wLink = `https://wa.me/91${wDig}?text=${encodeURIComponent(wMsg)}`;
      await sendMessage(chatId, `📱 *WhatsApp Ready*\n\n👤 ${lead.name || ''}\n📱 +${wDig}\n🚗 ${lead.regNo}\n\n👇 Tap button:`, { inline_keyboard: [[{ text: '📱 Open WhatsApp Chat', url: wLink }]] });
      break;
    }
    
    case 'REVIEW':
      await editMessage(chatId, mid, buildLeadMsg(lead) + '\n\n📝 *Select Review:*', reviewButtons(regNo));
      break;
    
    case 'RINGING':
    case 'NOTCONN':
    case 'OUTAREA':
    case 'BUSY': {
      const map = { RINGING: 'RINGING', NOTCONN: 'NOT CONNECTED', OUTAREA: 'OUT OF AREA', BUSY: 'BUSY' };
      const rv = map[act];
      lead.review = rv;
      lead.staffName = staff.name;
      lead.coolingUntil = new Date(Date.now() + COOLING_MS);
      lead.lockedBy = chatId;
      lead.lockedAt = now;
      await lead.save();
      await editMessage(chatId, mid, buildLeadMsg(lead) + '\n\n⚠️ ' + rv, leadButtons(regNo, false));
      await sendMessage(chatId, `✅ ${rv}\n🔒 ${staff.name}\n⏱️ 2 HOURS to DONE!`, mainButtons);
      break;
    }
    
    case 'OTHER': {
      lead.staffName = staff.name;
      lead.review = 'PENDING_OTHER';
      lead.lockedBy = chatId;
      lead.lockedAt = now;
      lead.lastMessageId = mid;
      await lead.save();
      await editMessage(chatId, mid, buildLeadMsg(lead) + '\n\n✏️ *Type review & send*\n\n💡 Examples:\n• 1 ghante baad\n• kal\n• 28 ko\n• call disconnect', null);
      await sendMessage(chatId, `✏️ Type review & send\n🔐 PERMANENT LOCK: ${staff.name}\n/cancel to cancel`, null, true);
      break;
    }
    
    case 'DONE': {
      if (!lead.review || lead.review === 'PENDING_OTHER') {
        await sendMessage(chatId, '❌ REVIEW mandatory before DONE!', mainButtons);
        return;
      }
      const tempReviews = ['RINGING', 'NOT CONNECTED', 'OUT OF AREA', 'BUSY'];
      const isTemp = tempReviews.includes(lead.review.toUpperCase());
      
      lead.status = 'DONE';
      lead.doneTime = now;
      lead.lockedBy = '';
      if (isTemp) lead.coolingUntil = new Date(Date.now() + COOLING_MS);
      await lead.save();
      
      if (isTemp) {
        await deleteMessage(chatId, mid);
        await sendMessage(chatId, `✅ ${lead.review} DONE!\n🔒 ${staff.name}\n⏱️ 2 HOURS cooling.\n\nClick ▶️ START LEAD`, mainButtons);
      } else {
        await editMessage(chatId, mid, buildLeadMsg(lead) + `\n\n✅ COMPLETED by ${staff.name} at ${tStr}`, null);
        await sendMessage(chatId, '✅ Done!\nClick ▶️ START LEAD for next.', mainButtons);
      }
      break;
    }
    
    case 'SKIP': {
      if (lead.review && lead.review !== 'PENDING_OTHER') {
        await sendMessage(chatId, `❌ SKIP blocked! Review done: ${lead.review}\nClick DONE.`, mainButtons);
        return;
      }
      await incrementCounter(staff, 'SKIP');
      lead.staffName = '';
      lead.status = '';
      lead.review = '';
      lead.lockedBy = '';
      lead.coolingUntil = null;
      await lead.save();
      await sendMessage(chatId, '⏭️ Skipped.\nClick ▶️ START LEAD', mainButtons);
      break;
    }
  }
}

async function sendNext(chatId, staff) {
  const active = await Lead.findOne({ lockedBy: chatId, status: { $ne: 'DONE' } });
  if (active) {
    const msg = active.review && active.review !== 'PENDING_OTHER' ? '⚠️ DONE MANDATORY!' : '⚠️ Active lead! Complete it:';
    await sendMessage(chatId, msg, mainButtons);
    await sendMessage(chatId, buildLeadMsg(active), leadButtons(active.regNo, !active.review));
    return;
  }

  const now = new Date();
  const lead = await Lead.findOneAndUpdate(
    {
      status: { $nin: ['DONE', 'SENT'] },
      staffName: '',
      $or: [{ coolingUntil: null }, { coolingUntil: { $lte: now } }]
    },
    {
      staffName: staff.name,
      status: 'SENT',
      sentTime: now,
      lockedBy: chatId,
      lockedAt: now
    },
    { sort: { remark: -1, expired: 1 }, new: true }
  );

  if (!lead) {
    await sendMessage(chatId, '🎉 No leads available now. 🏆', mainButtons);
    return;
  }

  await incrementCounter(staff, 'LEADS');
  await sendMessage(chatId, buildLeadMsg(lead), leadButtons(lead.regNo, true));
}

async function sendReport(chatId, staff) {
  const tds = new Date().toLocaleDateString('en-GB');
  const msg = `📊 *TODAY — ${tds}*\n👤 ${staff.name}\n\n📥 Leads: ${staff.tLeads}\n📞 Calls: ${staff.tCalls}\n💬 WhatsApp: ${staff.tWa}\n⏭️ Skips: ${staff.tSkip}\n\n━━━━━━━━━━━━\n🏆 *OVERALL*\n📥 ${staff.oLeads} | 📞 ${staff.oCalls} | 💬 ${staff.oWa} | ⏭️ ${staff.oSkip}`;
  await sendMessage(chatId, msg, mainButtons);
}

// ============================================
// REMINDER CRON
// ============================================
cron.schedule('* * * * *', async () => {
  const now = new Date();
  const reminders = await Reminder.find({ fired: false, fireAt: { $lte: now } });
  for (const r of reminders) {
    const msg = `⏰ *CALLBACK REMINDER*\n\n🚗 Reg: ${r.regNo}\n📝 ${r.reviewText}\n⏱️ ${r.reminderType}\n\n👉 *Call back now!*`;
    await sendMessage(r.chatId, msg, mainButtons);
    r.fired = true;
    await r.save();
  }
});

// ============================================
// DATA IMPORT
// ============================================
app.post('/import-staff', async (req, res) => {
  try {
    await Staff.insertMany(req.body.staffList, { ordered: false });
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: true, note: 'Some duplicates skipped' });
  }
});

app.post('/import-leads', async (req, res) => {
  try {
    await Lead.insertMany(req.body.leadsList, { ordered: false });
    res.json({ ok: true });
  } catch (e) {
    res.json({ ok: true, note: 'Some duplicates skipped' });
  }
});

app.get('/', (req, res) => res.send('✅ Lead Bot is running!'));
async function syncStaffFromSheet() {
  try {
    const res = await axios.get(process.env.SHEET_API);
    const staffList = res.data;

    for (const s of staffList) {
      if (!s.userName) continue;

      await Staff.updateOne(
        { userName: s.userName },
        { $set: s },
        { upsert: true }
      );
    }

    console.log("✅ Staff synced from Google Sheet:", staffList.length);
  } catch (err) {
    console.error("❌ Sync error:", err.message);
  }
}
// ============================================
// START
// ============================================
const PORT = process.env.PORT || 8080;

mongoose.connect(process.env.MONGODB_URI)
  .then(() => {
    console.log('✅ MongoDB connected');
    app.listen(PORT, '0.0.0.0', () => {
      console.log(`🚀 Server on port ${PORT}`);
      axios.post(`${API_URL}/setWebhook`, { url: process.env.WEBHOOK_URL })
        .then(() => console.log('✅ Webhook set'))
        .catch(err => console.error('Webhook error:', err.message));
    });
  })
  .catch(err => console.error('MongoDB error:', err));
