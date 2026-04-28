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
  tCalls: { type: Number, default: 
