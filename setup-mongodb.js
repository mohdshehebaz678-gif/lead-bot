// ==================== MONGODB SETUP SCRIPT ====================
// Run this ONCE after connecting to MongoDB
// node setup-mongodb.js

require('dotenv').config();
const { MongoClient } = require('mongodb');

const mongoUri = process.env.MONGODB_URI;
const dbName = process.env.DB_NAME || 'leadbot_db';

async function setup() {
  const client = new MongoClient(mongoUri, {
    maxPoolSize: 10,
    serverSelectionTimeoutMS: 5000
  });

  try {
    await client.connect();
    console.log('✅ Connected to MongoDB');

    const db = client.db(dbName);

    // 1. Create collections (if not exist)
    const collections = [
      'staffs',
      'reminders', 
      'tempLocks',
      'staff_stats',
      'activeAssignments',
      'pendingSheetUpdates'
    ];

    for (const collName of collections) {
      const collectionsList = await db.listCollections({ name: collName }).toArray();
      if (collectionsList.length === 0) {
        await db.createCollection(collName);
        console.log(`✅ Created collection: ${collName}`);
      } else {
        console.log(`⚡ Collection already exists: ${collName}`);
      }
    }

    // 2. Create indexes
    console.log('\n🔧 Creating indexes...');

    // staffs indexes
    await db.collection('staffs').createIndex({ userName: 1 }, { unique: true });
    console.log('✅ staffs.userName index');
    await db.collection('staffs').createIndex({ chatId: 1 });
    console.log('✅ staffs.chatId index');

    // reminders indexes
    await db.collection('reminders').createIndex({ fireAt: 1 });
    console.log('✅ reminders.fireAt index');
    await db.collection('reminders').createIndex({ regNo: 1 });
    console.log('✅ reminders.regNo index');
    await db.collection('reminders').createIndex({ staffName: 1 });
    console.log('✅ reminders.staffName index');

    // tempLocks indexes
    await db.collection('tempLocks').createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 });
    console.log('✅ tempLocks.expiresAt TTL index');
    await db.collection('tempLocks').createIndex({ regNo: 1 });
    console.log('✅ tempLocks.regNo index');

    // staff_stats indexes
    await db.collection('staff_stats').createIndex({ staffName: 1, date: 1 }, { unique: true });
    console.log('✅ staff_stats.staffName_date index');

    // activeAssignments indexes (NEW - CRITICAL)
    await db.collection('activeAssignments').createIndex({ regNo: 1 }, { unique: true });
    console.log('✅ activeAssignments.regNo UNIQUE index');
    await db.collection('activeAssignments').createIndex({ chatId: 1 });
    console.log('✅ activeAssignments.chatId index');
    await db.collection('activeAssignments').createIndex({ staffName: 1 });
    console.log('✅ activeAssignments.staffName index');
    await db.collection('activeAssignments').createIndex({ expiresAt: 1 }, { expireAfterSeconds: 0 });
    console.log('✅ activeAssignments.expiresAt TTL index (4 hours)');

    // pendingSheetUpdates indexes (NEW)
    await db.collection('pendingSheetUpdates').createIndex({ createdAt: 1 });
    console.log('✅ pendingSheetUpdates.createdAt index');
    await db.collection('pendingSheetUpdates').createIndex({ regNo: 1 });
    console.log('✅ pendingSheetUpdates.regNo index');

    console.log('\n🎉 MongoDB setup complete!');
    console.log('\n📋 Collections created:');
    console.log('  1. staffs - Staff login data');
    console.log('  2. reminders - Callback reminders');
    console.log('  3. tempLocks - 2Hr cooling locks');
    console.log('  4. staff_stats - Daily statistics');
    console.log('  5. activeAssignments - Lead assignments (NEW)');
    console.log('  6. pendingSheetUpdates - Background sheet sync queue (NEW)');

  } catch (err) {
    console.error('❌ Setup error:', err.message);
  } finally {
    await client.close();
    console.log('\n🔌 Connection closed');
  }
}

setup();
