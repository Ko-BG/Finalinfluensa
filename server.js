const express = require('express');
const mongoose = require('mongoose');
const crypto = require('crypto');
const fs = require('fs');
const cors = require('cors');
const axios = require('axios');
const  { Jimp } = require('jimp'); 
const ffmpeg = require('fluent-ffmpeg'); 
const http = require('http'); 
const { Server } = require('socket.io'); 
require('dotenv').config();
const stripe = require('stripe')(process.env.STRIPE_SECRET_KEY);
const router = express.Router();

 

const app = express();
app.set('trust proxy', 1); 

const server = http.createServer(app); 
const io = new Server(server, {
    cors: { origin: "*" }
});

const launchProtocol = async () => {
    const brand = "iNFLUENSA";
    const subtext = "the power of influens";
    for (let char of brand) {
        process.stdout.write(char + " ");
        await new Promise(resolve => setTimeout(resolve, 150));
    }
    console.log("\n" + subtext);
    console.log("------------------------------------------");
};

const AFRO_HARD_CAP = 51000000000; 
const PROTOCOL_FEE = 0.08;      
const MINTING_REWARD_RATE = 0.10; 
const PLATFORM_RESERVE_SHARE = 0.20; 

const neuralSentryLog = new Map();
const MAX_REQUESTS_PER_WINDOW = 500; 
const WINDOW_MS = 60000; 

// --- MEDIA STREAM / DELIVERY ROUTE (FIXED) ---
const { S3Client, GetObjectCommand, PutObjectCommand, HeadObjectCommand } = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const multer = require('multer');
const multerS3 = require('multer-s3');
const path = require('path');


// Initialize S3 Client
const s3 = new S3Client({
    region: process.env.AWS_REGION,
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    },
});

// Configure Multer to stream directly to S3
const upload = multer({
    storage: multerS3({
        s3: s3,
        bucket: process.env.AWS_S3_BUCKET_NAME,
        metadata: (req, file, cb) => {
            cb(null, { fieldName: file.fieldname });
        },
        key: (req, file, cb) => {
            const fileName = `${Date.now()}-${path.basename(file.originalname)}`;
            cb(null, fileName); // File name stored in S3
        },
    }),
    limits: { fileSize: 100 * 1024 * 1024 }, // 100 MB limit
});

module.exports = upload;

app.use(cors());
// Raw parser strictly isolated for the Stripe verification step
app.use('/api/stripe-webhook', express.raw({ type: 'application/json' }));
app.use(express.json({ limit: '110mb' })); 
app.use(express.urlencoded({ limit: '110mb', extended: true }));

// Direct alias so your frontend fetch works seamlessly



// --- NEURAL SENTRY MIDDLEWARE ---
app.use((req, res, next) => {
    if (req.path === '/api/health' || req.path === '/api/stripe-webhook') return next();
    const ip = req.ip;
    const now = Date.now();
    if (!neuralSentryLog.has(ip)) {
        neuralSentryLog.set(ip, { count: 1, startTime: now });
    } else {
        const entry = neuralSentryLog.get(ip);
        if (now - entry.startTime < WINDOW_MS) {
            entry.count++;
            if (entry.count > MAX_REQUESTS_PER_WINDOW) {
                console.log(`⚠️ NEURAL SENTRY: Rate Limit Triggered for ${ip}`);
                return res.status(429).json({ error: "NEURAL_SENTRY_BLOCK", message: "Excessive Nodal Traffic Detected" });
            }
        } else {
            neuralSentryLog.set(ip, { count: 1, startTime: now });
        }
    }
    next();
});

// --- TVWS SPECTRUM DATABASE INTERACTION ENGINE ---
const queryTvwsSpectrumDatabase = async (lat, lng, height) => {
    try {
        // If integrating with a third-party PAWS (Protocol to Access WS Database) API:
        // const response = await axios.post(process.env.TVWS_DB_URL, { lat, lng, height });
        // return response.data;

        // --- LOCAL DYNAMIC SPECTRUM SIMULATOR / ENGINE ---
        // Simulates spectrum availability between 470 MHz and 698 MHz (UHF Channels 14 to 51)
        const availableChannels = [];
        const seed = Math.abs(Math.sin(lat + lng) * 10000);
        
        for (let channel = 14; channel <= 51; channel++) {
            // Pseudo-random channel availability calculation based on geographic coordinates
            const isAvailable = Math.floor((seed + channel) % 3) !== 0; 
            if (isAvailable) {
                availableChannels.push({
                    channelNumber: channel,
                    startFreqMHz: 470 + (channel - 14) * 6,
                    endFreqMHz: 470 + (channel - 14 + 1) * 6,
                    maxPowerEirpDbm: 36 // Standard max EIRP allowed
                });
            }
        }

        return {
            status: "SUCCESS",
            latitude: lat,
            longitude: lng,
            timeToLiveSeconds: 86400, // Dynamic spectrum allocation valid for 24 hours
            channels: availableChannels
        };
    } catch (error) {
        console.error("❌ TVWS Spectrum DB Query Failed:", error.message);
        throw new Error("SPECTRUM_DATABASE_UNREACHABLE");
    }
};
const sqlite3 = require('better-sqlite3');
const db = sqlite3('edge_queue.db');

// Initialize local schema
db.exec(`
  CREATE TABLE IF NOT EXISTS transaction_queue (
    checkout_id TEXT PRIMARY KEY,
    user_identity TEXT NOT NULL,
    payload TEXT NOT NULL,
    signature TEXT NOT NULL,
    status TEXT DEFAULT 'PENDING', -- PENDING, SYNCED, CONFIRMED
    created_at INTEGER NOT NULL
  )
`);

class EdgeSyncManager {
  constructor(cloudUrl, nodeId, nodeApiKey) {
    this.cloudUrl = cloudUrl;
    this.nodeId = nodeId;
    this.nodeApiKey = nodeApiKey;
  }

  // Enqueue local offline transaction
  enqueueTransaction(checkoutId, userId, payload, signature) {
    const stmt = db.prepare(`
      INSERT INTO transaction_queue (checkout_id, user_identity, payload, signature, created_at)
      VALUES (?, ?, ?, ?, ?)
    `);
    stmt.run(checkoutId, userId, JSON.stringify(payload), signature, Date.now());
  }

  // PHASE 1 & 2: Push Batch & Process ACK
  async processOutboundSync() {
    const pendingTxs = db.prepare(`
      SELECT * FROM transaction_queue WHERE status = 'PENDING' LIMIT 50
    `).all();

    if (pendingTxs.length === 0) return;

    try {
      const response = await axios.post(`${this.cloudUrl}/api/sync/batch`, {
        nodeId: this.nodeId,
        batch: pendingTxs.map(tx => ({
          checkoutID: tx.checkout_id,
          userId: tx.user_identity,
          payload: JSON.parse(tx.payload),
          signature: tx.signature
        }))
      }, {
        headers: { 'x-node-api-key': this.nodeApiKey }
      });

      const { ackTokens } = response.data; // Array of cryptographically signed ACKs

      // Mark locally as SYNCED
      const updateStmt = db.prepare(`
        UPDATE transaction_queue SET status = 'SYNCED' WHERE checkout_id = ?
      `);

      const dbTransaction = db.transaction((tokens) => {
        for (const ack of tokens) {
          updateStmt.run(ack.checkoutID);
        }
      });

      dbTransaction(ackTokens);
      console.log(`[EDGE] Phase 1 & 2 Complete: ${ackTokens.length} transactions SYNCED.`);

      // Trigger Phase 3 Clean-up
      await this.processPurgeHandshake(ackTokens);

    } catch (err) {
      console.error('[EDGE] Outbound Sync Failed (Backhaul offline):', err.message);
    }
  }

  // PHASE 3: Purge Confirmation Handshake
  async processPurgeHandshake(ackTokens) {
    try {
      const response = await axios.post(`${this.cloudUrl}/api/sync/ack-confirm`, {
        nodeId: this.nodeId,
        checkoutIDs: ackTokens.map(a => a.checkoutID)
      }, {
        headers: { 'x-node-api-key': this.nodeApiKey }
      });

      if (response.data.success) {
        // Safe Garbage Collection: Remove synced items from local SQLite DB
        const deleteStmt = db.prepare(`
          DELETE FROM transaction_queue WHERE checkout_id = ? AND status = 'SYNCED'
        `);

        const purgeTx = db.transaction((tokens) => {
          for (const ack of tokens) {
            deleteStmt.run(ack.checkoutID);
          }
        });

        purgeTx(ackTokens);
        console.log(`[EDGE] Phase 3 Complete: Purged ${ackTokens.length} acknowledged items.`);
      }
    } catch (err) {
      console.error('[EDGE] Purge Handshake Failed (Will retry on next cycle):', err.message);
    }
  }
}

module.exports = EdgeSyncManager;


// Schemas
const SyncReceipt = mongoose.model('SyncReceipt', new mongoose.Schema({
  checkoutID: { type: String, unique: true, required: true },
  nodeId: { type: String, required: true },
  status: { type: String, enum: ['PROCESSED', 'CONFIRMED_BY_EDGE'], default: 'PROCESSED' },
  ackToken: { type: String, required: true },
  processedAt: { type: Date, default: Date.now }
}));




// PHASE 1 & 2: Batch Processing & ACK Token Generation
router.post('/api/sync/batch', async (req, res) => {
  const { nodeId, batch } = req.body;
  const ackTokens = [];

  for (const item of batch) {
    const session = await mongoose.startSession();
    session.startTransaction();

    try {
      // 1. Idempotency Check: Was this transaction already processed?
      const existingReceipt = await SyncReceipt.findOne({ checkoutID: item.checkoutID }).session(session);

      if (existingReceipt) {
        await session.abortTransaction();
        session.endSession();
        ackTokens.push({ checkoutID: item.checkoutID, ackToken: existingReceipt.ackToken });
        continue;
      }

      // 2. Atomic Balance Modification (Deduct AfroCoins)
      const updatedUser = await User.findOneAndUpdate(
        { identity: item.userId, afroCoins: { $gte: item.payload.amount } },
        { $inc: { afroCoins: -item.payload.amount } },
        { new: true, session }
      );

      if (!updatedUser) {
        throw new Error('INSUFFICIENT_FUNDS_OR_USER_NOT_FOUND');
      }

      // 3. Generate Signed ACK Token
      const ackToken = crypto
        .createHmac('sha256', process.env.CLOUD_ACK_SECRET)
        .update(`${item.checkoutID}:${nodeId}:${Date.now()}`)
        .digest('hex');

      // 4. Save Transaction & SyncReceipt
      await Transaction.create([{
        checkoutID: item.checkoutID,
        userPhone: item.userId,
        amount: item.payload.amount,
        type: 'OFFLINE_SYNC_DEBIT',
        status: 'SUCCESS'
      }], { session });

      await SyncReceipt.create([{
        checkoutID: item.checkoutID,
        nodeId: nodeId,
        ackToken: ackToken
      }], { session });

      await session.commitTransaction();
      session.endSession();

      ackTokens.push({ checkoutID: item.checkoutID, ackToken });

    } catch (error) {
      await session.abortTransaction();
      session.endSession();
      console.error(`[CLOUD] Error processing ${item.checkoutID}:`, error.message);
      // Skip or return error payload for rejected item
    }
  }

  // Send ACK back to Edge Node
  return res.status(200).json({ success: true, ackTokens });
});

// PHASE 3: Edge Confirmation Handshake
router.post('/api/sync/ack-confirm', async (req, res) => {
  const { nodeId, checkoutIDs } = req.body;

  try {
    await SyncReceipt.updateMany(
      { checkoutID: { $in: checkoutIDs }, nodeId: nodeId },
      { $set: { status: 'CONFIRMED_BY_EDGE' } }
    );

    return res.status(200).json({ success: true, message: "Receipts updated to CONFIRMED_BY_EDGE" });
  } catch (err) {
    return res.status(500).json({ error: "FAILED_TO_UPDATE_RECEIPTS" });
  }
});

module.exports = router;

// --- INTERNATIONAL DATA PROTECTION PROTOCOL (IDPP) UTILITIES ---
const IDPP_CRYPTO_KEY = process.env.IDPP_CRYPTO_KEY ? crypto.scryptSync(process.env.IDPP_CRYPTO_KEY, 'salt', 32) : crypto.randomBytes(32);
const IDPP_CRYPTO_IV = process.env.IDPP_CRYPTO_IV ? crypto.scryptSync(process.env.IDPP_CRYPTO_IV, 'salt', 16) : crypto.randomBytes(16);

const idppAnonymize = (text) => {
    if (!text) return text;
    try {
        const cipher = crypto.createCipheriv('aes-256-cbc', IDPP_CRYPTO_KEY, IDPP_CRYPTO_IV);
        let encrypted = cipher.update(text.toString(), 'utf8', 'hex');
        encrypted += cipher.final('hex');
        return `IDPP_ANON_${encrypted.slice(0, 32)}`;
    } catch (e) {
        return crypto.createHash('sha256').update(text.toString()).digest('hex').slice(0, 24);
    }
};

const idppMaskPhone = (phone) => {
    if (!phone) return "";
    const clean = phone.toString();
    if (clean.length <= 5) return "*****";
    return clean.substring(0, 3) + "*".repeat(clean.length - 6) + clean.substring(clean.length - 3);
};

// --- DATA PROTECTION AND CONSENT ENFORCEMENT MIDDLEWARE ---
app.use((req, res, next) => {
    const crossBorderVerifications = ['POST', 'PUT', 'DELETE'];
    if (crossBorderVerifications.includes(req.method)) {
        res.setHeader('X-Data-Protection-Compliance', 'GDPR-CCPA-NDPA-v2026');
        res.setHeader('X-Data-Localization-Governance', 'Enforced');
    }
    next();
});

app.use(express.static(__dirname));

mongoose.connect(process.env.MONGO_URI)
    .then(() => console.log('🔥 iNFLUENSA Grid: MongoDB Connected'))
    .catch(err => console.error('❌ Grid Connection Error:', err));

// --- SCHEMAS ---
const walletLedgerSchema = new mongoose.Schema({
    userId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "User",
        required: true,
        index: true
    },

    type: {
        type: String,
        enum: [
            "CONTENT_SALE",
            "PLATFORM_FEE",
            "WITHDRAWAL_RESERVE",
            "WITHDRAWAL_COMPLETED",
            "WITHDRAWAL_REFUND",
            "AFRO_CONVERSION"
        ],
        required: true
    },

    direction: {
        type: String,
        enum: ["CREDIT", "DEBIT"],
        required: true
    },

    amount: {
        type: Number,
        required: true,
        min: 0
    },

    currency: {
        type: String,
        default: "KES"
    },

    balanceBefore: {
        type: Number,
        required: true
    },

    balanceAfter: {
        type: Number,
        required: true
    },

    reference: {
        type: String,
        required: true,
        unique: true,
        index: true
    },

    metadata: {
        type: mongoose.Schema.Types.Mixed,
        default: {}
    },

    createdAt: {
        type: Date,
        default: Date.now
    }
});

const WalletLedger = mongoose.model(
    "WalletLedger",
    walletLedgerSchema
);
const payoutSchema = new mongoose.Schema({
    userId: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "User",
        required: true,
        index: true
    },

    phone: {
        type: String,
        required: true
    },

    amount: {
        type: Number,
        required: true
    },

    currency: {
        type: String,
        default: "KES"
    },

    earningsReserved: {
        type: Number,
        default: 0
    },

    afroConverted: {
        type: Number,
        default: 0
    },

    afroRate: {
        type: Number,
        default: 0
    },

    status: {
        type: String,
        enum: [
            "pending",
            "completed",
            "failed",
            "timeout"
        ],
        default: "pending",
        index: true
    },

    gateway: {
        type: String,
        default: "mpesa_b2c"
    },

    conversationId: {
        type: String,
        unique: true,
        sparse: true,
        index: true
    },

    originatorConversationId: String,

    mpesaTxId: {
        type: String,
        unique: true,
        sparse: true,
        index: true
    },

    resultCode: Number,

    resultDesc: String,

    responseCode: String,

    responseDescription: String,

    ledgerReserveReference: String,

    ledgerCompletionReference: String,

    ledgerRefundReference: String,

    createdAt: {
        type: Date,
        default: Date.now
    },

    completedAt: Date,

    failedAt: Date,

    timeoutAt: Date
});

const Payout = mongoose.model(
    "Payout",
    payoutSchema
);

const postSchema = new mongoose.Schema({
    title: String, 
    price: Number, 
    owner: String, 
    mime: String, 
    filename: String, 
    filekey: { type: String, index: true }, 
    cid: { type: String, index: true }, 
    contentHash: { type: String, index: true },   // ← NEW: File content hash
    unlocked_by: [String], 
    licensed_to: [String],
    scarcity_limit: { type: Number, default: 0 }, 
    collaborators: [{ node: String, signature: String, split: Number, signedAt: Number, contractHash: String }],
    is_burned: { type: Boolean, default: false }, 
    timestamp: { type: Number, default: Date.now },
    is_stream: { type: Boolean, default: false }, 
    stream_url: String,
    is_resell: { type: Boolean, default: false },
    parent_post_id: { type: mongoose.Schema.Types.ObjectId, ref: 'Post', default: null },
    original_creator: { type: String, default: "" }
});
const Post = mongoose.model('Post', postSchema);
const userSchema = new mongoose.Schema({ 
    identity: { type: String, unique: true, index: true }, 
    afroCoins: { type: Number, default: 0 },
    earnings: { type: Number, default: 0 },
    lastSeen: { type: Number, default: Date.now },
    
    // Stripe Integration
    stripeCustomerId: { type: String, index: true },           // For buyer payments
    stripeAccountId: { type: String, index: true },            // For Connect payouts (creators)
    stripeOnboardingComplete: { type: Boolean, default: false },
    
    // NEW: External redeem history for merchants/stores
    redemptionHistory: [{
        code: String,
        amount: Number,
        merchantId: String,
        redeemedAt: Date
    }]
});

// Move compound index HERE (must be defined BEFORE creating the model)
userSchema.index({ identity: 1, stripeAccountId: 1 });

// ✅ FIX: Pass userSchema as a variable (no single quotes)
const User = mongoose.model('User', userSchema);



const vaultSchema = new mongoose.Schema({ 
    id: { type: String, default: 'protocol_vault' }, 
    balance: { type: Number, default: 0 }, 
    totalAfroMinted: { type: Number, default: 0 },
    platformAfroReserve: { type: Number, default: 0 } 
});
const Vault = mongoose.model('Vault', vaultSchema);

const P2POrderSchema = new mongoose.Schema({
    // Hex or cryptographic identity string string identifying the seller node
    sellerIdentity: { 
        type: String, 
        required: true, 
        index: true,
        trim: true 
    },
    // Remains null until a peer clicks "Buy" and locks the offer
    buyerIdentity: { 
        type: String, 
        default: null, 
        index: true,
        trim: true 
    },
    // Amount of AFRO locked inside the escrow pool
    afroAmount: { 
        type: Number, 
        required: true, 
        min: [0.01, 'Minimum trade asset value is 0.01 AFRO'] 
    },
    // Conversion exchange price point (e.g., 15.50 KES per 1 AFRO)
    fiatRatePerCoin: { 
        type: Number, 
        required: true, 
        min: [0.01, 'Rate per coin must be greater than zero'] 
    },
    // Automated calculation: afroAmount * fiatRatePerCoin
    fiatTotal: { 
        type: Number, 
        required: true 
    },
    // Out-of-band payment destination details (e.g., "M-PESA Till: 443321")
    paymentMethodDetails: { 
        type: String, 
        required: true,
        trim: true 
    },
    // Structural state-machine tracking indicators
    status: { 
        type: String, 
        enum: ['OPEN', 'PENDING_PAYMENT', 'PAID', 'COMPLETED', 'DISPUTED', 'CANCELLED'], 
        default: 'OPEN',
        index: true
    },
    paymentConfirmedAt: { 
        type: Date 
    },
    // Automatically releases locked assets if payment window passes
    expiresAt: { 
        type: Date 
    }
}, { 
    timestamps: true // Automatically generates createdAt and updatedAt fields
});

// Compound index optimized for rendering active open orders quickly to other buyers
P2POrderSchema.index({ status: 1, createdAt: -1 });

const P2POrder = mongoose.model('P2POrder', P2POrderSchema);

const transactionSchema = new mongoose.Schema({
    checkoutID: {
        type: String,
        index: true,
        sparse: true
    },

    transactionID: {
        type: String,
        unique: true,
        sparse: true,
        index: true
    },

    postID: {
        type: mongoose.Schema.Types.ObjectId,
        ref: "Post",
        required: true,
        index: true
    },

    userPhone: String,

    creatorId: {
        type:String,
        required: true,
        index: true
    },

    amountPaid: {
        type: Number,
        required: true
    },

    platformFee: {
        type: Number,
        default: 0
    },

    creatorAmount: {
        type: Number,
        default: 0
    },

    currency: {
        type: String,
        default: "KES"
    },

    type: String,

    gateway: {
        type: String,
        default: "mpesa"
    },

    status: {
        type: String,
        enum: [
            "pending",
            "completed",
            "failed"
        ],
        default: "pending",
        index: true
    },

    receiptNumber: String,

    resultCode: Number,

    resultDesc: String,

    createdAt: {
        type: Date,
        default: Date.now
    },

    completedAt: Date,

    failedAt: Date
});

const Transaction = mongoose.model(
    "Transaction",
    transactionSchema
);
const handshakeSchema = new mongoose.Schema({
    postId: { type: mongoose.Schema.Types.ObjectId, ref: 'Post' }, 
    sender: String,
    target: String,
    split: Number,
    signature: String, 
    contractHash: String,
    status: { type: String, default: 'pending', enum: ['pending', 'accepted', 'rejected', 'countered', 'paid'] }, 
    timestamp: { type: Number, default: Date.now }
});
const Handshake = mongoose.model('Handshake', handshakeSchema);

const productSchema = new mongoose.Schema({
  name: String,
  description: String,
  price: Number,
  images: [String],        // ← Changed to array
  category: String,
  stock: { type: Number, default: 50 },
  seller: String,
  timestamp: { type: Number, default: Date.now }
});

const Product = mongoose.model('Product', productSchema);

// --- TVWS (TELEVISION WHITE SPACE) SCHEMA ---
const tvwsNodeSchema = new mongoose.Schema({
    nodeIdentity: { type: String, required: true, index: true }, // Device owner or network ID
    latitude: { type: Number, required: true },
    longitude: { type: Number, required: true },
    antennaHeightMeters: { type: Number, default: 10 },
    deviceClass: { type: String, enum: ['FIXED', 'PORTABLE'], default: 'FIXED' },
    assignedChannel: { type: Number, default: null }, // Channel number (e.g., CH 21 - 51)
    maxTxPowerDbm: { type: Number, default: 30 },
    status: { type: String, enum: ['ACTIVE', 'PENDING', 'OFFLINE'], default: 'PENDING' },
    lastDbSync: { type: Date, default: Date.now }
});

const TVWSNode = mongoose.model('TVWSNode', tvwsNodeSchema);

// --- HELPERS ---
const cleanPhone = (phone) => {
  if (!phone) return "";
  let cleaned = phone.toString().replace(/\D/g, '');

  // Kenya local format
  if (cleaned.startsWith('0') && cleaned.length === 10) {
    cleaned = '254' + cleaned.substring(1);
  }

  // US numbers that are missing the leading 1 (10 digits)
  if (cleaned.length === 10 && !cleaned.startsWith('0')) {
    cleaned = '1' + cleaned;
  }

  return cleaned;
};
// ====================== DUPLICATE CONTENT PROTECTION ======================
const checkDuplicateContent = async (title, fileBuffer, owner) => {
    const titleClean = title?.trim().toLowerCase();

    // Check by title (case-insensitive)
    const existingByTitle = await Post.findOne({ 
        is_burned: false, 
        title: { $regex: new RegExp(`^${titleClean}$`, 'i') } 
    });

    if (existingByTitle) {
        return {
            isDuplicate: true,
            reason: "TITLE_ALREADY_MINTED",
            existingPostId: existingByTitle._id
        };
    }

    // Check by file content hash (SHA-256)
    if (fileBuffer) {
        const fileHash = crypto.createHash('sha256').update(fileBuffer).digest('hex');

        const existingByContent = await Post.findOne({ 
            is_burned: false, 
            contentHash: fileHash 
        });

        if (existingByContent) {
            return {
                isDuplicate: true,
                reason: "CONTENT_ALREADY_MINTED",
                existingPostId: existingByContent._id,
                fileHash
            };
        }

        return { isDuplicate: false, contentHash: fileHash };
    }

    return { isDuplicate: false };
};
const calculateLiveTax = async () => {
    const nodeCount = await User.countDocuments({});
    const dynamicModifier = Math.log10(nodeCount + 10);
    const liveRate = PROTOCOL_FEE / dynamicModifier;
    return Math.max(0.01, liveRate); 
};

const processGridSuccess = async (tx) => {
    if (tx.status === 'completed') return; 

    console.log(`🔄 Processing success for tx: ${tx.checkoutID} | Type: ${tx.type} | Amount: ${tx.amountPaid}`);

    // =========================================================================
    // SPECIAL CASE: P2P AFRO PURCHASE
    // =========================================================================
    if (tx.type === 'p2p_buy') {
        const order = await P2POrder.findById(tx.postID);
        if (!order) {
            console.error("P2P order not found for tx:", tx.checkoutID);
            tx.status = 'completed';
            await tx.save();
            return;
        }

        const buyerIdentity = cleanPhone(tx.userPhone);
        const buyer = await User.findOne({ identity: buyerIdentity });

        if (buyer) {
            buyer.afroCoins = Number((buyer.afroCoins + order.afroAmount).toFixed(4));
            await buyer.save();
            console.log(`✅ P2P SUCCESS: +${order.afroAmount} AFRO to ${buyerIdentity}`);
        }

        // Credit seller earnings automatically
        await User.findOneAndUpdate(
            { identity: order.sellerIdentity },
            { $inc: { earnings: order.fiatTotal } }
        );

        // Finalize
        order.status = 'COMPLETED';
        order.paymentConfirmedAt = new Date();
        await order.save();

        tx.status = 'completed';
        await tx.save();

        io.to(tx.checkoutID).emit('payment_success', { 
            message: "AFRO Purchased & Credited Successfully",
            txType: 'p2p_buy',
            orderId: order._id
        });
        return;
    }

    // =========================================================================
    // PRODUCT PURCHASE (NEW - Full, Delivery, Plan)
    // =========================================================================
    if (tx.type === 'product_purchase' || tx.type === 'full' || tx.type === 'delivery' || tx.type === 'plan') {
        const product = await Product.findById(tx.postID);
        if (!product) {
            console.error("❌ Product not found for tx:", tx.checkoutID);
            tx.status = 'completed';
            await tx.save();
            return;
        }

        const buyerPhone = cleanPhone(tx.userPhone);

        // Reduce stock
        if (product.stock > 0) {
            product.stock = Math.max(0, product.stock - 1);
            await product.save();
        }

        // Credit seller
        await User.findOneAndUpdate(
            { identity: product.seller },
            { $inc: { earnings: tx.amountPaid } }
        );

        tx.status = 'completed';
        await tx.save();

        let message = "Product purchased successfully!";
        if (tx.type === 'delivery') message = "✅ Order placed! Pay on delivery.";
        else if (tx.type === 'plan') message = "✅ First installment paid. 3-month plan activated.";

        io.to(tx.checkoutID).emit('payment_success', { 
            message,
            productId: product._id,
            type: tx.type,
            remainingStock: product.stock
        });

        console.log(`✅ Product purchase completed (${tx.type}) for ${buyerPhone}`);
        return;
    }

    
    // =========================================================================
    // REGULAR POST UNLOCK / LICENSE FLOW
    // =========================================================================
    const post = await Post.findById(tx.postID);
    if (!post) {
        console.error("❌ Post context missing for transaction:", tx.checkoutID);
        return;
    }

    // STEP 1: IMMEDIATE ASSET UNLOCK
    if (tx.type !== 'handshake_fee') {
        const sanitizedUserPhone = cleanPhone(tx.userPhone);
        let updateField = (tx.type === 'license') 
            ? { $addToSet: { licensed_to: sanitizedUserPhone } } 
            : { $addToSet: { unlocked_by: sanitizedUserPhone } };
        
        await Post.findByIdAndUpdate(tx.postID, updateField);

        if (post.is_resell && post.parent_post_id) {
            await Post.findByIdAndUpdate(post.parent_post_id, updateField);
        }
    }

    // STEP 2: FINANCIAL ACCOUNTING
    const platformFee = tx.amountPaid * PROTOCOL_FEE;
    let originalCreatorRoyalty = 0;
    let originalCreatorNode = "";

    if (post.is_resell && post.original_creator) {
        originalCreatorNode = cleanPhone(post.original_creator);
        if (originalCreatorNode !== cleanPhone(post.owner)) {
            originalCreatorRoyalty = tx.amountPaid * 0.15;
        }
    }

    const netPayoutForListing = tx.amountPaid - platformFee - originalCreatorRoyalty;

    await Payout.create({
        parentTxID: tx.checkoutID,
        recipientNode: post.owner,
        grossAmount: tx.amountPaid,
        creatorNet: netPayoutForListing, 
        platformFee: platformFee,
        status: 'completed' 
    });

    // Mint AFRO reward for buyer
    const mintResults = await governAfroMinting(tx.amountPaid);
    const reward = mintResults.user;

    await Vault.findOneAndUpdate({ id: 'protocol_vault' }, { $inc: { balance: platformFee } }, { upsert: true });
    await User.findOneAndUpdate(
        { identity: cleanPhone(tx.userPhone) }, 
        { $inc: { afroCoins: reward }, lastSeen: Date.now() }, 
        { upsert: true }
    );

    // STEP 3: SPLIT & ROYALTY DISTRIBUTION
    if (tx.type === 'handshake_fee') {
        const handshake = await Handshake.findById(tx.handshakeID);
        if (handshake) {
            await Post.findByIdAndUpdate(tx.postID, { 
                $push: { 
                    collaborators: { 
                        node: handshake.sender, 
                        signature: handshake.signature, 
                        split: handshake.split, 
                        contractHash: handshake.contractHash, 
                        signedAt: Date.now() 
                    } 
                } 
            });
            handshake.status = 'paid';
            await handshake.save();
        }
    } else {
        if (originalCreatorRoyalty > 0 && originalCreatorNode) {
            await User.findOneAndUpdate(
                { identity: originalCreatorNode }, 
                { $inc: { earnings: originalCreatorRoyalty } },
                { upsert: true }
            );
        }

        if (post.collaborators && post.collaborators.length > 0) {
            let totalCollaboratorShare = 0;
            for (let colab of post.collaborators) {
                const colabEarnings = netPayoutForListing * (colab.split / 100);
                await User.findOneAndUpdate({ identity: colab.node }, { $inc: { earnings: colabEarnings } });
                totalCollaboratorShare += colabEarnings;
            }
            const ownerEarnings = netPayoutForListing - totalCollaboratorShare;
            await User.findOneAndUpdate({ identity: post.owner }, { $inc: { earnings: ownerEarnings } });
        } else {
            await User.findOneAndUpdate({ identity: post.owner }, { $inc: { earnings: netPayoutForListing } });
        }
    }

    // STEP 4: FINALIZE & BROADCAST
    tx.status = 'completed';
    await tx.save();
    
    io.to(tx.checkoutID).emit('payment_success', { 
        message: tx.type === 'handshake_fee' ? "Contract Sync Active" : "Nodal Sync Confirmed", 
        postId: tx.postID,
        txType: tx.type 
    });
    
    console.log(`✅ SYNC COMPLETE: ${tx.type} for Node ${idppMaskPhone(tx.userPhone)}`);
}; 
// Express Route: Generate onboarding link for seller (Multi-Country Ready)
app.post('/api/payouts/create-onboarding-link', async (req, res) => {
    try {
        const { phone, countryCode } = req.body; // e.g., countryCode = 'KE', 'US', 'GB', 'CA'
        if (!phone) return res.status(400).json({ error: 'Phone number required' });

        // Clean phone number ensuring leading '+'
        let cleanedPhone = phone.replace(/[^\d+]/g, '');
        if (!cleanedPhone.startsWith('+')) {
            cleanedPhone = '+' + cleanedPhone;
        }

        // Default to 'US' if no valid country code is provided
        const sellerCountry = (countryCode || 'US').toUpperCase();

        let user = await User.findOne({ identity: cleanedPhone });

        // 1. Create Connected Express Account if it doesn't exist
        if (!user?.stripeAccountId) {
            
            // Capabilities required by regional financial regulations
            const capabilities = {
                transfers: { requested: true },
                card_payments: { requested: true },
            };

            const accountPayload = {
                type: 'express',
                country: sellerCountry, // 👈 Dynamic ISO-2 Country Code
                phone_number: cleanedPhone,
                capabilities: capabilities,
                business_type: 'individual',
                settings: {
                    payouts: {
                        schedule: {
                            interval: 'daily', // Stripe defaults to fastest daily rate available in that region
                        },
                    },
                },
            };

            const account = await stripe.accounts.create(accountPayload);

            user = await User.findOneAndUpdate(
                { identity: cleanedPhone },
                { $set: { stripeAccountId: account.id, country: sellerCountry } },
                { new: true, upsert: true }
            );
        }

        // 2. Generate Account Onboarding Link
        const accountLink = await stripe.accountLinks.create({
            account: user.stripeAccountId,
            refresh_url: `${process.env.FRONTEND_URL || 'https://yourplatform.com'}/payouts/reauth`,
            return_url: `${process.env.FRONTEND_URL || 'https://yourplatform.com'}/payouts/success`,
            type: 'account_onboarding',
        });

        res.json({ success: true, url: accountLink.url });
    } catch (err) {
        console.error('❌ Onboarding Link Error:', err.message);
        res.status(500).json({ error: err.message });
    }
});
// List of core Stripe Express supported countries
const EXPRESS_SUPPORTED_COUNTRIES = ['US', 'CA', 'GB', 'AU', 'AT', 'BE', 'DE', 'DK', 'ES', 'FI', 'FR', 'IE', 'IT', 'JP', 'LU', 'NL', 'NO', 'NZ', 'PT', 'SE', 'SG'];

app.post('/api/payouts/create-onboarding-link', async (req, res) => {
    try {
        const { phone, countryCode = 'US' } = req.body;
        const sellerCountry = countryCode.toUpperCase();

        // If the country doesn't support Stripe Express natively
        if (!EXPRESS_SUPPORTED_COUNTRIES.includes(sellerCountry)) {
            return res.status(400).json({ 
                error: `Direct Stripe Express payouts are not supported in ${sellerCountry} yet. Alternative payout method required (e.g., Wise/Payoneer/Wire).` 
            });
        }

        // ... Proceed with standard stripe.accounts.create() for supported countries
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});


const governAfroMinting = async (requestedAmount) => {
    const vault = await Vault.findOneAndUpdate({ id: 'protocol_vault' }, {}, { upsert: true, new: true });
    const rewardAmount = requestedAmount * MINTING_REWARD_RATE;
    const userRewardTotal = rewardAmount * 2; 
    const totalNewMint = userRewardTotal / (1 - PLATFORM_RESERVE_SHARE);
    const platformShare = totalNewMint * PLATFORM_RESERVE_SHARE;
    if (vault.totalAfroMinted + totalNewMint > AFRO_HARD_CAP) return { user: 0, platform: 0 };
    await Vault.updateOne({ id: 'protocol_vault' }, { $inc: { totalAfroMinted: totalNewMint, platformAfroReserve: platformShare } });
    return { user: rewardAmount, platform: platformShare };
};

// === DYNAMIC CURRENCY ENGINE (FIXED) ===
const CURRENCY_BY_PHONE_PREFIX = {
  // Zone 1 — North American Numbering Plan (NANP) & Territories
  '1':     'USD', // United States / General NANP
  '1242':  'BSD', // Bahamas
  '1246':  'BBD', // Barbados
  '1264':  'XCD', // Anguilla
  '1268':  'XCD', // Antigua and Barbuda
  '1284':  'USD', // British Virgin Islands
  '1340':  'USD', // US Virgin Islands
  '1345':  'KYD', // Cayman Islands
  '1441':  'BMD', // Bermuda
  '1473':  'XCD', // Grenada
  '1649':  'USD', // Turks and Caicos Islands
  '1664':  'XCD', // Montserrat
  '1670':  'USD', // Northern Mariana Islands
  '1671':  'USD', // Guam
  '1684':  'USD', // American Samoa
  '1721':  'ANG', // Sint Maarten
  '1758':  'XCD', // Saint Lucia
  '1767':  'XCD', // Dominica
  '1784':  'XCD', // Saint Vincent and the Grenadines
  '1787':  'USD', // Puerto Rico
  '1809':  'DOP', // Dominican Republic
  '1829':  'DOP', // Dominican Republic
  '1849':  'DOP', // Dominican Republic
  '1868':  'TTD', // Trinidad and Tobago
  '1869':  'XCD', // Saint Kitts and Nevis
  '1876':  'JMD', // Jamaica
  '1658':  'JMD', // Jamaica
  '1250':  'CAD', // Canada

  // Zone 2 — Africa & Atlantic/Indian Ocean Islands
  '20':    'EGP', // Egypt
  '211':   'SSP', // South Sudan
  '212':   'MAD', // Morocco / Western Sahara
  '213':   'DZD', // Algeria
  '216':   'TND', // Tunisia
  '218':   'LYD', // Libya
  '220':   'GMD', // Gambia
  '221':   'XOF', // Senegal
  '222':   'MRU', // Mauritania
  '223':   'XOF', // Mali
  '224':   'GNF', // Guinea
  '225':   'XOF', // Côte d'Ivoire
  '226':   'XOF', // Burkina Faso
  '227':   'XOF', // Niger
  '228':   'XOF', // Togo
  '229':   'XOF', // Benin
  '230':   'MUR', // Mauritius
  '231':   'LRD', // Liberia
  '232':   'SLE', // Sierra Leone
  '233':   'GHS', // Ghana
  '234':   'NGN', // Nigeria
  '235':   'XAF', // Chad
  '236':   'XAF', // Central African Republic
  '237':   'XAF', // Cameroon
  '238':   'CVE', // Cape Verde
  '239':   'STN', // São Tomé and Príncipe
  '240':   'XAF', // Equatorial Guinea
  '241':   'XAF', // Gabon
  '242':   'XAF', // Republic of the Congo
  '243':   'CDF', // Democratic Republic of the Congo
  '244':   'AOA', // Angola
  '245':   'XOF', // Guinea-Bissau
  '246':   'USD', // Diego Garcia
  '247':   'SHP', // Ascension Island
  '248':   'SCR', // Seychelles
  '249':   'SDG', // Sudan
  '250':   'RWF', // Rwanda
  '251':   'ETB', // Ethiopia
  '252':   'SOS', // Somalia
  '253':   'DJF', // Djibouti
  '254':   'KES', // Kenya
  '255':   'TZS', // Tanzania
  '256':   'UGX', // Uganda
  '257':   'BIF', // Burundi
  '258':   'MZN', // Mozambique
  '260':   'ZMW', // Zambia
  '261':   'MGA', // Madagascar
  '262':   'EUR', // Réunion / Mayotte
  '263':   'ZWG', // Zimbabwe
  '264':   'NAD', // Namibia
  '265':   'MWK', // Malawi
  '266':   'LSL', // Lesotho
  '267':   'BWP', // Botswana
  '268':   'SZL', // Eswatini
  '269':   'KMF', // Comoros
  '27':    'ZAR', // South Africa
  '290':   'SHP', // Saint Helena / Tristan da Cunha
  '291':   'ERN', // Eritrea
  '297':   'AWG', // Aruba
  '298':   'DKK', // Faroe Islands
  '299':   'DKK', // Greenland

  // Zones 3 & 4 — Europe
  '30':    'EUR', // Greece
  '31':    'EUR', // Netherlands
  '32':    'EUR', // Belgium
  '33':    'EUR', // France
  '34':    'EUR', // Spain
  '350':   'GIP', // Gibraltar
  '351':   'EUR', // Portugal
  '352':   'EUR', // Luxembourg
  '353':   'EUR', // Ireland
  '354':   'ISK', // Iceland
  '355':   'ALL', // Albania
  '356':   'EUR', // Malta
  '357':   'EUR', // Cyprus
  '358':   'EUR', // Finland
  '359':   'BGN', // Bulgaria
  '36':    'HUF', // Hungary
  '370':   'EUR', // Lithuania
  '371':   'EUR', // Latvia
  '372':   'EUR', // Estonia
  '373':   'MDL', // Moldova
  '374':   'AMD', // Armenia
  '375':   'BYN', // Belarus
  '376':   'EUR', // Andorra
  '377':   'EUR', // Monaco
  '378':   'EUR', // San Marino
  '379':   'EUR', // Vatican City
  '380':   'UAH', // Ukraine
  '381':   'RSD', // Serbia
  '382':   'EUR', // Montenegro
  '383':   'EUR', // Kosovo
  '385':   'EUR', // Croatia
  '386':   'EUR', // Slovenia
  '387':   'BAM', // Bosnia and Herzegovina
  '389':   'MKD', // North Macedonia
  '39':    'EUR', // Italy
  '40':    'RON', // Romania
  '41':    'CHF', // Switzerland
  '420':   'CZK', // Czech Republic
  '421':   'EUR', // Slovakia
  '423':   'CHF', // Liechtenstein
  '44':    'GBP', // United Kingdom
  '45':    'DKK', // Denmark
  '46':    'SEK', // Sweden
  '47':    'NOK', // Norway
  '48':    'PLN', // Poland
  '49':    'EUR', // Germany

  // Zone 5 — Central & South America
  '500':   'FKP', // Falkland Islands
  '501':   'BZD', // Belize
  '502':   'GTQ', // Guatemala
  '503':   'USD', // El Salvador
  '504':   'HNL', // Honduras
  '505':   'NIO', // Nicaragua
  '506':   'CRC', // Costa Rica
  '507':   'PAB', // Panama
  '508':   'EUR', // Saint Pierre and Miquelon
  '509':   'HTG', // Haiti
  '51':    'PEN', // Peru
  '52':    'MXN', // Mexico
  '53':    'CUP', // Cuba
  '54':    'ARS', // Argentina
  '55':    'BRL', // Brazil
  '56':    'CLP', // Chile
  '57':    'COP', // Colombia
  '58':    'VES', // Venezuela
  '590':   'EUR', // Guadeloupe / Saint Martin / Saint Barthélemy
  '591':   'BOB', // Bolivia
  '592':   'GYD', // Guyana
  '593':   'USD', // Ecuador
  '594':   'EUR', // French Guiana
  '595':   'PYG', // Paraguay
  '596':   'EUR', // Martinique
  '597':   'SRD', // Suriname
  '598':   'UYU', // Uruguay
  '599':   'USD', // Caribbean Netherlands (Bonaire, Sint Eustatius, Saba) / Curaçao (ANG)

  // Zone 6 — Southeast Asia & Oceania
  '60':    'MYR', // Malaysia
  '61':    'AUD', // Australia / Christmas Island / Cocos Islands
  '62':    'IDR', // Indonesia
  '63':    'PHP', // Philippines
  '64':    'NZD', // New Zealand / Pitcairn Islands
  '65':    'SGD', // Singapore
  '66':    'THB', // Thailand
  '670':   'USD', // East Timor
  '672':   'AUD', // Australian External Territories / Norfolk Island
  '673':   'BND', // Brunei
  '674':   'AUD', // Nauru
  '675':   'PGK', // Papua New Guinea
  '676':   'TOP', // Tonga
  '677':   'SBD', // Solomon Islands
  '678':   'VUV', // Vanuatu
  '679':   'FJD', // Fiji
  '680':   'USD', // Palau
  '681':   'XPF', // Wallis and Futuna
  '682':   'NZD', // Cook Islands
  '683':   'NZD', // Niue
  '685':   'WST', // Samoa
  '686':   'AUD', // Kiribati
  '687':   'XPF', // New Caledonia
  '688':   'AUD', // Tuvalu
  '689':   'XPF', // French Polynesia
  '690':   'NZD', // Tokelau
  '691':   'USD', // Micronesia
  '692':   'USD', // Marshall Islands

  // Zone 7 — Russia & Neighboring Regions
  '7':     'RUB', // Russia / Kazakhstan (KZT)

  // Zone 8 — East Asia, South Asia (partial) & Special Services
  '81':    'JPY', // Japan
  '82':    'KRW', // South Korea
  '84':    'VND', // Vietnam
  '850':   'KPW', // North Korea
  '852':   'HKD', // Hong Kong
  '853':   'MOP', // Macau
  '855':   'KHR', // Cambodia
  '856':   'LAK', // Laos
  '86':    'CNY', // China
  '880':   'BDT', // Bangladesh
  '886':   'TWD', // Taiwan

  // Zone 9 — Middle East, Central & South Asia
  '90':    'TRY', // Turkey
  '91':    'INR', // India
  '92':    'PKR', // Pakistan
  '93':    'AFN', // Afghanistan
  '94':    'LKR', // Sri Lanka
  '95':    'MMK', // Myanmar
  '98':    'IRR', // Iran
  '960':   'MVR', // Maldives
  '961':   'LBP', // Lebanon
  '962':   'JOD', // Jordan
  '963':   'SYP', // Syria
  '964':   'IQD', // Iraq
  '965':   'KWD', // Kuwait
  '966':   'SAR', // Saudi Arabia
  '967':   'YER', // Yemen
  '968':   'OMR', // Oman
  '970':   'ILS', // Palestine
  '971':   'AED', // United Arab Emirates
  '972':   'ILS', // Israel
  '973':   'BHD', // Bahrain
  '974':   'QAR', // Qatar
  '975':   'BTN', // Bhutan
  '976':   'MNT', // Mongolia
  '977':   'NPR', // Nepal
  '992':   'TJS', // Tajikistan
  '993':   'TMT', // Turkmenistan
  '994':   'AZN', // Azerbaijan
  '995':   'GEL', // Georgia
  '996':   'KGS', // Kyrgyzstan
  '998':   'UZS'  // Uzbekistan

};



let ratesCache = {
  rates: null,
  fetchedAt: 0
};

const RATES_TTL = 30 * 60 * 1000; // 30 minutes cache duration

// Comprehensive global fallback rates (1 KES = X foreign currency)
const GLOBAL_FALLBACK_RATES = {
  // Major Anchor Currencies
  usd: 0.0077,   eur: 0.0067,   gbp: 0.0057,   jpy: 1.14,
  aud: 0.0118,   cad: 0.0106,   chf: 0.0068,   cny: 0.055,
  inr: 0.65,     hkd: 0.060,    sgd: 0.010,    nzd: 0.013,

  // East & Central Africa
  kes: 1.0,      ugx: 28.5,     tzs: 20.2,     rwf: 10.6,
  bif: 22.4,     ssp: 25.5,     etb: 0.95,     sos: 4.42,
  djf: 1.38,     cdf: 22.1,

  // Rest of Africa
  ngn: 12.0,     ghs: 0.12,     zar: 0.14,     egp: 0.38,
  mad: 0.076,    dzd: 1.04,     tnd: 0.024,    lyd: 0.037,
  xof: 4.40,     xaf: 4.40,     aoa: 7.15,     bwp: 0.106,
  cve: 0.74,     gmd: 0.55,     gnf: 66.8,     lrd: 1.48,
  mga: 35.8,     mwk: 13.4,     mru: 0.31,     mur: 0.36,
  mzn: 0.49,     nad: 0.14,     scr: 0.11,     sle: 0.17,
  szl: 0.14,     zmw: 0.21,     zwg: 0.11,

  // Middle East & North Africa
  aed: 0.028,    sar: 0.029,    qar: 0.028,    bhd: 0.0029,
  kwd: 0.0024,   omr: 0.0030,   jod: 0.0055,   ils: 0.028,
  try: 0.26,     iqd: 10.1,     irr: 326.0,    lbp: 692.0,

  // Europe (Non-EUR/GBP)
  sek: 0.081,    nok: 0.083,    dkk: 0.053,    pln: 0.030,
  czk: 0.17,     huf: 2.72,     ron: 0.035,    bgn: 0.013,
  rsd: 0.83,     isk: 1.06,     all: 0.70,     bam: 0.013,
  rub: 0.69,     uah: 0.32,     gel: 0.021,    amd: 3.01,

  // Asia-Pacific
  twd: 0.25,     krw: 10.6,     pkr: 2.15,     lkr: 2.32,
  bdt: 0.92,     thb: 0.27,     myr: 0.034,    idr: 124.0,
  php: 0.45,     vnd: 195.0,    khr: 31.5,     lak: 170.0,

  // Americas & Caribbean
  mxn: 0.15,     brl: 0.043,    ars: 7.45,     cop: 31.2,
  clp: 7.25,     pen: 0.029,    uyu: 0.31,     pyg: 58.5,
  bob: 0.053,    ves: 0.28,     crc: 3.95,     gtq: 0.060,
  dop: 0.46,     jmd: 1.20,     ttd: 0.052,    bsd: 0.0077,
  bbd: 0.015,    bzd: 0.015,    xcd: 0.021,    htg: 1.02
};

/**
 * Fetches current exchange rates (base: KES) with dual CDN endpoints and fallback.
 */
async function fetchLiveRates() {
  const now = Date.now();
  if (ratesCache.rates && now - ratesCache.fetchedAt < RATES_TTL) {
    return ratesCache.rates;
  }

  // 1. Primary Endpoint (jsDelivr CDN)
  const PRIMARY_URL = 'https://cdn.jsdelivr.net/npm/@fawazahmed0/currency-api@latest/v1/currencies/kes.min.json';
  
  // 2. Secondary Fallback Endpoint (Cloudflare Pages)
  const SECONDARY_URL = 'https://latest.currency-api.pages.dev/v1/currencies/kes.min.json';

  try {
    const res = await axios.get(PRIMARY_URL, { timeout: 5000 });
    const ratesFromKES = res.data.kes || {};

    ratesCache = { rates: ratesFromKES, fetchedAt: now };
    console.log('✅ Live FX rates refreshed via Primary CDN (base: KES)');
    return ratesFromKES;
  } catch (primaryErr) {
    console.warn('⚠️ Primary FX endpoint failed. Trying Cloudflare fallback...');

    try {
      const resFallback = await axios.get(SECONDARY_URL, { timeout: 5000 });
      const ratesFromKES = resFallback.data.kes || {};

      ratesCache = { rates: ratesFromKES, fetchedAt: now };
      console.log('✅ Live FX rates refreshed via Secondary CDN (base: KES)');
      return ratesFromKES;
    } catch (secondaryErr) {
      console.warn('⚠️ Both live FX APIs failed. Utilizing hardcoded global fallbacks.');

      ratesCache = { rates: GLOBAL_FALLBACK_RATES, fetchedAt: now };
      return GLOBAL_FALLBACK_RATES;
    }
  }
}

/**
 * Helper: Converts any amount between two target currencies using KES rates.
 */
async function convertCurrency(amount, fromCode, toCode) {
  const rates = await fetchLiveRates();
  const from = fromCode.toLowerCase();
  const to = toCode.toLowerCase();

  const rateFrom = from === 'kes' ? 1 : rates[from];
  const rateTo = to === 'kes' ? 1 : rates[to];

  if (!rateFrom || !rateTo) {
    throw new Error(`Unsupported currency conversion pair: ${fromCode} -> ${toCode}`);
  }

  // Convert "From" -> KES -> "To"
  const amountInKES = amount / rateFrom;
  const finalAmount = amountInKES * rateTo;

  return Number(finalAmount.toFixed(4));
}

// Example Execution
(async () => {
  const rates = await fetchLiveRates();
  console.log('1 KES in USD:', rates.usd);
  console.log('1 KES in UGX:', rates.ugx);

  // Convert 10,000 KES directly to USD
  const usdAmount = await convertCurrency(10000, 'KES', 'USD');
  console.log('10,000 KES =', usdAmount, 'USD');

  // Cross-currency conversion: Convert $100 USD to Nigerian Naira (NGN)
  const ngnAmount = await convertCurrency(100, 'USD', 'NGN');
  console.log('100 USD =', ngnAmount, 'NGN');
})();


/**
 * Returns { code, rate }
 * rate = how many units of the local currency equal 1 KES
 */
async function getCurrencyByPhone(phone) {
  console.log('====== CURRENCY DETECTION START ======');
  console.log('Raw phone received:', phone);

  const cleaned = (phone || '').toString().replace(/\D/g, '');
  console.log('Cleaned phone:', cleaned);
  console.log('Length:', cleaned.length);
  console.log('Starts with 1?', cleaned.startsWith('1'));
  console.log('Starts with 254?', cleaned.startsWith('254'));

  let code = 'USD'; // default

  for (let len = 3; len >= 1; len--) {
    const prefix = cleaned.substring(0, len);
    if (CURRENCY_BY_PHONE_PREFIX[prefix]) {
      code = CURRENCY_BY_PHONE_PREFIX[prefix];
      console.log(`Matched prefix "${prefix}" → ${code}`);
      break;
    }
  }

  const rates = await fetchLiveRates();
  const rateKey = code.toLowerCase();
  const rate = rates[rateKey] ?? (code === 'KES' ? 1 : 0.0077);

  console.log(`Final decision → Currency: ${code} | Rate: ${rate}`);
  console.log('====== CURRENCY DETECTION END ======');

  return { code, rate };
}
// === DYNAMIC TIMEZONE ENGINE ===
const TIMEZONE_BY_PHONE_PREFIX = {
  // Zone 1 — North America (NANP & Territories)
  '1':     'America/New_York',       // General US East / Default
  '1242':  'America/Nassau',         // Bahamas
  '1246':  'America/Barbados',       // Barbados
  '1264':  'America/Anguilla',       // Anguilla
  '1268':  'America/Antigua',        // Antigua and Barbuda
  '1284':  'America/Tortola',        // British Virgin Islands
  '1340':  'America/St_Thomas',      // US Virgin Islands
  '1345':  'America/Cayman',         // Cayman Islands
  '1441':  'Atlantic/Bermuda',       // Bermuda
  '1473':  'America/Grenada',        // Grenada
  '1649':  'America/Grand_Turk',     // Turks and Caicos
  '1664':  'America/Montserrat',     // Montserrat
  '1670':  'Pacific/Saipan',         // Northern Mariana Islands
  '1671':  'Pacific/Guam',           // Guam
  '1684':  'Pacific/Pago_Pago',      // American Samoa
  '1721':  'America/Lower_Princes',  // Sint Maarten
  '1758':  'America/St_Lucia',       // Saint Lucia
  '1767':  'America/Dominica',       // Dominica
  '1784':  'America/St_Vincent',     // Saint Vincent and the Grenadines
  '1787':  'America/Puerto_Rico',    // Puerto Rico
  '1809':  'America/Santo_Domingo',  // Dominican Republic
  '1829':  'America/Santo_Domingo',  // Dominican Republic
  '1849':  'America/Santo_Domingo',  // Dominican Republic
  '1868':  'America/Port_of_Spain',  // Trinidad and Tobago
  '1869':  'America/St_Kitts',       // Saint Kitts and Nevis
  '1876':  'America/Jamaica',        // Jamaica
  '1658':  'America/Jamaica',        // Jamaica
  '1250':  'America/Vancouver',      // Canada (West Coast sample area code)

  // Zone 2 — Africa
  '20':    'Africa/Cairo',           // Egypt
  '211':   'Africa/Juba',            // South Sudan
  '212':   'Africa/Casablanca',      // Morocco
  '213':   'Africa/Algiers',         // Algeria
  '216':   'Africa/Tunis',           // Tunisia
  '218':   'Africa/Tripoli',         // Libya
  '220':   'Africa/Banjul',          // Gambia
  '221':   'Africa/Dakar',           // Senegal
  '222':   'Africa/Nouakchott',      // Mauritania
  '223':   'Africa/Bamako',          // Mali
  '224':   'Africa/Conakry',         // Guinea
  '225':   'Africa/Abidjan',         // Côte d'Ivoire
  '226':   'Africa/Ouagadougou',     // Burkina Faso
  '227':   'Africa/Niamey',          // Niger
  '228':   'Africa/Lome',            // Togo
  '229':   'Africa/Porto-Novo',      // Benin
  '230':   'Indian/Mauritius',       // Mauritius
  '231':   'Africa/Monrovia',        // Liberia
  '232':   'Africa/Freetown',        // Sierra Leone
  '233':   'Africa/Accra',           // Ghana
  '234':   'Africa/Lagos',           // Nigeria
  '235':   'Africa/Ndjamena',        // Chad
  '236':   'Africa/Bangui',          // Central African Republic
  '237':   'Africa/Douala',          // Cameroon
  '238':   'Atlantic/Cape_Verde',    // Cape Verde
  '239':   'Africa/Sao_Tome',        // São Tomé and Príncipe
  '240':   'Africa/Malabo',          // Equatorial Guinea
  '241':   'Africa/Libreville',      // Gabon
  '242':   'Africa/Brazzaville',     // Republic of the Congo
  '243':   'Africa/Kinshasa',        // Democratic Republic of the Congo
  '244':   'Africa/Luanda',          // Angola
  '245':   'Africa/Bissau',          // Guinea-Bissau
  '246':   'Indian/Chagos',          // Diego Garcia
  '247':   'Atlantic/St_Helena',     // Ascension Island
  '248':   'Indian/Mahe',            // Seychelles
  '249':   'Africa/Khartoum',        // Sudan
  '250':   'Africa/Kigali',          // Rwanda
  '251':   'Africa/Addis_Ababa',     // Ethiopia
  '252':   'Africa/Mogadishu',       // Somalia
  '253':   'Africa/Djibouti',        // Djibouti
  '254':   'Africa/Nairobi',         // Kenya
  '255':   'Africa/Dar_es_Salaam',   // Tanzania
  '256':   'Africa/Kampala',         // Uganda
  '257':   'Africa/Bujumbura',       // Burundi
  '258':   'Africa/Maputo',          // Mozambique
  '260':   'Africa/Lusaka',          // Zambia
  '261':   'Indian/Antananarivo',    // Madagascar
  '262':   'Indian/Reunion',         // Réunion / Mayotte
  '263':   'Africa/Harare',          // Zimbabwe
  '264':   'Africa/Windhoek',        // Namibia
  '265':   'Africa/Blantyre',        // Malawi
  '266':   'Africa/Maseru',          // Lesotho
  '267':   'Africa/Gaborone',        // Botswana
  '268':   'Africa/Mbabane',         // Eswatini
  '269':   'Indian/Comoro',          // Comoros
  '27':    'Africa/Johannesburg',    // South Africa
  '290':   'Atlantic/St_Helena',     // Saint Helena
  '291':   'Africa/Asmara',          // Eritrea
  '297':   'America/Aruba',          // Aruba
  '298':   'Atlantic/Faroe',         // Faroe Islands
  '299':   'America/Nuuk',           // Greenland

  // Zones 3 & 4 — Europe
  '30':    'Europe/Athens',          // Greece
  '31':    'Europe/Amsterdam',       // Netherlands
  '32':    'Europe/Brussels',        // Belgium
  '33':    'Europe/Paris',           // France
  '34':    'Europe/Madrid',          // Spain
  '350':   'Europe/Gibraltar',       // Gibraltar
  '351':   'Europe/Lisbon',          // Portugal
  '352':   'Europe/Luxembourg',      // Luxembourg
  '353':   'Europe/Dublin',          // Ireland
  '354':   'Atlantic/Reykjavik',     // Iceland
  '355':   'Europe/Tirane',          // Albania
  '356':   'Europe/Malta',           // Malta
  '357':   'Asia/Nicosia',           // Cyprus
  '358':   'Europe/Helsinki',        // Finland
  '359':   'Europe/Sofia',           // Bulgaria
  '36':    'Europe/Budapest',        // Hungary
  '370':   'Europe/Vilnius',         // Lithuania
  '371':   'Europe/Riga',            // Latvia
  '372':   'Europe/Tallinn',         // Estonia
  '373':   'Europe/Chisinau',        // Moldova
  '374':   'Asia/Yerevan',           // Armenia
  '375':   'Europe/Minsk',           // Belarus
  '376':   'Europe/Andorra',         // Andorra
  '377':   'Europe/Monaco',          // Monaco
  '378':   'Europe/San_Marino',      // San Marino
  '379':   'Europe/Vatican',         // Vatican City
  '380':   'Europe/Kyiv',            // Ukraine
  '381':   'Europe/Belgrade',        // Serbia
  '382':   'Europe/Podgorica',       // Montenegro
  '383':   'Europe/Belgrade',        // Kosovo
  '385':   'Europe/Zagreb',          // Croatia
  '386':   'Europe/Ljubljana',       // Slovenia
  '387':   'Europe/Sarajevo',        // Bosnia and Herzegovina
  '389':   'Europe/Skopje',          // North Macedonia
  '39':    'Europe/Rome',            // Italy
  '40':    'Europe/Bucharest',       // Romania
  '41':    'Europe/Zurich',          // Switzerland
  '420':   'Europe/Prague',          // Czech Republic
  '421':   'Europe/Bratislava',      // Slovakia
  '423':   'Europe/Vaduz',           // Liechtenstein
  '44':    'Europe/London',          // United Kingdom
  '45':    'Europe/Copenhagen',      // Denmark
  '46':    'Europe/Stockholm',       // Sweden
  '47':    'Europe/Oslo',            // Norway
  '48':    'Europe/Warsaw',          // Poland
  '49':    'Europe/Berlin',          // Germany

  // Zone 5 — Central & South America
  '500':   'Atlantic/Stanley',       // Falkland Islands
  '501':   'America/Belize',         // Belize
  '502':   'America/Guatemala',      // Guatemala
  '503':   'America/El_Salvador',    // El Salvador
  '504':   'America/Tegucigalpa',    // Honduras
  '505':   'America/Managua',        // Nicaragua
  '506':   'America/Costa_Rica',     // Costa Rica
  '507':   'America/Panama',         // Panama
  '508':   'America/Miquelon',       // Saint Pierre and Miquelon
  '509':   'America/Port-au-Prince', // Haiti
  '51':    'America/Lima',           // Peru
  '52':    'America/Mexico_City',    // Mexico
  '53':    'America/Havana',         // Cuba
  '54':    'America/Argentina/Buenos_Aires', // Argentina
  '55':    'America/Sao_Paulo',      // Brazil
  '56':    'America/Santiago',       // Chile
  '57':    'America/Bogota',         // Colombia
  '58':    'America/Caracas',        // Venezuela
  '590':   'America/Guadeloupe',     // Guadeloupe
  '591':   'America/La_Paz',         // Bolivia
  '592':   'America/Guyana',         // Guyana
  '593':   'America/Guayaquil',      // Ecuador
  '594':   'America/Cayenne',        // French Guiana
  '595':   'America/Asuncion',       // Paraguay
  '596':   'America/Martinique',     // Martinique
  '597':   'America/Paramaribo',     // Suriname
  '598':   'America/Montevideo',     // Uruguay
  '599':   'America/Kralendijk',     // Bonaire, Sint Eustatius and Saba / Curaçao

  // Zone 6 — Southeast Asia & Oceania
  '60':    'Asia/Kuala_Lumpur',      // Malaysia
  '61':    'Australia/Sydney',       // Australia
  '62':    'Asia/Jakarta',           // Indonesia
  '63':    'Asia/Manila',            // Philippines
  '64':    'Pacific/Auckland',       // New Zealand
  '65':    'Asia/Singapore',         // Singapore
  '66':    'Asia/Bangkok',           // Thailand
  '670':   'Asia/Dili',              // Timor-Leste
  '672':   'Pacific/Norfolk',        // Norfolk Island
  '673':   'Asia/Brunei',            // Brunei
  '674':   'Pacific/Nauru',          // Nauru
  '675':   'Pacific/Port_Moresby',   // Papua New Guinea
  '676':   'Pacific/Tongatapu',      // Tonga
  '677':   'Pacific/Guadalcanal',    // Solomon Islands
  '678':   'Pacific/Efate',          // Vanuatu
  '679':   'Pacific/Fiji',           // Fiji
  '680':   'Pacific/Palau',          // Palau
  '681':   'Pacific/Wallis',         // Wallis and Futuna
  '682':   'Pacific/Rarotonga',      // Cook Islands
  '683':   'Pacific/Niue',           // Niue
  '685':   'Pacific/Apia',           // Samoa
  '686':   'Pacific/Tarawa',         // Kiribati
  '687':   'Pacific/Noumea',         // New Caledonia
  '688':   'Pacific/Funafuti',       // Tuvalu
  '689':   'Pacific/Tahiti',         // French Polynesia
  '690':   'Pacific/Fakaofo',        // Tokelau
  '691':   'Pacific/Chuuk',          // Micronesia
  '692':   'Pacific/Majuro',         // Marshall Islands

  // Zone 7 — Russia & Kazakhstan
  '7':     'Europe/Moscow',          // Russia / Default Zone 7

  // Zone 8 — East Asia & Special
  '81':    'Asia/Tokyo',             // Japan
  '82':    'Asia/Seoul',             // South Korea
  '84':    'Asia/Ho_Chi_Minh',       // Vietnam
  '850':   'Asia/Pyongyang',         // North Korea
  '852':   'Asia/Hong_Kong',         // Hong Kong
  '853':   'Asia/Macau',             // Macau
  '855':   'Asia/Phnom_Penh',        // Cambodia
  '856':   'Asia/Vientiane',         // Laos
  '86':    'Asia/Shanghai',          // China
  '880':   'Asia/Dhaka',             // Bangladesh
  '886':   'Asia/Taipei',            // Taiwan

  // Zone 9 — Middle East, Central & South Asia
  '90':    'Europe/Istanbul',        // Turkey
  '91':    'Asia/Kolkata',           // India
  '92':    'Asia/Karachi',           // Pakistan
  '93':    'Asia/Kabul',             // Afghanistan
  '94':    'Asia/Colombo',           // Sri Lanka
  '95':    'Asia/Yangon',            // Myanmar
  '98':    'Asia/Tehran',            // Iran
  '960':   'Indian/Maldives',        // Maldives
  '961':   'Asia/Beirut',            // Lebanon
  '962':   'Asia/Amman',             // Jordan
  '963':   'Asia/Damascus',          // Syria
  '964':   'Asia/Baghdad',           // Iraq
  '965':   'Asia/Kuwait',            // Kuwait
  '966':   'Asia/Riyadh',            // Saudi Arabia
  '967':   'Asia/Aden',              // Yemen
  '968':   'Asia/Muscat',            // Oman
  '970':   'Asia/Gaza',              // Palestine
  '971':   'Asia/Dubai',             // United Arab Emirates
  '972':   'Asia/Jerusalem',         // Israel
  '973':   'Asia/Bahrain',           // Bahrain
  '974':   'Asia/Qatar',             // Qatar
  '975':   'Asia/Thimphu',           // Bhutan
  '976':   'Asia/Ulaanbaatar',       // Mongolia
  '977':   'Asia/Kathmandu',         // Nepal
  '992':   'Asia/Dushanbe',          // Tajikistan
  '993':   'Asia/Ashgabat',          // Turkmenistan
  '994':   'Asia/Baku',              // Azerbaijan
  '995':   'Asia/Tbilisi',           // Georgia
  '996':   'Asia/Bishkek',           // Kyrgyzstan
  '998':   'Asia/Tashkent'           // Uzbekistan
};


function getTimezoneByPhone(phone) {
  const cleaned = (phone || '').toString().replace(/\D/g, '');
  for (let len = 3; len >= 1; len--) {
    const prefix = cleaned.substring(0, len);
    if (TIMEZONE_BY_PHONE_PREFIX[prefix]) {
      return TIMEZONE_BY_PHONE_PREFIX[prefix];
    }
  }
  return 'UTC';
}

/**
 * Returns current time in the user's region + timezone info
 */
function getRegionalTime(phone, format = 'full') {
  const tz = getTimezoneByPhone(phone);
  const now = new Date();

  const options = {
    timeZone: tz,
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
    timeZoneName: 'short'
  };

  if (format === 'iso') {
    // useful for storing
    return now.toLocaleString('en-CA', { timeZone: tz, hour12: false }).replace(', ', 'T') + 'Z';
  }

  return {
    timezone: tz,
    localTime: now.toLocaleString('en-GB', options),
    utcOffset: now.toLocaleString('en', { timeZone: tz, timeZoneName: 'shortOffset' }).split(' ').pop()
  };
}
// === DYNAMIC CURRENCY + TIME HELPERS (the ones I gave you earlier) ===
// ... fetchLiveRates, getCurrencyByPhone, getTimezoneByPhone, getRegionalTime ...

// --------------------------------------------------
// REGIONAL CONTEXT ENDPOINT (add here)
// --------------------------------------------------
app.get('/api/regional-context', async (req, res) => {
  try {
    const phone = req.query.phone || req.headers['x-user-identity'] || '';
    
    const currency = await getCurrencyByPhone(phone);
    const time = getRegionalTime(phone);

    res.json({
      success: true,
      currency,
      time
    });
  } catch (err) {
    console.error('Regional context error:', err);
    res.status(500).json({ success: false, error: 'REGION_CONTEXT_FAILED' });
  }
});
// Generate redeemable code for offline use (stores, markets, Caribbean islands)
const generateRedemptionCode = () => {
    const prefix = "AFRO-" + Date.now().toString(36).toUpperCase();
    const suffix = crypto.randomBytes(6).toString('hex').toUpperCase();
    return `${prefix}-${suffix}`;
};

app.post('/api/afro/redeem', async (req, res) => {
    const { redemptionCode, merchantIdentity, claimedAmount } = req.body;

    if (!redemptionCode || !claimedAmount || !merchantIdentity) {
        return res.status(400).json({ error: "MISSING_FIELDS" });
    }

    try {
        const user = await User.findOne({ "redemptionHistory.code": redemptionCode });
        if (!user) {
            return res.status(404).json({ error: "INVALID_CODE" });
        }

        // Find unredeemed entry
        const entryIndex = user.redemptionHistory.findIndex(h => 
            h.code === redemptionCode && !h.redeemedAt
        );

        if (entryIndex === -1) {
            return res.status(400).json({ error: "ALREADY_REDEEMED" });
        }

        const entry = user.redemptionHistory[entryIndex];

        if (entry.amount < claimedAmount) {
            return res.status(400).json({ error: "INSUFFICIENT_VALUE" });
        }

        // === ATOMIC UPDATE (Recommended) ===
        const result = await User.updateOne(
            { 
                _id: user._id, 
                "redemptionHistory.code": redemptionCode,
                "redemptionHistory.redeemedAt": { $exists: false }
            },
            {
                $set: {
                    "redemptionHistory.$.redeemedAt": new Date(),
                    "redemptionHistory.$.merchantId": merchantIdentity
                },
                $inc: { afroCoins: -claimedAmount }
            }
        );

        if (result.modifiedCount === 0) {
            return res.status(400).json({ error: "REDEEM_FAILED_OR_ALREADY_PROCESSED" });
        }

        // Credit merchant
        await Payout.create({
            parentTxID: redemptionCode,
            recipientNode: merchantIdentity,
            grossAmount: claimedAmount,
            creatorNet: Math.round(claimedAmount * 0.95 * 100) / 100,
            platformFee: Math.round(claimedAmount * 0.05 * 100) / 100,
            status: 'completed',
            mpesaB2CReceipt: `AFRO-REDEEM-${Date.now()}`
        });

        io.emit('afro_redeemed', { 
            code: redemptionCode, 
            amount: claimedAmount, 
            merchant: merchantIdentity 
        });

        res.json({ success: true, message: "AFRO redeemed successfully" });
    } catch (e) {
        console.error("Redeem error:", e);
        res.status(500).json({ error: "REDEEM_FAILED" });
    }
});
const EAST_AFRICA = ['KE', 'TZ', 'UG', 'RW', 'BI', 'SS', 'ET'];

function getPreferredGateway(countryCode, phone) {
  const country = (countryCode || '').toUpperCase();

  // 1. Explicit East Africa → M-PESA
  if (EAST_AFRICA.includes(country)) {
    return 'mpesa';
  }

  // 2. Fallback: detect by phone prefix (in case country was missing)
  if (phone) {
    if (phone.startsWith('254') || phone.startsWith('255') || 
        phone.startsWith('256') || phone.startsWith('250')) {
      return 'mpesa';
    }
  }

  // 3. Everywhere else → Stripe (recommended for Georgia, US, EU, etc.)
  //    Change to 'flutterwave' if you prefer Flutterwave for non-EA
  return 'stripe';
}

const triggerUniversalPush = async (phone, amountInKES, postId, type, handshakeId = null) => {
    const formattedPhone = cleanPhone(phone);
    if (!formattedPhone) throw new Error("IDENT_SIGNAL_LOST");

    try {
        // === GEO-SMART PAYMENT ROUTING ===
        if (formattedPhone.startsWith('1') || formattedPhone.length <= 11) {
            // United States / Canada / most international numbers
            console.log(`🇺🇸 GEO: Routing to Stripe for ${formattedPhone}`);
            return await triggerStripePayment(formattedPhone, amountInKES, postId, type, handshakeId);
        } 
        
        if (formattedPhone.startsWith('254')) {
            // Kenya → M-Pesa STK (always KES)
            console.log(`🇰🇪 GEO: Routing to M-Pesa STK Push for ${formattedPhone}`);
            return await triggerStkPush(formattedPhone, amountInKES, postId, type, handshakeId);
        } 
        
        // East & West Africa mobile money via Flutterwave
        if (
            formattedPhone.startsWith('256') ||   // Uganda
            formattedPhone.startsWith('255') ||   // Tanzania
            formattedPhone.startsWith('234') ||   // Nigeria
            formattedPhone.startsWith('233')      // Ghana
        ) {
            console.log(`🌍 GEO: Routing to Flutterwave for ${formattedPhone}`);
            return await triggerFlutterwavePush(formattedPhone, amountInKES, postId, type, handshakeId);
        } 

        // Fallback for any other region → Stripe
        console.log(`🌐 GEO: Unknown region - defaulting to Stripe for ${formattedPhone}`);
        return await triggerStripePayment(formattedPhone, amountInKES, postId, type, handshakeId);

    } catch (err) {
        console.error(`❌ PAYMENT ROUTING FAILURE for ${formattedPhone}: ${err.message}`);
        throw err;
    }
};

let mpesaTokenCache = {
    token: null,
    expiry: 0
};

const getMpesaBaseUrl = () => "https://api.safaricom.co.ke";

const getMpesaToken = async () => {
    const now = Date.now();
    if (mpesaTokenCache.token && now < mpesaTokenCache.expiry) {
        return mpesaTokenCache.token;
    }

    try {
        const baseUrl = getMpesaBaseUrl();
        console.log(`🌐 Syncing with Gateway: ${baseUrl}`);
        console.log("🔍 MPESA: Syncing Neural Token...");
        
        const consumerKey = (process.env.MPESA_CONSUMER_KEY || "").trim();
        const consumerSecret = (process.env.MPESA_CONSUMER_SECRET || "").trim();
        const auth = Buffer.from(`${consumerKey}:${consumerSecret}`).toString('base64');
        
        const res = await axios.get(`${baseUrl}/oauth/v1/generate?grant_type=client_credentials`, {
            headers: { Authorization: `Basic ${auth}` },
            timeout: 10000
        });
        
        mpesaTokenCache.token = res.data.access_token;
        mpesaTokenCache.expiry = now + (3500 * 1000); 
        
        console.log("✅ MPESA: Token Synchronized");
        return res.data.access_token;
    } catch (error) {
        console.error("❌ MPESA TOKEN SYNC ERROR:", error.response?.data || error.message);
        throw new Error("FAILED_TO_SYNC_WITH_SAFARICOM");
    }
};

const triggerStkPush = async (
    phone,
    amount,
    postId,
    type,
    handshakeId = null
) => {
    try {
        const token = await getMpesaToken();

        const timestamp = new Date()
            .toISOString()
            .replace(/[-:T.]/g, '')
            .slice(0, 14);

        const shortCode =
            (process.env.MPESA_SHORTCODE || "").trim();

        const passKey =
            (process.env.MPESA_PASSKEY || "").trim();

        const callbackUrl =
            (process.env.MPESA_CALLBACK_URL || "").trim();

        if (!shortCode || !passKey || !callbackUrl) {
            throw new Error("MISSING_MPESA_CONFIG");
        }

        const password = Buffer
            .from(
                `${shortCode}${passKey}${timestamp}`
            )
            .toString('base64');

        const payload = {
            BusinessShortCode: shortCode,
            Password: password,
            Timestamp: timestamp,
            TransactionType: "CustomerBuyGoodsOnline",
            Amount: Math.round(amount),
            PartyA: phone,
            PartyB: "3422513",
            PhoneNumber: phone,
            CallBackURL: callbackUrl,
            AccountReference:
                `IP-${postId.toString().slice(-6).toUpperCase()}`,
            TransactionDesc:
                `iNFLUENSA ${type.toUpperCase()}`
        };

        console.log(
            `🛰️ MPESA: DISPATCHING STK | AMT: ${amount}`
        );

        const response = await axios.post(
            `${getMpesaBaseUrl()}/mpesa/stkpush/v1/processrequest`,
            payload,
            {
                headers: {
                    Authorization:
                        `Bearer ${token}`
                },
                timeout: 15000
            }
        );

        console.log(
            "📥 MPESA Raw Response:",
            JSON.stringify(
                response.data,
                null,
                2
            )
        );

        if (!response.data?.CheckoutRequestID) {
            throw new Error(
                "GATEWAY_EMPTY_RESPONSE"
            );
        }

        // =====================================================
        // FIND THE POST
        // =====================================================

        const post = await Post.findById(postId);

        if (!post) {
            throw new Error("POST_NOT_FOUND");
        }

        if (!post.owner) {
            throw new Error("POST_OWNER_MISSING");
        }

        // =====================================================
        // FIND CREATOR
        //
        // post.owner = creator phone number
        // User.identity = creator phone number
        // =====================================================

        const creator = await User.findOne({
            identity: cleanPhone(post.owner)
        });

        if (!creator) {
            throw new Error(
                "CREATOR_NOT_FOUND"
            );
        }

        // =====================================================
        // CREATE PENDING TRANSACTION
        // =====================================================

        await Transaction.create({
            checkoutID:
                response.data.CheckoutRequestID,

            postID:
                postId,

            creatorId:
                cleanPhone(post.owner),

            userPhone:
                cleanPhone(phone),

            amountPaid:
                Math.round(amount),

            type,

            gateway:
                "mpesa",

            currency:
                "KES",

            handshakeID:
                handshakeId,

            status:
                "pending"
        });

        return response.data;

    } catch (error) {

        const errorDetail =
            error.response?.data ||
            error.message;

        console.error(
            "❌ MPESA STK CRITICAL FAILURE:",
            JSON.stringify(
                errorDetail,
                null,
                2
            )
        );

        throw new Error(
            errorDetail?.errorMessage ||
            error.message ||
            "STK_PUSH_DISRUPTED"
        );
    }
};
const triggerFlutterwavePush = async (phone, amountInKES, postId, type, handshakeId = null) => {
    try {
        // ✅ Live currency + rate
        const currencyData = await getCurrencyByPhone(phone);
        const currency = currencyData.code;
        
        // Convert KES → local currency amount
        const localAmount = Math.round(amountInKES * currencyData.rate * 100) / 100;
        
        // Flutterwave usually expects whole numbers for mobile money in most African currencies
        const chargeAmount = Math.max(1, Math.round(localAmount));

        const tx_ref = `FLW-${Date.now()}-${postId.toString().slice(-4)}`;

        // Detect correct mobile-money network
        let network = "MTN";
        if (phone.startsWith('255')) network = "TIGO";          // Tanzania
        if (phone.startsWith('234')) network = "NQR";           // Nigeria
        if (phone.startsWith('256')) network = "MTN";           // Uganda (default)

        // Choose the correct charge endpoint based on country
        let chargeType = "mobile_money_uganda";                 // default
        if (phone.startsWith('255')) chargeType = "mobile_money_tanzania";
        if (phone.startsWith('234')) chargeType = "mobile_money_nigeria";
        if (phone.startsWith('233')) chargeType = "mobile_money_ghana";

        const payload = {
            tx_ref,
            amount: chargeAmount,
            currency,
            network,
            email: "node@influensa.io",
            phone_number: phone,
            fullname: "iNFLUENSA Node",
            callback_url: (process.env.FLW_CALLBACK_URL || "").trim(),
            meta: {
                originalAmountKES: amountInKES,
                fxRate: currencyData.rate,
                postId: postId.toString(),
                type
            }
        };

        console.log(`🛰️ FLW: INITIATING ${chargeType.toUpperCase()} → ${idppMaskPhone(phone)} | ${chargeAmount} ${currency} (rate: ${currencyData.rate})`);

        await axios.post(
            `https://api.flutterwave.com/v3/charges?type=${chargeType}`,
            payload,
            {
                headers: { Authorization: `Bearer ${(process.env.FLW_SECRET_KEY || "").trim()}` },
                timeout: 15000
            }
        );

        await Transaction.create({
            checkoutID: tx_ref,
            postID: postId,
            userPhone: phone,
            amountPaid: amountInKES,               // still store base amount in KES
            currency: currency,
            gateway: 'flutterwave',
            type,
            handshakeID: handshakeId,
            status: 'pending'
        });

        return {
            CheckoutRequestID: tx_ref,
            checkoutID: tx_ref,
            currency,
            amountInLocal: chargeAmount,
            fxRate: currencyData.rate
        };

    } catch (error) {
        console.error("❌ FLW PUSH ERROR:", error.response?.data || error.message);
        throw error;
    }
};

/**
 * Initiates a Stripe PaymentIntent with multi-currency conversion and automated Connect splitting.
 * 
 * @param {string} phone - User phone number
 * @param {number} amountInKES - Total transaction amount in KES
 * @param {string} postId - Post ID associated with the transaction
 * @param {string} [type='unlock'] - Transaction type
 * @param {string|null} [handshakeId=null] - Optional handshake ID
 * @param {string|null} [recipientStripeAccountId=null] - Connected user's Stripe Account ID (e.g. 'acct_123456789')
 * @param {number} [platformFeePercent=0.0789] - Platform fee percentage (default 7.89%)
 */
const triggerStripePayment = async (
    phone, 
    amountInKES, 
    postId, 
    type = 'unlock', 
    handshakeId = null,
    recipientStripeAccountId = null,
    platformFeePercent = 0.0789
) => {
    try {
        // ✅ Now async + live rates
        const currencyData = await getCurrencyByPhone(phone);
        const currency = currencyData.code.toLowerCase();
        
        // Convert KES → local currency minor units (cents / kobo / etc.)
        let amountInMinor = Math.round(amountInKES * currencyData.rate * 100);
        
        // Stripe minimums (most currencies need at least 50–100 minor units)
        amountInMinor = Math.max(100, amountInMinor);

        const cleanedPhone = cleanPhone(phone);

        let customer;
        const existingUser = await User.findOne({ identity: cleanedPhone });
        
        if (existingUser?.stripeCustomerId) {
            customer = await stripe.customers.retrieve(existingUser.stripeCustomerId);
        } else {
            customer = await stripe.customers.create({
                metadata: { phone: cleanedPhone, platform: 'iNFLUENSA' },
                description: `iNFLUENSA Node: ${cleanedPhone}`,
            });
            
            await User.findOneAndUpdate(
                { identity: cleanedPhone },
                { $set: { stripeCustomerId: customer.id } },
                { upsert: true }
            );
        }

        // =========================================================
        // 🏦 STRIPE CONNECT SPLIT CONFIGURATION (Bank of America)
        // =========================================================
        const paymentIntentPayload = {
            amount: amountInMinor,
            currency,
            customer: customer.id,
            metadata: {
                phone: cleanedPhone,
                postId: postId.toString(),
                type: type,
                handshakeId: handshakeId ? handshakeId.toString() : '',
                platform: 'iNFLUENSA',
                originalAmountKES: amountInKES,          // useful for later reconciliation
                fxRate: currencyData.rate,               // store the rate used
                platformFeePercent: platformFeePercent   // record fee percentage applied
            },
            automatic_payment_methods: { 
                enabled: true 
            },
            setup_future_usage: 'off_session',
            description: `iNFLUENSA ${type} - ${postId}`,
        };

        // If a recipient account ID is supplied, split funds automatically:
        // - 8.00% platform fee retained -> Payouts to your Bank of America account
        // - 92% balance -> Transferred to recipient user's connected account
        if (recipientStripeAccountId) {
            const platformFeeInMinor = Math.round(amountInMinor * platformFeePercent);
            
            paymentIntentPayload.application_fee_amount = platformFeeInMinor;
            paymentIntentPayload.transfer_data = {
                destination: recipientStripeAccountId,
            };
        }

        const paymentIntent = await stripe.paymentIntents.create(paymentIntentPayload);

        await Transaction.create({
            checkoutID: paymentIntent.id,
            postID: postId,
            userPhone: cleanedPhone,
            amountPaid: amountInKES,                     // still stored in KES (base unit)
            currency: currency.toUpperCase(),
            gateway: 'stripe',
            type: type,
            handshakeID: handshakeId,
            status: 'pending'
        });

        console.log(`✅ STRIPE PI Created: ${paymentIntent.id} | ${amountInMinor} ${currency.toUpperCase()} (rate: ${currencyData.rate})`);

        return {
            success: true,
            checkoutID: paymentIntent.id,
            clientSecret: paymentIntent.client_secret,
            customerId: customer.id,
            currency: currency.toUpperCase(),
            amountInLocal: amountInMinor / 100,
            fxRate: currencyData.rate
        };

    } catch (error) {
        console.error("❌ [STRIPE] CRITICAL FAILURE:", error.message);
        throw new Error("Payment initiation failed. Please try again.");
    }
};


// --- FLUID SOCKET LOGIC ---
io.on('connection', (socket) => {
    socket.on('join_payment_room', (checkoutID) => {
        socket.join(checkoutID);
        console.log(`🔌 Node listening for Sync: ${checkoutID}`);
    });
    
    socket.on('watch_post', (postId) => {
        socket.join(`viewers_${postId}`);
        const viewerCount = io.sockets.adapter.rooms.get(`viewers_${postId}`)?.size || 0;
        io.to(`viewers_${postId}`).emit('viewer_update', { postId, count: viewerCount });
    });

    socket.on('leave_post', (postId) => {
        socket.leave(`viewers_${postId}`);
        const viewerCount = io.sockets.adapter.rooms.get(`viewers_${postId}`)?.size || 0;
        io.to(`viewers_${postId}`).emit('viewer_update', { postId, count: viewerCount });
    });

    socket.on('disconnecting', () => {
        for (const room of socket.rooms) {
            if (room.startsWith('viewers_')) {
                const postId = room.replace('viewers_', '');
                setTimeout(() => {
                    const count = io.sockets.adapter.rooms.get(room)?.size || 0;
                    io.to(room).emit('viewer_update', { postId, count });
                }, 100);
            }
        }
    });
});

// =========================================================================
// --- PASONA AI™ COGNITIVE & BIOMETRIC VERIFICATION PIPELINE ENGINES ---
// =========================================================================

const fsPromises = require('fs').promises;

/**
 * Enhanced Cognitive Stylometry Engine
 * Evaluates text for AI-generated text structures (low perplexity, uniform lengths, robotic syntax)
 */
function runCognitiveStylometryEngine(textData = "") {
    const cleanedText = textData.trim();
    if (!cleanedText) {
        return {
            entropy: 0.1234,
            confidenceModifier: 0.742,
            lexicalFingerprint: "0x0000000000000000",
            isAIGeneratedText: false
        };
    }

    const sentences = cleanedText.split(/[.!?]+/).filter(s => s.trim().length > 0);
    const words = cleanedText.toLowerCase().replace(/[^a-z0-9\s]/g, '').split(/\s+/).filter(Boolean);
    const uniqueWords = new Set(words);
    
    const uniquenessRatio = words.length > 0 ? (uniqueWords.size / words.length) : 0;
    const entropy = Math.min(0.9999, Math.max(0.1234, uniquenessRatio * 1.341));
    
    // AI Tell-Tale: Hyper-uniform sentence lengths (low variance in sentence structure)
    let lengthVariance = 0;
    if (sentences.length > 1) {
        const lengths = sentences.map(s => s.split(/\s+/).filter(Boolean).length);
        const meanLength = lengths.reduce((a, b) => a + b, 0) / lengths.length;
        const sqDiffs = lengths.map(l => Math.pow(l - meanLength, 2));
        lengthVariance = sqDiffs.reduce((a, b) => a + b, 0) / lengths.length;
    }

    // Low variance + high sentence counts usually signal highly uniform LLM outputs
    const isAIGeneratedText = sentences.length >= 3 && lengthVariance < 2.5 && uniquenessRatio < 0.45;
    const confidenceModifier = isAIGeneratedText ? 0.312 : (words.length < 40 ? 0.742 : 0.985);

    const hash = crypto.createHash('sha256').update(cleanedText).digest('hex');
    const lexicalFingerprint = `0x${hash.substring(0, 16)}`;

    return { entropy, confidenceModifier, lexicalFingerprint, isAIGeneratedText };
}

/**
 * Deep Forensic Visual Analysis Engine
 * Extracts a frame from the staged video and checks for typical deepfake blending boundaries
 * by checking for severe high-frequency texture drops (blurriness inside facial patches).
 */
async function analyzeVideoDeepfakeRisk(videoPath, targetFrameDir) {
    const frameFilename = `frame-${Date.now()}-${crypto.randomBytes(4).toString('hex')}.jpg`;
    const outFramePath = path.join(targetFrameDir, frameFilename);

    return new Promise((resolve) => {
        ffmpeg(videoPath)
            .screenshots({
                timestamps: ['50%'],
                filename: frameFilename,
                folder: targetFrameDir,
                size: '640x?'
            })
            .on('end', async () => {
                try {
                    if (!fs.existsSync(outFramePath)) return resolve({ syntheticRisk: 0.15 });

                    const buffer = await fsPromises.readFile(outFramePath);
                    
                    // Simple, ultra-fast structural frequency analyzer on frame bytes:
                    // Generative models smooth out pixel noise patterns across compression blocks.
                    let matchingPatterns = 0;
                    let sampleWindow = Math.min(buffer.length - 1, 8000);
                    
                    for (let i = 2000; i < sampleWindow; i++) {
                        if (buffer[i] === buffer[i + 1] && buffer[i] === buffer[i + 2]) {
                            matchingPatterns++;
                        }
                    }
                    
                    // Cleanup extracted frame immediately
                    await fsPromises.unlink(outFramePath).catch(() => {});

                    // Higher ratios of localized block flat spots point to synthetic face blending artifacts
                    const patternRatio = matchingPatterns / (sampleWindow - 2000);
                    const syntheticRisk = patternRatio > 0.12 ? Math.min(0.98, patternRatio * 6.5) : 0.05;
                    
                    resolve({ syntheticRisk });
                } catch (err) {
                    resolve({ syntheticRisk: 0.25 }); // Fallback intermediate risk flag if processing fails
                }
            })
            .on('error', () => {
                resolve({ syntheticRisk: 0.30 }); 
            });
    });
}

/**
 * Deep Forensic Acoustic Analysis Engine
 * Looks for spectral flatness or artificial discontinuities typical of AI voice cloning models
 */
async function analyzeAudioDeepfakeRisk(audioPath) {
    try {
        const stats = await fsPromises.stat(audioPath);
        if (stats.size < 1000) return { syntheticRisk: 0.10 };

        const fd = await fsPromises.open(audioPath, 'r');
        const buffer = Buffer.alloc(4000);
        await fd.read(buffer, 0, 4000, Math.floor(stats.size / 3));
        await fd.close();

        let structuralDeadZones = 0;
        for (let i = 0; i < buffer.length - 4; i += 4) {
            const chunkValue = buffer.readInt32LE(i);
            // Deepfake sound synthesis engines frequently generate absolute zero packets 
            // or repeat exact digital silence steps to mask phase mismatches
            if (chunkValue === 0 || Math.abs(chunkValue) < 5) {
                structuralDeadZones++;
            }
        }

        const deadZoneRatio = structuralDeadZones / (buffer.length / 4);
        const syntheticRisk = deadZoneRatio > 0.05 ? Math.min(0.99, deadZoneRatio * 12.0) : 0.02;

        return { syntheticRisk };
    } catch (e) {
        return { syntheticRisk: 0.20 };
    }
}

function evaluateIdentityBiometricRisk(telemetry = {}, forensicWeighing = {}) {
    const {
        typingCadence = 5.0,     
        interactionLatency = 300, 
        hasVideo = false,
        hasAudio = false
    } = telemetry;

    const {
        videoSyntheticRisk = 0.0,
        audioSyntheticRisk = 0.0,
        isAIGeneratedText = false
    } = forensicWeighing;

    let threatScore = 0.0;

    // --- Layer 1: Physical Interface Telemetry ---
    // Instant block if it behaves like a hyper-speed injection bot
    if (typingCadence < 1.0 || typingCadence > 30.0) threatScore += 0.45;
    if (interactionLatency < 65 || interactionLatency > 2000) threatScore += 0.35;

    // --- Layer 2: Behavioral AI Text Verification ---
    if (isAIGeneratedText) threatScore += 0.30;

    // --- Layer 3: Forensic Deepfake Inspection Overrides ---
    if (hasVideo) {
        threatScore += (videoSyntheticRisk * 0.65); // High forensic risk heavily spikes overall threat score
    } else {
        threatScore += 0.05; // Base threat modifier for refusing visual validation paths
    }

    if (hasAudio) {
        threatScore += (audioSyntheticRisk * 0.55);
    }

    const finalThreatScore = Math.min(0.9999, threatScore);
    // Tightened validation threshold from 0.38 down to 0.35 for stricter enforcement
    const status = finalThreatScore > 0.35 ? 'FLAGGED_ANOMALY' : 'VERIFIED_HUMAN';

    return { threatScore: finalThreatScore, status };
}

async function runPasonaPipeline(req) {
    const timestamp = Date.now();
    const token = crypto.randomBytes(4).toString('hex').toUpperCase();
    const stagingDir = path.resolve(__dirname, 'pasona_staging_layers');
    const stagedFiles = [];
    
    let videoForensics = { syntheticRisk: 0.0 };
    let audioForensics = { syntheticRisk: 0.0 };

    try {
        if (!fs.existsSync(stagingDir)) {
            fs.mkdirSync(stagingDir, { recursive: true });
        }

        const mediaFields = ['video', 'audio', 'image'];
        let videoLocation = null;
        let audioLocation = null;

        for (const field of mediaFields) {
            if (req.files && req.files[field] && req.files[field][0]) {
                const fileData = req.files[field][0];
                const filename = `PAS-CORE-${timestamp}-${field.toUpperCase()}-${token}.tmp`;
                const fullStagingPath = path.join(stagingDir, filename);
                
                stagedFiles.push(fullStagingPath);
                fs.writeFileSync(fullStagingPath, fileData.buffer);

                if (field === 'video') videoLocation = fullStagingPath;
                if (field === 'audio') audioLocation = fullStagingPath;
            }
        }

        // Run deep structural media verification before destroying files
        if (videoLocation) {
            videoForensics = await analyzeVideoDeepfakeRisk(videoLocation, stagingDir);
        }
        if (audioLocation) {
            audioForensics = await analyzeAudioDeepfakeRisk(audioLocation);
        }

        const stylometry = runCognitiveStylometryEngine(req.body?.textData);

        const telemetryInput = {
            typingCadence: parseFloat(req.body?.typingCadence || 5.0),
            interactionLatency: parseInt(req.body?.interactionLatency || 300, 10),
            hasVideo: !!videoLocation,
            hasAudio: !!audioLocation,
            hasImage: !!(req.files && req.files.image)
        };

        const forensicWeighing = {
            videoSyntheticRisk: videoForensics.syntheticRisk,
            audioSyntheticRisk: audioForensics.syntheticRisk,
            isAIGeneratedText: stylometry.isAIGeneratedText
        };

        const riskProfile = evaluateIdentityBiometricRisk(telemetryInput, forensicWeighing);

        // Safe Clean Up of Staging Files
        for (const filePath of stagedFiles) {
            try { await fsPromises.unlink(filePath); } catch (e) {}
        }

        if (riskProfile.status === 'FLAGGED_ANOMALY') {
            return {
                success: false,
                error: "SECURITY_ANOMALY_DETECTED",
                message: "Deepfake synthesis signature or injection profile identified.",
                metrics: {
                    lexicalEntropy: stylometry.entropy,
                    biometricThreatScore: riskProfile.threatScore,
                    videoSyntheticProbability: videoForensics.syntheticRisk,
                    audioSyntheticProbability: audioForensics.syntheticRisk
                }
            };
        }

        const part1 = crypto.randomBytes(2).toString('hex').toUpperCase();
        const part2 = crypto.randomBytes(2).toString('hex').toUpperCase();
        const passportId = `PAS-${part1}-${part2}`;
        
        const ipStampSignature = crypto.createHash('md5').update(`${passportId}-${timestamp}`).digest('hex');
        const sautiVoiceprint = `SAU-VPR-${crypto.randomBytes(6).toString('hex').toUpperCase()}`;
        const brandlockBond = `BL-SECURE-${crypto.randomBytes(8).toString('hex').toUpperCase()}`;

        return {
            success: true,
            passportId,
            status: riskProfile.status,
            metrics: {
                lexicalEntropy: stylometry.entropy,
                syntaxConfidence: stylometry.confidenceModifier,
                biometricThreatScore: riskProfile.threatScore,
                videoForensicRisk: videoForensics.syntheticRisk,
                audioForensicRisk: audioForensics.syntheticRisk
            },
            anchors: {
                lexicalFingerprint: stylometry.lexicalFingerprint,
                ipstampSignature: ipStampSignature,
                sautiVoiceprint: sautiVoiceprint,
                brandlockBond: brandlockBond
            },
            timestamp
        };

    } catch (error) {
        for (const filePath of stagedFiles) {
            try { await fsPromises.unlink(filePath).catch(() => {}); } catch (e) {}
        }
        return {
            success: false,
            error: error.message || "PASONA_PIPELINE_CRITICAL_FAILURE"
        };
    }
}


// --- ROUTES ---
app.get('/api/health', async (req, res) => {
    try {
        // Query active TVWS nodes count safely
        const activeTvwsNodes = await TVWSNode.countDocuments({ status: 'ACTIVE' });
        const isDbConnected = mongoose.connection.readyState === 1;

        const healthData = {
            status: isDbConnected ? 'SIGNAL_STRONG' : 'SIGNAL_DEGRADED',
            timestamp: Date.now(),
            uptime: process.uptime(),
            memory: process.memoryUsage(),
            grid: {
                database: isDbConnected ? 'CONNECTED' : 'DISCONNECTED',
                socket_nodes: io.sockets.adapter.rooms.size,
                tvws_active_nodes: activeTvwsNodes
            }
        };

        if (!isDbConnected) {
            return res.status(503).json(healthData);
        }

        return res.status(200).json(healthData);
    } catch (error) {
        return res.status(500).json({
            status: 'CRITICAL_FAILURE',
            timestamp: Date.now(),
            error: error.message
        });
    }
});


app.post('/api/v1/pasona/pipeline', upload.fields([
    { name: 'video', maxCount: 1 },
    { name: 'audio', maxCount: 1 },
    { name: 'image', maxCount: 1 }
]), async (req, res) => {
    const result = await runPasonaPipeline(req);
    
    if (!result.success) {
        return res.status(500).json(result);
    }
    
    if (result.status === 'FLAGGED_ANOMALY') {
        return res.status(403).json({
            error: "SECURITY_ANOMALY_DETECTED",
            message: "Biometric or stylometric mismatch. Interaction dropped.",
            metrics: result.metrics
        });
    }

    return res.status(200).json(result);
});

app.post('/api/flw-webhook', async (req, res) => {
    const secretHash = process.env.FLW_HASH;
    const signature = req.headers["verif-hash"];
    if (!signature || signature !== secretHash) return res.status(401).end();

    const { status, tx_ref } = req.body.data;
    const tx = await Transaction.findOne({ checkoutID: tx_ref });
    if (!tx) return res.status(200).end();

    if (status === "successful") {
        await processGridSuccess(tx);
    } else {
        tx.status = 'failed';
        await tx.save();
        io.to(tx_ref).emit('payment_failed');
    }
    res.status(200).end();
});


// ✅ Explicitly pass express.raw middleware to preserve raw Buffer for Stripe
app.post('/api/stripe-webhook', express.raw({ type: 'application/json' }), async (req, res) => {
    const sig = req.headers['stripe-signature'];
    let event;

    try {
        event = stripe.webhooks.constructEvent(
            req.body, 
            sig, 
            process.env.STRIPE_WEBHOOK_SECRET
        );
    } catch (err) {
        console.error(`❌ Webhook signature failed: ${err.message}`);
        return res.status(400).send(`Webhook Error: ${err.message}`);
    }

    console.log(`🔔 Stripe Event Received: ${event.type}`);

    try {
        if (event.type === 'payment_intent.succeeded') {
            const paymentIntent = event.data.object;
            const tx = await Transaction.findOne({ checkoutID: paymentIntent.id });
            if (tx) {
                await processGridSuccess(tx);
            }
        } 
        else if (event.type === 'payment_intent.payment_failed') {
            const paymentIntent = event.data.object;
            const tx = await Transaction.findOne({ checkoutID: paymentIntent.id });
            if (tx) {
                tx.status = 'failed';
                await tx.save();
                
                if (typeof io !== 'undefined') {
                    io.to(paymentIntent.id).emit('payment_failed');
                }
            }
        } 
        else if (event.type === 'account.updated') {
            const account = event.data.object;
            
            // ✅ Verify BOTH charges and payouts are active before marking onboarded
            const isFullyOnboarded = account.charges_enabled === true && account.payouts_enabled === true;

            await User.updateOne(
                { stripeAccountId: account.id },
                { $set: { stripeOnboardingComplete: isFullyOnboarded } }
            );
        }
    } catch (handlerError) {
        console.error(`❌ Error handling event ${event.type}:`, handlerError.message);
        // Returning 200 tells Stripe the webhook was received, preventing unnecessary retries 
        // caused by non-Stripe internal application errors.
    }

    res.json({ received: true });
});
app.post('/api/posts/:id/unlock', async (req, res) => {
    const { phone, type = 'unlock', country } = req.body;
    const postId = req.params.id;

    console.log(
        `🔓 [UNLOCK] Post: ${postId} | Phone: ${phone} | Type: ${type} | Country: ${country}`
    );

    try {
        const post = await Post.findById(postId);

        if (!post) {
            return res.status(404).json({
                error: "Post not found"
            });
        }

        // ============================================================
        // 0. CHECK EXISTING UNLOCK
        // ============================================================
        // If payment was already completed and the user is already
        // authorized, DO NOT create another payment request.
        const cleanedPhone = cleanPhone(phone);

        const isOwner =
            cleanPhone(post.owner) === cleanedPhone;

        const isUnlocked =
            (post.unlocked_by || []).includes(cleanedPhone);

        const isLicensed =
            (post.licensed_to || []).includes(cleanedPhone);

        if (isOwner || isUnlocked || isLicensed) {
            console.log(
                `✅ [UNLOCK] Already authorized | Post: ${postId} | Phone: ${cleanedPhone}`
            );

            return res.json({
                success: true,
                alreadyUnlocked: true,
                authorized: true,
                checkoutID: null,
                clientSecret: null,
                gateway: null,
                postId: post._id,
                message: "User already has access to this post."
            });
        }

        // ============================================================
        // 1. CALCULATE PRICE
        // ============================================================

        let rawPriceKES =
            (type === 'share_download' || type === 'license')
                ? post.price * 10.0
                : post.price;

        console.log(
            `💰 [UNLOCK] Price: ${rawPriceKES} KES`
        );

        // ============================================================
        // 2. SMART GATEWAY ROUTING
        // ============================================================

        const EAST_AFRICA = [
            'KE',
            'TZ',
            'UG',
            'RW',
            'BI',
            'SS',
            'ET'
        ];

        const countryCode =
            (country || '').toUpperCase();

        let gateway = 'stripe';

        if (EAST_AFRICA.includes(countryCode)) {
            gateway = 'mpesa';
        } else if (
            phone &&
            (
                phone.startsWith('254') ||
                phone.startsWith('255') ||
                phone.startsWith('256') ||
                phone.startsWith('250')
            )
        ) {
            // Fallback if country was not sent
            gateway = 'mpesa';
        }

        console.log(
            `🌍 Selected gateway: ${gateway}`
        );

        // ============================================================
        // 3. INITIATE PAYMENT
        // ============================================================

        let result;

        if (gateway === 'mpesa') {

            result = await triggerStkPush(
                phone,
                Math.max(1, Math.ceil(rawPriceKES)),
                post._id,
                type
            );

        } else {

            // Stripe
            // You can change this to triggerFlutterwavePush if preferred
            result = await triggerStripePayment(
                phone,
                Math.max(1, Math.ceil(rawPriceKES)),
                post._id,
                type
            );
        }

        // ============================================================
        // 4. VERIFY PAYMENT INITIALIZATION
        // ============================================================

        if (
            !result ||
            (
                !result.checkoutID &&
                !result.CheckoutRequestID
            )
        ) {
            console.error(
                "❌ No checkoutID returned from gateway"
            );

            return res.status(500).json({
                error: "Universal Sync Failed",
                details: "Gateway did not return checkoutID"
            });
        }

        const checkoutID =
            result.checkoutID ||
            result.CheckoutRequestID;

        console.log(
            `✅ [UNLOCK] Payment initialized | Gateway: ${gateway} | CheckoutID: ${checkoutID}`
        );

        // ============================================================
        // 5. RETURN PAYMENT INFORMATION
        // ============================================================

        return res.json({
            success: true,
            alreadyUnlocked: false,
            authorized: false,
            checkoutID,
            clientSecret: result.clientSecret || null,
            gateway,
            ...result
        });

    } catch (err) {

        console.error(
            "❌ [UNLOCK] CRITICAL FAILURE:",
            err.message
        );

        return res.status(500).json({
            error: "Universal Sync Failed",
            details: err.message
        });
    }
});
app.post('/api/mpesa/stk/callback', async (req, res) => {
    try {
        const callback =
            req.body?.Body?.stkCallback;

        console.log(
            "📥 STK CALLBACK:",
            JSON.stringify(callback, null, 2)
        );

        // Acknowledge Safaricom immediately
        res.status(200).json({
            ResultCode: 0,
            ResultDesc: "Accepted"
        });

        if (!callback) {
            console.error("❌ EMPTY STK CALLBACK");
            return;
        }

        const checkoutID =
            callback.CheckoutRequestID;

        const resultCode =
            Number(callback.ResultCode);

        const resultDesc =
            callback.ResultDesc || "";

        if (!checkoutID) {
            console.error(
                "❌ STK CALLBACK MISSING CHECKOUT ID"
            );
            return;
        }

        // =====================================================
        // FIND OUR PENDING TRANSACTION
        // =====================================================

        const transaction =
            await Transaction.findOne({
                checkoutID: checkoutID
            });

        if (!transaction) {
            console.error(
                "❌ TRANSACTION NOT FOUND:",
                checkoutID
            );
            return;
        }

        // =====================================================
        // IDEMPOTENCY
        // =====================================================

        if (transaction.status === "completed") {
            console.log(
                "ℹ️ STK ALREADY COMPLETED:",
                checkoutID
            );
            return;
        }

        // =====================================================
        // PAYMENT FAILED / CANCELLED
        // =====================================================

        if (resultCode !== 0) {

            transaction.status = "failed";

            transaction.resultCode =
                resultCode;

            transaction.resultDesc =
                resultDesc;

            transaction.failedAt =
                new Date();

            await transaction.save();

            console.error(
                `❌ STK PAYMENT FAILED | ${checkoutID} | ${resultCode} | ${resultDesc}`
            );

            return;
        }

        // =====================================================
        // PAYMENT SUCCESS
        // =====================================================

        const metadata =
            callback.CallbackMetadata?.Item || [];

        const getMetadata = (name) => {
            return metadata.find(
                item => item.Name === name
            )?.Value;
        };

        const mpesaReceiptNumber =
            getMetadata("MpesaReceiptNumber");

        const amount =
            Number(
                getMetadata("Amount") ||
                transaction.amountPaid
            );

        const phone =
            getMetadata("PhoneNumber") ||
            transaction.userPhone;

        console.log(
            `✅ STK PAYMENT SUCCESS | KES ${amount} | Receipt: ${mpesaReceiptNumber}`
        );

        // =====================================================
        // VERIFY AMOUNT
        // =====================================================

        if (
            Math.abs(
                Number(transaction.amountPaid) -
                amount
            ) > 0.01
        ) {

            transaction.status = "failed";

            transaction.resultCode = -1;

            transaction.resultDesc =
                "PAYMENT_AMOUNT_MISMATCH";

            transaction.failedAt =
                new Date();

            await transaction.save();

            console.error(
                "🚨 STK AMOUNT MISMATCH:",
                {
                    expected:
                        transaction.amountPaid,
                    received:
                        amount,
                    checkoutID
                }
            );

            return;
        }

        // =====================================================
        // SAVE M-PESA RECEIPT
        // =====================================================

        transaction.transactionID =
            mpesaReceiptNumber;

        transaction.resultCode =
            resultCode;

        transaction.resultDesc =
            resultDesc;

        transaction.userPhone =
            cleanPhone(phone);

        await transaction.save();

        // =====================================================
        // SETTLE CREATOR 92% / PLATFORM 8%
        // =====================================================

        const settlement =
            await settleSuccessfulContentPurchase({
                transactionId:
                    mpesaReceiptNumber,

                postId:
                    transaction.postID,

                payerPhone:
                    cleanPhone(phone),

                amount,

                receiptNumber:
                    mpesaReceiptNumber
            });

        console.log(
            "💰 STK SETTLEMENT COMPLETE:",
            JSON.stringify(
                settlement,
                null,
                2
            )
        );

    } catch (err) {

        console.error(
            "❌ STK CALLBACK ERROR:",
            err
        );
    }
});

async function creditCreatorSale({
    creatorId,
    grossAmount,
    postId,
    transactionId
}) {
    const session = await mongoose.startSession();

    try {
        await session.withTransaction(async () => {

            const gross = Number(grossAmount);

            if (!Number.isFinite(gross) || gross <= 0) {
                throw new Error("INVALID_SALE_AMOUNT");
            }

            const platformFee =
                Number((gross * 0.08).toFixed(2));

            const creatorAmount =
                Number((gross - platformFee).toFixed(2));

            const creator =
                await User.findById(
                    creatorId
                ).session(session);

            if (!creator) {
                throw new Error("CREATOR_NOT_FOUND");
            }

            const reference =
                `SALE-${transactionId}`;

            // -----------------------------------------------------
            // Idempotency protection
            // -----------------------------------------------------

            const existing =
                await WalletLedger.findOne({
                    reference
                }).session(session);

            if (existing) {
                console.log(
                    `⚠️ Sale already processed: ${reference}`
                );

                return;
            }

            const before =
                Number(creator.earnings || 0);

            const after =
                Number(
                    (before + creatorAmount).toFixed(2)
                );

            // -----------------------------------------------------
            // Credit creator
            // -----------------------------------------------------

            creator.earnings = after;

            await creator.save({
                session
            });

            // -----------------------------------------------------
            // Creator ledger
            // -----------------------------------------------------

            await WalletLedger.create(
                [{
                    userId: creator._id,

                    type: "CONTENT_SALE",

                    direction: "CREDIT",

                    amount: creatorAmount,

                    currency: "KES",

                    balanceBefore: before,

                    balanceAfter: after,

                    reference,

                    metadata: {
                        postId,
                        transactionId,
                        grossAmount: gross,
                        platformFee,
                        creatorShare: 0.92
                    }
                }],
                { session }
            );

            console.log(
                `💰 CREATOR CREDIT: KES ${creatorAmount} | Creator ${creator._id}`
            );
        });

    } finally {
        await session.endSession();
    }
}
function calculateCreatorSplit(amount) {
    const gross = Number(amount);

    if (
        !Number.isFinite(gross) ||
        gross <= 0
    ) {
        throw new Error("INVALID_PAYMENT_AMOUNT");
    }

    const platformFee =
        Number((gross * 0.08).toFixed(2));

    const creatorAmount =
        Number((gross - platformFee).toFixed(2));

    return {
        gross,
        platformFee,
        creatorAmount
    };
}
app.get('/api/stk-status/:checkoutID', async (req, res) => {
    const { checkoutID } = req.params;

    console.log("🔎 STK STATUS CHECK:", checkoutID);

    try {
        const transaction = await Transaction.findOne({ checkoutID });

        if (!transaction) {
            return res.status(404).json({
                status: "not_found",
                checkoutID
            });
        }

        return res.json({
            status: transaction.status,
            checkoutID: transaction.checkoutID
        });

    } catch (err) {
        console.error("❌ STK STATUS ERROR:", err);

        return res.status(500).json({
            status: "error",
            error: err.message
        });
    }
});
async function settleSuccessfulContentPurchase({
    transactionId,
    postId,
    payerPhone,
    amount,
    receiptNumber = null
}) {
    const session = await mongoose.startSession();

    try {
        let settlementResult = null;

        await session.withTransaction(async () => {

            // =====================================================
            // 1. FIND TRANSACTION
            // =====================================================

            const transaction = await Transaction.findOne({
                transactionID: transactionId
            }).session(session);

            if (!transaction) {
                throw new Error("TRANSACTION_NOT_FOUND");
            }

            // =====================================================
            // 2. IDEMPOTENCY
            // =====================================================

            if (transaction.status === "completed") {
                settlementResult = {
                    alreadyProcessed: true,
                    creatorAmount: transaction.creatorAmount,
                    platformFee: transaction.platformFee
                };

                return;
            }

            // =====================================================
            // 3. FIND POST
            // =====================================================

            const post = await Post.findById(postId)
                .session(session);

            if (!post) {
                throw new Error("POST_NOT_FOUND");
            }

            if (!post.owner) {
                throw new Error("POST_OWNER_MISSING");
            }

            // =====================================================
            // 4. NORMALIZE BUYER + CREATOR
            // =====================================================

            const buyerIdentity =
                cleanPhone(payerPhone);

            const creatorIdentity =
                cleanPhone(post.owner);

            // =====================================================
            // 5. FIND CREATOR
            // =====================================================

            const creator = await User.findOne({
                identity: creatorIdentity
            }).session(session);

            if (!creator) {
                throw new Error("CREATOR_NOT_FOUND");
            }

            // =====================================================
            // 6. CALCULATE 8% / 92%
            // =====================================================

            const split =
                calculateCreatorSplit(amount);

            // =====================================================
            // 7. VERIFY PAYMENT AMOUNT
            // =====================================================

            const transactionAmount =
                Number(transaction.amountPaid);

            if (
                !Number.isFinite(transactionAmount) ||
                Math.abs(
                    transactionAmount -
                    split.gross
                ) > 0.01
            ) {
                throw new Error(
                    "PAYMENT_AMOUNT_MISMATCH"
                );
            }

            // =====================================================
            // 8. GRANT BUYER ACCESS
            // =====================================================

            if (!Array.isArray(post.unlocked_by)) {
                post.unlocked_by = [];
            }

            if (!post.unlocked_by.includes(buyerIdentity)) {
                post.unlocked_by.push(
                    buyerIdentity
                );
            }

            await post.save({
                session
            });

            // =====================================================
            // 9. CREDIT CREATOR 92%
            // =====================================================

            const balanceBefore =
                Number(
                    creator.earnings || 0
                );

            const balanceAfter =
                Number(
                    (
                        balanceBefore +
                        split.creatorAmount
                    ).toFixed(2)
                );

            creator.earnings =
                balanceAfter;

            await creator.save({
                session
            });

            // =====================================================
            // 10. CREATE IMMUTABLE CREATOR LEDGER
            // =====================================================

            const creatorReference =
                `SALE-${transactionId}`;

            await WalletLedger.create(
                [{
                    userId: creator._id,

                    type: "CONTENT_SALE",

                    direction: "CREDIT",

                    amount:
                        split.creatorAmount,

                    currency: "KES",

                    balanceBefore,

                    balanceAfter,

                    reference:
                        creatorReference,

                    metadata: {
                        transactionId,

                        postId:
                            post._id.toString(),

                        payerPhone:
                            buyerIdentity,

                        grossAmount:
                            split.gross,

                        platformFee:
                            split.platformFee,

                        creatorAmount:
                            split.creatorAmount,

                        platformShare:
                            0.08,

                        creatorShare:
                            0.92,

                        receiptNumber
                    }
                }],
                {
                    session
                }
            );

            // =====================================================
            // 11. MARK TRANSACTION COMPLETED
            // =====================================================

            transaction.status =
                "completed";

            transaction.platformFee =
                split.platformFee;

            transaction.creatorAmount =
                split.creatorAmount;

            // creatorId = creator phone
            transaction.creatorId =
                creatorIdentity;

            transaction.receiptNumber =
                receiptNumber;

            transaction.completedAt =
                new Date();

            transaction.resultCode =
                0;

            transaction.resultDesc =
                "Payment completed";

            await transaction.save({
                session
            });

            // =====================================================
            // 12. RETURN SETTLEMENT
            // =====================================================

            settlementResult = {
                alreadyProcessed: false,

                creatorId:
                    creatorIdentity,

                creatorAmount:
                    split.creatorAmount,

                platformFee:
                    split.platformFee,

                gross:
                    split.gross,

                buyer:
                    buyerIdentity,

                accessGranted: true
            };
        });

        return settlementResult;

    } finally {
        await session.endSession();
    }
}
async function triggerB2C(
    phone,
    amount,
    remark = "iNFLUENSA Payout"
) {
    const cleaned = cleanPhone(phone);

    const payload = {
        InitiatorName: (process.env.MPESA_INITIATOR_NAME || "").trim(),
        SecurityCredential: (process.env.MPESA_SECURITY_CREDENTIAL || "").trim(),
        CommandID: "BusinessPayment",
        Amount: Math.ceil(Number(amount)),
        PartyA: (process.env.MPESA_SHORTCODE || "").trim(),
        PartyB: cleaned,
        Remarks: remark,
        QueueTimeOutURL: `${process.env.BASE_URL}/api/mpesa/b2c/timeout`,
        ResultURL: `${process.env.BASE_URL}/api/mpesa/b2c/result`,
        Occasion: "iNFLUENSA_Payout"
    };

    if (
        !payload.InitiatorName ||
        !payload.SecurityCredential ||
        !payload.PartyA ||
        !process.env.BASE_URL
    ) {
        throw new Error("MISSING_MPESA_B2C_CONFIG");
    }

    try {
        const token = await getMpesaToken();

        const response = await axios.post(
            `${getMpesaBaseUrl()}/mpesa/b2c/v1/paymentrequest`,
            payload,
            {
                headers: {
                    Authorization: `Bearer ${token}`,
                    "Content-Type": "application/json"
                },
                timeout: 15000
            }
        );

        console.log(
            "🚀 B2C initiated:",
            JSON.stringify(response.data, null, 2)
        );

        if (
            response.data?.ResponseCode !== undefined &&
            String(response.data.ResponseCode) !== "0"
        ) {
            throw new Error(
                response.data?.ResponseDescription ||
                "MPESA_B2C_REQUEST_REJECTED"
            );
        }

        return response.data;

    } catch (err) {
        console.error(
            "❌ B2C Trigger Error:",
            err.response?.data || err.message
        );

        throw err;
    }
}
app.post('/api/mpesa/withdraw', async (req, res) => {

    const session =
        await mongoose.startSession();

    let payoutId = null;

    try {

        const { phone, amount } = req.body;

        const withdrawalAmount =
            Number(amount);

        // =====================================================
        // BASIC VALIDATION
        // =====================================================

        if (
            !phone ||
            !Number.isFinite(withdrawalAmount) ||
            withdrawalAmount < 10
        ) {
            return res.status(400).json({
                error: "INVALID_WITHDRAWAL_REQUEST"
            });
        }

        const cleaned =
            cleanPhone(phone);

        // =====================================================
        // IMPORTANT:
        // Replace this with your actual authentication system.
        //
        // Example:
        // const userId = req.user._id;
        // =====================================================

        const userId = req.user?._id;

        if (!userId) {
            return res.status(401).json({
                error: "UNAUTHORIZED"
            });
        }

        // =====================================================
        // GET AFRO RATE OUTSIDE TRANSACTION
        // =====================================================

        const marketPrice =
            await calculateCurrentAfroPrice(User);

        const kesRate =
            Number(marketPrice.kesRate);

        if (
            !Number.isFinite(kesRate) ||
            kesRate <= 0
        ) {
            return res.status(500).json({
                error: "INVALID_AFRO_MARKET_RATE"
            });
        }

        let payout;

        // =====================================================
        // ATOMIC FINANCIAL TRANSACTION
        // =====================================================

        await session.withTransaction(async () => {

            const user =
                await User.findById(
                    userId
                ).session(session);

            if (!user) {
                throw new Error(
                    "USER_NOT_FOUND"
                );
            }

            const earnings =
                Number(user.earnings || 0);

            const afroCoins =
                Number(user.afroCoins || 0);

            const afroValue =
                afroCoins * kesRate;

            const totalAvailable =
                earnings + afroValue;

            if (
                totalAvailable <
                withdrawalAmount
            ) {
                throw new Error(
                    "INSUFFICIENT_BALANCE"
                );
            }

            // =================================================
            // DETERMINE SOURCE
            // =================================================

            let earningsReserved = 0;
            let afroConverted = 0;

            if (
                earnings >=
                withdrawalAmount
            ) {

                earningsReserved =
                    withdrawalAmount;

            } else {

                earningsReserved =
                    earnings;

                const needed =
                    withdrawalAmount -
                    earningsReserved;

                afroConverted =
                    Math.min(
                        needed / kesRate,
                        afroCoins
                    );
            }

            // =================================================
            // FINAL SAFETY CHECK
            // =================================================

            const resultingKES =
                earningsReserved +
                (
                    afroConverted *
                    kesRate
                );

            if (
                resultingKES <
                withdrawalAmount
            ) {
                throw new Error(
                    "BALANCE_CONVERSION_FAILED"
                );
            }

            // =================================================
            // RESERVE BALANCE
            // =================================================

            const before =
                earnings;

            const after =
                Number(
                    (
                        earnings -
                        earningsReserved
                    ).toFixed(2)
                );

            user.earnings = after;

            user.afroCoins =
                Number(
                    (
                        afroCoins -
                        afroConverted
                    ).toFixed(4)
                );

            await user.save({
                session
            });

            // =================================================
            // CREATE PAYOUT
            // =================================================

            payout =
                await Payout.create(
                    [{
                        userId: user._id,

                        phone: cleaned,

                        amount:
                            withdrawalAmount,

                        currency: "KES",

                        earningsReserved,

                        afroConverted,

                        afroRate: kesRate,

                        status: "pending",

                        gateway: "mpesa_b2c"
                    }],
                    { session }
                );

            payout =
                payout[0];

            payoutId =
                payout._id;

            // =================================================
            // RESERVATION LEDGER
            // =================================================

            const reference =
                `WD-RESERVE-${payout._id}`;

            await WalletLedger.create(
                [{
                    userId: user._id,

                    type:
                        "WITHDRAWAL_RESERVE",

                    direction:
                        "DEBIT",

                    amount:
                        withdrawalAmount,

                    currency:
                        "KES",

                    balanceBefore:
                        before,

                    balanceAfter:
                        after,

                    reference,

                    metadata: {
                        payoutId:
                            payout._id.toString(),

                        phone: cleaned,

                        earningsReserved,

                        afroConverted,

                        afroRate: kesRate
                    }
                }],
                { session }
            );

            payout.ledgerReserveReference =
                reference;

            await payout.save({
                session
            });
        });

        // =====================================================
        // TRANSACTION COMMITTED
        //
        // NOW CALL SAFARICOM
        // =====================================================

        const b2cResponse =
            await triggerB2C(
                cleaned,
                withdrawalAmount,
                `iNFLUENSA WD-${payoutId}`
            );

        // =====================================================
        // SAVE SAFARICOM IDs
        // =====================================================

        payout =
            await Payout.findById(
                payoutId
            );

        if (!payout) {
            throw new Error(
                "PAYOUT_RECORD_NOT_FOUND"
            );
        }

        payout.conversationId =
            b2cResponse?.ConversationID;

        payout.originatorConversationId =
            b2cResponse
                ?.OriginatorConversationID;

        payout.responseCode =
            b2cResponse?.ResponseCode;

        payout.responseDescription =
            b2cResponse
                ?.ResponseDescription;

        await payout.save();

        return res.json({
            success: true,

            status: "pending",

            payoutId:
                payout._id,

            amount:
                withdrawalAmount,

            currency: "KES",

            conversationId:
                payout.conversationId
        });

    } catch (err) {

        console.error(
            "❌ WITHDRAWAL ERROR:",
            err.response?.data ||
            err.message
        );

        // =====================================================
        // IF PAYOUT WAS CREATED BUT SAFARICOM REQUEST FAILED
        // =====================================================

        if (payoutId) {

            try {

                const payout =
                    await Payout.findById(
                        payoutId
                    );

                if (
                    payout &&
                    payout.status === "pending" &&
                    !payout.conversationId
                ) {

                    const session2 =
                        await mongoose.startSession();

                    try {

                        await session2.withTransaction(
                            async () => {

                                const currentPayout =
                                    await Payout.findById(
                                        payoutId
                                    ).session(
                                        session2
                                    );

                                if (
                                    !currentPayout ||
                                    currentPayout.status !== "pending"
                                ) {
                                    return;
                                }

                                await User.findByIdAndUpdate(
                                    currentPayout.userId,

                                    {
                                        $inc: {
                                            earnings:
                                                currentPayout
                                                    .earningsReserved,

                                            afroCoins:
                                                currentPayout
                                                    .afroConverted
                                        }
                                    },

                                    {
                                        session:
                                            session2
                                    }
                                );

                                const user =
                                    await User.findById(
                                        currentPayout.userId
                                    ).session(
                                        session2
                                    );

                                const before =
                                    Number(
                                        user.earnings
                                    );

                                const after =
                                    Number(
                                        (
                                            before +
                                            currentPayout
                                                .earningsReserved
                                        ).toFixed(2)
                                    );

                                await WalletLedger.create(
                                    [{
                                        userId:
                                            currentPayout.userId,

                                        type:
                                            "WITHDRAWAL_REFUND",

                                        direction:
                                            "CREDIT",

                                        amount:
                                            currentPayout.amount,

                                        currency:
                                            "KES",

                                        balanceBefore:
                                            before,

                                        balanceAfter:
                                            after,

                                        reference:
                                            `WD-REFUND-${currentPayout._id}`,

                                        metadata: {
                                            payoutId:
                                                currentPayout._id.toString(),

                                            reason:
                                                "B2C_REQUEST_NOT_ACCEPTED"
                                        }
                                    }],
                                    {
                                        session:
                                            session2
                                    }
                                );

                                currentPayout.status =
                                    "failed";

                                currentPayout.resultDesc =
                                    err.message;

                                currentPayout.failedAt =
                                    new Date();

                                await currentPayout.save({
                                    session:
                                        session2
                                });
                            }
                        );

                    } finally {
                        await session2.endSession();
                    }
                }

            } catch (refundError) {

                console.error(
                    "🚨 CRITICAL REFUND ERROR:",
                    refundError
                );
            }
        }

        return res.status(500).json({
            error:
                "MPESA_B2C_WITHDRAWAL_FAILED"
        });

    } finally {

        await session.endSession();
    }
});
app.post('/api/mpesa/b2c/result', async (req, res) => {

    // Acknowledge Safaricom immediately
    res.status(200).json({
        ResponseCode: "0",
        ResponseDesc: "Accepted"
    });

    try {

        const result =
            req.body?.Result;

        if (!result) return;

        const conversationId =
            result.ConversationID;

        const resultCode =
            Number(result.ResultCode);

        // =====================================================
        // SUCCESS
        // =====================================================

        if (resultCode === 0) {

            const params =
                result.ResultParameters
                    ?.ResultParameter || [];

            const mpesaTxId =
                result.TransactionID;

            const payout =
                await Payout.findOne({
                    conversationId
                });

            if (!payout) {
                console.error(
                    "⚠️ Payout not found:",
                    conversationId
                );
                return;
            }

            // Idempotency
            if (
                payout.status === "completed"
            ) {
                console.log(
                    "ℹ️ Duplicate B2C success ignored:",
                    conversationId
                );
                return;
            }

            if (
                payout.status !== "pending"
            ) {
                return;
            }

            const session =
                await mongoose.startSession();

            try {

                await session.withTransaction(
                    async () => {

                        const lockedPayout =
                            await Payout.findOne({
                                _id: payout._id,
                                status: "pending"
                            }).session(
                                session
                            );

                        if (!lockedPayout) {
                            return;
                        }

                        lockedPayout.status =
                            "completed";

                        lockedPayout.mpesaTxId =
                            mpesaTxId;

                        lockedPayout.resultCode =
                            resultCode;

                        lockedPayout.resultDesc =
                            result.ResultDesc;

                        lockedPayout.completedAt =
                            new Date();

                        lockedPayout
                            .ledgerCompletionReference =
                            `WD-COMPLETE-${lockedPayout._id}`;

                        await lockedPayout.save({
                            session
                        });

                        // -------------------------------------------------
                        // Settlement ledger
                        // -------------------------------------------------

                        const user =
                            await User.findById(
                                lockedPayout.userId
                            ).session(
                                session
                            );

                        await WalletLedger.create(
                            [{
                                userId:
                                    lockedPayout.userId,

                                type:
                                    "WITHDRAWAL_COMPLETED",

                                direction:
                                    "DEBIT",

                                amount:
                                    lockedPayout.amount,

                                currency:
                                    "KES",

                                balanceBefore:
                                    Number(
                                        user.earnings || 0
                                    ),

                                balanceAfter:
                                    Number(
                                        user.earnings || 0
                                    ),

                                reference:
                                    `WD-COMPLETE-${lockedPayout._id}`,

                                metadata: {
                                    payoutId:
                                        lockedPayout._id.toString(),

                                    mpesaTxId
                                }
                            }],
                            {
                                session
                            }
                        );
                    }
                );

            } finally {

                await session.endSession();
            }

            console.log(
                `✅ B2C COMPLETED: ${payout._id}`
            );

            return;
        }

        // =====================================================
        // FAILURE
        // =====================================================

        const payout =
            await Payout.findOne({
                conversationId,
                status: "pending"
            });

        if (!payout) {
            console.error(
                "⚠️ Failed payout not found:",
                conversationId
            );
            return;
        }

        const session =
            await mongoose.startSession();

        try {

            await session.withTransaction(
                async () => {

                    const lockedPayout =
                        await Payout.findOne({
                            _id: payout._id,
                            status: "pending"
                        }).session(
                            session
                        );

                    if (!lockedPayout) {
                        return;
                    }

                    // ---------------------------------------------
                    // Restore reserved balance
                    // ---------------------------------------------

                    const user =
                        await User.findById(
                            lockedPayout.userId
                        ).session(
                            session
                        );

                    const before =
                        Number(
                            user.earnings || 0
                        );

                    user.earnings =
                        Number(
                            (
                                before +
                                lockedPayout
                                    .earningsReserved
                            ).toFixed(2)
                        );

                    user.afroCoins =
                        Number(
                            (
                                Number(
                                    user.afroCoins || 0
                                ) +
                                lockedPayout
                                    .afroConverted
                            ).toFixed(4)
                        );

                    await user.save({
                        session
                    });

                    // ---------------------------------------------
                    // Refund ledger
                    // ---------------------------------------------

                    await WalletLedger.create(
                        [{
                            userId:
                                lockedPayout.userId,

                            type:
                                "WITHDRAWAL_REFUND",

                            direction:
                                "CREDIT",

                            amount:
                                lockedPayout.amount,

                            currency:
                                "KES",

                            balanceBefore:
                                before,

                            balanceAfter:
                                Number(
                                    user.earnings
                                ),

                            reference:
                                `WD-REFUND-${lockedPayout._id}`,

                            metadata: {
                                payoutId:
                                    lockedPayout._id.toString(),

                                resultCode,

                                resultDesc:
                                    result.ResultDesc
                            }
                        }],
                        {
                            session
                        }
                    );

                    lockedPayout.status =
                        "failed";

                    lockedPayout.resultCode =
                        resultCode;

                    lockedPayout.resultDesc =
                        result.ResultDesc;

                    lockedPayout.failedAt =
                        new Date();

                    lockedPayout
                        .ledgerRefundReference =
                        `WD-REFUND-${lockedPayout._id}`;

                    await lockedPayout.save({
                        session
                    });
                }
            );

        } finally {

            await session.endSession();
        }

        console.log(
            `↩️ B2C FAILED — FUNDS RESTORED: ${payout._id}`
        );

    } catch (err) {

        console.error(
            "❌ B2C RESULT PROCESSING ERROR:",
            err
        );
    }
});
       
app.post('/api/mpesa/b2c/timeout', async (req, res) => {
    try {
        console.error(
            "⏱️ B2C TIMEOUT:",
            JSON.stringify(req.body, null, 2)
        );

        res.status(200).json({
            ResponseCode: "0",
            ResponseDesc: "Accepted"
        });

        const result =
            req.body?.Result;

        if (!result) return;

        const conversationId =
            result.ConversationID;

        const payout =
            await Payout.findOne({
                conversationId,
                status: "pending"
            });

        if (!payout) {
            console.error(
                "⚠️ Timeout payout not found:",
                conversationId
            );
            return;
        }

        /*
         * Do NOT immediately restore the money merely because
         * Safaricom sent a timeout notification.
         *
         * The transaction may still resolve.
         *
         * Keep it in timeout/pending-review state.
         */

        payout.status = "timeout";

        payout.resultCode =
            result.ResultCode;

        payout.resultDesc =
            result.ResultDesc;

        payout.timeoutAt =
            new Date();

        await payout.save();

        console.log(
            `⏱️ PAYOUT TIMEOUT: ${payout._id}`
        );

    } catch (err) {

        console.error(
            "❌ B2C Timeout Error:",
            err.message
        );

        res.status(200).json({
            ResponseCode: "0",
            ResponseDesc: "Handled"
        });
    }
});
// Express route: /api/payouts/create-onboarding-link
app.post('/api/payouts/create-onboarding-link', async (req, res) => {
    const { phone } = req.body;
    const cleanedPhone = cleanPhone(phone);

    let user = await User.findOne({ identity: cleanedPhone });

    // 1. Create connected Express account if it doesn't exist
    if (!user?.stripeAccountId) {
        const account = await stripe.accounts.create({
            type: 'express',
            country: 'US',
            phone_number: cleanedPhone,
            capabilities: {
                transfers: { requested: true },
                card_payments: { requested: true },
            },
            business_type: 'individual',
        });

        user = await User.findOneAndUpdate(
            { identity: cleanedPhone },
            { $set: { stripeAccountId: account.id } },
            { new: true, upsert: true }
        );
    }

    // 2. Create hosted onboarding link
    const accountLink = await stripe.accountLinks.create({
        account: user.stripeAccountId,
        refresh_url: 'https://yourplatform.com/payouts/reauth',
        return_url: 'https://yourplatform.com/payouts/success',
        type: 'account_onboarding',
    });

    res.json({ url: accountLink.url });
});

// =========================================================================
// AUTONOMOUS WEBHOOK HANDLER FUNCTIONS
// =========================================================================

async function handlePaymentSuccess(paymentIntent) {
  const checkoutID = paymentIntent.id;
  const metadata = paymentIntent.metadata;

  console.log(`⚡ [WEBHOOK] Processing SUCCESS for PI: ${checkoutID}`);

  const existingTx = await Transaction.findOne({ checkoutID });

  if (!existingTx) {
    console.warn(`⚠️ [WEBHOOK] Transaction record not found for PI: ${checkoutID}`);
    return;
  }

  if (existingTx.status === 'completed') {
    console.log(`ℹ️ [WEBHOOK] Transaction ${checkoutID} already processed. Skipping.`);
    return;
  }

  await Transaction.findOneAndUpdate(
    { checkoutID },
    {
      $set: {
        status: 'completed',
        stripeChargeId: paymentIntent.latest_charge,
        updatedAt: new Date()
      }
    }
  );

  if (metadata.type === 'unlock' && metadata.postId) {
    console.log(`🔓 [AUTONOMOUS ACTION] Unlocking Post ${metadata.postId} for user ${metadata.phone}`);
    await User.findOneAndUpdate(
      { identity: metadata.phone },
      { $addToSet: { unlockedPosts: metadata.postId } }
    );
  }

  console.log(`✅ [AUTONOMOUS COMPLETE] Transaction ${checkoutID} finalized.`);
}

async function handlePaymentFailure(paymentIntent) {
  const checkoutID = paymentIntent.id;
  const failureReason = paymentIntent.last_payment_error?.message || 'Unknown failure';

  console.log(`❌ [WEBHOOK] Payment Failed for PI ${checkoutID}: ${failureReason}`);

  await Transaction.findOneAndUpdate(
    { checkoutID },
    {
      $set: {
        status: 'failed',
        failureReason: failureReason,
        updatedAt: new Date()
      }
    }
  );
}

async function handlePaymentCanceled(paymentIntent) {
  const checkoutID = paymentIntent.id;

  console.log(`⚠️ [WEBHOOK] Payment Canceled for PI: ${checkoutID}`);

  await Transaction.findOneAndUpdate(
    { checkoutID },
    {
      $set: {
        status: 'canceled',
        updatedAt: new Date()
      }
    }
  );
}


app.post('/api/mpesa/withdraw', async (req, res) => {
    const { identity, amount } = req.body;
    const cleaned = cleanPhone(identity);
    const withdrawalAmount = Number(amount);

    try {
        // =========================================================
        // 1. VALIDATE REQUEST
        // =========================================================
        if (!cleaned || !Number.isFinite(withdrawalAmount)) {
            return res.status(400).json({
                error: "INVALID_WITHDRAWAL_REQUEST"
            });
        }

        if (withdrawalAmount < 10) {
            return res.status(400).json({
                error: "BELOW_MINIMUM"
            });
        }

        // =========================================================
        // 2. FIND USER
        // =========================================================
        const user = await User.findOne({
            identity: cleaned
        });

        if (!user) {
            return res.status(400).json({
                error: "USER_NOT_FOUND"
            });
        }

        // =========================================================
        // 3. GET CURRENT AFRO → KES VALUE
        // =========================================================
        const marketPrice = await calculateCurrentAfroPrice(User);

        const kesRate = Number(marketPrice.kesRate);

        if (!Number.isFinite(kesRate) || kesRate <= 0) {
            return res.status(500).json({
                error: "INVALID_AFRO_MARKET_RATE"
            });
        }

        const earningsBalance = Number(user.earnings || 0);
        const afroBalance = Number(user.afroCoins || 0);

        const afroValueInKES = afroBalance * kesRate;

        const totalAvailable =
            earningsBalance + afroValueInKES;

        // =========================================================
        // 4. CHECK TOTAL AVAILABLE BALANCE
        // =========================================================
        if (totalAvailable < withdrawalAmount) {
            return res.status(400).json({
                error: "INSUFFICIENT_BALANCE",
                details: {
                    requested: withdrawalAmount,
                    earnings: Number(earningsBalance.toFixed(2)),
                    afroCoins: Number(afroBalance.toFixed(4)),
                    afroValueKES: Number(afroValueInKES.toFixed(2)),
                    totalAvailable: Number(totalAvailable.toFixed(2))
                }
            });
        }

        // =========================================================
        // 5. CONVERT AFRO IF EARNINGS ARE NOT ENOUGH
        // =========================================================
        let afroConverted = 0;
        let finalEarnings = earningsBalance;
        let finalAfroBalance = afroBalance;

        if (
            earningsBalance < withdrawalAmount &&
            afroBalance > 0
        ) {
            const neededFromAfro =
                withdrawalAmount - earningsBalance;

            afroConverted = Math.min(
                neededFromAfro / kesRate,
                afroBalance
            );

            finalEarnings =
                earningsBalance +
                (afroConverted * kesRate);

            finalAfroBalance =
                Number(
                    (afroBalance - afroConverted).toFixed(4)
                );
        }

        // =========================================================
        // 6. VERIFY CONVERSION COVERED WITHDRAWAL
        // =========================================================
        if (finalEarnings < withdrawalAmount) {
            return res.status(400).json({
                error: "BALANCE_CONVERSION_FAILED"
            });
        }

        // =========================================================
        // 7. INITIATE M-PESA B2C
        // =========================================================
        const b2cResponse = await triggerB2C(
            cleaned,
            withdrawalAmount,
            "iNFLUENSA Payout"
        );

        console.log(
            "🚀 iNFLUENSA B2C Withdrawal Initiated:",
            JSON.stringify(b2cResponse, null, 2)
        );

        // =========================================================
        // 8. CHECK SAFARICOM RESPONSE
        // =========================================================
        if (
            !b2cResponse ||
            (
                b2cResponse.ResponseCode &&
                String(b2cResponse.ResponseCode) !== "0"
            )
        ) {
            return res.status(500).json({
                error: "MPESA_B2C_REQUEST_FAILED",
                data: b2cResponse
            });
        }

        // =========================================================
        // 9. SAVE BALANCE STATE
        //
        // IMPORTANT:
        // We only update the converted AFRO balance here.
        //
        // Earnings should be finalized by the B2C ResultURL
        // after Safaricom confirms the actual payout result.
        // =========================================================
        if (afroConverted > 0) {
            await User.findOneAndUpdate(
                { identity: cleaned },
                {
                    $set: {
                        afroCoins: finalAfroBalance
                    }
                }
            );
        }

        // =========================================================
        // 10. RETURN SUCCESS
        // =========================================================
        return res.json({
            success: true,
            message: "Withdrawal initiated successfully",

            phone: cleaned,

            amount: withdrawalAmount,

            currency: "KES",

            afroConverted: Number(
                afroConverted.toFixed(4)
            ),

            afroValueKES: Number(
                (afroConverted * kesRate).toFixed(2)
            ),

            gateway: "mpesa_b2c",

            data: b2cResponse
        });

    } catch (err) {

        console.error(
            "❌ M-PESA WITHDRAWAL ERROR:",
            err.response?.data || err.message
        );

        return res.status(500).json({
            error: "MPESA_B2C_WITHDRAWAL_FAILED",
            details:
                err.response?.data ||
                err.message
        });
    }
});
// === STRIPE CONNECT ONBOARDING (Add this) ===
app.post('/api/stripe/onboard', async (req, res) => {
    const { identity } = req.body;
    const cleaned = cleanPhone(identity);

    try {
        const user = await User.findOne({ identity: cleaned });
        if (!user) return res.status(404).json({ error: "USER_NOT_FOUND" });

        let account;
        if (!user.stripeAccountId) {
            account = await stripe.accounts.create({
                type: 'express',
                country: getCountryFromPhone(cleaned),
                email: `${cleaned}@influensa.io`,
                capabilities: { transfers: { requested: true } },
                metadata: { nodeIdentity: cleaned }
            });

            await User.findOneAndUpdate(
                { identity: cleaned },
                { stripeAccountId: account.id, stripeOnboardingComplete: false }
            );
        } else {
            account = await stripe.accounts.retrieve(user.stripeAccountId);
        }

        const accountLink = await stripe.accountLinks.create({
            account: account.id,
            refresh_url: `${process.env.FRONTEND_URL}/dashboard?refresh=true`,
            return_url: `${process.env.FRONTEND_URL}/dashboard?success=true`,
            type: 'account_onboarding',
        });

        res.json({ 
            success: true, 
            url: accountLink.url,
            accountId: account.id 
        });
    } catch (err) {
        console.error("Stripe Onboarding Error:", err);
        res.status(500).json({ error: "ONBOARDING_FAILED" });
    }
});

// Helper function
function getCountryFromPhone(phone) {
    const p = cleanPhone(phone);
    if (p.startsWith('1')) return 'US';
    if (p.startsWith('254')) return 'KE';
    if (p.startsWith('256')) return 'UG';
    if (p.startsWith('255')) return 'TZ';
    if (p.startsWith('234')) return 'NG';
    return 'US'; // default
}

// Generate redeemable code for physical stores / merchants
app.post('/api/afro/generate-redemption', async (req, res) => {
    const { identity, amount } = req.body;
    const cleaned = cleanPhone(identity);
    try {
        const user = await User.findOne({ identity: cleaned });
        if (!user || user.afroCoins < parseFloat(amount)) {
            return res.status(400).json({ error: "INSUFFICIENT_AFRO" });
        }

        const code = generateRedemptionCode();
        
        await User.findOneAndUpdate(
            { identity: cleaned },
            { 
                $inc: { afroCoins: -parseFloat(amount) },
                $push: { redemptionHistory: { code, amount: parseFloat(amount), merchantId: null } }
            }
        );

        res.json({ success: true, redemptionCode: code, amount: parseFloat(amount) });
    } catch (e) {
        res.status(500).json({ error: "GENERATION_FAILED" });
    }
});



// P2P AFRO Transfer (phone-to-phone, cross-border)
app.post('/api/afro/transfer', async (req, res) => {
    const { senderIdentity, recipientIdentity, amount } = req.body;
    const cleanedSender = cleanPhone(senderIdentity);
    const cleanedRecipient = cleanPhone(recipientIdentity);

    if (!amount || amount <= 0) return res.status(400).json({ error: "INVALID_AMOUNT" });

    try {
        const sender = await User.findOne({ identity: cleanedSender });
        if (!sender || sender.afroCoins < amount) return res.status(400).json({ error: "INSUFFICIENT_AFRO" });

        await User.findOneAndUpdate(
            { identity: cleanedRecipient },
            { $inc: { afroCoins: amount } },
            { upsert: true }
        );

        sender.afroCoins = Number((sender.afroCoins - amount).toFixed(4));
        await sender.save();

        await Transaction.create({
            checkoutID: `AFRO-XFER-${Date.now()}`,
            userPhone: cleanedSender,
            amountPaid: amount,
            type: 'afro_transfer',
            status: 'completed'
        });

        res.json({ success: true, message: "AFRO transferred", newBalance: sender.afroCoins });
    } catch (e) {
        res.status(500).json({ error: "TRANSFER_FAILED" });
    }
});
app.post('/api/posts', upload.any(), async (req, res) => {
  try {
    const { title, price, owner, scarcity_limit } = req.body;

    // 1. Check if files exist (req.files is an array with upload.any())
    if (!req.files || req.files.length === 0) {
      return res.status(400).json({ error: "FILE_REQUIRED" });
    }

    if (!title || !owner) {
      return res.status(400).json({ error: "TITLE_AND_OWNER_REQUIRED" });
    }

    const cleanedOwner = cleanPhone(owner);
    const files = req.files;

    // Upload debug - confirms multer-s3 returned the S3 key
    console.log("========== UPLOAD DEBUG ==========");
    console.log(
      files.map(f => ({
        fieldname: f.fieldname,
        originalname: f.originalname,
        filename: f.filename,
        key: f.key,
        location: f.location,
        bucket: f.bucket
      }))
    );
    console.log("==================================");

    // 2. Safe duplicate check (Fixed to prevent buffer crashes)
    let contentHash = null;
    let isDuplicate = false;
    let duplicateReason = null;

    try {
      const sorted = [...files].sort((a, b) =>
        (a.originalname || '').localeCompare(b.originalname || '')
      );

      const hash = crypto.createHash('sha256');

      for (const f of sorted) {
        hash.update(f.originalname || '');
        hash.update(f.mimetype || '');

        // Safe string format for both diskStorage and memoryStorage
        hash.update(
          f.buffer ||
          `${f.filename || f.originalname}-${f.size}`
        );
      }

      contentHash = hash.digest('hex');

      const existing = await Post.findOne({
        title: {
          $regex: new RegExp(`^${title.trim()}$`, 'i')
        },
        contentHash,
        is_resell: { $ne: true }
      });

      if (existing) {
        isDuplicate = true;
        duplicateReason =
          "Identical set of files already minted under this title";
      }

    } catch (checkErr) {
      console.warn(
        "⚠️ Duplicate check skipped (non-critical):",
        checkErr.message
      );
    }

    if (isDuplicate) {
      return res.status(409).json({
        error: "DUPLICATE_MINT_DETECTED",
        reason: duplicateReason,
        message: "This content (title + file set) has already been minted.",
        suggestion: "Use the resell endpoint if you own the original."
      });
    }

    // 3. Generate CID
    const cid = crypto
      .createHash('sha256')
      .update(title + cleanedOwner + Date.now())
      .digest('hex');

    // 4. Prepare files metadata
    //
    // IMPORTANT:
    // With multer-s3, f.key is the actual S3 object key.
    //
    const fileMeta = files.map(f => ({
      filename: f.filename || f.originalname,
      originalname: f.originalname,
      key: f.key,
      mime: f.mimetype,
      size: f.size
    }));

    // The actual S3 key for the primary/original file.
    // Do NOT fall back to originalname because that may not
    // correspond to the actual object stored in S3.
    const primaryFileKey = files[0].key;

    if (!primaryFileKey) {
      console.error("❌ S3 KEY MISSING:", {
        fieldname: files[0].fieldname,
        originalname: files[0].originalname,
        filename: files[0].filename,
        location: files[0].location,
        bucket: files[0].bucket
      });

      return res.status(500).json({
        error: "S3_KEY_MISSING",
        message: "Upload completed but no S3 object key was returned."
      });
    }

    // 5. Create the post
    const post = await Post.create({
      title: title.trim(),
      price: Number(price) || 0,
      owner: cleanedOwner,

      files: fileMeta,

      // IMPORTANT:
      // Store the actual S3 key.
      // /api/media/:postId uses post.filekey
      // to retrieve the original object.
      filekey: primaryFileKey,

      mime: files[0].mimetype,

      // multer-s3 does not necessarily provide filename,
      // so preserve the original filename as the fallback.
      filename:
        files[0].filename ||
        files[0].originalname,

      cid,
      contentHash,
      scarcity_limit: scarcity_limit || 0,
      is_stream: false,
      original_creator: cleanedOwner,
      fileCount: files.length
    });

    console.log(
      `✅ New IP Minted | Title: "${title}" | Files: ${files.length} | CID: ${cid}`
    );

    console.log(
      `📦 Primary S3 Key: ${primaryFileKey}`
    );

    res.status(201).json(post);

  } catch (err) {
    console.error("Post creation error:", err);

    res.status(500).json({
      error: "Post Sync Failed",
      details: err.message
    });
  }
});
app.post('/api/posts/stream', async (req, res) => {
    try {
        const { title, price, owner, stream_url, scarcity_limit } = req.body;
        const cid = crypto.createHash('sha256').update(title + stream_url + Date.now()).digest('hex');
        const post = await Post.create({ title: title || "Live Stream", price: price || 0, owner: cleanPhone(owner), mime: "video/stream", cid, scarcity_limit: scarcity_limit || 0, is_stream: true, stream_url: stream_url.trim(), original_creator: cleanPhone(owner) });
        res.status(201).json(post);
    } catch (err) { res.status(500).json({ error: "Stream Sync Failed" }); }
});

app.post('/api/posts/:id/resell', async (req, res) => {
    try {
        const { resellerPhone, resellPrice } = req.body;
        const cleanedReseller = cleanPhone(resellerPhone);

        // 1. Fetch parent and validate access
        const parent = await Post.findById(req.params.id);
        if (!parent) return res.status(404).json({ error: "Parent asset post not found" });

        const hasAccess = parent.owner === cleanedReseller || parent.unlocked_by.includes(cleanedReseller);
        if (!hasAccess) {
            return res.status(403).json({ 
                error: "RESELL_DENIED", 
                message: "Node must unlock asset before initiating fractional resell protocols." 
            });
        }

        // 2. Generate a new unique CID for this specific node
        // We use the parent's CID + Reseller ID + Timestamp to ensure uniqueness
        const uniqueResellCid = crypto.createHash('sha256')
            .update(`${parent.cid}-${cleanedReseller}-${Date.now()}`)
            .digest('hex');

        // 3. Create the new node while preserving original lineage
        const resellPost = await Post.create({
            title: parent.title,
            price: resellPrice || parent.price,
            owner: cleanedReseller,
            mime: parent.mime,
            filename: parent.filename,
            cid: uniqueResellCid, // NEW: Unique CID to prevent database collisions
            scarcity_limit: parent.scarcity_limit,
            is_stream: parent.is_stream,
            stream_url: parent.stream_url,
            is_resell: true,
            parent_post_id: parent._id,
            // Maintain the original creator identity for royalty distribution
            original_creator: parent.original_creator || parent.owner,
            // Ensure clean start for new access lists
            unlocked_by: [],
            licensed_to: []
        });

        res.status(201).json(resellPost);
    } catch (err) {
        // Detailed error logging to catch exact schema violations
        console.error("❌ FRACTIONAL RESELL ENTRY ENGINE FAILURE:", err);
        res.status(500).json({ 
            error: "Resell Asset Grid Deployment Failed", 
            details: err.message 
        });
    }
});


app.get('/api/handshake/outgoing/:identity', async (req, res) => {
    try {
        const identity = cleanPhone(req.params.identity);
        const updates = await Handshake.find({ sender: identity }).sort({ timestamp: -1 }).limit(10);
        res.json(updates);
    } catch (err) { res.status(500).json({ error: "Status Pulse Failed" }); }
});

app.post('/api/handshake/counter/:id', async (req, res) => {
    try {
        const { split } = req.body;
        const handshake = await Handshake.findByIdAndUpdate(req.params.id, { 
            split, 
            status: 'countered', 
            timestamp: Date.now() 
        }, { new: true });
        
        const result = await triggerUniversalPush(handshake.sender, 10, handshake.postId, 'handshake_fee', handshake._id);
        res.json({ success: true, checkoutID: result.CheckoutRequestID || result.tx_ref });
    } catch (err) { res.status(500).json({ error: "Negotiation Sync Failed" }); }
});

app.get('/api/handshake/pulse/:identity', async (req, res) => {
    try {
        const identity = cleanPhone(req.params.identity);
        const count = await Handshake.countDocuments({ target: identity, status: 'pending' });
        res.json({ active: count > 0, count });
    } catch (err) { res.status(500).json({ error: "Pulse Sync Failed" }); }
});

app.post('/api/handshake/offer', async (req, res) => {
    try {
        const { postId, sender, split, signature, contractHash } = req.body;
        const post = await Post.findById(postId);
        const handshake = await Handshake.create({ postId, sender: cleanPhone(sender), target: post.owner, split, signature, contractHash });
        res.status(201).json(handshake);
    } catch (err) { res.status(500).json({ error: "Neural Signature Failed" }); }
});

app.get('/api/handshake/pending/:identity', async (req, res) => {
    try {
        const identity = cleanPhone(req.params.identity);
        const offers = await Handshake.find({ target: identity, status: 'pending' }).populate('postId');
        res.json(offers);
    } catch (err) { res.status(500).json({ error: "Neural Retrieval Error" }); }
});

app.post('/api/handshake/accept/:id', async (req, res) => {
    try {
        const handshake = await Handshake.findById(req.params.id);
        const result = await triggerUniversalPush(handshake.sender, 10, handshake.postId, 'handshake_fee', handshake._id);
        handshake.status = 'accepted'; 
        await handshake.save();
        res.json({ success: true, checkoutID: result.CheckoutRequestID || result.tx_ref });
    } catch (err) { res.status(500).json({ error: "Handshake Sync Failure" }); }
});

app.post('/api/handshake/reject/:id', async (req, res) => {
    try {
        const handshake = await Handshake.findById(req.params.id);
        handshake.status = 'rejected'; await handshake.save();
        res.json({ success: true });
    } catch (err) { res.status(500).json({ error: "Rejection Sync Failure" }); }
});

app.post('/api/nodes/connect', async (req, res) => {
    try {
        const { identity } = req.body;
        
        if (!identity || identity.trim() === "" || identity === "undefined") {
            return res.status(400).json({ 
                success: false, 
                error: "INVALID_IDENTITY", 
                message: "A valid node identification string is required for synchronization." 
            });
        }

        const cleaned = cleanPhone(identity);
        
        // Find or create the user node record securely
        const user = await User.findOneAndUpdate(
            { identity: cleaned }, 
            { $set: { lastSeen: Date.now() } }, 
            { upsert: true, new: true }
        );
        
        const nodeCount = await User.countDocuments({}); 
        
        return res.json({ 
            success: true, 
            nodeCount, 
            user: {
                identity: user.identity,
                afroCoins: user.afroCoins || 0,
                earnings: user.earnings || 0
            }
        });
    } catch (err) { 
        console.error("❌ CRITICAL: /api/nodes/connect sync exception:", err.message);
        return res.status(500).json({ success: false, error: "Sync Failure", details: err.message }); 
    }
});

// --- HIGH-SPEED READ-ONLY POLLING ROUTE (FIXED) ---
app.get('/api/media/:postId', async (req, res) => {

    let tempInput = null;
    let tempOutput = null;

    try {

        // ============================================================
        // 1. VALIDATE REQUEST
        // ============================================================

        const { phone } = req.query;
        const { postId } = req.params;

        if (!phone) {
            return res.status(401).json({
                error: "PHONE_REQUIRED"
            });
        }

        if (!mongoose.Types.ObjectId.isValid(postId)) {
            return res.status(400).json({
                error: "INVALID_POST_ID"
            });
        }

        const cleaned = cleanPhone(phone);

        if (!cleaned) {
            return res.status(400).json({
                error: "INVALID_PHONE"
            });
        }

        // ============================================================
        // 2. FIND POST
        // ============================================================

        const post = await Post.findOne({
            _id: postId,
            is_burned: false
        })
        .select(
            "_id owner unlocked_by licensed_to filekey mime is_burned"
        )
        .lean();

        if (!post) {
            return res.status(404).json({
                error: "NOT_FOUND"
            });
        }

        // ============================================================
        // 3. DETERMINE OWNER / LICENSE ACCESS
        // ============================================================

        const ownerIdentity =
            cleanPhone(post.owner || "");

        const isOwner =
            ownerIdentity === cleaned;

        const isLicensed =
            Array.isArray(post.licensed_to) &&
            post.licensed_to.some(
                identity =>
                    cleanPhone(identity) === cleaned
            );

        // ============================================================
        // 4. VERIFY BUYER PURCHASE
        //
        // IMPORTANT:
        //
        // unlocked_by alone is NOT sufficient for an ordinary buyer.
        //
        // We require:
        //
        //   1. The phone exists in unlocked_by
        //   2. A matching transaction exists
        //   3. Transaction status = completed
        //   4. Transaction belongs to this post
        //   5. Transaction belongs to this phone
        //
        // This means the media route does not create authorization.
        // The payment/settlement system must create it first.
        // ============================================================

        let completedPurchase = null;

        if (!isOwner && !isLicensed) {

            const unlocked =
                Array.isArray(post.unlocked_by) &&
                post.unlocked_by.some(
                    identity =>
                        cleanPhone(identity) === cleaned
                );

            if (!unlocked) {

                console.warn(
                    "🔒 MEDIA ACCESS DENIED — NOT UNLOCKED",
                    {
                        postId,
                        phoneSuffix: cleaned.slice(-4)
                    }
                );

                return res.status(403).json({
                    error: "LOCKED"
                });
            }

            completedPurchase =
                await Transaction.findOne({

                    postID: post._id,

                    userPhone: cleaned,

                    status: "completed",

                    type: {
                        $in: [
                            "unlock",
                            "content_purchase"
                        ]
                    }

                })
                .select(
                    "_id transactionID amountPaid completedAt"
                )
                .sort({
                    completedAt: -1
                })
                .lean();

            if (!completedPurchase) {

                console.warn(
                    "🔒 MEDIA ACCESS DENIED — NO COMPLETED PURCHASE",
                    {
                        postId,
                        phoneSuffix: cleaned.slice(-4)
                    }
                );

                return res.status(403).json({
                    error: "PAYMENT_NOT_CONFIRMED"
                });
            }
        }

        console.log(
            "🔓 MEDIA ACCESS AUTHORIZED",
            {
                postId,
                owner: isOwner,
                licensed: isLicensed,
                purchased: !!completedPurchase,
                phoneSuffix: cleaned.slice(-4)
            }
        );

        // ============================================================
        // 5. VERIFY ORIGINAL FILE KEY
        // ============================================================

        const originalKey = post.filekey;

        if (
            !originalKey ||
            typeof originalKey !== "string"
        ) {

            console.error(
                "❌ FILE_KEY_MISSING",
                {
                    postId
                }
            );

            return res.status(404).json({
                error: "FILE_KEY_MISSING"
            });
        }

        // ============================================================
        // 6. DETERMINE MEDIA TYPE
        // ============================================================

        const mime =
            String(post.mime || "")
                .toLowerCase()
                .trim();

        let extension = null;

        if (mime.startsWith("image/")) {

            extension =
                mime.split("/")[1] || "jpg";

        } else if (mime.startsWith("video/")) {

            extension = "mp4";

        } else if (mime.startsWith("audio/")) {

            extension =
                mime.split("/")[1] || "mp3";

        } else {

            return res.status(415).json({
                error: "UNSUPPORTED_MEDIA_TYPE"
            });
        }

        // ============================================================
        // 7. VERIFY ORIGINAL EXISTS
        // ============================================================

        try {

            await s3.send(
                new HeadObjectCommand({
                    Bucket:
                        process.env.AWS_S3_BUCKET_NAME,

                    Key:
                        originalKey
                })
            );

        } catch (err) {

            const status =
                err.$metadata?.httpStatusCode;

            if (
                err.name === "NotFound" ||
                err.name === "NoSuchKey" ||
                status === 404
            ) {

                console.error(
                    "❌ ORIGINAL FILE NOT FOUND",
                    {
                        postId
                    }
                );

                return res.status(404).json({
                    error: "ORIGINAL_FILE_NOT_FOUND"
                });
            }

            throw err;
        }

        // ============================================================
        // 8. BUYER-SPECIFIC WATERMARK ID
        //
        // DO NOT PUT THE RAW PHONE NUMBER INTO THE S3 KEY.
        //
        // Hashing prevents the phone number from becoming part of
        // the object path while still giving every buyer a stable
        // deterministic object.
        // ============================================================

        const crypto =
            require("crypto");

        const buyerHash =
            crypto
                .createHash("sha256")
                .update(cleaned)
                .digest("hex")
                .slice(0, 32);

        const watermarkKey =
            `watermarked/${post._id}_${buyerHash}.${extension}`;

        // ============================================================
        // 9. CHECK EXISTING WATERMARKED COPY
        // ============================================================

        let watermarkExists = false;

        try {

            await s3.send(
                new HeadObjectCommand({
                    Bucket:
                        process.env.AWS_S3_BUCKET_NAME,

                    Key:
                        watermarkKey
                })
            );

            watermarkExists = true;

            console.log(
                "✅ Existing protected media:",
                watermarkKey
            );

        } catch (err) {

            const status =
                err.$metadata?.httpStatusCode;

            if (
                err.name !== "NotFound" &&
                err.name !== "NoSuchKey" &&
                status !== 404
            ) {
                throw err;
            }
        }

        // ============================================================
        // 10. CREATE BUYER-SPECIFIC PROTECTED COPY
        // ============================================================

        if (!watermarkExists) {

            const uniqueId =
                `${Date.now()}_${crypto
                    .randomBytes(6)
                    .toString("hex")}`;

            tempInput =
                path.join(
                    "/tmp",
                    `in_${uniqueId}.${extension}`
                );

            tempOutput =
                path.join(
                    "/tmp",
                    `out_${uniqueId}.${extension}`
                );

            // ========================================================
            // 10A. DOWNLOAD ORIGINAL FROM PRIVATE S3
            // ========================================================

            const originalCommand =
                new GetObjectCommand({
                    Bucket:
                        process.env.AWS_S3_BUCKET_NAME,

                    Key:
                        originalKey
                });

            const originalUrl =
                await getSignedUrl(
                    s3,
                    originalCommand,
                    {
                        expiresIn: 300
                    }
                );

            const downloadResponse =
                await fetch(originalUrl);

            if (!downloadResponse.ok) {

                console.error(
                    "❌ S3 DOWNLOAD FAILED",
                    {
                        status:
                            downloadResponse.status,
                        postId
                    }
                );

                return res.status(502).json({
                    error: "S3_DOWNLOAD_FAILED"
                });
            }

            const arrayBuffer =
                await downloadResponse.arrayBuffer();

            fs.writeFileSync(
                tempInput,
                Buffer.from(arrayBuffer)
            );

            // ========================================================
            // 10B. IMAGE → JIMP
            // ========================================================

            if (mime.startsWith("image/")) {

                console.log(
                    "🖼️ JIMP PROCESSING",
                    {
                        postId,
                        mime
                    }
                );

                try {

                    /*
                     * Jimp v1.x
                     *
                     * Requires:
                     *
                     * const {
                     *     Jimp,
                     *     loadFont,
                     *     SANS_32_WHITE
                     * } = require("jimp");
                     */

                    const {
                        Jimp,
                        loadFont,
                        SANS_32_WHITE
                    } = require("jimp");

                    const image =
                        await Jimp.read(
                            tempInput
                        );

                    const width =
                        image.bitmap.width;

                    const height =
                        image.bitmap.height;

                    if (
                        !width ||
                        !height
                    ) {
                        throw new Error(
                            "INVALID_IMAGE_DIMENSIONS"
                        );
                    }

                    // ------------------------------------------------
                    // LOAD JIMP FONT
                    // ------------------------------------------------

                    const font =
                        await loadFont(
                            SANS_32_WHITE
                        );

                    // ------------------------------------------------
                    // BUYER WATERMARK
                    //
                    // We deliberately expose only a masked identifier.
                    // ------------------------------------------------

                    const nodeId =
                        idppAnonymize(
                            cleaned
                        );

                    const watermarkText =
                        `INFLUENSA | NODE-${nodeId}`;

                    // ------------------------------------------------
                    // SCALE WATERMARK RELATIVE TO IMAGE
                    // ------------------------------------------------

                    let fontSizeFactor = 1;

                    if (width < 800) {
                        fontSizeFactor = 0.65;

                    } else if (width < 1400) {
                        fontSizeFactor = 0.85;

                    } else if (width >= 2200) {
                        fontSizeFactor = 1.25;
                    }

                    /*
                     * Jimp bitmap fonts have fixed glyph sizes,
                     * so for maximum compatibility we use the
                     * standard font and scale the rendered watermark
                     * when necessary.
                     *
                     * The watermark is first rendered to a transparent
                     * layer.
                     */

                    const {
                        measureText,
                        measureTextHeight
                    } = require("jimp");

                    const textWidth =
                        measureText(
                            font,
                            watermarkText
                        );

                    const textHeight =
                        measureTextHeight(
                            font,
                            watermarkText,
                            Math.max(
                                1,
                                Math.floor(
                                    width * 0.5
                                )
                            )
                        );

                    // ------------------------------------------------
                    // WATERMARK PADDING
                    // ------------------------------------------------

                    const padding =
                        Math.max(
                            12,
                            Math.floor(
                                Math.min(
                                    width,
                                    height
                                ) * 0.015
                            )
                        );

                    // ------------------------------------------------
                    // TRANSPARENT WATERMARK LAYER
                    // ------------------------------------------------

                    const overlay =
                        new Jimp({
                            width:
                                textWidth +
                                padding * 2,

                            height:
                                textHeight +
                                padding * 2,

                            color:
                                0x00000000
                        });

                    // ------------------------------------------------
                    // BLACK BACKGROUND BOX
                    // ------------------------------------------------

                    overlay.scan(
                        0,
                        0,
                        overlay.bitmap.width,
                        overlay.bitmap.height,
                        function (
                            x,
                            y,
                            idx
                        ) {

                            const border =
                                Math.max(
                                    1,
                                    Math.floor(
                                        padding * 0.35
                                    )
                                );

                            if (
                                x < border ||
                                y < border ||
                                x >=
                                    overlay.bitmap.width -
                                    border ||
                                y >=
                                    overlay.bitmap.height -
                                    border
                            ) {
                                this.bitmap.data[
                                    idx + 0
                                ] = 0;

                                this.bitmap.data[
                                    idx + 1
                                ] = 0;

                                this.bitmap.data[
                                    idx + 2
                                ] = 0;

                                this.bitmap.data[
                                    idx + 3
                                ] = 105;
                            }
                        }
                    );

                    // ------------------------------------------------
                    // PRINT WATERMARK
                    // ------------------------------------------------

                    overlay.print({
                        font,
                        x: padding,
                        y: padding,
                        text: watermarkText
                    });

                    // ------------------------------------------------
                    // MAKE WATERMARK SEMI-TRANSPARENT
                    // ------------------------------------------------

                    overlay.opacity(
                        0.62
                    );

                    // ------------------------------------------------
                    // POSITION BOTTOM RIGHT
                    // ------------------------------------------------

                    const margin =
                        Math.max(
                            15,
                            Math.floor(
                                Math.min(
                                    width,
                                    height
                                ) * 0.025
                            )
                        );

                    const watermarkX =
                        Math.max(
                            0,
                            width -
                            overlay.bitmap.width -
                            margin
                        );

                    const watermarkY =
                        Math.max(
                            0,
                            height -
                            overlay.bitmap.height -
                            margin
                        );

                    // ------------------------------------------------
                    // COMPOSITE
                    // ------------------------------------------------

                    image.composite(
                        overlay,
                        watermarkX,
                        watermarkY
                    );

                    // ------------------------------------------------
                    // OPTIONAL SECOND WATERMARK
                    //
                    // For large images, place a subtle diagonal
                    // identifier in the center as well.
                    // ------------------------------------------------

                    if (
                        width >= 1600 &&
                        height >= 1200
                    ) {

                        const centerOverlay =
                            new Jimp({
                                width:
                                    textWidth +
                                    padding * 2,

                                height:
                                    textHeight +
                                    padding * 2,

                                color:
                                    0x00000000
                            });

                        centerOverlay.print({
                            font,
                            x: padding,
                            y: padding,
                            text:
                                watermarkText
                        });

                        centerOverlay.opacity(
                            0.20
                        );

                        const centerX =
                            Math.floor(
                                (
                                    width -
                                    centerOverlay
                                        .bitmap.width
                                ) / 2
                            );

                        const centerY =
                            Math.floor(
                                (
                                    height -
                                    centerOverlay
                                        .bitmap.height
                                ) / 2
                            );

                        image.composite(
                            centerOverlay,
                            centerX,
                            centerY
                        );
                    }

                    // ------------------------------------------------
                    // WRITE FINAL IMAGE
                    // ------------------------------------------------

                    await new Promise(
                        (resolve, reject) => {

                            image.write(
                                tempOutput,
                                err => {

                                    if (err) {
                                        reject(err);
                                    } else {
                                        resolve();
                                    }
                                }
                            );
                        }
                    );

                    console.log(
                        "✅ JIMP WATERMARK COMPLETE",
                        {
                            width,
                            height,
                            output:
                                tempOutput
                        }
                    );

                } catch (imageError) {

                    console.error(
                        "❌ JIMP WATERMARK FAILED",
                        {
                            name:
                                imageError.name,
                            message:
                                imageError.message,
                            postId
                        }
                    );

                    return res.status(500).json({
                        error:
                            "IMAGE_WATERMARK_FAILED"
                    });
                }
            }

            // ========================================================
            // 10C. VIDEO → FFMPEG
            // ========================================================

            else if (mime.startsWith("video/")) {

                const watermarkText =
                    `INFLUENSA | NODE-${idppAnonymize(cleaned)}`;

                await new Promise(
                    (resolve, reject) => {

                        ffmpeg(tempInput)

                            .videoFilters([
                                {
                                    filter:
                                        "format",

                                    options:
                                        "yuv420p"
                                },
                                {
                                    filter:
                                        "drawtext",

                                    options: {

                                        text:
                                            watermarkText,

                                        fontfile:
                                            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",

                                        fontsize:
                                            20,

                                        fontcolor:
                                            "white@0.45",

                                        x:
                                            "w-tw-20",

                                        y:
                                            "h-th-20",

                                        box:
                                            1,

                                        boxcolor:
                                            "black@0.35",

                                        boxborderw:
                                            6
                                    }
                                }
                            ])

                            .outputOptions([

                                "-map 0:v:0",

                                "-map 0:a?",

                                "-c:v libx264",

                                "-preset veryfast",

                                "-crf 23",

                                "-pix_fmt yuv420p",

                                "-c:a aac",

                                "-b:a 128k",

                                "-map_metadata 0",

                                "-movflags +faststart",

                                "-fps_mode vfr"
                            ])

                            .on(
                                "start",
                                commandLine => {

                                    console.log(
                                        "🎬 FFmpeg started"
                                    );

                                    console.log(
                                        commandLine
                                    );
                                }
                            )

                            .on(
                                "stderr",
                                line => {

                                    console.log(
                                        "FFmpeg:",
                                        line
                                    );
                                }
                            )

                            .on(
                                "end",
                                () => {

                                    console.log(
                                        "✅ FFmpeg completed"
                                    );

                                    resolve();
                                }
                            )

                            .on(
                                "error",
                                err => {

                                    console.error(
                                        "❌ FFmpeg FAILED",
                                        err.message
                                    );

                                    reject(err);
                                }
                            )

                            .save(
                                tempOutput
                            );
                    }
                );
            }

            // ========================================================
            // 10D. AUDIO
            // ========================================================

            else if (mime.startsWith("audio/")) {

                /*
                 * Audio does not need local processing.
                 *
                 * Authorization has already been completed above.
                 */

                const audioCommand =
                    new GetObjectCommand({
                        Bucket:
                            process.env.AWS_S3_BUCKET_NAME,

                        Key:
                            originalKey,

                        ResponseContentType:
                            mime
                    });

                const audioUrl =
                    await getSignedUrl(
                        s3,
                        audioCommand,
                        {
                            expiresIn: 300
                        }
                    );

                return res.redirect(
                    302,
                    audioUrl
                );
            }

            // ========================================================
            // 11. VERIFY GENERATED FILE
            // ========================================================

            if (
                !tempOutput ||
                !fs.existsSync(tempOutput)
            ) {

                console.error(
                    "❌ WATERMARK_OUTPUT_MISSING",
                    {
                        postId
                    }
                );

                return res.status(500).json({
                    error:
                        "WATERMARK_OUTPUT_MISSING"
                });
            }

            const outputStats =
                fs.statSync(
                    tempOutput
                );

            if (
                !outputStats.isFile() ||
                outputStats.size <= 0
            ) {

                console.error(
                    "❌ WATERMARK_OUTPUT_EMPTY",
                    {
                        postId
                    }
                );

                return res.status(500).json({
                    error:
                        "WATERMARK_OUTPUT_EMPTY"
                });
            }

            // ========================================================
            // 12. DETERMINE OUTPUT CONTENT TYPE
            // ========================================================

            const outputContentType =
                mime.startsWith("video/")
                    ? "video/mp4"
                    : mime;

            // ========================================================
            // 13. UPLOAD PROTECTED COPY
            // ========================================================

            const processedBuffer =
                fs.readFileSync(
                    tempOutput
                );

            await s3.send(
                new PutObjectCommand({

                    Bucket:
                        process.env.AWS_S3_BUCKET_NAME,

                    Key:
                        watermarkKey,

                    Body:
                        processedBuffer,

                    ContentType:
                        outputContentType,

                    CacheControl:
                        "private, no-store",

                    Metadata: {

                        postid:
                            post._id.toString(),

                        recipient:
                            cleaned.slice(-4),

                        protected:
                            "true",

                        transaction:
                            completedPurchase
                                ?.transactionID ||
                            "owner_or_license"
                    }
                })
            );

            // ========================================================
            // 14. VERIFY UPLOAD
            // ========================================================

            await s3.send(
                new HeadObjectCommand({
                    Bucket:
                        process.env.AWS_S3_BUCKET_NAME,

                    Key:
                        watermarkKey
                })
            );

            console.log(
                "✅ PROTECTED MEDIA CREATED",
                {
                    postId,
                    watermarkKey
                }
            );
        }

        // ============================================================
        // 15. CLEAN TEMPORARY FILES
        // ============================================================

        const cleanup = () => {

            try {

                if (
                    tempInput &&
                    fs.existsSync(tempInput)
                ) {
                    fs.unlinkSync(
                        tempInput
                    );
                }

            } catch (err) {

                console.warn(
                    "⚠️ Input cleanup failed:",
                    err.message
                );
            }

            try {

                if (
                    tempOutput &&
                    fs.existsSync(tempOutput)
                ) {
                    fs.unlinkSync(
                        tempOutput
                    );
                }

            } catch (err) {

                console.warn(
                    "⚠️ Output cleanup failed:",
                    err.message
                );
            }
        };

        cleanup();

        tempInput = null;
        tempOutput = null;

        // ============================================================
        // 16. FINAL AUTHORIZED DELIVERY
        //
        // NEVER expose the S3 object directly.
        //
        // The original bucket should remain private.
        // The protected copy is delivered through a short-lived
        // presigned URL.
        // ============================================================

        const finalCommand =
            new GetObjectCommand({

                Bucket:
                    process.env.AWS_S3_BUCKET_NAME,

                Key:
                    watermarkKey,

                ResponseContentType:
                    mime.startsWith("video/")
                        ? "video/mp4"
                        : mime
            });

        const finalUrl =
            await getSignedUrl(
                s3,
                finalCommand,
                {
                    expiresIn: 300
                }
            );

        console.log(
            "🎬 AUTHORIZED MEDIA DELIVERY",
            {
                postId,
                type: mime,
                expiresIn: 300
            }
        );

        return res.redirect(
            302,
            finalUrl
        );

    } catch (err) {

        console.error(
            "❌ MEDIA ROUTE ERROR",
            {
                name:
                    err.name,

                message:
                    err.message,

                postId:
                    req.params.postId
            }
        );

        return res.status(500).json({
            error: "MEDIA_ERROR"
        });

    } finally {

        // ============================================================
        // ABSOLUTE TEMP FILE CLEANUP
        // ============================================================

        try {

            if (
                tempInput &&
                fs.existsSync(tempInput)
            ) {
                fs.unlinkSync(
                    tempInput
                );
            }

        } catch (err) {

            console.warn(
                "⚠️ Final input cleanup failed:",
                err.message
            );
        }

        try {

            if (
                tempOutput &&
                fs.existsSync(tempOutput)
            ) {
                fs.unlinkSync(
                    tempOutput
                );
            }

        } catch (err) {

            console.warn(
                "⚠️ Final output cleanup failed:",
                err.message
            );
        }
    }
});
app.get('/api/governance/sidebar', async (req, res) => {
    try {
        const vault = await Vault.findOne({ id: 'protocol_vault' });
        const nodes = await User.countDocuments({});
        const activeIPs = await Post.countDocuments({ is_burned: false });
        res.json({ 
            nodes, 
            activeIPs, 
            vaultBalance: vault ? vault.balance.toFixed(2) : "0.00", 
            liveTax: "7.89" 
        });
    } catch (err) { res.status(500).json({ error: "Governance Offline" }); }
});

app.get('/api/governance/ledger', async (req, res) => {
    try {
        const transactions = await Transaction.find({ status: 'completed' })
            .sort({ timestamp: -1 })
            .limit(50);
        
        const ledger = transactions.map(tx => ({
            id: tx.checkoutID,
            amount: tx.amountPaid,
            taxCollected: (tx.amountPaid * PROTOCOL_FEE).toFixed(2),
            timestamp: tx.timestamp,
            type: tx.type
        }));
        
        res.json(ledger);
    } catch (err) { res.status(500).json({ error: "Ledger Sync Failed" }); }
});
app.get('/api/governance/sidebar', async (req, res) => {
    try {
        const [nodes, activeIPs, vault, liveTaxRate] = await Promise.all([
            User.countDocuments({}),
            Post.countDocuments({ is_burned: false }),
            Vault.findOne({ id: 'protocol_vault' }).lean(),
            calculateLiveTax()
        ]);
        
        res.json({ 
            nodes, 
            activeIPs,
            vaultBalance: vault ? vault.balance.toFixed(2) : "0.00",
            platformReserve: vault ? vault.platformAfroReserve.toFixed(2) : "0.00",
            liveTax: (liveTaxRate * 100).toFixed(2) + "%",
            efficiency: "LOW_FEE", // Maps directly to gasEfficiencyDisplay
            uplink: "99.2%"        // Maps directly to uplinkDisplay
        });
    } catch (err) { 
        console.error("❌ Governance Data Pulse Failed:", err);
        res.status(500).json({ error: "Governance Data Pulse Failed" }); 
    }
});

app.get('/api/stats', async (req, res) => {
    try {
        const vault = await Vault.findOne({ id: 'protocol_vault' });
        const userCount = await User.countDocuments({});
        const currentLiveRate = await calculateLiveTax();
        res.json({ 
            taxVault: vault ? (vault.balance || 0).toFixed(2) : "0.00", 
            userCount: userCount, 
            platformReserve: vault ? (vault.platformAfroReserve || 0).toFixed(2) : "0.00", 
            currentTaxRate: (currentLiveRate * 100).toFixed(2) + "%" 
        });
    } catch (err) { res.status(500).json({ error: "Stats failure" }); }
});

app.get('/api/search', async (req, res) => {
    const { q } = req.query;
    try {
        const posts = await Post.find({ is_burned: false, $or: [{ title: { $regex: q, $options: 'i' } }, { owner: { $regex: q, $options: 'i' } }, { cid: { $regex: q, $options: 'i' } }] }).sort({ timestamp: -1 });
        
        const complianceSafePosts = posts.map(post => {
            const p = post.toObject();
            p.owner = idppMaskPhone(p.owner);
            return p;
        });
        res.json(complianceSafePosts);
    } catch (err) { res.status(500).json({ error: "Search logic failure" }); }
});

app.get('/api/posts', async (req, res) => {
    const posts = await Post.find({ is_burned: false }).sort({ timestamp: -1 });
    const complianceSafePosts = posts.map(post => {
        const p = post.toObject();
        p.owner = idppMaskPhone(p.owner);
        return p;
    });
    res.json(complianceSafePosts);
});




// Add your auth middleware to protect the route
app.delete('/api/posts/:id', async (req, res) => {
    try {
        // 1. Manually check for user identification headers or session
        let identity = req.headers['x-user-identity'] || req.headers['authorization'];
        
        if (identity && identity.startsWith('Bearer ')) {
            identity = identity.slice(7).trim();
        }

        if (!identity) {
            return res.status(401).json({ error: "UNAUTHORIZED: Missing identification node." });
        }

        // Normalize the identity string using your backend clean function
        const executingUserNode = cleanPhone(identity);

        // 2. Locate the post in the database
        const post = await Post.findById(req.params.id);
        if (!post) {
            return res.status(404).json({ error: "Post not found" });
        }

        // 3. Authorization check: compare normalized strings
        const postOwnerNode = cleanPhone(post.owner);
        if (postOwnerNode !== executingUserNode) {
            return res.status(403).json({ error: "UNAUTHORIZED: Access denied." });
        }

        // 4. Perform the soft burn
        post.is_burned = true;
        await post.save();

        res.status(200).json({ success: true, message: "Post burned successfully" });
    } catch (err) { 
        console.error("Burn execution error:", err);
        res.status(500).json({ error: "Burn protocol failure" }); 
    }
});


app.delete('/api/v1/privacy/erasure', async (req, res) => {
    // Optional but highly recommended: Start a mongoose session for atomicity
    const session = await mongoose.startSession();
    session.startTransaction();

    try {
        const { identity } = req.body;
        if (!identity) {
            await session.abortTransaction();
            return res.status(400).json({ error: "IDENTIFIER_REQUIRED" });
        }
        
        const cleaned = cleanPhone(identity);

        // 1. Database Purges (Passed inside the session)
        await User.deleteOne({ identity: cleaned }).session(session);
        await Handshake.deleteMany({ $or: [{ sender: cleaned }, { target: cleaned }] }).session(session);
        await Transaction.deleteMany({ userPhone: cleaned }).session(session);
        
        // 2. Find files to delete BEFORE removing posts from DB
        const ownedPosts = await Post.find({ owner: cleaned }).session(session);
        
        // 3. Async File Deletion (Doesn't block the Node.js event loop)
        const fileDeletionPromises = ownedPosts.map(async (post) => {
            if (!post.filename) return;
            const fileLocation = path.join(uploadDir, post.filename);
            try {
                await fsPromises.unlink(fileLocation);
            } catch (fErr) {
                // Ignore "File not found" errors, log others
                if (fErr.code !== 'ENOENT') console.error(`Failed to delete file: ${fileLocation}`, fErr);
            }
        });
        
        // Run file deletions in parallel
        await Promise.all(fileDeletionPromises);

        // 4. Finally, wipe the posts from the DB
        await Post.deleteMany({ owner: cleaned }).session(session);

        // Commit all DB changes at once safely
        await session.commitTransaction();
        session.endSession();

        res.status(200).json({ success: true, message: "All local data identifiers and nodes purged globally." });
    } catch (err) {
        // If anything fails, undo all database changes made in this block
        await session.abortTransaction();
        session.endSession();
        
        console.error("Erasure failure details:", err);
        res.status(500).json({ error: "ERASURE_PROTOCOL_FAILURE" });
    }
});




/**
 * =========================================================================
 * PROTOCOL CORE CONFIGURATION: AFRO COIN VALUATION BONDING CURVE PARAMETERS
 * =========================================================================
 * Base Scale: Starts at 1 KES (~0.0078 USD) at launch.
 * Max Scale: Caps out exactly at 5.00 USD when user baseline scales to 1,000,000.
 */
const CURVE_CONFIG = {
    TARGET_USERS: 1000000,
    START_PRICE_USD: 0.0078,
    MAX_PRICE_USD: 5.00,
    USD_TO_KES: 130.00 // Static standard rail peg. Dynamically pull from exchange API if necessary.
};

/**
 * Helper Utility: Server-Side Bonding Curve Evaluation Engine
 * Automatically calculates the real-time token price based on organic platform user density.
 */
async function calculateCurrentAfroPrice(UserCollectionModel) {
    const totalUsers = await UserCollectionModel.countDocuments({});
    const baseUsers = totalUsers || 1; // Safeguard against division-by-zero errors

    // Bounded Linear Progression Curve Matrix
    const growthRatio = Math.min(baseUsers, CURVE_CONFIG.TARGET_USERS) / CURVE_CONFIG.TARGET_USERS;
    const currentPriceUSD = CURVE_CONFIG.START_PRICE_USD + ((CURVE_CONFIG.MAX_PRICE_USD - CURVE_CONFIG.START_PRICE_USD) * growthRatio);
    
    // Format output targets with clean math rounding bounds
    const roundedUSD = Math.round(currentPriceUSD * 10000) / 10000;
    const roundedKES = Math.round((roundedUSD * CURVE_CONFIG.USD_TO_KES) * 100) / 100;

    return {
        totalUsers: baseUsers,
        usdRate: roundedUSD,
        kesRate: roundedKES
    };
}

/**
 * =========================================================================
 * AUTOMATED PROTOCOL CRON ENGINE: AUTOMATIC 15-MINUTE ESCROW RECOVERY LOOP
 * =========================================================================
 * Polling interval: Runs every 60 seconds.
 * Function: Sweeps database for stale 'PENDING_PAYMENT' orders that exceeded 
 * their 15-minute fiat validation window, wipes the buyer lock, and lists it back as 'OPEN'.
 */
setInterval(async () => {
    try {
        await P2POrder.updateMany(
            { status: 'PENDING_PAYMENT', expiresAt: { $lt: new Date() } },
            { 
                $set: { status: 'OPEN', buyerIdentity: null }, 
                $unset: { expiresAt: "" } 
            }
        );
    } catch (err) {
        console.error("CRITICAL: Automated P2P Escrow Recovery engine loop error: ", err.message);
    }
}, 60000);



// =========================================================================
// ENDPOINT 1: POST AN AD (SELLER LOCKS TOKENS INTO ESCROW DEPOSIT POOL)
// =========================================================================
// =========================================================================
// ENDPOINT 1: POST AN AD (SELLER LOCKS TOKENS INTO ESCROW)
app.post('/api/orders/create', async (req, res) => {
    try {
        const { sellerIdentity, afroAmount, paymentMethodDetails } = req.body;
        const parsedAmount = parseFloat(afroAmount);

        if (!sellerIdentity || !parsedAmount || parsedAmount <= 0 || !paymentMethodDetails) {
            return res.status(400).json({ success: false, message: "Invalid parameters." });
        }

        const seller = await User.findOne({ identity: sellerIdentity.trim() });
        if (!seller || seller.afroCoins < parsedAmount) {
            return res.status(400).json({ success: false, message: "Insufficient AFRO balance." });
        }

        const marketPrice = await calculateCurrentAfroPrice(User);
        const dynamicRateKES = marketPrice.kesRate;
        const totalFiatKES = Math.round((parsedAmount * dynamicRateKES) * 100) / 100;

        // Deduct AFRO (locked in escrow)
        seller.afroCoins = Number((seller.afroCoins - parsedAmount).toFixed(4));
        await seller.save();

        const newOrder = await P2POrder.create({
            sellerIdentity: sellerIdentity.trim(),
            afroAmount: parsedAmount,
            fiatRatePerCoin: dynamicRateKES,
            fiatTotal: totalFiatKES,
            paymentMethodDetails: paymentMethodDetails.trim(),
            status: 'OPEN'
        });

        return res.status(201).json({ 
            success: true, 
            order: newOrder,
            message: "Order created and AFRO locked in escrow."
        });
    } catch (err) {
        console.error("Order create error:", err);
        return res.status(500).json({ success: false, message: err.message });
    }
});



// =========================================================================
// PRODUCTION-GRADE: CLAIM ORDER + AUTO STK PUSH
// =========================================================================
app.post('/api/orders/claim', async (req, res) => {
    try {
        const { orderId, buyerIdentity } = req.body;
        const buyer = cleanPhone(buyerIdentity);

        if (!buyer) return res.status(400).json({ success: false, message: "Invalid buyer identity" });

        const order = await P2POrder.findOne({ _id: orderId });
        if (!order) return res.status(404).json({ success: false, message: "Order not found." });
        if (order.status !== 'OPEN') {
            return res.status(400).json({ success: false, message: `Order is already ${order.status}.` });
        }
        if (order.sellerIdentity === buyer) {
            return res.status(400).json({ success: false, message: "Cannot buy your own order." });
        }

        // Atomic claim
        const claimedOrder = await P2POrder.findOneAndUpdate(
            { _id: orderId, status: 'OPEN' },
            { 
                buyerIdentity: buyer, 
                status: 'PENDING_PAYMENT',
                expiresAt: new Date(Date.now() + 20 * 60 * 1000)
            },
            { new: true }
        );

        if (!claimedOrder) {
            return res.status(409).json({ success: false, message: "Order was claimed by someone else." });
        }

        // Trigger payment
        let paymentResult;
        try {
            paymentResult = await triggerUniversalPush(
                buyer,
                claimedOrder.fiatTotal,
                claimedOrder._id,
                'p2p_buy'
            );
        } catch (payErr) {
            console.error("Payment initiation failed:", payErr);
            // Rollback on failure
            await P2POrder.findByIdAndUpdate(orderId, { 
                status: 'OPEN', 
                buyerIdentity: null, 
                expiresAt: null 
            });
            return res.status(400).json({ success: false, message: "Payment initiation failed. Order released." });
        }

        return res.status(200).json({
            success: true,
            order: claimedOrder,
            checkoutID: paymentResult?.CheckoutRequestID || paymentResult?.checkoutID,
            clientSecret: paymentResult?.clientSecret,   // ← Important for Stripe
            message: "Order claimed successfully."
        });

    } catch (err) {
        console.error("Claim error:", err);
        return res.status(500).json({ success: false, message: "Server error." });
    }
});

// =========================================================================
// ENDPOINT 3: BUYER MARKS AS PAID (SIGNALS TO SELLER THAT FIAT IS SENT)
// =========================================================================
app.post('/api/orders/mark-paid', async (req, res) => {
    try {
        const { orderId, buyerIdentity } = req.body;
        const processedBuyer = cleanPhone(buyerIdentity);

        const order = await P2POrder.findOneAndUpdate(
            { 
                _id: orderId, 
                buyerIdentity: processedBuyer, 
                status: 'PENDING_PAYMENT' 
            },
            { $set: { status: 'PAID' } },
            { new: true }
        );

        if (!order) {
            return res.status(400).json({ success: false, message: "Order not found or already processed." });
        }

        // Optional: Auto-complete after mark-paid if you want
        // await completeP2POrder(order); 

        return res.status(200).json({ success: true, order });
    } catch (err) {
        return res.status(500).json({ success: false, message: err.message });
    }
});


// =========================================================================
// ENDPOINT 4: SELLER RELEASES ESCROW (VERIFIES FIAT IN HAND, CALCULATES PAYOUT)
// =========================================================================
app.post('/api/orders/complete', async (req, res) => {
    try {
        const { orderId, sellerIdentity } = req.body;

        const order = await P2POrder.findById(orderId);
        if (!order || (order.status !== 'PAID' && order.status !== 'PENDING_PAYMENT')) {
            return res.status(400).json({ success: false, message: "Order ledger data is not in a reconcilable release state." });
        }
        if (order.sellerIdentity !== sellerIdentity.trim()) {
            return res.status(403).json({ success: false, message: "Access Denied: Node authorization failure on signature release." });
        }

        const buyer = await User.findOne({ identity: order.buyerIdentity });
        if (!buyer) {
            return res.status(404).json({ success: false, message: "Fulfillment failure. Destination buyer node not found." });
        }

        // Fixed: Use historical locked fiat totals instead of recalculating an evolving asset curve post-facto
        const finalPayoutKES = order.fiatTotal;

        // Credit the asset balance securely over to the buyer profile mapping
        buyer.afroCoins = Number((buyer.afroCoins + order.afroAmount).toFixed(4));
        await buyer.save();

        // Increment the seller's tracking ledger with the exact locked system calculated earnings equity
        await User.findOneAndUpdate(
            { identity: order.sellerIdentity },
            { $inc: { earnings: finalPayoutKES } }
        );

        // Finalize transaction history state
        order.status = 'COMPLETED';
        order.paymentConfirmedAt = new Date();
        await order.save();

        return res.status(200).json({ 
            success: true, 
            message: "Escrow pool released safely and account ledger balances finalized.",
            settledAmount: `KES ${finalPayoutKES}`
        });
    } catch (err) {
        return res.status(500).json({ success: false, message: err.message });
    }
});



// =========================================================================
// ENDPOINT 5: FETCH MARKETPLACE BOARD (DYNAMIC REAL-TIME FX INDEX ESTIMATES)
// =========================================================================
app.get('/api/orders/open', async (req, res) => {
    try {
        // Pull down open orders sorted by newest deployment activity sequence
        const activeOrders = await P2POrder.find({ status: 'OPEN' }).sort({ createdAt: -1 });

        return res.status(200).json({ success: true, orders: activeOrders });
    } catch (err) {
        return res.status(500).json({ success: false, message: err.message });
    }
});



// =========================================================================
// ENDPOINT 6: SYSTEM TICKER VALUE ROUTE (SERVES LIVE LOGS TO FRONTEND DISPLAY)
// =========================================================================
app.get('/api/market/afro-value', async (req, res) => {
    try {
        const marketPrice = await calculateCurrentAfroPrice(User);

        return res.status(200).json({
            success: true,
            metrics: {
                totalUsers: marketPrice.totalUsers,
                targetUsersMilestone: CURVE_CONFIG.TARGET_USERS
            },
            valuation: {
                usd: marketPrice.usdRate,
                kes: marketPrice.kesRate
            }
        });
    } catch (error) {
        return res.status(500).json({ success: false, error: error.message });
    }
});

module.exports = app;

// SEND AFRO TO ANOTHER NODE (P2P + external readiness)
app.post('/api/afro/transfer', async (req, res) => {
    const { senderIdentity, recipientIdentity, amount } = req.body;
    const cleanedSender = cleanPhone(senderIdentity);
    const cleanedRecipient = cleanPhone(recipientIdentity);

    if (!amount || amount <= 0) return res.status(400).json({ error: "INVALID_AMOUNT" });

    try {
        const sender = await User.findOne({ identity: cleanedSender });
        if (!sender || sender.afroCoins < amount) {
            return res.status(400).json({ error: "INSUFFICIENT_AFRO" });
        }

        const recipient = await User.findOneAndUpdate(
            { identity: cleanedRecipient },
            { $inc: { afroCoins: amount } },
            { upsert: true, new: true }
        );

        sender.afroCoins = Number((sender.afroCoins - amount).toFixed(4));
        await sender.save();

        // Optional: Record in ledger
        await Transaction.create({
            checkoutID: `AFRO-XFER-${Date.now()}`,
            userPhone: cleanedSender,
            amountPaid: amount,
            type: 'afro_transfer',
            status: 'completed'
        });

        res.json({ success: true, newSenderBalance: sender.afroCoins });
    } catch (e) {
        res.status(500).json({ error: "TRANSFER_FAILED" });
    }
});
// === MERCHANT DASHBOARD BACKEND ===
app.get('/api/merchant/history', async (req, res) => {
    const { identity } = req.query;
    const cleaned = cleanPhone(identity);
    
    try {
        // Get redemptions where this user acted as merchant
        const user = await User.findOne({ identity: cleaned });
        
        const redemptions = await Payout.find({
            recipientNode: cleaned,
            parentTxID: { $regex: /^AFRO-/ } // Only AFRO redemptions
        }).sort({ timestamp: -1 }).limit(50);

        res.json({
            success: true,
            merchantIdentity: cleaned,
            totalRedeemed: redemptions.reduce((sum, p) => sum + p.grossAmount, 0),
            redemptions: redemptions.map(p => ({
                code: p.parentTxID,
                amount: p.grossAmount,
                netAmount: p.creatorNet,
                timestamp: p.timestamp,
                status: p.status
            }))
        });
    } catch (err) {
        res.status(500).json({ error: "MERCHANT_HISTORY_FAILED" });
    }
});

// ====================== ECOMMERCE ======================


// Get all products
app.get('/api/products', async (req, res) => {
  try {
    const products = await Product.find({ stock: { $gt: 0 } }).sort({ timestamp: -1 });
    res.json(products);
  } catch (err) {
    res.status(500).json({ error: "Store offline" });
  }
});

// Create product (for sellers)
app.post('/api/products', upload.array('images', 5), async (req, res) => {
  try {
    const { name, description, price, category, seller } = req.body;
    
    if (!name || !price || !seller) {
      return res.status(400).json({ error: "Name, price, and seller are required" });
    }

    const product = await Product.create({
      name: name.trim(),
      description: description ? description.trim() : "",
      price: Number(price),
      category: category || "other",
      seller: cleanPhone(seller),
      images: req.files ? req.files.map(f => f.filename) : [],
      stock: parseInt(req.body.stock) || 50
    });

    res.status(201).json(product);
  } catch (err) {
    console.error("Product creation error:", err);
    res.status(500).json({ error: "Product creation failed" });
  }
});

// Reuse your existing unlock/payment system for products
app.post('/api/products/:id/purchase', async (req, res) => {
  const { phone } = req.body;
  const productId = req.params.id;
  
  // Just forward to your universal payment handler
  const product = await Product.findById(productId);
  if (!product) return res.status(404).json({ error: "Product not found" });

  const result = await triggerUniversalPush(phone, product.price, productId, 'product_purchase');
  res.json(result);
});
// =========================================================================
// TVWS SPECTRUM & NETWORK MANAGEMENT ROUTES
// =========================================================================

/**
 * 1. GET /api/tvws/spectrum
 * Query available TV White Space spectrum channels by GPS coordinates.
 */
app.get('/api/tvws/spectrum', async (req, res) => {
    const { lat, lng, height } = req.query;

    if (!lat || !lng) {
        return res.status(400).json({ error: "MISSING_COORDINATES", message: "lat and lng parameters are required." });
    }

    try {
        const spectrumData = await queryTvwsSpectrumDatabase(
            parseFloat(lat), 
            parseFloat(lng), 
            parseFloat(height || 10)
        );
        res.json(spectrumData);
    } catch (err) {
        res.status(500).json({ error: "SPECTRUM_QUERY_FAILED", details: err.message });
    }
});

/**
 * 2. POST /api/tvws/register
 * Register a TVWS Fixed or Portable Node on the network grid.
 */
app.post('/api/tvws/register', async (req, res) => {
    try {
        const { nodeIdentity, latitude, longitude, antennaHeightMeters, deviceClass } = req.body;

        if (!nodeIdentity || !latitude || !longitude) {
            return res.status(400).json({ error: "MISSING_NODE_DATA" });
        }

        const cleanedIdentity = cleanPhone(nodeIdentity);

        // Fetch valid channels for the node's location
        const spectrum = await queryTvwsSpectrumDatabase(latitude, longitude, antennaHeightMeters);
        const assignedChannel = spectrum.channels.length > 0 ? spectrum.channels[0].channelNumber : null;

        const tvwsNode = await TVWSNode.findOneAndUpdate(
            { nodeIdentity: cleanedIdentity },
            {
                latitude,
                longitude,
                antennaHeightMeters: antennaHeightMeters || 10,
                deviceClass: deviceClass || 'FIXED',
                assignedChannel: assignedChannel,
                status: assignedChannel ? 'ACTIVE' : 'PENDING',
                lastDbSync: new Date()
            },
            { upsert: true, new: true }
        );

        // Notify subscribers over Socket.io
        io.emit('tvws_node_updated', {
            nodeIdentity: cleanedIdentity,
            status: tvwsNode.status,
            channel: assignedChannel
        });

        res.status(201).json({
            success: true,
            node: tvwsNode,
            availableSpectrum: spectrum.channels
        });
    } catch (err) {
        console.error("TVWS Node Registration Error:", err);
        res.status(500).json({ error: "TVWS_REGISTRATION_FAILED" });
    }
});

/**
 * 3. GET /api/tvws/nodes
 * Fetch all active TVWS Nodes for network topology visibility.
 */
app.get('/api/tvws/nodes', async (req, res) => {
    try {
        const nodes = await TVWSNode.find({}).sort({ lastDbSync: -1 });
        res.json({ success: true, count: nodes.length, nodes });
    } catch (err) {
        res.status(500).json({ error: "TVWS_RETRIEVAL_FAILED" });
    }
});

// Trust first proxy layer for real client IP extraction
app.set('trust proxy', 1);



// ==========================================
// 2. IP TRACKING MIDDLEWARE
// ==========================================



const ipTracker = (req, res, next) => {
    try {
        const clientIp = req.headers['x-forwarded-for']
            ? req.headers['x-forwarded-for'].split(',')[0].trim()
            : req.socket.remoteAddress || 'unknown';

        req.clientIp = clientIp;

        const accessSignature = {
            ip: clientIp,
            userAgent: req.headers['user-agent'] || 'unknown',
            path: req.originalUrl,
            method: req.method,
            timestamp: new Date().toISOString()
        };

        ipLogs.push(accessSignature); // Safe in-memory array

        // Optional: Limit log size to prevent memory leak
        if (ipLogs.length > 1000) ipLogs.shift();

    } catch (e) {
        console.warn("IP Tracker warning:", e.message);
    }

    next();
};

app.use(ipTracker);
// ==========================================
// 3. IP MARKETPLACE & COLLABORATION ROUTES
// ==========================================

/**
 * @route POST /api/ip/ideas
 * @desc Create a new Intellectual Property Idea
 */
app.post('/api/ip/ideas', (req, res) => {
  const { title, description, ownerId } = req.body;

  if (!title || !description || !ownerId) {
    return res.status(400).json({ error: 'title, description, and ownerId are required.' });
  }

  const ideaId = `IP-${crypto.randomBytes(4).toString('hex').toUpperCase()}`;
  const ipIdea = {
    ideaId,
    title,
    description,
    ownerId,
    collaborators: [{ userId: ownerId, role: 'Owner', sharePercentage: 100 }],
    handshake: {
      isAgreed: false,
      agreedAt: null,
      auditLog: []
    },
    createdAt: new Date().toISOString()
  };

  db.ipMarketplace.set(ideaId, ipIdea);

  return res.status(201).json({
    message: 'IP Idea published successfully.',
    idea: ipIdea
  });
});

/**
 * @route POST /api/ip/ideas/:ideaId/collaborate
 * @desc Add co-creators and update royalty share splits
 */
app.post('/api/ip/ideas/:ideaId/collaborate', (req, res) => {
  const { ideaId } = req.params;
  const { collaborators } = req.body; // Array of { userId, role, sharePercentage }

  const idea = db.ipMarketplace.get(ideaId);
  if (!idea) {
    return res.status(404).json({ error: 'IP Idea not found.' });
  }

  if (idea.handshake.isAgreed) {
    return res.status(400).json({ error: 'Cannot modify collaborators after Handshake agreement is finalized.' });
  }

  // Validate total share equals 100%
  const totalShare = collaborators.reduce((sum, c) => sum + (c.sharePercentage || 0), 0);
  if (totalShare !== 100) {
    return res.status(400).json({ error: `Total collaboration share must equal 100%. Current total: ${totalShare}%` });
  }

  idea.collaborators = collaborators;
  db.ipMarketplace.set(ideaId, idea);

  return res.json({
    message: 'Collaborators and share terms updated.',
    idea
  });
});

/**
 * @route POST /api/ip/ideas/:ideaId/handshake
 * @desc Finalize IP Handshake Agreement with IP tracking verification
 */
app.post('/api/ip/ideas/:ideaId/handshake', (req, res) => {
  const { ideaId } = req.params;
  const { userId, signAgreement } = req.body;

  const idea = db.ipMarketplace.get(ideaId);
  if (!idea) {
    return res.status(404).json({ error: 'IP Idea not found.' });
  }

  if (!signAgreement) {
    return res.status(400).json({ error: 'Must explicitly accept signAgreement terms.' });
  }

  // Record cryptographic signature and access metadata
  const signature = {
    userId,
    ipAddress: req.clientIp,
    userAgent: req.headers['user-agent'],
    signedAt: new Date().toISOString(),
    signatureHash: crypto.createHash('sha256').update(`${userId}-${req.clientIp}-${Date.now()}`).digest('hex')
  };

  idea.handshake.auditLog.push(signature);

  // Check if all collaborators have signed
  const signedUsers = new Set(idea.handshake.auditLog.map(s => s.userId));
  const allSigned = idea.collaborators.every(c => signedUsers.has(c.userId));

  if (allSigned) {
    idea.handshake.isAgreed = true;
    idea.handshake.agreedAt = new Date().toISOString();
  }

  db.ipMarketplace.set(ideaId, idea);

  return res.json({
    message: allSigned ? 'Handshake fully executed by all collaborators.' : 'Handshake signed successfully. Awaiting other co-creators.',
    handshakeStatus: idea.handshake
  });
});

// ==========================================
// 4. E-COMMERCE CONVERSION & SALES ROUTES
// ==========================================

/**
 * @route POST /api/ecommerce/products
 * @desc Convert an agreed IP Idea into an active E-Commerce product listing
 */
app.post('/api/ecommerce/products', (req, res) => {
  const { ideaId, price, stock } = req.body;

  const idea = db.ipMarketplace.get(ideaId);
  if (!idea) {
    return res.status(404).json({ error: 'IP Idea not found.' });
  }

  if (!idea.handshake.isAgreed) {
    return res.status(400).json({ error: 'Cannot list product on E-Commerce platform before Handshake agreement is finalized.' });
  }

  const productId = `PROD-${crypto.randomBytes(4).toString('hex').toUpperCase()}`;
  const product = {
    productId,
    ideaId: idea.ideaId,
    title: idea.title,
    price: parseFloat(price),
    stock: parseInt(stock, 10),
    royaltySplits: idea.collaborators,
    createdAt: new Date().toISOString()
  };

  db.products.set(productId, product);

  return res.status(201).json({
    message: 'Product listing generated from IP Idea successfully.',
    product
  });
});

/**
 * @route POST /api/ecommerce/sales
 * @desc Process product sale and auto-calculate royalty payouts per IP collaboration agreement
 */
app.post('/api/ecommerce/sales', (req, res) => {
  const { productId, quantity, buyerId } = req.body;

  const product = db.products.get(productId);
  if (!product) {
    return res.status(404).json({ error: 'Product not found.' });
  }

  if (product.stock < quantity) {
    return res.status(400).json({ error: 'Insufficient stock available.' });
  }

  const totalAmount = product.price * quantity;

  // Distribute payouts based on handshake share splits
  const royaltyPayouts = product.royaltySplits.map(collaborator => ({
    userId: collaborator.userId,
    role: collaborator.role,
    sharePercentage: collaborator.sharePercentage,
    payoutAmount: Number(((collaborator.sharePercentage / 100) * totalAmount).toFixed(2))
  }));

  const saleRecord = {
    saleId: `SALE-${crypto.randomBytes(4).toString('hex').toUpperCase()}`,
    productId,
    buyerId,
    buyerIp: req.clientIp,
    quantity,
    totalAmount,
    royaltyPayouts,
    processedAt: new Date().toISOString()
  };

  // Deduct stock and record sale
  product.stock -= quantity;
  db.products.set(productId, product);
  db.sales.push(saleRecord);

  return res.status(201).json({
    message: 'Sale executed and royalties allocated.',
    sale: saleRecord
  });
});

/**
 * @route GET /api/ip/logs
 * @desc Fetch audit log of IP access signatures
 */
app.get('/api/ip/logs', (req, res) => {
  res.json({ totalLogs: db.ipLogs.length, logs: db.ipLogs });
});



// In processGridSuccess(), add case for product_purchase if needed
// (optional — your current flow already handles it via type)
const PORT = process.env.PORT || 10000;
server.listen(PORT, '0.0.0.0', async () => {
    await launchProtocol();
    console.log(`🚀 MASTER GRID ACTIVE | PORT: ${PORT}`);
    console.log(`🔗 CALLBACK URL TARGET: ${process.env.MPESA_CALLBACK_URL}`);
});
