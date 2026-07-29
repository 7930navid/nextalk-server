require('dotenv').config();
const express = require('express');
const cors = require('cors');
const http = require('http');
const { Server } = require('socket.io');
const { Pool } = require('pg');

const app = express();
const server = http.createServer(app);

/* =========================
   SOCKET.IO SETUP
========================= */
const io = new Server(server, {
  cors: {
    origin: "*",
    methods: ["GET", "POST", "PUT", "DELETE"]
  }
});

/* =========================
   MIDDLEWARE
========================= */
app.use(cors());
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true }));

app.set('trust proxy', 1);

/* =========================
   DATABASE CONNECTION
========================= */
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

pool.connect((err, client, release) => {
    if (err) {
        return console.error('❌ Error acquiring client', err.stack);
    }
    console.log('✅ Connected to PostgreSQL Draft DB');
    release();
});

/* =========================
   SOCKET.IO REAL-TIME LOGIC
========================= */
const onlineUsers = new Map();

io.on("connection", (socket) => {
    console.log("⚡ User connected:", socket.id);

    // ১. ইউজার রেজিস্ট্রেশন
    socket.on("register_user", (userId) => {
        if (userId) {
            socket.join(userId);
            onlineUsers.set(userId, socket.id);
            console.log(`👤 User ${userId} registered with socket ${socket.id}`);
        }
    });

    // ২. মেসেজ সেন্ড
    socket.on("send_message", async (data) => {
        const { msgId, sender_id, receiver_id, message_text, original_text, media_payload, is_edited, is_deleted } = data;

        if (onlineUsers.has(receiver_id)) {
            io.to(receiver_id).emit("receive_message", {
                msgId,
                sender_id,
                receiver_id,
                message_text,
                original_text: original_text || message_text,
                media_payload,
                created_at: new Date(),
                is_edited: is_edited || false,
                is_deleted: is_deleted || false
            });
        } else {
            try {
                const query = `
                    INSERT INTO draft_messages (msg_id, sender_id, receiver_id, message_text, original_text, media_payload, is_edited, is_deleted)
                    VALUES ($1, $2, $3, $4, $5, $6, $7, $8);
                `;
                await pool.query(query, [
                    msgId, 
                    sender_id, 
                    receiver_id, 
                    message_text || null, 
                    original_text || message_text || null, 
                    media_payload ? JSON.stringify(media_payload) : null,
                    is_edited || false,
                    is_deleted || false
                ]);
                console.log(`📥 Message saved to draft queue for offline user: ${receiver_id}`);
            } catch (err) {
                console.error("❌ Failed to save draft message via Socket:", err);
            }
        }
    });

    // ৩. মেসেজ এডিট (Socket)
    socket.on("edit_message", (data) => {
        const { msgId, receiver_id, new_text } = data;
        if (onlineUsers.has(receiver_id)) {
            io.to(receiver_id).emit("message_edited", { msgId, new_text });
        }
    });

    // ৪. মেসেজ ডিলিট (Socket)
    socket.on("delete_message", (data) => {
        const { msgId, receiver_id } = data;
        if (onlineUsers.has(receiver_id)) {
            io.to(receiver_id).emit("message_deleted", { msgId });
        }
    });

    socket.on("disconnect", () => {
        for (let [userId, socketId] of onlineUsers.entries()) {
            if (socketId === socket.id) {
                onlineUsers.delete(userId);
                console.log(`❌ User ${userId} disconnected`);
                break;
            }
        }
    });
});

/* =========================================================
   REST API ROUTES FOR DRAFT MESSAGES
   ========================================================= */

// ১. POST: Save or Update Draft message (Handle New, Edit, and Delete Sync)
app.post('/api/bridge/drafts', async (req, res) => {
    try {
        const { msgId, sender_id, receiver_id, message_text, original_text, media_payload, is_edited, is_deleted } = req.body;

        if (!sender_id || !receiver_id) {
            return res.status(400).json({ success: false, error: "Sender and Receiver IDs are required." });
        }

        // ✅ FIXED: Correct SQL Syntax for SELECT query with LIMIT
        const checkQuery = `
            SELECT * FROM draft_messages 
            WHERE msg_id = $1 OR (sender_id = $2 AND receiver_id = $3 AND message_text = $4) 
            LIMIT 1;
        `;
        const existing = await pool.query(checkQuery, [msgId, sender_id, receiver_id, message_text]);

        let result;
        if (existing.rows.length > 0) {
            // Update existing message record (e.g. edited or deleted)
            const updateQuery = `
                UPDATE draft_messages 
                SET message_text = COALESCE($1, message_text),
                    is_edited = COALESCE($2, is_edited),
                    is_deleted = COALESCE($3, is_deleted)
                WHERE msg_id = $4 OR id = $5
                RETURNING *;
            `;
            result = await pool.query(updateQuery, [message_text, is_edited, is_deleted, msgId, existing.rows[0].id]);
        } else {
            // Insert new draft record
            const insertQuery = `
                INSERT INTO draft_messages (msg_id, sender_id, receiver_id, message_text, original_text, media_payload, is_edited, is_deleted)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                RETURNING *;
            `;
            const values = [
                msgId || 'msg_' + Date.now(), 
                sender_id, 
                receiver_id, 
                message_text || null, 
                original_text || message_text || null, 
                media_payload ? JSON.stringify(media_payload) : null,
                is_edited || false,
                is_deleted || false
            ];
            result = await pool.query(insertQuery, values);
        }

        res.status(201).json({
            success: true,
            message: "Draft message saved/synced successfully",
            data: result.rows[0]
        });
    } catch (error) {
        console.error("POST Draft Error:", error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// ২. GET & CLEAR: Fetch and delete pending draft messages for receiver
app.get('/api/bridge/drafts/:receiver_id', async (req, res) => {
    const client = await pool.connect();

    try {
        const { receiver_id } = req.params;

        await client.query('BEGIN');

        const selectQuery = `
            SELECT 
                id,
                msg_id AS "msgId",
                sender_id,
                receiver_id,
                message_text,
                original_text,
                media_payload,
                is_edited,
                is_deleted,
                created_at
            FROM draft_messages 
            WHERE receiver_id = $1 
            ORDER BY created_at ASC;
        `;
        const result = await client.query(selectQuery, [receiver_id]);

        if (result.rows.length > 0) {
            const deleteQuery = `
                DELETE FROM draft_messages 
                WHERE receiver_id = $1;
            `;
            await client.query(deleteQuery, [receiver_id]);
        }

        await client.query('COMMIT');

        res.status(200).json({
            success: true,
            count: result.rows.length,
            data: result.rows
        });
    } catch (error) {
        await client.query('ROLLBACK');
        console.error("GET & DELETE Error:", error);
        res.status(500).json({ success: false, error: error.message });
    } finally {
        client.release();
    }
});

// ৩. Ping route
app.get("/get/:name", (req, res) => {
  const name = req.params.name;
  res.send(`${name} NexTalk server has been pinged`);
});

/* =========================
   SERVER START
========================= */
const PORT = process.env.PORT || 5000;
server.listen(PORT, () => {
    console.log(`🚀 NexTalk Bridge Server running on port ${PORT}`);
});
