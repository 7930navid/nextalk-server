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
    origin: "*", // আপনার ফ্রন্টএন্ড URL দিতে পারেন
    methods: ["GET", "POST", "PUT", "DELETE"]
  }
});

/* =========================
   MIDDLEWARE
========================= */
app.use(cors());
app.use(express.json({ limit: "50mb" }));
app.use(express.urlencoded({ limit: "50mb", extended: true }));

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
// অনলাইনে থাকা ইউজারদের ট্র্যাকিং (userId -> socketId)
const onlineUsers = new Map();

io.on("connection", (socket) => {
    console.log("⚡ User connected:", socket.id);

    // ইউজার অনলাইন হলে এবং নিজের রুম জয়েন করলে
    socket.on("register_user", (userId) => {
        if (userId) {
            socket.join(userId);
            onlineUsers.set(userId, socket.id);
            console.log(`👤 User ${userId} registered with socket ${socket.id}`);
        }
    });

    // মেসেজ রিসিভ ও রিডাইরেক্ট করা
    socket.on("send_message", async (data) => {
        const { sender_id, receiver_id, text, media } = data;

        // প্রাপক যদি এখন অনলাইনে থাকে
        if (onlineUsers.has(receiver_id)) {
            io.to(receiver_id).emit("receive_message", {
                sender_id,
                receiver_id,
                message_text: text,
                media_payload: media,
                created_at: new Date()
            });
        } else {
            // প্রাপক অফলাইনে থাকলে ড্যাটাবেজে ড্রাফট হিসেব সেভ করা
            try {
                const query = `
                    INSERT INTO draft_messages (sender_id, receiver_id, message_text, media_payload)
                    VALUES ($1, $2, $3, $4);
                `;
                await pool.query(query, [sender_id, receiver_id, text || null, media ? JSON.stringify(media) : null]);
                console.log(`📥 Message saved to draft queue for offline user: ${receiver_id}`);
            } catch (err) {
                console.error("❌ Failed to save draft message via Socket:", err);
            }
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

// 1. POST: Save message to Draft DB via HTTP
app.post('/api/bridge/drafts', async (req, res) => {
    try {
        const { sender_id, receiver_id, text, media } = req.body;

        if (!sender_id || !receiver_id) {
            return res.status(400).json({ success: false, error: "Sender and Receiver IDs are required." });
        }

        const query = `
            INSERT INTO draft_messages (sender_id, receiver_id, message_text, media_payload)
            VALUES ($1, $2, $3, $4)
            RETURNING *;
        `;
        const values = [sender_id, receiver_id, text || null, media ? JSON.stringify(media) : null];

        const result = await pool.query(query, values);

        res.status(201).json({
            success: true,
            message: "Draft message queued successfully",
            data: result.rows[0]
        });
    } catch (error) {
        console.error("POST Error:", error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// 2. GET & CLEAR: Fetch and delete pending draft messages for a specific receiver
app.get('/api/bridge/drafts/:receiver_id', async (req, res) => {
    const client = await pool.connect();

    try {
        const { receiver_id } = req.params;

        await client.query('BEGIN');

        const selectQuery = `
            SELECT * FROM draft_messages 
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

// 3. PUT: Update message status
app.put('/api/bridge/drafts/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { status } = req.body;

        const query = `
            UPDATE draft_messages 
            SET status = $1 
            WHERE id = $2 
            RETURNING *;
        `;
        const result = await pool.query(query, [status, id]);

        if (result.rows.length === 0) {
            return res.status(404).json({ success: false, error: "Message draft not found." });
        }

        res.status(200).json({
            success: true,
            message: "Draft status updated",
            data: result.rows[0]
        });
    } catch (error) {
        console.error("PUT Error:", error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// 4. DELETE: Single message
app.delete('/api/bridge/drafts/:id', async (req, res) => {
    try {
        const { id } = req.params;

        const query = `DELETE FROM draft_messages WHERE id = $1 RETURNING *;`;
        const result = await pool.query(query, [id]);

        if (result.rows.length === 0) {
            return res.status(404).json({ success: false, error: "Draft not found or already deleted." });
        }

        res.status(200).json({
            success: true,
            message: "Draft message removed from server queue."
        });
    } catch (error) {
        console.error("DELETE Error:", error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// 5. DELETE ALL
app.delete('/api/bridge/drafts/clear/:receiver_id', async (req, res) => {
    try {
        const { receiver_id } = req.params;

        const query = `DELETE FROM draft_messages WHERE receiver_id = $1;`;
        await pool.query(query, [receiver_id]);

        res.status(200).json({
            success: true,
            message: `All synced draft messages cleared for receiver: ${receiver_id}`
        });
    } catch (error) {
        console.error("CLEAR ALL Error:", error);
        res.status(500).json({ success: false, error: error.message });
    }
});

// Ping route
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
