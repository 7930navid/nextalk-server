const express = require('express');
const cors = require('cors');
const { Pool } = require('pg');
require('dotenv').config();

const app = express();

// Middleware
app.use(cors());
app.use(express.json());

// PostgreSQL Connection Setup
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    ssl: process.env.NODE_ENV === 'production' ? { rejectUnauthorized: false } : false
});

// Test Database Connection
pool.connect((err, client, release) => {
    if (err) {
        return console.error('Error acquiring client', err.stack);
    }
    console.log('✅ Connected to PostgreSQL Draft DB');
    release();
});

/* =========================================================
   BRIDGE CRUD ROUTES FOR DRAFT MESSAGES
   ========================================================= */

// 1. POST: Create / Send a message to Draft DB (If receiver is offline)
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
    const client = await pool.connect(); // Transaction-এর জন্য ক্লায়েন্ট কানেক্ট করা হলো

    try {
        const { receiver_id } = req.params;

        await client.query('BEGIN'); // Transaction শুরু

        // ১. আগে ড্রাফট মেসেজগুলো তুলে আনা
        const selectQuery = `
            SELECT * FROM draft_messages 
            WHERE receiver_id = $1 
            ORDER BY created_at ASC;
        `;
        const result = await client.query(selectQuery, [receiver_id]);

        // ২. যদি মেসেজ থাকে, তবে সেগুলো ডিলিট করে দেওয়া
        if (result.rows.length > 0) {
            const deleteQuery = `
                DELETE FROM draft_messages 
                WHERE receiver_id = $1;
            `;
            await client.query(deleteQuery, [receiver_id]);
        }

        await client.query('COMMIT'); // Transaction সফলভাবে শেষ করা হলো

        res.status(200).json({
            success: true,
            count: result.rows.length,
            data: result.rows
        });
    } catch (error) {
        await client.query('ROLLBACK'); // কোনো ভুল হলে আগের অবস্থায় ফেরত যাওয়া
        console.error("GET & DELETE Error:", error);
        res.status(500).json({ success: false, error: error.message });
    } finally {
        client.release(); // ক্লায়েন্ট রিলিজ করা
    }
});
// 3. PUT: Update message status (e.g., mark as 'delivered')
app.put('/api/bridge/drafts/:id', async (req, res) => {
    try {
        const { id } = req.params;
        const { status } = req.body; // status: 'delivered' or 'read'

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

// 4. DELETE: Remove single delivered message from Draft DB
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

// 5. DELETE ALL: Clear all delivered messages for a receiver after sync
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

// Start Server
const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
    console.log(`🚀 NexTalk Bridge Server running on port ${PORT}`);
});


app.get("/get/:name", (req, res) => {
  const name = req.params.name;
  const message = `${name} NexTalk server has been pinged`;
  res.send(message);
});