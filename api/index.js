import express from 'express';
import mysql from 'mysql2';
import fs from 'fs';
import bodyParser from 'body-parser';
import {runBatchUpdate} from '../batchUpdator.js';
import dotenv from 'dotenv';

dotenv.config();

console.log(process.env.host, process.env.port, process.env.user, process.env.password, process.env.database);


const app = express();
const port = 8000;

app.use(bodyParser.json());

// MySQL connection
const connection = mysql.createConnection({
    host: process.env.host,
    port: process.env.port,
    user: process.env.user,
    password: process.env.password,
    database: process.env.database,
    ssl: {
        ca: fs.readFileSync('./ca.pem')
    }
});

connection.connect((err) => {
    if (err) {
        console.error('Error connecting to MySQL:', err.stack);
        return;
    }
    console.log('Connected to MySQL as id ' + connection.threadId);
});

///////////////////////////////
// Tables Creation (Optional)
///////////////////////////////
// const initDB = () => {
//   const createCreators = `
//     CREATE TABLE IF NOT EXISTS creators (
//       id INT AUTO_INCREMENT PRIMARY KEY,
//       name VARCHAR(255),
//       platform VARCHAR(255)
//     )`;

//   const createPosts = `
//     CREATE TABLE IF NOT EXISTS posts (
//       id INT AUTO_INCREMENT PRIMARY KEY,
//       creator_id INT,
//       title VARCHAR(255),
//       content TEXT,
//       FOREIGN KEY (creator_id) REFERENCES creators(id)
//     )`;

//   const createFiles = `
//     CREATE TABLE IF NOT EXISTS files (
//       id INT AUTO_INCREMENT PRIMARY KEY,
//       post_id INT,
//       url VARCHAR(500),
//       type VARCHAR(100),
//       FOREIGN KEY (post_id) REFERENCES posts(id)
//     )`;

//   connection.query(createCreators);
//   connection.query(createPosts);
//   connection.query(createFiles);
// };

// initDB();

///////////////////////////////////////
// CRUD Routes: Creators
///////////////////////////////////////

// CREATE Creator
app.post('/creators', (req, res) => {
    const { name, platform } = req.body;
    const query = 'INSERT INTO creators (name, platform) VALUES (?, ?)';
    connection.query(query, [name, platform], (err, results) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ id: results.insertId, name, platform });
    });
});

// READ all Creators
app.get('/creators', (req, res) => {
    const { limit, offset } = req.query;
    const query = 'SELECT * FROM creators LIMIT ? OFFSET ?';
    connection.query(query, [parseInt(limit) || 10, parseInt(offset) || 0], (err, results) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(results);
    });
});

// UPDATE Creator
app.put('/creators/:id', (req, res) => {
    const { name, platform } = req.body;
    const { id } = req.params;
    const query = 'UPDATE creators SET name = ?, platform = ? WHERE id = ?';
    connection.query(query, [name, platform, id], (err) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ message: 'Creator updated' });
    });
});

// DELETE Creator
app.delete('/creators/:id', (req, res) => {
    const { id } = req.params;
    connection.query('DELETE FROM creators WHERE id = ?', [id], (err) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ message: 'Creator deleted' });
    });
});

///////////////////////////////////////
// CRUD Routes: Posts
///////////////////////////////////////

// CREATE Post
app.post('/posts', (req, res) => {
    const { creator_id, title, content } = req.body;
    const query = 'INSERT INTO posts (creator_id, title, content) VALUES (?, ?, ?)';
    connection.query(query, [creator_id, title, content], (err, results) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ id: results.insertId, creator_id, title, content });
    });
});

// READ all Posts
app.get('/posts', (req, res) => {
    const { limit, offset } = req.query;
    const query = 'SELECT * FROM posts LIMIT ? OFFSET ?';
    connection.query(query, [parseInt(limit) || 10, parseInt(offset) || 0], (err, results) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(results);
    });
});

// UPDATE Post
app.put('/posts/:id', (req, res) => {
    const { title, content } = req.body;
    const { id } = req.params;
    const query = 'UPDATE posts SET title = ?, content = ? WHERE id = ?';
    connection.query(query, [title, content, id], (err) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ message: 'Post updated' });
    });
});

// DELETE Post
app.delete('/posts/:id', (req, res) => {
    const { id } = req.params;
    connection.query('DELETE FROM posts WHERE id = ?', [id], (err) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ message: 'Post deleted' });
    });
});

///////////////////////////////////////
// CRUD Routes: Files
///////////////////////////////////////

// CREATE File
app.post('/files', (req, res) => {
    const { post_id, url, type } = req.body;
    const query = 'INSERT INTO files (post_id, url, type) VALUES (?, ?, ?)';
    connection.query(query, [post_id, url, type], (err, results) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ id: results.insertId, post_id, url, type });
    });
});

// READ all Files
app.get('/files', (req, res) => {
    connection.query('SELECT * FROM files', (err, results) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json(results);
    });
});

// UPDATE File
app.put('/files/:id', (req, res) => {
    const { url, type } = req.body;
    const { id } = req.params;
    const query = 'UPDATE files SET url = ?, type = ? WHERE id = ?';
    connection.query(query, [url, type, id], (err) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ message: 'File updated' });
    });
});

// DELETE File
app.delete('/files/:id', (req, res) => {
    const { id } = req.params;
    connection.query('DELETE FROM files WHERE id = ?', [id], (err) => {
        if (err) return res.status(500).json({ error: err.message });
        res.json({ message: 'File deleted' });
    });
});

app.get('/files/size', (req, res) => {
    const { limit, offset } = req.query;
    const query = 'SELECT * FROM files LIMIT ? OFFSET ?';

    connection.query(query, [parseInt(limit) || 10, parseInt(offset) || 0], (err, results) => {
        if (err) return res.status(500).json({ error: err.message });

        const userSet = new Set();
        const userStats = {};

        for (const row of results) {
            const user = row.user;
            const size = parseInt(row.size, 10);

            if (!userSet.has(user)) {
                userSet.add(user);
                userStats[user] = { totalSize: 0, count: 0 };
            }

            userStats[user].totalSize += size;
            userStats[user].count += 1;
        }

        const response = Array.from(userSet).map(user => ({
            user,
            averageSizeOfVideo: +(userStats[user].totalSize / userStats[user].count / 1024 / 1024).toFixed(2)
        }));

        res.json(response);
    });
});






///////////////////////////////
// Start Server
///////////////////////////////
app.listen(port, () => {
    console.log(`Server running at http://localhost:${port}`);
});





process.on('unhandledRejection', (reason, promise) => {
    console.error('Unhandled Rejection at:', promise, 'reason:', reason);
    process.exit(1);
});

runBatchUpdate().catch(error => {
    console.error('Fatal error:', error);
    process.exit(1);
});
