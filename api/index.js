import express from 'express';
import mysql from 'mysql2';
import fs from 'fs';
import bodyParser from 'body-parser';
import {runBatchUpdate} from './batchUpdator.js';
import dotenv from 'dotenv';

dotenv.config();



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
        ca: `-----BEGIN CERTIFICATE-----
MIIETTCCArWgAwIBAgIUL5OrT2Eed15xuKUUw9P7zg0CPewwDQYJKoZIhvcNAQEM
BQAwQDE+MDwGA1UEAww1MDIxMmU3NTItNTQ3MS00NGE3LWEzMTgtODg5NDBjNTgx
NjE4IEdFTiAxIFByb2plY3QgQ0EwHhcNMjUwNDIwMDc1ODExWhcNMzUwNDE4MDc1
ODExWjBAMT4wPAYDVQQDDDUwMjEyZTc1Mi01NDcxLTQ0YTctYTMxOC04ODk0MGM1
ODE2MTggR0VOIDEgUHJvamVjdCBDQTCCAaIwDQYJKoZIhvcNAQEBBQADggGPADCC
AYoCggGBAK6/uCLN0i2JKoOxiH6/bfYl9IKdSRrYhoIBeWXfJmMscZIM6amRrkRH
pwrTGJfrPryHcXZ0Yfs+3645oudCW/s5TYGjdm1esyp2ysoYRTVtP11hu5Ouliq/
I9dynFsW0AjvNw81173hE10tf6yASoFGjDTJuoZciKmHe3HCRO0mWMWKU39C//t3
HkKA1wQiocQA0zfWRpEJgPs6eTU4VAMHWLmpuQJUfiaeRbzKiBg6+N+8q93/bA64
a914Dy93kR/nfHLNXDa71heM/QiE0+DQ23fJWq+pWRHRuuQJsrpk0vt8cfAkJzdy
CC0Mw0WAWrgwZe7v8s/H0eHRqGqN0X9yvZU+5i/q/NdR/5MnOuAgJqnngMmBGVIc
TuUxiy0Qyeu01d5RjLSphquvg7v6xtnj0cnaBiGSInfOr59GNqROm5waqMJtpCcM
zPgWty87K3J5rfzHYhn0oE8e92C0+ciUpl6/yH+iUhCKD/xHZ5GbIQHsPE2GKdnI
fqv+M/Z1QQIDAQABoz8wPTAdBgNVHQ4EFgQUDLC6249ieb/1941YirTj3qtWfmMw
DwYDVR0TBAgwBgEB/wIBADALBgNVHQ8EBAMCAQYwDQYJKoZIhvcNAQEMBQADggGB
AF+VvLdWvDR2pTkhX1zPjwzg42So67KnGoDjlY7g7wqJ1XHTSd3FKwXNkMQtWGEL
phAex2W7KuZc0gJWDPC1tp/0Ntbn5MfSlpqsqbH9nhqoaeN4aaExBwGL18JKBXR5
7hPJgpPgTVOUrHHgwcpMJGLw7XxMP65TkEIEbzA/7JSPiBuQckA2EP4EfOyMK5PD
of4CPLll4LVtj/lcqghIUxtM5thG5HRwt7xT72t1yi59Md7IbUXqQjmAJMWL3BQN
3AuaMN1TfVRXAciBk4NZfmLMe/ydOVW5HzVcWEsAnDjkT8fnIU4zod6SWgo2PIqW
2JJ66gBNByuMv1K1aXTgjLnA+PKhBlKjxGl3Ke2lSET8iMJI9cpFm3L8xKmwgWQR
cjbysWvhRYpt3XB8RTHplP21fh5Bl+bgKJiNWUnJw/9HKQ03RwE1QgSMPyTQjKsE
YCYtrS3sYv+4HZK7lyzKZf/NKu4YD1Cq6Uj6fmQIYv2giuAtq1cGqshrZS3p/lAC
dg==
-----END CERTIFICATE-----`
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





// process.on('unhandledRejection', (reason, promise) => {
//     console.error('Unhandled Rejection at:', promise, 'reason:', reason);
//     process.exit(1);
// });

// runBatchUpdate().catch(error => {
//     console.error('Fatal error:', error);
//     process.exit(1);
// });
