import mysql from 'mysql2/promise';
import axios from 'axios';
import dotenv from 'dotenv';
dotenv.config();

const CONFIG = {
    maxCreators: 182491,
    maxRetries: 5,
    delayOnError: 10000, // 10 seconds delay when any unknown error occurs
};

const DB_CONFIG = {
    host: 'coomer-db-rahuldhiman3855-25e2.j.aivencloud.com',
    port: 22652,
    user: 'avnadmin',
    password: 'AVNS_hIUOlyrjQ6aJw2oadb0',
    database: 'coomerDB',
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
};

const pool = mysql.createPool(DB_CONFIG);

const axiosInstance = axios.create({
    timeout: 5000,
    maxRedirects: 5,
    validateStatus: () => true
});

// --- Safe fetch with retry ---
async function fetchWithRetry(url, attempt = 1) {
    try {
        const response = await axiosInstance.get(url);
        if (response.status >= 400) throw new Error(`HTTP ${response.status}`);
        return response.data;
    } catch (error) {
        if (attempt >= CONFIG.maxRetries) {
            console.error(`❌ [${attempt}/${CONFIG.maxRetries}] Fetch failed: ${url}`);
            throw error;
        }
        console.warn(`⚠️ [${attempt}/${CONFIG.maxRetries}] Retrying fetch: ${url}`);
        await new Promise(res => setTimeout(res, 3000 * attempt));
        return fetchWithRetry(url, attempt + 1);
    }
}

// --- Process one creator ---
async function processCreator(creator) {
    console.log(`🔵 Processing creator: ${creator.id} (${creator.service})`);

    try {
        const posts = await fetchWithRetry(`https://coomer.su/api/v1/${creator.service}/user/${creator.id}`);
        const videoPosts = posts.filter(post => post.file && post.file.path && !post.file.path.endsWith('.jpg'));

        if (videoPosts.length === 0) {
            console.log(`✅ No video posts for creator ${creator.id}`);
            return;
        }

        const connection = await pool.getConnection();
        try {
            await connection.beginTransaction();

            for (const post of videoPosts) {
                const hash = post.file.path.split('/').pop().split('.')[0];
                let filesData;
                try {
                    filesData = await fetchWithRetry(`https://coomer.su/api/v1/search_hash/${hash}`);
                } catch (fileErr) {
                    console.warn(`⚠️ No file found for hash: ${hash}`);
                    continue; // Skip this post if file info not found
                }

                if (!filesData || !filesData.id) {
                    console.warn(`⚠️ Invalid file data for hash: ${hash}`);
                    continue;
                }

                await connection.execute(
                    'INSERT IGNORE INTO files (id, user, hash, size) VALUES (?, ?, ?, ?)',
                    [filesData.id, filesData.posts[0].user, hash, filesData.size]
                );

                await connection.execute(
                    'INSERT IGNORE INTO posts (id, user, title, file_path) VALUES (?, ?, ?, ?)',
                    [post.id, post.user, post.title, post.file.path]
                );
            }

            await connection.commit();
            console.log(`✅ Creator ${creator.id} processed successfully.`);
        } catch (error) {
            await connection.rollback();
            console.error(`❌ Transaction failed for creator ${creator.id}:`, error.message);
        } finally {
            connection.release();
        }
    } catch (error) {
        console.error(`❌ Failed processing creator ${creator.id}:`, error.message);
        await new Promise(res => setTimeout(res, CONFIG.delayOnError)); // Slow down a bit after errors
    }
}

// --- Main Runner ---
async function runSequentialProcessing() {
    let offset = 14650;
    let processed = 0;

    while (true) {
        try {
            const [rows] = await pool.query(
                'SELECT * FROM creators LIMIT 1 OFFSET ?',
                [offset]
            );

            if (rows.length === 0) {
                console.log('🎉 All creators processed.');
                break;
            }

            const creator = rows[0];
            await processCreator(creator);

            offset++;
            processed++;

            if (processed % 100 === 0) {
                console.log(`📈 Progress: ${processed} creators processed.`);
            }
        } catch (error) {
            console.error('❌ Error fetching next creator:', error.message);
            await new Promise(res => setTimeout(res, CONFIG.delayOnError));
        }
    }

    await pool.end();
}

runSequentialProcessing();
