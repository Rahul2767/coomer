// import mysql from 'mysql2/promise';
// import axios from 'axios';
// import fs from 'fs';
// import dotenv from 'dotenv';
// dotenv.config();

// const CONFIG = {
//     batchSize: 100,
//     maxCreators: 182491,
//     concurrency: 1,
//     maxRetries: 10,
// };

// const DB_CONFIG = {
//     host: process.env.host,
//     port: process.env.port,
//     user: process.env.user,
//     password: process.env.password,
//     database: process.env.database,
//     ssl: {
//         ca: `-----BEGIN CERTIFICATE-----
// MIIETTCCArWgAwIBAgIUL5OrT2Eed15xuKUUw9P7zg0CPewwDQYJKoZIhvcNAQEM
// BQAwQDE+MDwGA1UEAww1MDIxMmU3NTItNTQ3MS00NGE3LWEzMTgtODg5NDBjNTgx
// NjE4IEdFTiAxIFByb2plY3QgQ0EwHhcNMjUwNDIwMDc1ODExWhcNMzUwNDE4MDc1
// ODExWjBAMT4wPAYDVQQDDDUwMjEyZTc1Mi01NDcxLTQ0YTctYTMxOC04ODk0MGM1
// ODE2MTggR0VOIDEgUHJvamVjdCBDQTCCAaIwDQYJKoZIhvcNAQEBBQADggGPADCC
// AYoCggGBAK6/uCLN0i2JKoOxiH6/bfYl9IKdSRrYhoIBeWXfJmMscZIM6amRrkRH
// pwrTGJfrPryHcXZ0Yfs+3645oudCW/s5TYGjdm1esyp2ysoYRTVtP11hu5Ouliq/
// I9dynFsW0AjvNw81173hE10tf6yASoFGjDTJuoZciKmHe3HCRO0mWMWKU39C//t3
// HkKA1wQiocQA0zfWRpEJgPs6eTU4VAMHWLmpuQJUfiaeRbzKiBg6+N+8q93/bA64
// a914Dy93kR/nfHLNXDa71heM/QiE0+DQ23fJWq+pWRHRuuQJsrpk0vt8cfAkJzdy
// CC0Mw0WAWrgwZe7v8s/H0eHRqGqN0X9yvZU+5i/q/NdR/5MnOuAgJqnngMmBGVIc
// TuUxiy0Qyeu01d5RjLSphquvg7v6xtnj0cnaBiGSInfOr59GNqROm5waqMJtpCcM
// zPgWty87K3J5rfzHYhn0oE8e92C0+ciUpl6/yH+iUhCKD/xHZ5GbIQHsPE2GKdnI
// fqv+M/Z1QQIDAQABoz8wPTAdBgNVHQ4EFgQUDLC6249ieb/1941YirTj3qtWfmMw
// DwYDVR0TBAgwBgEB/wIBADALBgNVHQ8EBAMCAQYwDQYJKoZIhvcNAQEMBQADggGB
// AF+VvLdWvDR2pTkhX1zPjwzg42So67KnGoDjlY7g7wqJ1XHTSd3FKwXNkMQtWGEL
// phAex2W7KuZc0gJWDPC1tp/0Ntbn5MfSlpqsqbH9nhqoaeN4aaExBwGL18JKBXR5
// 7hPJgpPgTVOUrHHgwcpMJGLw7XxMP65TkEIEbzA/7JSPiBuQckA2EP4EfOyMK5PD
// of4CPLll4LVtj/lcqghIUxtM5thG5HRwt7xT72t1yi59Md7IbUXqQjmAJMWL3BQN
// 3AuaMN1TfVRXAciBk4NZfmLMe/ydOVW5HzVcWEsAnDjkT8fnIU4zod6SWgo2PIqW
// 2JJ66gBNByuMv1K1aXTgjLnA+PKhBlKjxGl3Ke2lSET8iMJI9cpFm3L8xKmwgWQR
// cjbysWvhRYpt3XB8RTHplP21fh5Bl+bgKJiNWUnJw/9HKQ03RwE1QgSMPyTQjKsE
// YCYtrS3sYv+4HZK7lyzKZf/NKu4YD1Cq6Uj6fmQIYv2giuAtq1cGqshrZS3p/lAC
// dg==
// -----END CERTIFICATE-----`
//     }
// };

// const pool = mysql.createPool(DB_CONFIG);
// const axiosInstance = axios.create({
//     timeout: 5000,
//     maxRedirects: 5,
//     validateStatus: () => true
// });

// // Simplified retry logic with single exponential backoff
// async function fetchWithRetry(url, retries = 1) {
//     if (retries > CONFIG.maxRetries) {
//         console.error(`Max retries reached for ${url}`);
//         throw new Error(`Max retries reached for ${url}`);
//     }
//     const delay = 10000 * retries; // Exponential backoff delay

//     try {
//         const response = await axiosInstance.get(url);
//         if (response.status >= 400) throw new Error(`HTTP error! status: ${response.status}`);
//         return response.data;
//     } catch (error) {
//         console.error(`[${CONFIG.maxRetries - retries + 1}/${CONFIG.maxRetries}] Retrying ${url} in ${delay}ms`);
//         await new Promise(resolve => setTimeout(resolve, delay));
//         return fetchWithRetry(url, retries + 1);
//     }
// }

// // Combined creator and post processing
// async function processCreatorBatch(creators) {
//     console.log(`Processing ${creators.length} creators...`);
//     const postPromises = creators.map(async creator => {
//         try {
//             const posts = await fetchWithRetry(`https://coomer.su/api/v1/${creator.service}/user/${creator.id}`);
//             const videoPosts = posts.filter(post => Object.keys(post.file).length > 0 && !post.file.path.endsWith('.jpg'));

//             if (videoPosts.length === 0) {
//                 console.log(`✅ No video posts for creator ${creator.id}`);
//                 return null;
//             }

//             // Process all posts in a single database transaction
//             const connection = await pool.getConnection();
//             try {
//                 for (const post of videoPosts) {
//                     const hash = post.file.path.split('/').pop().split('.')[0];
//                     const filesData = await fetchWithRetry(`https://coomer.su/api/v1/search_hash/${hash}`);

//                     if (!filesData) {
//                         console.warn(`⚠️ No file found for hash: ${hash}`);
//                         continue;
//                     }

//                     connection.beginTransaction();
//                     await connection.execute(`INSERT INTO files (id, user, hash, size) VALUES (?, ?, ?, ?)`, [filesData.id, filesData.posts[0].user, hash, filesData.size]);
//                     await connection.execute(`INSERT INTO posts (id, user, title, file_path) VALUES (?, ?, ?, ?)`, [post.id, post.user, post.title, post.file.path]);
//                     connection.commit();
//                 }
//                 connection.release();
//                 console.log(`✅ Processed ${videoPosts.length} posts for creator ${creator.id}`);
//             } catch (error) {
//                 await pool.execute('ROLLBACK');
//                 console.error(`❌ Error processing creator ${creator.id}:`, error);
//                 throw error;
//             }
//         } catch (error) {
//             console.error(`❌ Error fetching creator ${creator.id}:`, error);
//             return null;
//         }
//     });

//     return Promise.all(postPromises);
// }

// // Simplified main runner
// export async function runBatchUpdate() {
//     let offset = 0;
//     let totalProcessed = 0;

//     while (offset < CONFIG.maxCreators) {
//         try {
//             const creators = await pool.query(
//                 'SELECT * FROM creators ORDER BY CAST(updated AS UNSIGNED) DESC LIMIT ? OFFSET ?',
//                 [CONFIG.batchSize, offset]
//             );

//             if (creators[0].length === 0) break;

//             const successful = await processCreatorBatch(creators[0]);
//             totalProcessed += successful.filter(Boolean).length;

//             // fs.writeFileSync(
//             //     'progress.json',
//             //     JSON.stringify({ offset, totalProcessed, timestamp: new Date().toISOString() }, null, 2)
//             // );

//             offset += CONFIG.batchSize;
//         } catch (error) {
//             console.error('❌ Error in batch processing:', error);
//             throw error;
//         }
//     }

//     console.log('🎉 All creators processed successfully!');
//     await pool.end();
// }

