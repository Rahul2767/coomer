import mysql from 'mysql2/promise';
import axios from 'axios';
import fs from 'fs';
import dotenv from 'dotenv';
dotenv.config();

const CONFIG = {
    batchSize: 100,
    maxCreators: 182491,
    concurrency: 1,
    maxRetries: 10,
};

const DB_CONFIG = {
    host: process.env.host,
    port: process.env.port,
    user: process.env.user,
    password: process.env.password,
    database: process.env.database,
    ssl: {
        ca: fs.readFileSync('./ca.pem')
    }
};

const pool = mysql.createPool(DB_CONFIG);
const axiosInstance = axios.create({
    timeout: 5000,
    maxRedirects: 5,
    validateStatus: () => true
});

// Simplified retry logic with single exponential backoff
async function fetchWithRetry(url, retries = 1) {
    if (retries > CONFIG.maxRetries) {
        console.error(`Max retries reached for ${url}`);
        throw new Error(`Max retries reached for ${url}`);
    }
    const delay = 10000 * retries; // Exponential backoff delay

    try {
        const response = await axiosInstance.get(url);
        if (response.status >= 400) throw new Error(`HTTP error! status: ${response.status}`);
        return response.data;
    } catch (error) {
        console.error(`[${CONFIG.maxRetries - retries + 1}/${CONFIG.maxRetries}] Retrying ${url} in ${delay}ms`);
        await new Promise(resolve => setTimeout(resolve, delay));
        return fetchWithRetry(url, retries + 1);
    }
}

// Combined creator and post processing
async function processCreatorBatch(creators) {
    console.log(`Processing ${creators.length} creators...`);
    const postPromises = creators.map(async creator => {
        try {
            const posts = await fetchWithRetry(`https://coomer.su/api/v1/${creator.service}/user/${creator.id}`);
            const videoPosts = posts.filter(post => Object.keys(post.file).length > 0 && !post.file.path.endsWith('.jpg'));

            if (videoPosts.length === 0) {
                console.log(`✅ No video posts for creator ${creator.id}`);
                return null;
            }

            // Process all posts in a single database transaction
            const connection = await pool.getConnection();
            try {
                for (const post of videoPosts) {
                    const hash = post.file.path.split('/').pop().split('.')[0];
                    const filesData = await fetchWithRetry(`https://coomer.su/api/v1/search_hash/${hash}`);

                    if (!filesData) {
                        console.warn(`⚠️ No file found for hash: ${hash}`);
                        continue;
                    }

                    connection.beginTransaction();
                    await connection.execute(`INSERT INTO files (id, user, hash, size) VALUES (?, ?, ?, ?)`, [filesData.id, filesData.posts[0].user, hash, filesData.size]);
                    await connection.execute(`INSERT INTO posts (id, user, title, file_path) VALUES (?, ?, ?, ?)`, [post.id, post.user, post.title, post.file.path]);
                    connection.commit();
                }
                connection.release();
                console.log(`✅ Processed ${videoPosts.length} posts for creator ${creator.id}`);
            } catch (error) {
                await pool.execute('ROLLBACK');
                console.error(`❌ Error processing creator ${creator.id}:`, error);
                throw error;
            }
        } catch (error) {
            console.error(`❌ Error fetching creator ${creator.id}:`, error);
            return null;
        }
    });

    return Promise.all(postPromises);
}

// Simplified main runner
export async function runBatchUpdate() {
    let offset = 0;
    let totalProcessed = 0;

    while (offset < CONFIG.maxCreators) {
        try {
            const creators = await pool.query(
                'SELECT * FROM creators ORDER BY CAST(updated AS UNSIGNED) DESC LIMIT ? OFFSET ?',
                [CONFIG.batchSize, offset]
            );

            if (creators[0].length === 0) break;

            const successful = await processCreatorBatch(creators[0]);
            totalProcessed += successful.filter(Boolean).length;

            // fs.writeFileSync(
            //     'progress.json',
            //     JSON.stringify({ offset, totalProcessed, timestamp: new Date().toISOString() }, null, 2)
            // );

            offset += CONFIG.batchSize;
        } catch (error) {
            console.error('❌ Error in batch processing:', error);
            throw error;
        }
    }

    console.log('🎉 All creators processed successfully!');
    await pool.end();
}

