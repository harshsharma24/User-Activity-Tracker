// server.js

require('dotenv').config();
const fs          = require('fs');
const yaml        = require('js-yaml');
const path        = require('path');
const express     = require('express');
const helmet      = require('helmet');
const cors        = require('cors');
const rateLimit   = require('express-rate-limit');
const jwt         = require('jsonwebtoken');
const mysql       = require('mysql2/promise');
const Redis       = require('ioredis');
const Joi         = require('joi');
const Queue       = require('bull');
const axios       = require('axios');
const winston     = require('winston');
const swaggerUi   = require('swagger-ui-express');
const retry       = require('async-retry');

// --- 1. Config Validation ---
const envSchema = Joi.object({
  PORT:            Joi.number().default(3000),
  DB_HOST:         Joi.string().required(),
  DB_USER:         Joi.string().required(),
  DB_PASS:         Joi.string().required(),
  DB_NAME:         Joi.string().required(),
  REDIS_URL:       Joi.string().uri().required(),
  NOTIF_URL:       Joi.string().uri().required(),
  JWT_SECRET:      Joi.string().required(),
  ALLOWED_ORIGINS: Joi.string().default('')
}).unknown();

const { error: envErr, value: env } = envSchema.validate(process.env);
if (envErr) {
  console.error('Config validation error:', envErr.message);
  process.exit(1);
}

const {
  PORT, DB_HOST, DB_USER, DB_PASS, DB_NAME,
  REDIS_URL, NOTIF_URL, JWT_SECRET, ALLOWED_ORIGINS
} = env;

// --- 2. Logger ---
const logger = winston.createLogger({
  level: 'info',
  format: winston.format.json(),
  transports: [ new winston.transports.Console() ],
});

// --- 3. Express Setup ---
const app = express();
app.use(helmet());
app.use(cors({ origin: ALLOWED_ORIGINS.split(',') }));
app.use(express.json());
app.use(rateLimit({
  windowMs: 60 * 1000,
  max: 100,
  standardHeaders: true,
  legacyHeaders: false
}));

// --- 4. Swagger UI / OpenAPI ---
const specPath    = path.join(__dirname, 'open_api.yaml');
const openapiSpec = yaml.load(fs.readFileSync(specPath, 'utf8'));

app.use(
  '/api-docs',
  swaggerUi.serve,
  swaggerUi.setup(openapiSpec, { explorer: true })
);

// --- 5. Authentication Middleware ---
function authMiddleware(req, res, next) {
  const auth = req.headers.authorization;
  if (!auth) {
    return res.status(401).json({ code:'UNAUTHORIZED', message:'Missing Authorization header' });
  }
  try {
    req.user = jwt.verify(auth.split(' ')[1], JWT_SECRET);
    next();
  } catch {
    res.status(403).json({ code:'FORBIDDEN', message:'Invalid token' });
  }
}
app.use('/api/v1', authMiddleware);

// --- 6. DB, Cache Clients & Queues ---
const dbPool         = mysql.createPool({
  host: DB_HOST, user: DB_USER, password: DB_PASS, database: DB_NAME,
  waitForConnections: true, connectionLimit: 10
});
const redis          = new Redis(REDIS_URL);
const notifQueue     = new Queue('notifications', REDIS_URL);
const upsertErrorQ   = new Queue('upsert-errors', REDIS_URL);
const rebuildCacheQ  = new Queue('rebuild-cache', REDIS_URL);

// --- 7. Queue Processors ---
notifQueue.process('notify', async job => {
  try {
    await axios.post(NOTIF_URL, job.data, { timeout: 2000 });
  } catch (err) {
    logger.error('Notification failure', { error: err.message, data: job.data });
    throw err;
  }
});

rebuildCacheQ.process(async job => {
  const { userIds } = job.data;
  const [rows] = await dbPool.query(
    'SELECT userId, likes, comments, shares FROM user_stats WHERE userId IN (?)',
    [userIds]
  );
  const pipe = redis.pipeline();
  rows.forEach(r => {
    pipe.hset(`user:${r.userId}`, {
      likes:    r.likes,
      comments: r.comments,
      shares:   r.shares
    });
  });
  await pipe.exec();
});

// --- 8. Joi Schemas ---
const activitySchema = Joi.object({
  activityId: Joi.string().uuid().required(),
  userId:     Joi.number().integer().positive().required(),
  type:       Joi.string().valid('likes','comments','shares').required(),
  timestamp:  Joi.string().isoDate().required()
}).strict();

const batchSchema = Joi.object({
  activities: Joi.array().items(activitySchema).min(1).max(1000).required()
}).strict();

// --- 9. Main Handler (Chunking, Retry, DLQ, Compensation) ---
app.post('/api/v1/user-activity', async (req, res) => {
  // 9.1 Validate
  const { error, value } = batchSchema.validate(req.body, { abortEarly: false });
  if (error) {
    return res.status(400).json({
      code: 'INVALID_PAYLOAD',
      message: 'Validation failed',
      details: error.details.map(d => d.message)
    });
  }
  let activities = value.activities;

  // 9.2 Deduplication
  const ids = activities.map(a => a.activityId);
  const [existing] = await dbPool.query(
    'SELECT activityId FROM user_activities WHERE activityId IN (?)',
    [ids]
  );
  const seen = new Set(existing.map(r => r.activityId));
  activities = activities.filter(a => !seen.has(a.activityId));
  if (!activities.length) {
    return res.status(200).json({ processed: 0 });
  }

  // 9.3 Aggregate deltas per user
  const agg = activities.reduce((m, a) => {
    m[a.userId] = m[a.userId] || { likes:0, comments:0, shares:0 };
    m[a.userId][a.type]++;
    return m;
  }, {});

  // 9.4 Chunked Bulk UPSERT with Error Queue
  const rows = Object.entries(agg).map(([u, c]) => [u, c.likes, c.comments, c.shares]);
  const chunkSize = 200;
  for (let i = 0; i < rows.length; i += chunkSize) {
    const chunk = rows.slice(i, i + chunkSize);
    const placeholders = chunk.map(() => '(?, ?, ?, ?)').join(',');
    const params = chunk.flat();
    let conn;
    try {
      conn = await dbPool.getConnection();
      await conn.beginTransaction();
      const sql = `
        INSERT INTO user_stats (userId, likes, comments, shares)
        VALUES ${placeholders}
        ON DUPLICATE KEY UPDATE
          likes    = likes    + VALUES(likes),
          comments = comments + VALUES(comments),
          shares   = shares   + VALUES(shares)
      `;
      await conn.query(sql, params);
      await conn.commit();
    } catch (err) {
      if (conn) await conn.rollback();
      logger.error('Chunk DB error', { error: err.message, batchIndex: i / chunkSize });
      await upsertErrorQ.add({ rows: chunk }, {
        attempts: 2,
        backoff:  { type: 'exponential', delay: 5000 },
        removeOnComplete: true,
        removeOnFail:     false
      });
    } finally {
      if (conn) conn.release();
    }
  }

  // 9.5 Mark processed activityIds
  try {
    const toInsert = activities.map(a => [a.activityId]);
    await dbPool.query('INSERT IGNORE INTO user_activities (activityId) VALUES ?', [toInsert]);
  } catch (err) {
    logger.error('Mark dedup error', { error: err.message });
  }

  // 9.6 Redis Pipeline + Retry + Compensation
  try {
    await retry(async () => {
      const pipe = redis.pipeline();
      Object.entries(agg).forEach(([u, c]) => {
        const key = `user:${u}`;
        if (c.likes)    pipe.hincrby(key, 'likes',    c.likes);
        if (c.comments) pipe.hincrby(key, 'comments', c.comments);
        if (c.shares)   pipe.hincrby(key, 'shares',   c.shares);
      });
      const results = await pipe.exec();
      if (results.some(([err]) => err)) {
        throw new Error('Redis pipeline command failed');
      }
    }, { retries: 3, minTimeout: 1000 });
  } catch (err) {
    logger.error('Redis error, scheduling rebuild', { error: err.message });
    await rebuildCacheQ.add({ userIds: Object.keys(agg) });
  }

  // 9.7 Enqueue Notifications
  const notifOpts = {
    attempts:       3,
    backoff:        { type: 'exponential', delay: 5000 },
    removeOnComplete: true,
    removeOnFail:     false
  };
  const jobs = activities.map(act => ({
    name: 'notify',
    data: act,
    opts: notifOpts
  }));
  await notifQueue.addBulk(jobs);

  // 9.8 Final Response
  res.status(200).json({ processed: activities.length });
});

// --- 10. Health & Readiness ---
app.get('/api/v1/healthz', (req, res) => res.sendStatus(200));
app.get('/api/v1/readyz', async (req, res) => {
  try {
    await dbPool.query('SELECT 1');
    await redis.ping();
    res.sendStatus(200);
  } catch {
    res.sendStatus(503);
  }
});

// --- 11. Graceful Shutdown ---
const server = app.listen(PORT, () => logger.info(`Server listening on port ${PORT}`));

async function shutdown() {
  logger.info('Shutting down...');
  server.close();
  await dbPool.end();
  await redis.quit();
  await notifQueue.close();
  process.exit(0);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
