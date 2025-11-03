/**
 * realistic_traffic_simulator_intense.js
 * Usage:
 *   node realistic_traffic_simulator_intense.js <concurrency> <durationSeconds> <users.csv>
 * Example:
 *   node realistic_traffic_simulator_intense.js 200 150 users.csv
 *
 * More aggressive version of the traffic simulator:
 * - Shorter session gaps (users reappear faster)
 * - Reduced think times (faster request cycles)
 * - More concurrent users by default
 * - Duration fixed to 150 seconds
 */

const fs = require("fs");
const csv = require("csv-parser");
const { Pool } = require("undici");

// 🖥️ Configuration
const TARGET = "http://localhost:3000";
const [,, concArg, durationArg, usersFileArg] = process.argv;

const CONCURRENCY = parseInt(concArg || "200", 10);
const DURATION_SECONDS = parseInt(durationArg || "150", 10);
const USERS_FILE = usersFileArg || "./users.csv";
const DURATION_MS = DURATION_SECONDS * 1000;

// 👥 User behavior categories (More intense version)
const USER_CATEGORIES = {
  POWER_USER: {
    probability: 0.08,       // 8% of users
    trafficShare: 0.45,      // Generate 45% of traffic
    sessionLength: [20, 40], // Requests per session
    thinkTime: [300, 1500],  // Shorter think times
    sessionGap: [10000, 60000], // Reappear more quickly
    routePreferences: {
      browse: 0.10,
      search: 0.15,
      login: 0.10,
      authenticated: 0.65
    }
  },
  REGULAR_USER: {
    probability: 0.20,
    trafficShare: 0.35,
    sessionLength: [10, 20],
    thinkTime: [800, 3000],
    sessionGap: [30000, 120000],
    routePreferences: {
      browse: 0.20,
      search: 0.25,
      login: 0.15,
      authenticated: 0.40
    }
  },
  OCCASIONAL_USER: {
    probability: 0.35,
    trafficShare: 0.15,
    sessionLength: [4, 10],
    thinkTime: [1500, 5000],
    sessionGap: [120000, 300000],
    routePreferences: {
      browse: 0.30,
      search: 0.30,
      login: 0.20,
      authenticated: 0.20
    }
  },
  LURKER: {
    probability: 0.37,
    trafficShare: 0.05,
    sessionLength: [1, 3],
    thinkTime: [2000, 8000],
    sessionGap: [240000, 600000],
    routePreferences: {
      browse: 0.50,
      search: 0.30,
      login: 0.15,
      authenticated: 0.05
    }
  }
};

// 🎯 Route definitions (same)
const ROUTES = {
  browse: [
    { path: "/", method: "GET", auth: false, weight: 3 },
    { path: "/health", method: "GET", auth: false, weight: 1 },
    { path: "/api/status", method: "GET", auth: false, weight: 2 },
  ],
  search: [
    { path: "/users/search", method: "GET", auth: false, weight: 5, addQuery: true },
    { path: "/users/list", method: "GET", auth: false, weight: 2 },
  ],
  login: [
    { path: "/login", method: "POST", auth: true, weight: 10 },
  ],
  authenticated: [
    { path: "/user/profile", method: "POST", auth: true, weight: 5 },
    { path: "/user/settings", method: "POST", auth: true, weight: 3 },
    { path: "/api/io/read", method: "POST", auth: true, weight: 4 },
    { path: "/api/io/write", method: "POST", auth: true, weight: 3 },
    { path: "/api/network/external", method: "POST", auth: true, weight: 3 },
    { path: "/api/db/aggregate", method: "POST", auth: true, weight: 2 },
  ]
};

const pool = new Pool(TARGET, { 
  connections: Math.min(CONCURRENCY * 2, 500),
  pipelining: 15
});

// 📊 Global stats
const stats = {
  totalRequests: 0,
  successfulRequests: 0,
  failedRequests: 0,
  statusCounts: {},
  latencies: [],
  categoryCounts: {},
  routeTypeCounts: {},
  sessionCount: 0,
  activeUsers: new Set(),
  startTime: Date.now()
};

// Utilities
function nowMs() {
  const [s, ns] = process.hrtime();
  return s * 1000 + Math.floor(ns / 1e6);
}
function randomBetween(min, max) {
  return Math.floor(Math.random() * (max - min + 1)) + min;
}
function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}
function weightedRandom(items, key = 'weight') {
  const total = items.reduce((sum, item) => sum + (item[key] || 1), 0);
  let r = Math.random() * total;
  for (const item of items) {
    r -= (item[key] || 1);
    if (r <= 0) return item;
  }
  return items[0];
}

// Load users
async function loadUsers() {
  return new Promise((resolve, reject) => {
    const result = [];
    fs.createReadStream(USERS_FILE)
      .pipe(csv())
      .on("data", row => result.push(row))
      .on("end", () => resolve(result))
      .on("error", reject);
  });
}

// User profile
class UserProfile {
  constructor(user, category) {
    this.username = user.username;
    this.password = user.password;
    this.category = category;
    this.config = USER_CATEGORIES[category];
    this.isInSession = false;
    this.currentSessionRequests = 0;
    this.sessionRequestsTarget = 0;
    this.lastRequestTime = 0;
    this.totalRequests = 0;
    this.totalSessions = 0;
    stats.categoryCounts[category] = (stats.categoryCounts[category] || 0) + 1;
  }

  shouldStartSession() {
    if (this.isInSession) return false;
    const timeSince = Date.now() - this.lastRequestTime;
    const [minGap, maxGap] = this.config.sessionGap;
    if (timeSince < minGap) return false;
    if (timeSince > maxGap) return true;
    const progress = (timeSince - minGap) / (maxGap - minGap);
    return Math.random() < progress * 0.7; // more likely to reappear
  }

  startSession() {
    this.isInSession = true;
    this.currentSessionRequests = 0;
    const [min, max] = this.config.sessionLength;
    this.sessionRequestsTarget = randomBetween(min, max);
    this.totalSessions++;
    stats.sessionCount++;
    stats.activeUsers.add(this.username);
  }

  endSession() {
    this.isInSession = false;
    this.currentSessionRequests = 0;
    this.sessionRequestsTarget = 0;
    stats.activeUsers.delete(this.username);
  }

  getNextRoute() {
    const prefs = this.config.routePreferences;
    const random = Math.random();
    let cumulative = 0;
    for (const [type, probability] of Object.entries(prefs)) {
      cumulative += probability;
      if (random <= cumulative) {
        stats.routeTypeCounts[type] = (stats.routeTypeCounts[type] || 0) + 1;
        const route = weightedRandom(ROUTES[type]);
        if (route.addQuery) {
          const terms = ['admin', 'test', 'john', 'jane', 'prod', 'alpha', 'beta'];
          const term = terms[Math.floor(Math.random() * terms.length)];
          return { ...route, path: `/users/search?q=${term}` };
        }
        return route;
      }
    }
    return ROUTES.browse[0];
  }

  getThinkTime() {
    const [min, max] = this.config.thinkTime;
    return randomBetween(min, max);
  }

  async makeRequest() {
    this.currentSessionRequests++;
    this.totalRequests++;
    this.lastRequestTime = Date.now();
    if (this.currentSessionRequests >= this.sessionRequestsTarget) {
      this.endSession();
    }
  }
}

// Request sender
async function sendRequest(route, user) {
  const start = nowMs();
  try {
    const opts = {
      path: route.path,
      method: route.method,
      headers: {
        "user-agent": `intense-traffic-simulator/1.0`,
        "content-type": "application/json",
      },
      bodyTimeout: 15000,
      headersTimeout: 15000,
    };
    if (route.auth && user) {
      opts.body = JSON.stringify({
        username: user.username,
        password: user.password,
      });
    }
    const { statusCode, body } = await pool.request(opts);
    await body.arrayBuffer();
    const latency = nowMs() - start;
    stats.latencies.push(latency);
    stats.statusCounts[statusCode] = (stats.statusCounts[statusCode] || 0) + 1;
    stats.successfulRequests++;
    return { success: true };
  } catch {
    stats.failedRequests++;
    return { success: false };
  } finally {
    stats.totalRequests++;
  }
}

// Session worker
async function sessionWorker(user) {
  while (Date.now() - stats.startTime < DURATION_MS) {
    if (!user.isInSession && user.shouldStartSession()) {
      user.startSession();
      while (user.isInSession && Date.now() - stats.startTime < DURATION_MS) {
        const route = user.getNextRoute();
        await sendRequest(route, route.auth ? user : null);
        await user.makeRequest();
        await sleep(user.getThinkTime());
      }
    }
    await sleep(2000); // check more often
  }
}

// Progress
function startProgressReporter() {
  const interval = setInterval(() => {
    const elapsed = (Date.now() - stats.startTime) / 1000;
    const rps = (stats.totalRequests / elapsed).toFixed(2);
    const successRate = ((stats.successfulRequests / stats.totalRequests) * 100).toFixed(1);
    console.log(
      `[${elapsed.toFixed(0)}s] ` +
      `Req: ${stats.totalRequests} | RPS: ${rps} | OK: ${successRate}% | ` +
      `Sessions: ${stats.sessionCount} | Active: ${stats.activeUsers.size}`
    );
  }, 5000);
  return interval;
}

// Percentile
function percentile(arr, p) {
  if (!arr.length) return 0;
  const sorted = arr.slice().sort((a, b) => a - b);
  const idx = (p / 100) * (sorted.length - 1);
  const lo = Math.floor(idx), hi = Math.ceil(idx);
  return lo === hi ? sorted[lo] : sorted[lo] + (sorted[hi] - sorted[lo]) * (idx - lo);
}

// 🚀 Main
(async () => {
  console.log("🔥 Intense Traffic Simulator - 150s Mode");
  console.log("=========================================\n");

  const users = await loadUsers();
  console.log(`✅ Loaded ${users.length} users from ${USERS_FILE}`);

  const profiles = [];
  for (const user of users) {
    const r = Math.random();
    let cumulative = 0;
    let cat = "LURKER";
    for (const [c, cfg] of Object.entries(USER_CATEGORIES)) {
      cumulative += cfg.probability;
      if (r <= cumulative) { cat = c; break; }
    }
    profiles.push(new UserProfile(user, cat));
  }

  console.log(`⚙️  Config: Target=${TARGET}, Duration=${DURATION_SECONDS}s, Concurrency=${CONCURRENCY}\n`);
  const progress = startProgressReporter();

  const workers = [];
  for (let i = 0; i < CONCURRENCY; i++) {
    workers.push(sessionWorker(profiles[i % profiles.length]));
  }

  await Promise.race([Promise.all(workers), sleep(DURATION_MS)]);
  clearInterval(progress);
  await pool.close();

  const totalTime = (Date.now() - stats.startTime) / 1000;
  const avgLatency = stats.latencies.length
    ? (stats.latencies.reduce((a, b) => a + b, 0) / stats.latencies.length).toFixed(2)
    : 0;

  console.log("\n\n📊 FINAL REPORT");
  console.log("===============================");
  console.log(`⏱️ Duration: ${totalTime.toFixed(1)}s`);
  console.log(`📈 Total Requests: ${stats.totalRequests}`);
  console.log(`✅ Successful: ${stats.successfulRequests}`);
  console.log(`❌ Failed: ${stats.failedRequests}`);
  console.log(`🚀 Throughput: ${(stats.totalRequests / totalTime).toFixed(2)} req/s`);
  console.log(`👥 Sessions: ${stats.sessionCount}`);
  console.log(`Avg Latency: ${avgLatency}ms`);
  console.log(`P95: ${percentile(stats.latencies, 95).toFixed(2)}ms`);
  console.log(`P99: ${percentile(stats.latencies, 99).toFixed(2)}ms`);
  console.log("✅ Simulation complete.\n");
})();
