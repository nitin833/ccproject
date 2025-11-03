/**
 * round_robin_lb.js
 * Simple load balancer using Round Robin algorithm
 * Tracks per-server request counts in server_stats.json
 */

const express = require('express');
const axios = require('axios');
const fs = require('fs');

// ====== Persistent Stats ======
const STATS_FILE = './server_stats.json';
let serverRequestStats = {};

function saveServerStats() {
  fs.writeFileSync(STATS_FILE, JSON.stringify(serverRequestStats, null, 2));
}

// ====== Logger Utility ======
class Logger {
  static info(msg, data = {}) {
    const logEntry = {
      timestamp: new Date().toISOString(),
      level: 'INFO',
      message: msg,
      data
    };
    fs.appendFileSync('./logs.json', JSON.stringify(logEntry) + '\n');
  }
  static warn(msg, data = {}) {
    const logEntry = {
      timestamp: new Date().toISOString(),
      level: 'WARN',
      message: msg,
      data
    };
    fs.appendFileSync('./logs.json', JSON.stringify(logEntry) + '\n');
  }
  static error(msg, error = {}) {
    const logEntry = {
      timestamp: new Date().toISOString(),
      level: 'ERROR',
      message: msg,
      error: error.message || error
    };
    fs.appendFileSync('./logs.json', JSON.stringify(logEntry) + '\n');
  }
  static debug(msg, data = {}) {
    const logEntry = {
      timestamp: new Date().toISOString(),
      level: 'DEBUG',
      message: msg,
      data
    };
    fs.appendFileSync('./logs.json', JSON.stringify(logEntry) + '\n');
  }
}

// ====== Server Registry ======
class ServerRegistry {
  constructor() {
    this.servers = [];
    Logger.info('ServerRegistry initialized');
  }

  register(ip, initialWeight = 1) {
    if (!ip) throw new Error('Server IP is required');

    const existing = this.servers.find(s => s.ip === ip);
    if (existing) {
      Logger.warn(`Server ${ip} already registered`);
      return existing;
    }

    const server = {
      ip,
      weight: initialWeight,
      metrics: { cpu: 0, memory: 0, latency: 0 },
      status: 'healthy',
      failureCount: 0,
      lastUpdated: Date.now()
    };

    this.servers.push(server);
    Logger.info(`Server registered: ${ip}`);
    return server;
  }

  deregister(ip) {
    const idx = this.servers.findIndex(s => s.ip === ip);
    if (idx === -1) throw new Error(`Server ${ip} not found`);
    this.servers.splice(idx, 1);
    Logger.info(`Server deregistered: ${ip}`);
  }

  getAll() {
    return this.servers;
  }

  markFailure(ip) {
    const s = this.servers.find(s => s.ip === ip);
    if (!s) return;
    s.failureCount++;
    s.lastUpdated = Date.now();

    if (s.failureCount >= 3) {
      s.status = 'unhealthy';
      Logger.error(`Server ${ip} marked unhealthy`);
    } else {
      Logger.warn(`Server ${ip} failed (${s.failureCount}/3)`);
    }
  }

  updateMetrics(ip, metrics) {
    const s = this.servers.find(s => s.ip === ip);
    if (!s) return;
    s.metrics = metrics;
    s.lastUpdated = Date.now();
    s.status = 'healthy';
    s.failureCount = 0;
    Logger.debug(`Updated metrics for ${ip}`, metrics);
  }
}

// ====== Round Robin Selector ======
class RoundRobinSelector {
  constructor() {
    this.index = 0;
  }

  getNextServer(servers) {
    if (!servers || servers.length === 0) return null;

    const healthyServers = servers.filter(s => s.status === 'healthy');
    if (healthyServers.length === 0) return null;

    const server = healthyServers[this.index % healthyServers.length];
    this.index = (this.index + 1) % healthyServers.length;
    return server.ip;
  }
}

// ====== LoadBalancer (Round Robin) ======
class LoadBalancer {
  constructor(config = {}) {
    this.registry = new ServerRegistry();
    this.selector = new RoundRobinSelector();

    this.config = {
      pollInterval: config.pollInterval || 20000,
      metricsPath: config.metricsPath || '/current-metrics'
    };

    this.pollIntervalId = null;
    Logger.info('Round Robin LoadBalancer initialized', this.config);
  }

  async startPolling() {
    Logger.info('Starting metrics polling');

    const poll = async () => {
      const servers = this.registry.getAll();
      if (servers.length === 0) return;

      Logger.debug(`Polling ${servers.length} servers`);
      const promises = servers.map(s => this.fetchMetrics(s.ip));
      await Promise.allSettled(promises);

      Logger.info('Polling cycle complete');
    };

    await poll();
    this.pollIntervalId = setInterval(poll, this.config.pollInterval);
  }

  stopPolling() {
    if (this.pollIntervalId) clearInterval(this.pollIntervalId);
    Logger.info('Stopped polling');
  }

  async fetchMetrics(ip) {
    try {
      const url = `http://${ip}:3000${this.config.metricsPath}`;
      const { data } = await axios.get(url, { timeout: 5000 });
      const raw = typeof data === 'string' ? JSON.parse(data) : data;

      const metrics = {
        cpu: parseFloat(raw.cpu_usage_percent) / 100,
        memory: parseFloat(raw.memory_usage_percent) / 100,
        latency: parseFloat(raw.network_latency_ms) / 1000
      };

      this.registry.updateMetrics(ip, metrics);
    } catch (err) {
      Logger.error(`Metrics fetch failed from ${ip}`, err);
      this.registry.markFailure(ip);
    }
  }

  registerServer(ip, weight = 1) {
    const s = this.registry.register(ip, weight);
    return s;
  }

  deregisterServer(ip) {
    this.registry.deregister(ip);
  }

  getNextServer() {
    const all = this.registry.getAll();
    return this.selector.getNextServer(all);
  }

  getStatus() {
    return {
      servers: this.registry.getAll(),
      config: this.config
    };
  }
}

// ====== Express Setup ======
const app = express();
app.use(express.json());

const loadBalancer = new LoadBalancer({
  pollInterval: 20000,
  metricsPath: '/current-metrics'
});

// Register server
app.post('/register', (req, res) => {
  try {
    const { ip } = req.body;
    if (!ip) return res.status(400).json({ error: 'IP required' });

    const server = loadBalancer.registerServer(ip);
    res.json({ status: 'ok', server });
  } catch (err) {
    Logger.error('Register failed', err);
    res.status(500).json({ error: err.message });
  }
});

// Deregister server
app.post('/deregister', (req, res) => {
  try {
    const { ip } = req.body;
    loadBalancer.deregisterServer(ip);
    res.json({ status: 'ok' });
  } catch (err) {
    Logger.error('Deregister failed', err);
    res.status(404).json({ error: err.message });
  }
});

// Status
app.get('/status', (req, res) => {
  res.json(loadBalancer.getStatus());
});

// Proxy Middleware
/*
app.use(async (req, res) => {
  try {
    const userId = req.header('x-user-id') || req.ip;
    Logger.info(`Incoming request for ${userId} ${req.method} ${req.originalUrl}`);

    const serverIp = loadBalancer.getNextServer();
    if (!serverIp) {
      Logger.warn('No healthy servers available');
      return res.status(503).json({ error: 'No backend servers available' });
    }

    // Tracking
    serverRequestStats[serverIp] = (serverRequestStats[serverIp] || 0) + 1;
    saveServerStats();
    Logger.info(`Request count updated for ${serverIp}: ${serverRequestStats[serverIp]}`);

    const backendUrl = `http://${serverIp}:3000${req.originalUrl}`;
    Logger.debug(`Proxying to ${backendUrl}`);

    const response = await axios({
      method: req.method,
      url: backendUrl,
      data: req.body,
      headers: { ...req.headers, host: serverIp },
      timeout: 30000
    });

    res.status(response.status).send(response.data);
  } catch (error) {
    if (error.response) {
      Logger.error(`Upstream error ${error.response.status}`, error);
      res.status(error.response.status).send(error.response.data);
    } else if (error.code === 'ECONNREFUSED') {
      Logger.error('Connection refused', error);
      res.status(502).json({ error: 'Backend unavailable' });
    } else if (error.code === 'ETIMEDOUT') {
      Logger.error('Timeout', error);
      res.status(504).json({ error: 'Backend timeout' });
    } else {
      Logger.error('Proxy error', error);
      res.status(502).json({ error: 'Proxy error' });
    }
  }
});*/



//proxy with body 
app.use(async (req, res) => {
    try {
        // ✅ Extract username from body (POST, PUT, etc.) or query (GET)
        const username = req.body?.username || req.query?.username || req.ip;

        Logger.info(`Incoming request: ${req.method} ${req.originalUrl} for user: ${username}`, { username });

        const serverIp = loadBalancer.getNextServer();

        if (!serverIp) {
            Logger.warn('No backend server available');
            return res.status(503).json({ error: 'No backend servers available' });
        }

        // ✅ Track requests per server
        if (!serverRequestStats[serverIp]) {
            serverRequestStats[serverIp] = { count: 0, totalTime: 0 };
        }

        const startTime = Date.now();

        // ✅ Forward the request to the backend server
        const backendUrl = `http://${serverIp}:3000${req.originalUrl}`;
        Logger.debug(`Proxying request to: ${backendUrl}`);

        const response = await axios({
            method: req.method,
            url: backendUrl,
            data: req.body,
            headers: {
                ...req.headers,
                host: serverIp
            },
            timeout: 30000
        });

        const endTime = Date.now();
        const responseTime = endTime - startTime;

        serverRequestStats[serverIp].count += 1;
        serverRequestStats[serverIp].totalTime += responseTime;
        saveServerStats();

        Logger.info(`Request successful: ${req.method} ${req.originalUrl} -> ${serverIp} (${response.status}) in ${responseTime}ms`);
        Logger.info(`✅ Updated request count for ${serverIp}: ${serverRequestStats[serverIp].count}, avg time: ${(serverRequestStats[serverIp].totalTime / serverRequestStats[serverIp].count).toFixed(2)}ms`);
        res.status(response.status).send(response.data);

    } catch (error) {
        if (error.response) {
            Logger.error(`Upstream error from backend: ${error.response.status}`, error);
            res.status(error.response.status).send(error.response.data);
        } else if (error.code === 'ECONNREFUSED') {
            Logger.error('Backend connection refused', error);
            res.status(502).json({ error: 'Backend server unavailable' });
        } else if (error.code === 'ETIMEDOUT') {
            Logger.error('Backend request timeout', error);
            res.status(504).json({ error: 'Backend request timeout' });
        } else {
            Logger.error('Proxy error', error);
            res.status(502).json({ error: 'Upstream error' });
        }
    }
});


// ====== Start Server ======
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  Logger.info(`Round Robin Load Balancer running on port ${PORT}`);
  loadBalancer.startPolling();
});

// ====== Graceful Shutdown ======
process.on('SIGTERM', () => {
  Logger.info('SIGTERM received. Shutting down.');
  loadBalancer.stopPolling();
  process.exit(0);
});

process.on('SIGINT', () => {
  Logger.info('SIGINT received. Shutting down.');
  loadBalancer.stopPolling();
  process.exit(0);
});
