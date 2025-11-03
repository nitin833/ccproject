const express = require('express');
const axios = require('axios');
const murmurhash3_32_gc = require('murmurhash-js/murmurhash3_gc');
const fs = require('fs');

// Server Request Tracking
const STATS_FILE = './server_stats.json';
let serverRequestStats = {};

function saveServerStats() {
    fs.writeFileSync(STATS_FILE, JSON.stringify(serverRequestStats, null, 2));
}

// Logger utility
class Logger {
    static info(message, data = {}) {
        console.log(`[INFO] ${message}`, data);
    }

    static error(message, error = {}) {
        console.error(`[ERROR] ${message}`, error.message || error);
    }

    static warn(message, data = {}) {
        console.log(`[WARN] ${message}`, data);
    }

    static debug(message, data = {}) {
        if (process.env.DEBUG) {
            console.log(`[DEBUG] ${message}`, data);
        }
    }
}

// Dynamic Weight Consistent Hashing Ring
class DynamicWeightHashRing {
    constructor() {
        this.ring = [];
        this.vnodeToServer = new Map();
        this.serverWeights = new Map();
        Logger.info('DynamicWeightHashRing initialized');
    }

    getHash(key, seed = 42) {
        return murmurhash3_32_gc(key, seed);
    }

    addServer(serverIp, weight, vnodes) {
        Logger.info(`Adding server: ${serverIp} (weight: ${weight}, vnodes: ${vnodes})`);
        
        this.serverWeights.set(serverIp, weight);
        
        for (let i = 0; i < vnodes; i++) {
            const vnodeKey = `${serverIp}#${i}`;
            const hash = this.getHash(vnodeKey);
            this.ring.push(hash);
            this.vnodeToServer.set(hash, serverIp);
        }
        
        this.ring.sort((a, b) => a - b);
    }

    removeServer(serverIp) {
        Logger.info(`Removing server: ${serverIp}`);
        
        this.ring = this.ring.filter(hash => {
            if (this.vnodeToServer.get(hash) === serverIp) {
                this.vnodeToServer.delete(hash);
                return false;
            }
            return true;
        });
        
        this.serverWeights.delete(serverIp);
    }

    rebuild(servers) {
        Logger.info('Rebuilding hash ring with dynamic weights');
        
        this.ring = [];
        this.vnodeToServer.clear();
        this.serverWeights.clear();

        for (const [ip, serverData] of Object.entries(servers)) {
            if (serverData.vnodes > 0 && serverData.status === 'healthy') {
                this.addServer(ip, serverData.weight, serverData.vnodes);
            }
        }
        
        Logger.info(`Ring rebuilt: ${this.ring.length} vnodes, ${Object.keys(servers).length} servers`);
    }

    getServerForKey(key) {
        if (this.ring.length === 0) {
            Logger.warn('Ring is empty, cannot route request');
            return null;
        }

        const hash = this.getHash(key);
        
        for (const point of this.ring) {
            if (hash <= point) {
                return this.vnodeToServer.get(point);
            }
        }
        
        return this.vnodeToServer.get(this.ring[0]);
    }

    getStats() {
        const serverDistribution = {};
        for (const [hash, ip] of this.vnodeToServer.entries()) {
            serverDistribution[ip] = (serverDistribution[ip] || 0) + 1;
        }
        return {
            totalVnodes: this.ring.length,
            serverCount: this.serverWeights.size,
            distribution: serverDistribution,
            weights: Object.fromEntries(this.serverWeights)
        };
    }
}

// Server Registry
class ServerRegistry {
    constructor() {
        this.servers = {};
        Logger.info('ServerRegistry initialized');
    }

    register(ip, initialWeight = 1.0, initialVnodes = 10) {
        if (!ip) {
            throw new Error('Server IP is required');
        }

        this.servers[ip] = {
            ip,
            weight: initialWeight,
            vnodes: initialVnodes,
            metrics: {
                cpu: 0,
                memory: 0,
                latency: 0,
                responseTime: 0
            },
            status: 'healthy',
            lastUpdated: Date.now(),
            failureCount: 0,
            requestCount: 0,
            totalResponseTime: 0
        };

        Logger.info(`Server registered: ${ip}`);
        return this.servers[ip];
    }

    deregister(ip) {
        if (!ip || !this.servers[ip]) {
            throw new Error(`Server ${ip} not found`);
        }

        delete this.servers[ip];
        Logger.info(`Server deregistered: ${ip}`);
    }

    get(ip) {
        return this.servers[ip];
    }

    getAll() {
        return this.servers;
    }

    updateMetrics(ip, metrics) {
        if (!this.servers[ip]) {
            Logger.warn(`Unknown server: ${ip}`);
            return false;
        }

        this.servers[ip].metrics = { ...metrics };
        this.servers[ip].lastUpdated = Date.now();
        this.servers[ip].status = 'healthy';
        this.servers[ip].failureCount = 0;

        return true;
    }

    markFailure(ip) {
        if (!this.servers[ip]) return;

        this.servers[ip].failureCount++;
        this.servers[ip].lastUpdated = Date.now();

        if (this.servers[ip].failureCount >= 3) {
            this.servers[ip].status = 'unhealthy';
            this.servers[ip].vnodes = 0;
            Logger.error(`Server ${ip} marked unhealthy (${this.servers[ip].failureCount} failures)`);
        } else {
            Logger.warn(`Server ${ip} health check failed (${this.servers[ip].failureCount}/3)`);
        }
    }

    updateRequestStats(ip, responseTime) {
        if (!this.servers[ip]) return;
        
        this.servers[ip].requestCount++;
        this.servers[ip].totalResponseTime += responseTime;
    }
}

// Heuristic Weight Optimizer (based on paper's approach)
class HeuristicWeightOptimizer {
    constructor(config = {}) {
        // Weight factors for different metrics
        this.alpha = config.alpha || 0.3;  // CPU weight
        this.beta = config.beta || 0.3;    // Memory weight
        this.gamma = config.gamma || 0.2;  // Latency weight
        this.delta = config.delta || 0.2;  // Response time weight
        
        // Vnode configuration
        this.vnodeConfig = {
            min: config.minVnodes || 5,
            max: config.maxVnodes || 50,
            base: config.baseVnodes || 20
        };

        // Optimization parameters
        this.targetLoadBalance = config.targetLoadBalance || 0.1; // 10% deviation acceptable
        this.weightAdjustmentRate = config.weightAdjustmentRate || 0.15;

        Logger.info('HeuristicWeightOptimizer initialized', {
            weights: { alpha: this.alpha, beta: this.beta, gamma: this.gamma, delta: this.delta },
            vnodes: this.vnodeConfig
        });
    }

    /**
     * Calculate comprehensive load score based on multiple metrics
     * Lower score = better performance (less loaded)
     */
    calculateLoadScore(metrics) {
        const { cpu, memory, latency, responseTime } = metrics;

        // Normalize all metrics to 0-1 range (already normalized from server)
        const normalizedCpu = Math.min(Math.max(cpu, 0), 1);
        const normalizedMemory = Math.min(Math.max(memory, 0), 1);
        const normalizedLatency = Math.min(Math.max(latency, 0), 1);
        const normalizedResponseTime = Math.min(Math.max(responseTime || 0, 0), 1);

        // Weighted sum of all factors
        const loadScore = 
            (this.alpha * normalizedCpu) +
            (this.beta * normalizedMemory) +
            (this.gamma * normalizedLatency) +
            (this.delta * normalizedResponseTime);

        return Math.min(Math.max(loadScore, 0), 1);
    }

    /**
     * Calculate dynamic weight using inverse load principle
     * Higher load → Lower weight → Fewer vnodes → Less traffic
     */
    calculateDynamicWeight(loadScore) {
        // Inverse relationship: low load = high weight
        // Using exponential decay for more aggressive weight reduction under high load
        const baseWeight = 1.0;
        const weight = baseWeight * Math.exp(-2 * loadScore);
        
        return Math.max(0.1, Math.min(2.0, weight)); // Clamp between 0.1 and 2.0
    }

    /**
     * Calculate vnodes based on weight
     * Higher weight → More vnodes → More traffic capacity
     */
    calculateVnodes(weight) {
        const { min, max, base } = this.vnodeConfig;
        
        // Linear scaling based on weight
        const vnodes = Math.floor(base * weight);
        
        return Math.max(min, Math.min(max, vnodes));
    }

    /**
     * Balance load across servers by adjusting weights
     * Implements heuristic optimization from the paper
     */
    optimizeWeights(servers) {
        const serverArray = Object.values(servers);
        
        if (serverArray.length === 0) return;

        // Calculate average load across all healthy servers
        const healthyServers = serverArray.filter(s => s.status === 'healthy');
        if (healthyServers.length === 0) return;

        const avgLoad = healthyServers.reduce((sum, s) => {
            return sum + this.calculateLoadScore(s.metrics);
        }, 0) / healthyServers.length;

        Logger.debug(`Average load across servers: ${avgLoad.toFixed(4)}`);

        // Adjust weights to balance load
        for (const server of healthyServers) {
            const currentLoad = this.calculateLoadScore(server.metrics);
            const loadDeviation = currentLoad - avgLoad;

            // If server is overloaded compared to average, reduce its weight
            if (Math.abs(loadDeviation) > this.targetLoadBalance) {
                const adjustment = -loadDeviation * this.weightAdjustmentRate;
                const oldWeight = server.weight;
                server.weight = Math.max(0.1, Math.min(2.0, server.weight + adjustment));
                
                Logger.debug(`Weight adjusted for ${server.ip}: ${oldWeight.toFixed(4)} → ${server.weight.toFixed(4)} (load: ${currentLoad.toFixed(4)})`);
            }
        }
    }

    /**
     * Update server configuration based on current metrics
     */
    updateServer(server) {
        const loadScore = this.calculateLoadScore(server.metrics);
        const newWeight = this.calculateDynamicWeight(loadScore);
        const newVnodes = this.calculateVnodes(newWeight);
        
        const oldWeight = server.weight;
        const oldVnodes = server.vnodes;
        
        server.weight = newWeight;
        server.vnodes = newVnodes;
        server.loadScore = loadScore;

        if (Math.abs(oldWeight - newWeight) > 0.05 || oldVnodes !== newVnodes) {
            Logger.info(`Server ${server.ip} updated: load=${loadScore.toFixed(4)}, weight=${oldWeight.toFixed(4)}→${newWeight.toFixed(4)}, vnodes=${oldVnodes}→${newVnodes}`);
        }

        return { weight: newWeight, vnodes: newVnodes, loadScore };
    }
}

// Main Load Balancer
class DynamicWeightLoadBalancer {
    constructor(config = {}) {
        this.registry = new ServerRegistry();
        this.hashRing = new DynamicWeightHashRing();
        this.optimizer = new HeuristicWeightOptimizer(config);
        
        this.config = {
            pollInterval: config.pollInterval || 20000,
            metricsPath: config.metricsPath || '/current-metrics',
            optimizationInterval: config.optimizationInterval || 60000,
            port: config.port || 3000
        };

        this.pollIntervalId = null;
        this.optimizationIntervalId = null;
        
        Logger.info('DynamicWeightLoadBalancer initialized', this.config);
    }

    async startPolling() {
        Logger.info('Starting metrics polling and optimization');
        
        const poll = async () => {
            const servers = this.registry.getAll();
            const serverIps = Object.keys(servers);

            if (serverIps.length === 0) {
                return;
            }

            const promises = serverIps.map(ip => this.fetchMetrics(ip));
            await Promise.allSettled(promises);

            // Update server weights and vnodes based on current metrics
            for (const ip of serverIps) {
                const server = this.registry.get(ip);
                if (server && server.status === 'healthy') {
                    this.optimizer.updateServer(server);
                }
            }

            this.hashRing.rebuild(this.registry.getAll());
            
            const stats = this.hashRing.getStats();
            Logger.info('Polling cycle complete', { 
                servers: serverIps.length, 
                vnodes: stats.totalVnodes 
            });
        };

        // Start optimization cycle (runs less frequently)
        const optimize = () => {
            const servers = this.registry.getAll();
            this.optimizer.optimizeWeights(servers);
            this.hashRing.rebuild(servers);
            Logger.info('Optimization cycle complete');
        };

        await poll();
        this.pollIntervalId = setInterval(poll, this.config.pollInterval);
        this.optimizationIntervalId = setInterval(optimize, this.config.optimizationInterval);
    }

    async fetchMetrics(ip) {
        try {
            const metricsUrl = `http://${ip}:3000${this.config.metricsPath}`;
            const { data } = await axios.get(metricsUrl, { timeout: 5000 });
            const rawMetrics = typeof data === 'string' ? JSON.parse(data) : data;

            // Use normalized metrics from server
            const metrics = {
                cpu: parseFloat(rawMetrics.normalized?.cpu || rawMetrics.cpu_usage_percent / 100),
                memory: parseFloat(rawMetrics.normalized?.memory || rawMetrics.memory_usage_percent / 100),
                latency: parseFloat(rawMetrics.normalized?.latency || rawMetrics.network_latency_ms / 1000),
                responseTime: parseFloat(rawMetrics.normalized?.response_time || 0)
            };

            if (isNaN(metrics.cpu) || isNaN(metrics.memory) || isNaN(metrics.latency)) {
                Logger.warn(`Invalid metrics from ${ip}`);
                return;
            }

            this.registry.updateMetrics(ip, metrics);

        } catch (error) {
            Logger.error(`Failed to fetch metrics from ${ip}`, error);
            this.registry.markFailure(ip);
        }
    }

    stopPolling() {
        if (this.pollIntervalId) {
            clearInterval(this.pollIntervalId);
        }
        if (this.optimizationIntervalId) {
            clearInterval(this.optimizationIntervalId);
        }
        Logger.info('Polling and optimization stopped');
    }

    registerServer(ip, initialWeight = 1.0, initialVnodes = 20) {
        const server = this.registry.register(ip, initialWeight, initialVnodes);
        this.hashRing.rebuild(this.registry.getAll());
        return server;
    }

    deregisterServer(ip) {
        this.registry.deregister(ip);
        this.hashRing.rebuild(this.registry.getAll());
    }

    getServerForUser(userId) {
        return this.hashRing.getServerForKey(userId);
    }

    getStatus() {
        const servers = this.registry.getAll();
        const serverStats = {};
        
        for (const [ip, server] of Object.entries(servers)) {
            serverStats[ip] = {
                weight: server.weight.toFixed(4),
                vnodes: server.vnodes,
                loadScore: server.loadScore?.toFixed(4) || 'N/A',
                status: server.status,
                metrics: server.metrics,
                requests: server.requestCount,
                avgResponseTime: server.requestCount > 0 
                    ? (server.totalResponseTime / server.requestCount).toFixed(2) + 'ms'
                    : 'N/A'
            };
        }

        return {
            servers: serverStats,
            ringStats: this.hashRing.getStats(),
            config: this.config,
            optimizer: {
                alpha: this.optimizer.alpha,
                beta: this.optimizer.beta,
                gamma: this.optimizer.gamma,
                delta: this.optimizer.delta
            }
        };
    }
}

// Express App Setup
const app = express();
app.use(express.json());

const loadBalancer = new DynamicWeightLoadBalancer({
    pollInterval: 20000,              // Poll metrics every 20s
    optimizationInterval: 60000,      // Optimize weights every 60s
    metricsPath: '/current-metrics',
    alpha: 0.3,    // CPU weight factor
    beta: 0.3,     // Memory weight factor
    gamma: 0.2,    // Latency weight factor
    delta: 0.2,    // Response time weight factor
    minVnodes: 5,
    maxVnodes: 50,
    baseVnodes: 20,
    targetLoadBalance: 0.1,
    weightAdjustmentRate: 0.15
});

// API Routes
app.post('/register', (req, res) => {
    try {
        const { ip, weight, vnodes } = req.body;
        
        if (!ip) {
            return res.status(400).json({ error: 'IP address required' });
        }

        const server = loadBalancer.registerServer(ip, weight, vnodes);
        Logger.info(`Server registered: ${ip}`);
        
        res.json({ 
            status: 'ok', 
            message: 'Server registered',
            server 
        });
    } catch (error) {
        Logger.error('Registration failed', error);
        res.status(500).json({ error: error.message });
    }
});

app.post('/deregister', (req, res) => {
    try {
        const { ip } = req.body;
        
        if (!ip) {
            return res.status(400).json({ error: 'IP address required' });
        }

        loadBalancer.deregisterServer(ip);
        Logger.info(`Server deregistered: ${ip}`);
        
        res.json({ 
            status: 'ok', 
            message: 'Server deregistered' 
        });
    } catch (error) {
        Logger.error('Deregistration failed', error);
        res.status(404).json({ error: error.message });
    }
});

app.get('/status', (req, res) => {
    try {
        const status = loadBalancer.getStatus();
        res.json(status);
    } catch (error) {
        Logger.error('Failed to get status', error);
        res.status(500).json({ error: error.message });
    }
});

// Proxy middleware with username-based routing
app.use(async (req, res) => {
    try {
        const username = req.body?.username || req.query?.username || req.ip;
        const hash = murmurhash3_32_gc(username, 42);

        Logger.info(`${req.method} ${req.originalUrl} for user: ${username} (hash: ${hash})`);

        const serverIp = loadBalancer.getServerForUser(username);

        if (!serverIp) {
            Logger.warn('No backend server available');
            return res.status(503).json({ error: 'No backend servers available' });
        }

        if (!serverRequestStats[serverIp]) {
            serverRequestStats[serverIp] = { count: 0, totalTime: 0 };
        }

        const startTime = Date.now();
        const backendUrl = `http://${serverIp}:3000${req.originalUrl}`;

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

        const responseTime = Date.now() - startTime;

        serverRequestStats[serverIp].count += 1;
        serverRequestStats[serverIp].totalTime += responseTime;
        saveServerStats();

        // Update server request stats in registry
        loadBalancer.registry.updateRequestStats(serverIp, responseTime);

        Logger.info(`✓ ${req.method} ${req.originalUrl} → ${serverIp} (${response.status}) ${responseTime}ms`);
        res.status(response.status).send(response.data);

    } catch (error) {
        if (error.response) {
            res.status(error.response.status).send(error.response.data);
        } else if (error.code === 'ECONNREFUSED') {
            res.status(502).json({ error: 'Backend server unavailable' });
        } else if (error.code === 'ETIMEDOUT') {
            res.status(504).json({ error: 'Backend timeout' });
        } else {
            Logger.error('Proxy error', error);
            res.status(502).json({ error: 'Upstream error' });
        }
    }
});

// Start server
const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
    Logger.info(`Dynamic Weight Load Balancer running on port ${PORT}`);
    loadBalancer.startPolling();
});

// Graceful shutdown
process.on('SIGTERM', () => {
    Logger.info('SIGTERM - shutting down gracefully');
    loadBalancer.stopPolling();
    process.exit(0);
});

process.on('SIGINT', () => {
    Logger.info('SIGINT - shutting down gracefully');
    loadBalancer.stopPolling();
    process.exit(0);
});