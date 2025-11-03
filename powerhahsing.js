const express = require('express');
const axios = require('axios');
const crypto = require('crypto');
const fs = require('fs');

// Server Request Tracking
const STATS_FILE = './server_stats.json';
let serverRequestStats = {};

function saveServerStats() {
    try {
        fs.writeFileSync(STATS_FILE, JSON.stringify(serverRequestStats, null, 2));
    } catch (error) {
        Logger.error('Failed to save server stats', error);
    }
}

// Load stats on startup
try {
    if (fs.existsSync(STATS_FILE)) {
        serverRequestStats = JSON.parse(fs.readFileSync(STATS_FILE, 'utf8'));
    }
} catch (error) {
    Logger.error('Failed to load server stats', error);
}

// Enhanced Logger utility
class Logger {
    static info(message, data = {}) {
        const timestamp = new Date().toISOString();
        console.log(`[${timestamp}] [INFO] ${message}`, Object.keys(data).length > 0 ? data : '');
    }

    static error(message, error = {}) {
        const timestamp = new Date().toISOString();
        console.error(`[${timestamp}] [ERROR] ${message}`, error.message || error);
    }

    static warn(message, data = {}) {
        const timestamp = new Date().toISOString();
        console.log(`[${timestamp}] [WARN] ${message}`, Object.keys(data).length > 0 ? data : '');
    }

    static debug(message, data = {}) {
        if (process.env.DEBUG) {
            const timestamp = new Date().toISOString();
            console.log(`[${timestamp}] [DEBUG] ${message}`, Object.keys(data).length > 0 ? data : '');
        }
    }

    static success(message, data = {}) {
        const timestamp = new Date().toISOString();
        console.log(`[${timestamp}] [✓] ${message}`, Object.keys(data).length > 0 ? data : '');
    }
}

// MurmurHash3 implementation (32-bit)
function murmurHash3(key, seed = 0) {
    const str = String(key);
    let h = seed;
    for (let i = 0; i < str.length; i++) {
        h = Math.imul(h ^ str.charCodeAt(i), 2654435761);
    }
    h = (h ^ (h >>> 16)) >>> 0;
    return h;
}

// ============================================================================
// POWER CONSISTENT HASHING (O(1) Expected Time)
// Based on "Fast Consistent Hashing in Constant Time" (Eric Leu, 2023)
// ============================================================================
class PowerConsistentHashRing {
    constructor() {
        this.servers = []; // Array of server IPs
        this.serverToIndex = new Map(); // IP -> index mapping
        this.serverWeights = new Map(); // IP -> weight mapping
        this.virtualServerArray = []; // Weighted virtual server array
        Logger.info('PowerConsistentHashRing initialized (O(1) lookup)');
    }

    // Find smallest power of 2 >= n
    nextPowerOf2(n) {
        if (n <= 0) return 1;
        let m = 1;
        while (m < n) m *= 2;
        return m;
    }

    // Auxiliary function f(key, m): maps key uniformly to [0, m-1]
    // m must be a power of 2
    f(key, m) {
        const hash = murmurHash3(key, 42);
        const kBits = hash & (m - 1);
        
        if (kBits === 0) return 0;

        // Find position of most significant bit set to 1
        let j = 0;
        let temp = kBits;
        while (temp > 1) {
            temp = temp >> 1;
            j++;
        }

        const h = 1 << j; // 2^j
        const rand = murmurHash3(key, j) & (h - 1);
        return h + rand;
    }

    // Auxiliary function g(key, n, s): weighted distribution
    // Returns value in [s, n-1] with specific probability distribution
    g(key, n, s) {
        let x = s;
        let iteration = 0;
        const maxIterations = 10; // Safety limit
        
        while (iteration < maxIterations) {
            const u = (murmurHash3(key, 1000 + iteration) % 10000) / 10000;
            
            // Find smallest j where u > (x+1)/(j+1)
            let r = x + 1;
            while (r < n && u <= (x + 1) / (r + 1)) {
                r++;
            }
            
            if (r >= n) break;
            x = r;
            iteration++;
        }
        
        return x;
    }

    // Main Power Consistent Hash function - O(1) expected time
    powerHash(key, n) {
        if (n <= 0) return 0;
        
        const m = this.nextPowerOf2(n);
        const halfM = Math.floor(m / 2);
        
        // Step 1: Apply f(key, m)
        const r1 = this.f(key, m);
        
        if (r1 < n) {
            return r1; // Direct hit
        }
        
        // Step 2: Apply g(key, n, m/2 - 1)
        const r2 = this.g(key, n, halfM - 1);
        
        if (r2 > halfM - 1) {
            return r2;
        }
        
        // Step 3: Apply f(key, m/2)
        return this.f(key, halfM);
    }

    // Add server with weight-based virtual nodes
    addServer(serverIp, weight, vnodes) {
        Logger.info(`Adding server: ${serverIp} (weight: ${weight.toFixed(3)}, vnodes: ${vnodes})`);
        Logger.info(`[VNODE_CHANGE] Assigned ${vnodes} virtual nodes to server ${serverIp}`);

        if (!this.servers.includes(serverIp)) {
            this.servers.push(serverIp);
            this.serverToIndex.set(serverIp, this.servers.length - 1);
        }

        this.serverWeights.set(serverIp, { weight, vnodes });
        this.rebuildVirtualArray();
    }

    // Remove server
    removeServer(serverIp) {
        Logger.info(`Removing server: ${serverIp}`);
        Logger.info(`[VNODE_CHANGE] Removed all virtual nodes from server ${serverIp}`);

        const index = this.servers.indexOf(serverIp);
        if (index !== -1) {
            this.servers.splice(index, 1);
            this.serverToIndex.delete(serverIp);
            this.serverWeights.delete(serverIp);

            // Rebuild index mapping
            this.serverToIndex.clear();
            this.servers.forEach((ip, idx) => {
                this.serverToIndex.set(ip, idx);
            });

            this.rebuildVirtualArray();
        }
    }

    // Rebuild virtual server array based on weights (vnodes)
    rebuildVirtualArray() {
        Logger.info(`[SERVER_BUILDING] Rebuilding virtual array with ${this.serverWeights.size} servers`);
        this.virtualServerArray = [];

        for (const [serverIp, data] of this.serverWeights.entries()) {
            // Add server multiple times based on vnodes (proportional to weight)
            for (let i = 0; i < data.vnodes; i++) {
                this.virtualServerArray.push(serverIp);
            }
        }

        Logger.debug(`Virtual array rebuilt: ${this.virtualServerArray.length} slots for ${this.servers.length} servers`);
    }

    // Rebuild entire ring
    rebuild(servers) {
        Logger.info('Rebuilding Power CH ring with dynamic weights');
        
        this.servers = [];
        this.serverToIndex.clear();
        this.serverWeights.clear();
        this.virtualServerArray = [];

        for (const [ip, serverData] of Object.entries(servers)) {
            if (serverData.vnodes > 0 && serverData.status === 'healthy') {
                this.addServer(ip, serverData.weight, serverData.vnodes);
            }
        }
        
        Logger.success(`Ring rebuilt: ${this.virtualServerArray.length} virtual slots, ${this.servers.length} servers`);
    }

    // Get server for key using Power Consistent Hashing - O(1) expected
    getServerForKey(key) {
        if (this.virtualServerArray.length === 0) {
            Logger.warn('Virtual array is empty, cannot route request');
            return null;
        }

        // Use power consistent hash to get index in virtual array
        const bucketIdx = this.powerHash(key, this.virtualServerArray.length);
        const server = this.virtualServerArray[bucketIdx];
        
        Logger.debug(`Key "${key}" → bucket ${bucketIdx} → ${server}`);
        return server;
    }

    // Get statistics
    getStats() {
        const serverDistribution = {};
        for (const ip of this.virtualServerArray) {
            serverDistribution[ip] = (serverDistribution[ip] || 0) + 1;
        }
        
        const weights = {};
        for (const [ip, data] of this.serverWeights.entries()) {
            weights[ip] = data.weight;
        }
        
        return {
            algorithm: 'Power Consistent Hashing (O(1))',
            totalVirtualSlots: this.virtualServerArray.length,
            serverCount: this.servers.length,
            distribution: serverDistribution,
            weights: weights
        };
    }
}

// ============================================================================
// SERVER REGISTRY
// ============================================================================
class ServerRegistry {
    constructor() {
        this.servers = {};
        Logger.info('ServerRegistry initialized');
    }

    register(ip, initialWeight = 1.0, initialVnodes = 20) {
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
            totalResponseTime: 0,
            loadScore: 0
        };

        Logger.success(`Server registered: ${ip}`);
        return this.servers[ip];
    }

    deregister(ip) {
        if (!ip || !this.servers[ip]) {
            throw new Error(`Server ${ip} not found`);
        }

        delete this.servers[ip];
        Logger.success(`Server deregistered: ${ip}`);
    }

    get(ip) {
        return this.servers[ip];
    }

    getAll() {
        return this.servers;
    }

    getHealthyServers() {
        return Object.values(this.servers).filter(s => s.status === 'healthy');
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

        const failureThreshold = 3;
        if (this.servers[ip].failureCount >= failureThreshold) {
            this.servers[ip].status = 'unhealthy';
            this.servers[ip].vnodes = 0;
            Logger.error(`Server ${ip} marked UNHEALTHY (${this.servers[ip].failureCount} failures)`);
        } else {
            Logger.warn(`Server ${ip} health check failed (${this.servers[ip].failureCount}/${failureThreshold})`);
        }
    }

    updateRequestStats(ip, responseTime) {
        if (!this.servers[ip]) return;
        
        this.servers[ip].requestCount++;
        this.servers[ip].totalResponseTime += responseTime;
        
        // Update rolling average response time metric
        const avgResponseTime = this.servers[ip].totalResponseTime / this.servers[ip].requestCount;
        this.servers[ip].metrics.responseTime = Math.min(avgResponseTime / 1000, 1); // Normalize to 0-1
    }
}

// ============================================================================
// HEURISTIC WEIGHT OPTIMIZER (Hunter-Prey Optimization approach)
// ============================================================================
class HeuristicWeightOptimizer {
    constructor(config = {}) {
        // Weight factors for different metrics (from paper)
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
        this.targetLoadBalance = config.targetLoadBalance || 0.15; // 15% deviation acceptable
        this.weightAdjustmentRate = config.weightAdjustmentRate || 0.2;
        this.amplitudeFactor = config.amplitudeFactor || 0.2; // From dynamic weights paper

        Logger.info('HeuristicWeightOptimizer initialized', {
            weights: { 
                cpu: this.alpha, 
                memory: this.beta, 
                latency: this.gamma, 
                responseTime: this.delta 
            },
            vnodes: this.vnodeConfig
        });
    }

    /**
     * Calculate comprehensive performance score
     * Lower score = better performance (less loaded)
     */
    calculateLoadScore(metrics) {
        const { cpu, memory, latency, responseTime } = metrics;

        // Ensure all metrics are normalized to 0-1
        const normalizedCpu = Math.min(Math.max(cpu || 0, 0), 1);
        const normalizedMemory = Math.min(Math.max(memory || 0, 0), 1);
        const normalizedLatency = Math.min(Math.max(latency || 0, 0), 1);
        const normalizedResponseTime = Math.min(Math.max(responseTime || 0, 0), 1);

        // Weighted sum of all performance factors
        const loadScore = 
            (this.alpha * normalizedCpu) +
            (this.beta * normalizedMemory) +
            (this.gamma * normalizedLatency) +
            (this.delta * normalizedResponseTime);

        return Math.min(Math.max(loadScore, 0), 1);
    }

    /**
     * Calculate dynamic weight using inverse load principle
     * High load → Low weight → Few vnodes → Less traffic
     * Low load → High weight → Many vnodes → More traffic
     */
    calculateDynamicWeight(loadScore) {
        // Exponential decay: weight decreases exponentially with load
        const baseWeight = 1.0;
        const weight = baseWeight * Math.exp(-2.5 * loadScore);
        
        return Math.max(0.1, Math.min(2.0, weight)); // Clamp [0.1, 2.0]
    }

    /**
     * Calculate vnodes based on weight
     * More weight = more vnodes = more hash ring presence = more traffic
     */
    calculateVnodes(weight) {
        const { min, max, base } = this.vnodeConfig;
        
        // Linear scaling based on weight
        const vnodes = Math.round(base * weight);
        
        return Math.max(min, Math.min(max, vnodes));
    }

    /**
     * Hunter-Prey Optimization: Balance load across servers
     * Dynamically adjusts weights to achieve equilibrium
     */
    optimizeWeights(servers) {
        const serverArray = Object.values(servers);
        
        if (serverArray.length === 0) return;

        // Only optimize healthy servers
        const healthyServers = serverArray.filter(s => s.status === 'healthy');
        if (healthyServers.length === 0) return;

        // Calculate average load across all healthy servers
        const avgLoad = healthyServers.reduce((sum, s) => {
            return sum + this.calculateLoadScore(s.metrics);
        }, 0) / healthyServers.length;

        Logger.debug(`Average load: ${avgLoad.toFixed(4)}`);

        // Adjust weights to balance load (Hunter-Prey approach)
        for (const server of healthyServers) {
            const currentLoad = this.calculateLoadScore(server.metrics);
            const loadDeviation = currentLoad - avgLoad;

            // If deviation exceeds threshold, adjust weight
            if (Math.abs(loadDeviation) > this.targetLoadBalance) {
                // Amplitude factor controls how aggressively we adjust
                const adjustment = -loadDeviation * this.weightAdjustmentRate * this.amplitudeFactor;
                const oldWeight = server.weight;
                
                server.weight = Math.max(0.1, Math.min(2.0, server.weight + adjustment));
                server.vnodes = this.calculateVnodes(server.weight);
                
                Logger.debug(`Optimized ${server.ip}: weight ${oldWeight.toFixed(3)} → ${server.weight.toFixed(3)}, ` +
                            `load=${currentLoad.toFixed(3)}, deviation=${loadDeviation.toFixed(3)}`);
            }
        }
    }

    /**
     * Update individual server based on current metrics
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

        const significantChange = Math.abs(oldWeight - newWeight) > 0.05 || oldVnodes !== newVnodes;
        
        if (significantChange) {
            Logger.info(`Server ${server.ip} adjusted: ` +
                       `load=${loadScore.toFixed(3)}, ` +
                       `weight=${oldWeight.toFixed(3)}→${newWeight.toFixed(3)}, ` +
                       `vnodes=${oldVnodes}→${newVnodes}`);
        }

        return { weight: newWeight, vnodes: newVnodes, loadScore };
    }
}

// ============================================================================
// MAIN LOAD BALANCER
// ============================================================================
class DynamicWeightLoadBalancer {
    constructor(config = {}) {
        this.registry = new ServerRegistry();
        this.hashRing = new PowerConsistentHashRing(); // Using Power CH instead of traditional
        this.optimizer = new HeuristicWeightOptimizer(config);
        
        this.config = {
            pollInterval: config.pollInterval || 15000,           // 15s
            optimizationInterval: config.optimizationInterval || 45000, // 45s
            metricsPath: config.metricsPath || '/current-metrics',
            healthCheckPath: config.healthCheckPath || '/health',
            port: config.port || 3000,
            requestTimeout: config.requestTimeout || 30000
        };

        this.pollIntervalId = null;
        this.optimizationIntervalId = null;
        this.isPolling = false;
        
        Logger.success('DynamicWeightLoadBalancer initialized', {
            algorithm: 'Power Consistent Hashing (O(1))',
            ...this.config
        });
    }

    async startPolling() {
        if (this.isPolling) {
            Logger.warn('Polling already started');
            return;
        }

        Logger.info('Starting metrics polling and optimization');
        this.isPolling = true;
        
        // Metrics polling function
        const poll = async () => {
            const servers = this.registry.getAll();
            const serverIps = Object.keys(servers);

            if (serverIps.length === 0) {
                Logger.debug('No servers to poll');
                return;
            }

            // Fetch metrics from all servers in parallel
            const promises = serverIps.map(ip => this.fetchMetrics(ip));
            const results = await Promise.allSettled(promises);

            let successCount = 0;
            results.forEach((result, idx) => {
                if (result.status === 'fulfilled') successCount++;
            });

            // Update server weights and vnodes based on current metrics
            for (const ip of serverIps) {
                const server = this.registry.get(ip);
                if (server && server.status === 'healthy') {
                    this.optimizer.updateServer(server);
                }
            }

            // Rebuild hash ring with updated weights
            this.hashRing.rebuild(this.registry.getAll());
            
            const stats = this.hashRing.getStats();
            Logger.info(`Poll complete: ${successCount}/${serverIps.length} servers, ${stats.totalVirtualSlots} vnodes`);
        };

        // Optimization function (runs less frequently)
        const optimize = () => {
            const servers = this.registry.getAll();
            this.optimizer.optimizeWeights(servers);
            this.hashRing.rebuild(servers);
            Logger.success('Optimization cycle complete');
        };

        // Initial poll
        await poll();
        
        // Start intervals
        this.pollIntervalId = setInterval(poll, this.config.pollInterval);
        this.optimizationIntervalId = setInterval(optimize, this.config.optimizationInterval);
        
        Logger.success('Polling and optimization started');
    }

    async fetchMetrics(ip) {
        try {
            const metricsUrl = `http://${ip}:3000${this.config.metricsPath}`;
            const { data } = await axios.get(metricsUrl, { timeout: 5000 });
            
            const rawMetrics = typeof data === 'string' ? JSON.parse(data) : data;

            // Extract normalized metrics
            const metrics = {
                cpu: parseFloat(rawMetrics.normalized?.cpu || rawMetrics.cpu_usage_percent / 100 || 0),
                memory: parseFloat(rawMetrics.normalized?.memory || rawMetrics.memory_usage_percent / 100 || 0),
                latency: parseFloat(rawMetrics.normalized?.latency || rawMetrics.network_latency_ms / 1000 || 0),
                responseTime: parseFloat(rawMetrics.normalized?.response_time || 0)
            };

            // Validate metrics
            if (isNaN(metrics.cpu) || isNaN(metrics.memory) || isNaN(metrics.latency)) {
                Logger.warn(`Invalid metrics from ${ip}`, metrics);
                return;
            }

            this.registry.updateMetrics(ip, metrics);
            Logger.debug(`Metrics updated for ${ip}`, metrics);

        } catch (error) {
            Logger.error(`Failed to fetch metrics from ${ip}`, error);
            this.registry.markFailure(ip);
        }
    }

    stopPolling() {
        if (this.pollIntervalId) {
            clearInterval(this.pollIntervalId);
            this.pollIntervalId = null;
        }
        if (this.optimizationIntervalId) {
            clearInterval(this.optimizationIntervalId);
            this.optimizationIntervalId = null;
        }
        this.isPolling = false;
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
                metrics: {
                    cpu: (server.metrics.cpu * 100).toFixed(2) + '%',
                    memory: (server.metrics.memory * 100).toFixed(2) + '%',
                    latency: (server.metrics.latency * 1000).toFixed(2) + 'ms',
                    responseTime: (server.metrics.responseTime * 1000).toFixed(2) + 'ms'
                },
                requests: server.requestCount,
                avgResponseTime: server.requestCount > 0 
                    ? (server.totalResponseTime / server.requestCount).toFixed(2) + 'ms'
                    : 'N/A',
                lastUpdated: new Date(server.lastUpdated).toISOString()
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
                delta: this.optimizer.delta,
                vnodeConfig: this.optimizer.vnodeConfig
            },
            totalRequests: Object.values(serverRequestStats).reduce((sum, s) => sum + s.count, 0)
        };
    }
}

// ============================================================================
// EXPRESS APP SETUP
// ============================================================================
const app = express();
app.use(express.json());

// Initialize load balancer with Power Consistent Hashing
const loadBalancer = new DynamicWeightLoadBalancer({
    pollInterval: 15000,              // Poll every 15s
    optimizationInterval: 45000,      // Optimize every 45s
    metricsPath: '/current-metrics',
    healthCheckPath: '/health',
    requestTimeout: 30000,
    
    // Weight factors (from paper)
    alpha: 0.3,    // CPU
    beta: 0.3,     // Memory
    gamma: 0.2,    // Latency
    delta: 0.2,    // Response time
    
    // Vnode configuration
    minVnodes: 5,
    maxVnodes: 50,
    baseVnodes: 20,
    
    // Optimization parameters
    targetLoadBalance: 0.15,
    weightAdjustmentRate: 0.2,
    amplitudeFactor: 0.2
});

// ============================================================================
// API ROUTES
// ============================================================================

// Register a new server
app.post('/register', (req, res) => {
    try {
        const { ip, weight, vnodes } = req.body;
        
        if (!ip) {
            return res.status(400).json({ error: 'IP address required' });
        }

        const server = loadBalancer.registerServer(
            ip, 
            weight || 1.0, 
            vnodes || 20
        );
        
        Logger.success(`Server registered via API: ${ip}`);
        
        res.json({ 
            status: 'success', 
            message: 'Server registered successfully',
            server: {
                ip: server.ip,
                weight: server.weight,
                vnodes: server.vnodes,
                status: server.status
            }
        });
    } catch (error) {
        Logger.error('Registration failed', error);
        res.status(500).json({ 
            status: 'error',
            error: error.message 
        });
    }
});

// Deregister a server
app.post('/deregister', (req, res) => {
    try {
        const { ip } = req.body;
        
        if (!ip) {
            return res.status(400).json({ error: 'IP address required' });
        }

        loadBalancer.deregisterServer(ip);
        Logger.success(`Server deregistered via API: ${ip}`);
        
        res.json({ 
            status: 'success', 
            message: 'Server deregistered successfully' 
        });
    } catch (error) {
        Logger.error('Deregistration failed', error);
        res.status(404).json({ 
            status: 'error',
            error: error.message 
        });
    }
});

// Get load balancer status
app.get('/status', (req, res) => {
    try {
        const status = loadBalancer.getStatus();
        res.json({
            status: 'success',
            data: status
        });
    } catch (error) {
        Logger.error('Failed to get status', error);
        res.status(500).json({ 
            status: 'error',
            error: error.message 
        });
    }
});

// Health check endpoint
app.get('/health', (req, res) => {
    const healthyServers = loadBalancer.registry.getHealthyServers();
    const allServers = Object.keys(loadBalancer.registry.getAll());
    
    res.json({
        status: 'healthy',
        algorithm: 'Power Consistent Hashing (O(1))',
        servers: {
            total: allServers.length,
            healthy: healthyServers.length,
            unhealthy: allServers.length - healthyServers.length
        },
        uptime: process.uptime()
    });
});

// Get server for specific user (testing endpoint)
app.get('/route/:userId', (req, res) => {
    try {
        const { userId } = req.params;
        const serverIp = loadBalancer.getServerForUser(userId);
        
        if (!serverIp) {
            return res.status(503).json({
                status: 'error',
                error: 'No backend servers available'
            });
        }
        
        res.json({
            status: 'success',
            userId,
            assignedServer: serverIp,
            hash: murmurHash3(userId, 42)
        });
    } catch (error) {
        Logger.error('Route lookup failed', error);
        res.status(500).json({
            status: 'error',
            error: error.message
        });
    }
});

// ============================================================================
// PROXY MIDDLEWARE (Main request handler)
// ============================================================================
app.use(async (req, res) => {
    try {
        // Extract user identifier from request
        const username = req.body?.username || 
                        req.query?.username || 
                        req.headers['x-user-id'] ||
                        req.ip;
        
        const hash = murmurHash3(username, 42);

        Logger.debug(`${req.method} ${req.originalUrl} for user: ${username} (hash: ${hash})`);

        // Get target server using Power Consistent Hashing
        const serverIp = loadBalancer.getServerForUser(username);

        if (!serverIp) {
            Logger.warn('No backend server available');
            return res.status(503).json({ 
                status: 'error',
                error: 'No backend servers available' 
            });
        }

        // Initialize server stats if needed
        if (!serverRequestStats[serverIp]) {
            serverRequestStats[serverIp] = { 
                count: 0, 
                totalTime: 0,
                errors: 0,
                lastRequest: null
            };
        }

        const startTime = Date.now();
        const backendUrl = `http://${serverIp}:3000${req.originalUrl}`;

        try {
            const response = await axios({
                method: req.method,
                url: backendUrl,
                data: req.body,
                params: req.query,
                headers: {
                    ...req.headers,
                    host: serverIp,
                    'x-forwarded-for': req.ip,
                    'x-forwarded-proto': req.protocol,
                    'x-original-url': req.originalUrl
                },
                timeout: loadBalancer.config.requestTimeout,
                validateStatus: () => true // Accept all status codes
            });

            const responseTime = Date.now() - startTime;

            // Update statistics
            serverRequestStats[serverIp].count += 1;
            serverRequestStats[serverIp].totalTime += responseTime;
            serverRequestStats[serverIp].lastRequest = new Date().toISOString();
            saveServerStats();

            // Update server request stats in registry
            loadBalancer.registry.updateRequestStats(serverIp, responseTime);

            Logger.success(`${req.method} ${req.originalUrl} → ${serverIp} (${response.status}) ${responseTime}ms`);
            
            // Forward response
            res.status(response.status)
               .set(response.headers)
               .send(response.data);

        } catch (error) {
            const responseTime = Date.now() - startTime;
            serverRequestStats[serverIp].errors += 1;
            saveServerStats();

            // Handle different error types
            if (error.code === 'ECONNREFUSED') {
                Logger.error(`Backend ${serverIp} connection refused`);
                loadBalancer.registry.markFailure(serverIp);
                res.status(502).json({ 
                    status: 'error',
                    error: 'Backend server unavailable',
                    server: serverIp
                });
            } else if (error.code === 'ETIMEDOUT') {
                Logger.error(`Backend ${serverIp} timeout after ${responseTime}ms`);
                res.status(504).json({ 
                    status: 'error',
                    error: 'Backend server timeout',
                    server: serverIp
                });
            } else if (error.response) {
                Logger.warn(`Backend ${serverIp} error: ${error.response.status}`);
                res.status(error.response.status).send(error.response.data);
            } else {
                Logger.error(`Proxy error for ${serverIp}`, error);
                res.status(502).json({ 
                    status: 'error',
                    error: 'Upstream error',
                    message: error.message
                });
            }
        }

    } catch (error) {
        Logger.error('Request handling failed', error);
        res.status(500).json({ 
            status: 'error',
            error: 'Internal load balancer error',
            message: error.message
        });
    }
});

// ============================================================================
// ERROR HANDLERS
// ============================================================================

// 404 handler
app.use((req, res) => {
    res.status(404).json({
        status: 'error',
        error: 'Route not found'
    });
});

// Global error handler
app.use((err, req, res, next) => {
    Logger.error('Unhandled error', err);
    res.status(500).json({
        status: 'error',
        error: 'Internal server error',
        message: err.message
    });
});

// ============================================================================
// SERVER STARTUP & SHUTDOWN
// ============================================================================

const PORT = process.env.PORT || 3000;
const server = app.listen(PORT, async () => {
    console.log('\n' + '='.repeat(70));
    console.log('  DYNAMIC WEIGHT LOAD BALANCER');
    console.log('  Algorithm: Power Consistent Hashing (O(1) Expected Time)');
    console.log('  Based on: "Fast Consistent Hashing in Constant Time" (Eric Leu, 2023)');
    console.log('='.repeat(70));
    Logger.success(`Load Balancer running on port ${PORT}`);
    console.log('='.repeat(70) + '\n');
    
    // Start polling after server is ready
    try {
        await loadBalancer.startPolling();
    } catch (error) {
        Logger.error('Failed to start polling', error);
    }
});

// Graceful shutdown handler
async function gracefulShutdown(signal) {
    Logger.info(`${signal} received - shutting down gracefully`);
    
    // Stop accepting new requests
    server.close(() => {
        Logger.info('HTTP server closed');
    });
    
    // Stop polling
    loadBalancer.stopPolling();
    
    // Save final stats
    saveServerStats();
    
    // Wait a bit for in-flight requests
    setTimeout(() => {
        Logger.success('Graceful shutdown complete');
        process.exit(0);
    }, 5000);
}

// Handle termination signals
process.on('SIGTERM', () => gracefulShutdown('SIGTERM'));
process.on('SIGINT', () => gracefulShutdown('SIGINT'));

// Handle uncaught errors
process.on('uncaughtException', (error) => {
    Logger.error('Uncaught Exception', error);
    gracefulShutdown('UNCAUGHT_EXCEPTION');
});

process.on('unhandledRejection', (reason, promise) => {
    Logger.error('Unhandled Rejection', { reason, promise });
});

// ============================================================================
// EXPORTS (for testing)
// ============================================================================
module.exports = {
    app,
    loadBalancer,
    PowerConsistentHashRing,
    HeuristicWeightOptimizer,
    ServerRegistry,
    murmurHash3
};