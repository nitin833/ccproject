const express = require('express');
const axios = require('axios');
const murmurhash = require('murmurhash-js');

const app = express();
app.use(express.json());

// -----------------------------
// Consistent Hash Ring
// -----------------------------
class ConsistentHashRing {
    constructor(domainName) {
        this.domainName = domainName;
        this.ring = new Map();
        this.sortedKeys = [];
        this.nodes = new Map();
    }

    _hash(key) {
        return murmurhash.murmur3(key, 0) >>> 0; // Use full 32-bit hash
    }

    addNode(nodeId, nodeUrl, weight = 1.0) {
        if (this.nodes.has(nodeId)) {
            return false;
        }

        this.nodes.set(nodeId, {
            url: nodeUrl,
            weight: weight,
            metrics: {
                cpu_usage: 0,
                memory_usage: 0,
                latency: 0,
                failure_rate: 0,
                network_traffic: 0,
                execution_time: 0
            },
            requestCount: 0
        });

        // Calculate virtual nodes based on weight
        const numVnodes = Math.floor(weight * 100);

        for (let i = 0; i < numVnodes; i++) {
            const vnodeKey = `${nodeId}:${i}`;
            const hashValue = this._hash(vnodeKey);
            this.ring.set(hashValue, nodeId);
        }

        this.sortedKeys = Array.from(this.ring.keys()).sort((a, b) => a - b);
        return true;
    }

    removeNode(nodeId) {
        if (!this.nodes.has(nodeId)) {
            return false;
        }

        // Remove all virtual nodes
        const keysToRemove = [];
        for (const [key, value] of this.ring.entries()) {
            if (value === nodeId) {
                keysToRemove.push(key);
            }
        }

        keysToRemove.forEach(key => this.ring.delete(key));
        this.nodes.delete(nodeId);
        this.sortedKeys = Array.from(this.ring.keys()).sort((a, b) => a - b);
        return true;
    }

    getNode(key) {
        if (this.ring.size === 0) {
            return null;
        }

        const hashValue = this._hash(key);

        // Find first node clockwise
        for (const ringKey of this.sortedKeys) {
            if (ringKey >= hashValue) {
                const nodeId = this.ring.get(ringKey);
                return { nodeId, ...this.nodes.get(nodeId) };
            }
        }

        // Wrap around to first node
        const nodeId = this.ring.get(this.sortedKeys[0]);
        return { nodeId, ...this.nodes.get(nodeId) };
    }

    updateNodeWeight(nodeId, newWeight) {
        if (!this.nodes.has(nodeId)) {
            return false;
        }

        const node = this.nodes.get(nodeId);
        const nodeUrl = node.url;
        const metrics = node.metrics;
        const requestCount = node.requestCount;

        this.removeNode(nodeId);
        this.addNode(nodeId, nodeUrl, newWeight);

        // Restore metrics
        const updatedNode = this.nodes.get(nodeId);
        updatedNode.metrics = metrics;
        updatedNode.requestCount = requestCount;
        return true;
    }

    updateMetrics(nodeId, metrics) {
        if (this.nodes.has(nodeId)) {
            const node = this.nodes.get(nodeId);
            node.metrics = { ...node.metrics, ...metrics };
            node.requestCount += 1;
        }
    }

    getNodes() {
        return Array.from(this.nodes.entries()).map(([nodeId, node]) => ({
            nodeId,
            ...node
        }));
    }
}

// -----------------------------
// Load Score Calculator
// -----------------------------
class LoadScoreCalculator {
    constructor() {
        this.metricsHistory = new Map();
    }

    addMetrics(nodeId, metrics, failureRate) {
        if (!this.metricsHistory.has(nodeId)) {
            this.metricsHistory.set(nodeId, []);
        }

        const history = this.metricsHistory.get(nodeId);
        history.push({
            cpu: metrics.cpu_usage || 0,
            memory: metrics.memory_usage || 0,
            latency: metrics.latency || 0,
            failure_rate: failureRate
        });

        // Keep only last 100 entries
        if (history.length > 100) {
            history.shift();
        }
    }

    calculatePearsonCorrelation(nodeId) {
        const history = this.metricsHistory.get(nodeId);
        
        if (!history || history.length < 5) {
            return { cpu: 0.5, memory: 0.3, latency: 0.2 };
        }

        const metricsData = {
            cpu: history.map(h => h.cpu),
            memory: history.map(h => h.memory),
            latency: history.map(h => h.latency)
        };
        const failureRates = history.map(h => h.failure_rate);

        const correlations = {};

        for (const [metricName, values] of Object.entries(metricsData)) {
            const correlation = this._pearson(values, failureRates);
            correlations[metricName] = Math.abs(correlation);
        }

        return correlations;
    }

    _pearson(x, y) {
        const n = x.length;
        if (n < 2) return 0.33;

        const xMean = x.reduce((a, b) => a + b, 0) / n;
        const yMean = y.reduce((a, b) => a + b, 0) / n;

        const xStd = Math.sqrt(x.reduce((sum, val) => sum + Math.pow(val - xMean, 2), 0) / n);
        const yStd = Math.sqrt(y.reduce((sum, val) => sum + Math.pow(val - yMean, 2), 0) / n);

        if (xStd === 0 || yStd === 0) return 0.33;

        const numerator = x.reduce((sum, val, i) => sum + (val - xMean) * (y[i] - yMean), 0);
        const denominator = Math.sqrt(
            x.reduce((sum, val) => sum + Math.pow(val - xMean, 2), 0) *
            y.reduce((sum, val) => sum + Math.pow(val - yMean, 2), 0)
        );

        return denominator === 0 ? 0.33 : numerator / denominator;
    }

    calculateWeights(correlations) {
        const total = Object.values(correlations).reduce((a, b) => a + b, 0);
        
        if (total === 0) {
            return { cpu: 0.33, memory: 0.33, latency: 0.34 };
        }

        const weights = {};
        for (const [key, value] of Object.entries(correlations)) {
            weights[key] = value / total;
        }

        return weights;
    }

    calculateLoadScore(nodeId, metrics) {
        const correlations = this.calculatePearsonCorrelation(nodeId);
        const weights = this.calculateWeights(correlations);

        const cpu = metrics.cpu_usage || 0;
        const memory = metrics.memory_usage || 0;
        const latency = metrics.latency || 0;

        const loadScore = weights.cpu * cpu + weights.memory * memory + weights.latency * latency;

        return { loadScore, weights };
    }
}

// -----------------------------
// Domain Router
// -----------------------------
class DomainRouter {
    constructor(pythonServerUrl) {
        this.rings = {
            io: new ConsistentHashRing('io'),
            network: new ConsistentHashRing('network'),
            compute: new ConsistentHashRing('compute')
        };
        this.loadCalculator = new LoadScoreCalculator();
        this.pythonServerUrl = pythonServerUrl;
    }

    registerServer(nodeId, nodeUrl, domain, weight = 1.0) {
        if (!this.rings[domain]) {
            return { success: false, message: `Invalid domain: ${domain}. Use: io, network, compute` };
        }

        const success = this.rings[domain].addNode(nodeId, nodeUrl, weight);
        
        if (success) {
            return { success: true, message: `Server ${nodeId} registered in ${domain} domain with weight ${weight}` };
        }
        return { success: false, message: `Server ${nodeId} already exists in ${domain} domain` };
    }

    deregisterServer(nodeId, domain) {
        if (!this.rings[domain]) {
            return { success: false, message: `Invalid domain: ${domain}` };
        }

        const success = this.rings[domain].removeNode(nodeId);
        
        if (success) {
            return { success: true, message: `Server ${nodeId} deregistered from ${domain} domain` };
        }
        return { success: false, message: `Server ${nodeId} not found in ${domain} domain` };
    }

    getServer(requestKey, domain) {
        if (!this.rings[domain]) {
            return null;
        }

        return this.rings[domain].getNode(requestKey);
    }

    updateServerMetrics(nodeId, domain, metrics, failureRate) {
        if (!this.rings[domain]) {
            return;
        }

        const ring = this.rings[domain];
        if (!ring.nodes.has(nodeId)) {
            return;
        }

        // Update metrics
        ring.updateMetrics(nodeId, metrics);

        // Add to load calculator
        this.loadCalculator.addMetrics(nodeId, metrics, failureRate);

        // Calculate new load score and weight
        const { loadScore, weights } = this.loadCalculator.calculateLoadScore(nodeId, metrics);

        // Inverse weight: lower load = higher weight
        const normalizedLoad = Math.max(0, Math.min(100, loadScore));
        const newWeight = Math.max(0.1, (100 - normalizedLoad) / 100);

        // Update weight in ring (recalculate vnodes)
        ring.updateNodeWeight(nodeId, newWeight);

        console.log(`[WEIGHT UPDATE] ${nodeId} in ${domain}: LoadScore=${loadScore.toFixed(2)}, NewWeight=${newWeight.toFixed(2)}, Weights(CPU:${(weights.cpu*100).toFixed(1)}%, Mem:${(weights.memory*100).toFixed(1)}%, Lat:${(weights.latency*100).toFixed(1)}%)`);
    }

    async predictDomain(url) {
        try {
            console.log(`[RL PREDICT] Calling Python /predict for URL: ${url}`);
            const response = await axios.post(`${this.pythonServerUrl}/predict`, { url }, { timeout: 3000 });
            const prediction = response.data.prediction;
            
            const domainMap = { 0: 'io', 1: 'network', 2: 'compute' };
            const predictedDomain = domainMap[prediction] || 'compute';
            
            console.log(`[RL PREDICT] Python returned prediction: ${prediction} → Domain: ${predictedDomain}`);
            return predictedDomain;
        } catch (error) {
            console.error(`[RL PREDICT ERROR] ${error.message}`);
            // Fallback to simple heuristic
            if (url.includes('file://')) return 'io';
            if (url.includes('network') || url.includes('api')) return 'network';
            return 'compute';
        }
    }

    async sendFeedback(url, actualLabel) {
        try {
            console.log(`[RL FEEDBACK] Calling Python /feedback with URL: ${url}, Label: ${actualLabel}`);
            await axios.post(`${this.pythonServerUrl}/feedback`, { 
                url, 
                label: actualLabel 
            }, { timeout: 3000 });
            console.log(`[RL FEEDBACK] Feedback sent successfully - RL model will retrain`);
        } catch (error) {
            console.error(`[RL FEEDBACK ERROR] ${error.message}`);
        }
    }

    async getTaskType(metrics) {
        try {
            console.log(`[ML PREDICT] Calling Python /ml_predict with metrics`);
            const response = await axios.post(`${this.pythonServerUrl}/ml_predict`, {
                cpu_usage: metrics.cpu_usage || 0,
                network_traffic: metrics.network_traffic || 0,
                execution_time: metrics.execution_time || 0
            }, { timeout: 3000 });
            
            const taskType = response.data.task_type_prediction;
            console.log(`[ML PREDICT] Python returned task type: ${taskType}`);
            return taskType;
        } catch (error) {
            console.error(`[ML PREDICT ERROR] ${error.message}`);
            return null;
        }
    }

    async fetchServerMetrics(serverUrl) {
        try {
            console.log(`[METRICS] Fetching from ${serverUrl}/metrics`);
            const response = await axios.get(`${serverUrl}/metrics`, { timeout: 2000 });
            console.log(`[METRICS] Received: CPU=${response.data.cpu_usage}%, Memory=${response.data.memory_usage}%, Network=${response.data.network_traffic}`);
            return response.data;
        } catch (error) {
            console.error(`[METRICS ERROR] Failed to fetch from ${serverUrl}/metrics: ${error.message}`);
            return null;
        }
    }

    getAllStatus() {
        const status = {};
        
        for (const [domainName, ring] of Object.entries(this.rings)) {
            status[domainName] = {
                totalNodes: ring.nodes.size,
                totalVirtualNodes: ring.ring.size,
                nodes: {}
            };

            for (const node of ring.getNodes()) {
                status[domainName].nodes[node.nodeId] = {
                    url: node.url,
                    weight: node.weight,
                    metrics: node.metrics,
                    requestCount: node.requestCount,
                    virtualNodes: Math.floor(node.weight * 100)
                };
            }
        }

        return status;
    }
}

// -----------------------------
// Initialize Router
// -----------------------------
const PYTHON_SERVER_URL = process.env.PYTHON_SERVER_URL || 'http://localhost:5000';
const domainRouter = new DomainRouter(PYTHON_SERVER_URL);

// -----------------------------
// Server Registration Endpoint
// -----------------------------
app.post('/register', (req, res) => {
    const { node_id, node_url, domain, weight = 1.0 } = req.body;

    if (!node_id || !node_url || !domain) {
        return res.status(400).json({ 
            error: 'Missing required fields', 
            required: ['node_id', 'node_url', 'domain'],
            example: {
                node_id: 'server1',
                node_url: 'http://localhost:5001',
                domain: 'io',
                weight: 1.0
            }
        });
    }

    const result = domainRouter.registerServer(node_id, node_url, domain, weight);

    if (result.success) {
        console.log(`[REGISTER] ${result.message}`);
        return res.json({ message: result.message });
    }
    
    console.log(`[REGISTER ERROR] ${result.message}`);
    return res.status(400).json({ error: result.message });
});

// -----------------------------
// Server Deregistration Endpoint
// -----------------------------
app.post('/deregister', (req, res) => {
    const { node_id, domain } = req.body;

    if (!node_id || !domain) {
        return res.status(400).json({ 
            error: 'Missing required fields',
            required: ['node_id', 'domain']
        });
    }

    const result = domainRouter.deregisterServer(node_id, domain);

    if (result.success) {
        console.log(`[DEREGISTER] ${result.message}`);
        return res.json({ message: result.message });
    }
    
    console.log(`[DEREGISTER ERROR] ${result.message}`);
    return res.status(400).json({ error: result.message });
});

// -----------------------------
// Main Request Handler
// -----------------------------
app.post('/request', async (req, res) => {
    const { url, payload = {}, user_id } = req.body;

    if (!url) {
        return res.status(400).json({ error: 'URL required' });
    }

    console.log(`\n${'='.repeat(80)}`);
    console.log(`[REQUEST START] URL: ${url}`);
    console.log(`${'='.repeat(80)}`);

    try {
        // STEP 1: Call Python RL endpoint to predict domain
        console.log('\n[STEP 1] Requesting domain prediction from Python RL model...');
        const predictedDomain = await domainRouter.predictDomain(url);
        console.log(`[STEP 1] ✓ Domain predicted: ${predictedDomain}`);

        // STEP 2: Get server from consistent hash ring based on predicted domain
        console.log(`\n[STEP 2] Finding server in ${predictedDomain} ring using consistent hashing...`);
        const requestKey = user_id ? `${user_id}:${url}` : url;
        const serverNode = domainRouter.getServer(requestKey, predictedDomain);

        if (!serverNode) {
            console.log(`[STEP 2] ✗ No servers available in ${predictedDomain} domain`);
            return res.status(503).json({ 
                error: `No servers available in ${predictedDomain} domain`,
                domain: predictedDomain
            });
        }

        console.log(`[STEP 2] ✓ Selected: ${serverNode.nodeId} at ${serverNode.url} (weight: ${serverNode.weight.toFixed(2)}, vnodes: ${Math.floor(serverNode.weight * 100)}, key: ${requestKey})`);

        // STEP 3: Forward request to selected server
        console.log(`\n[STEP 3] Forwarding request to backend server...`);
        const startTime = Date.now();
        let responseData;
        let statusCode;

        try {
            const response = await axios.post(
                serverNode.url,
                { ...payload, original_url: url },
                { timeout: 5000 }
            );
            responseData = response.data;
            statusCode = response.status;
            console.log(`[STEP 3] ✓ Request forwarded successfully, status: ${statusCode}`);
        } catch (error) {
            if (error.response) {
                responseData = error.response.data;
                statusCode = error.response.status;
                console.log(`[STEP 3] ✗ Request failed with status: ${statusCode}`);
            } else {
                console.log(`[STEP 3] ✗ Forward error: ${error.message}`);
                return res.status(500).json({
                    error: `Failed to forward request: ${error.message}`,
                    domain: predictedDomain,
                    server: serverNode.url
                });
            }
        }

        const requestLatency = Date.now() - startTime;

        // STEP 4: Fetch metrics from server's /metrics endpoint
        console.log(`\n[STEP 4] Fetching metrics from ${serverNode.url}/metrics...`);
        const serverMetrics = await domainRouter.fetchServerMetrics(serverNode.url);
        
        if (!serverMetrics) {
            console.log(`[STEP 4] ✗ Failed to fetch metrics, using defaults`);
        } else {
            console.log(`[STEP 4] ✓ Metrics fetched successfully`);
        }

        // Combine metrics with request latency
        const metrics = {
            cpu_usage: serverMetrics?.cpu_usage || 0,
            memory_usage: serverMetrics?.memory_usage || 0,
            network_traffic: serverMetrics?.network_traffic || 0,
            execution_time: serverMetrics?.execution_time || (requestLatency / 1000),
            latency: requestLatency
        };

        const failureRate = statusCode === 200 ? 0 : 1;

        // STEP 5: Send metrics to Python ML endpoint to determine actual task type
        console.log(`\n[STEP 5] Sending metrics to Python ML model to determine actual task type...`);
        const actualTaskType = await domainRouter.getTaskType(metrics);
        
        let actualDomain = predictedDomain;
        if (actualTaskType) {
            actualDomain = actualTaskType;
            console.log(`[STEP 5] ✓ Actual domain determined: ${actualDomain}`);
        } else {
            console.log(`[STEP 5] ⚠ Could not determine actual domain, using predicted: ${predictedDomain}`);
        }

        // STEP 6: Send feedback to Python RL endpoint for retraining
        console.log(`\n[STEP 6] Sending feedback to Python RL model for retraining...`);
        const domainLabelMap = { 'io': 0, 'network': 1, 'compute': 2 };
        const actualLabel = domainLabelMap[actualDomain] || domainLabelMap[predictedDomain];
        await domainRouter.sendFeedback(url, actualLabel);
        console.log(`[STEP 6] ✓ Feedback sent, RL model updated`);

        // STEP 7: Update server metrics and recalculate weight (with vnodes adjustment)
        console.log(`\n[STEP 7] Updating server metrics and recalculating weight...`);
        domainRouter.updateServerMetrics(serverNode.nodeId, predictedDomain, metrics, failureRate);
        console.log(`[STEP 7] ✓ Server weight updated, vnodes recalculated`);

        console.log(`\n${'='.repeat(80)}`);
        console.log(`[REQUEST COMPLETE] Domain Match: ${predictedDomain === actualDomain ? '✓ YES' : '✗ NO'}`);
        console.log(`${'='.repeat(80)}\n`);

        // Return response
        res.status(statusCode).json({
            success: statusCode === 200,
            predicted_domain: predictedDomain,
            actual_domain: actualDomain,
            domain_match: predictedDomain === actualDomain,
            server: {
                node_id: serverNode.nodeId,
                url: serverNode.url,
                weight: serverNode.weight,
                virtual_nodes: Math.floor(serverNode.weight * 100)
            },
            request_key: requestKey,
            user_id: user_id || null,
            metrics: metrics,
            latency_ms: requestLatency,
            response: responseData
        });

    } catch (error) {
        console.error(`\n[ERROR] Request handling failed: ${error.message}`);
        console.log(`${'='.repeat(80)}\n`);
        res.status(500).json({ 
            error: error.message,
            stack: process.env.NODE_ENV === 'development' ? error.stack : undefined
        });
    }
});

// -----------------------------
// Status Endpoint
// -----------------------------
app.get('/status', (req, res) => {
    const status = domainRouter.getAllStatus();
    res.json(status);
});

// -----------------------------
// Health Check
// -----------------------------
app.get('/health', (req, res) => {
    res.json({ 
        status: 'healthy', 
        timestamp: new Date().toISOString(),
        pythonServer: PYTHON_SERVER_URL
    });
});

// -----------------------------
// Start Server
// -----------------------------
const PORT = process.env.PORT || 3000;

app.listen(PORT, () => {
    console.log('='.repeat(80));
    console.log('Domain-Aware Load Balancer with Consistent Hashing & Dynamic Load Scoring');
    console.log('='.repeat(80));
    console.log('\nConfiguration:');
    console.log(`  Node.js Load Balancer: http://localhost:${PORT}`);
    console.log(`  Python RL/ML Server:   ${PYTHON_SERVER_URL}`);
    console.log('\nEndpoints:');
    console.log('  POST /register      - Register server to specific domain ring');
    console.log('  POST /deregister    - Deregister server from domain ring');
    console.log('  POST /request       - Main request handler with full flow');
    console.log('  GET  /status        - Get status of all domain rings');
    console.log('  GET  /health        - Health check');
    console.log('\nDomains: io, network, compute');
    console.log('\nRequest Flow:');
    console.log('  1. POST /request → Python /predict (RL predicts domain)');
    console.log('  2. Consistent hashing selects server from domain ring');
    console.log('  3. Forward request to selected server');
    console.log('  4. Fetch metrics from server/metrics endpoint');
    console.log('  5. Python /ml_predict (ML determines actual task type)');
    console.log('  6. Python /feedback (retrain RL model with actual label)');
    console.log('  7. Calculate load score & update server weight/vnodes');
    console.log('='.repeat(80));
});

module.exports = app;
