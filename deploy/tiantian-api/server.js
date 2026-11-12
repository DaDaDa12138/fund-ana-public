'use strict';

const http = require('node:http');
const fs = require('node:fs');
const path = require('node:path');
const { createHash, randomUUID } = require('node:crypto');
const { performance } = require('node:perf_hooks');
const axios = require('axios');
const provenance = require('./upstream.json');
const { version } = require('./package.json');

const RESPONSE_LIMIT = 16 * 1024 * 1024;
const MAX_CONCURRENT = 32;

function loadModules(root = __dirname) {
  // These are immutable copies of the named upstream commit. Fail startup if
  // any adapter or its shared transport has been edited without new provenance.
  for (const [name, expected] of Object.entries(provenance.files)) {
    const raw = fs.readFileSync(path.join(root, 'upstream', name));
    if (createHash('sha256').update(raw).digest('hex') !== expected) {
      throw new Error(`upstream_source_digest_mismatch:${name}`);
    }
  }
  const modules = Object.create(null);
  for (const name of provenance.routes) {
    modules[`/${name}`] = require(path.join(root, 'upstream/src/module', `${name}.js`));
  }
  return modules;
}

function configureTransport() {
  // Keep request parameters, upstream methods and data parsers in the pinned
  // adapters. Bound stalled connections and responses without filling values.
  axios.defaults.timeout = 25000;
  axios.defaults.maxContentLength = RESPONSE_LIMIT;
  axios.defaults.maxBodyLength = 1024 * 1024;
  axios.defaults.maxRedirects = 3;
}

function queryParameters(searchParams) {
  const result = Object.create(null);
  for (const [key, value] of searchParams) {
    if (Object.hasOwn(result, key)) {
      result[key] = Array.isArray(result[key]) ? [...result[key], value] : [result[key], value];
    } else {
      result[key] = value;
    }
  }
  return result;
}

function sendJson(response, status, value, headers = {}) {
  const body = Buffer.from(JSON.stringify(value, (key, item) => {
    if (item === undefined || (typeof item === 'number' && !Number.isFinite(item))) {
      throw Object.assign(new Error('upstream_value_not_representable_in_json'), { code: 'UPSTREAM_INVALID_JSON' });
    }
    return item;
  }));
  if (body.length > RESPONSE_LIMIT) {
    throw Object.assign(new Error('upstream_response_too_large'), { code: 'ERR_RESPONSE_TOO_LARGE' });
  }
  response.writeHead(status, {
    'Content-Type': 'application/json; charset=utf-8',
    'Content-Length': body.length,
    'Cache-Control': 'no-store',
    ...headers,
  });
  response.end(body);
  return body;
}

function createHandler({ modules = loadModules(), log = (row) => process.stdout.write(`${JSON.stringify(row)}\n`) } = {}) {
  let active = 0;
  return async function handle(request, response) {
    const requestId = randomUUID();
    const started = performance.now();
    let endpoint = null;
    let status = null;
    let body = null;
    let acquired = false;
    let upstreamAttempted = false;
    let errorCode = null;
    try {
      if (!request.url?.startsWith('/') || request.url.length > 8192) {
        status = 400;
        body = sendJson(response, status, { error: { type: 'invalid_request_target' } });
        return;
      }
      const url = new URL(request.url, 'http://tiantian-api');
      endpoint = url.pathname;
      if (request.method !== 'GET') {
        status = 405;
        body = sendJson(response, status, { error: { type: 'method_not_allowed' } }, { Allow: 'GET' });
        return;
      }
      if (endpoint === '/healthz' || endpoint === '/health') {
        status = 200;
        body = sendJson(response, status, {
          status: 'ok', version, upstream_commit: provenance.commit, upstream_checked: false,
        });
        return;
      }
      if (!Object.hasOwn(modules, endpoint)) {
        status = 404;
        body = sendJson(response, status, { error: { type: 'unknown_fund_endpoint' } });
        return;
      }
      if (active >= MAX_CONCURRENT) {
        status = 503;
        body = sendJson(response, status, { error: { type: 'proxy_busy' } }, { 'Retry-After': '1' });
        return;
      }
      active += 1;
      acquired = true;
      upstreamAttempted = true;
      const payload = await modules[endpoint](queryParameters(url.searchParams));
      if (payload !== null && (typeof payload !== 'object' || Buffer.isBuffer(payload))) {
        throw Object.assign(new Error('upstream_json_object_or_array_required'), { code: 'UPSTREAM_INVALID_JSON' });
      }
      // Forward business failures, empty arrays, nulls, strings and real zeros
      // inside the original JSON unchanged. HTTP success is only transport.
      status = 200;
      body = sendJson(response, status, payload, {
        'X-Fund-ANA-Upstream-Commit': provenance.commit,
        'X-Fund-ANA-Proxy-Version': version,
        'X-Request-ID': requestId,
      });
    } catch (error) {
      errorCode = typeof error.code === 'string' ? error.code : 'UPSTREAM_REQUEST_FAILED';
      const timedOut = ['ECONNABORTED', 'ETIMEDOUT'].includes(errorCode);
      status = timedOut ? 504 : 502;
      if (!response.headersSent && !response.destroyed) {
        body = sendJson(response, status, {
          endpoint,
          upstreamStatus: Number.isInteger(error.response?.status) ? error.response.status : null,
          error: {
            type: timedOut ? 'upstream_timeout' : 'upstream_request_failed',
            code: errorCode,
            message: timedOut ? 'Upstream request timed out' : 'Upstream response unavailable',
          },
        }, { 'X-Request-ID': requestId });
      }
    } finally {
      if (acquired) active -= 1;
      if (endpoint !== '/healthz' && endpoint !== '/health') {
        log({ observed_at: new Date(Date.now() + 8 * 60 * 60 * 1000).toISOString().replace('Z', '+08:00'),
          timezone: 'Asia/Shanghai', request_id: requestId, endpoint,
          http_status: status, upstream_attempted: upstreamAttempted,
          response_bytes: body?.length ?? null,
          response_sha256: body ? createHash('sha256').update(body).digest('hex') : null,
          elapsed_ms: Math.round(performance.now() - started), error_code: errorCode });
      }
    }
  };
}

if (require.main === module) {
  configureTransport();
  const port = Number(process.env.PORT || '3000');
  if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error('invalid_listen_port');
  const server = http.createServer(createHandler());
  server.requestTimeout = 30000;
  server.headersTimeout = 10000;
  server.listen(port, '0.0.0.0', () => {
    process.stdout.write(`${JSON.stringify({ event: 'listening', port, version, upstream_commit: provenance.commit })}\n`);
  });
  process.on('SIGTERM', () => {
    server.close(() => process.exit(0));
    setTimeout(() => process.exit(1), 30000).unref();
  });
}

module.exports = { configureTransport, createHandler, loadModules, queryParameters };
