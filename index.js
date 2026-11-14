import { createServer } from 'node:http';
import WebSocket, { WebSocketServer } from 'ws';

const PORT = Number(process.env.PORT || 7860);
const TARGET = (process.env.TARGET_URL || 'https://vela-server.vela-cloudflare.workers.dev').replace(/\/+$/, '');
const WS_TARGET = TARGET.replace(/^https:/, 'wss:').replace(/^http:/, 'ws:');

const server = createServer(async (req, res) => {
  if (req.url === '/proxy-health') {
    res.writeHead(200, { 'content-type': 'application/json' });
    return res.end(JSON.stringify({ ok: true, target: TARGET }));
  }

  try {
    const headers = { ...req.headers };
    delete headers.host;
    delete headers['content-length'];

    const chunks = [];
    for await (const c of req) chunks.push(c);
    const body = chunks.length && !['GET', 'HEAD'].includes(req.method) ? Buffer.concat(chunks) : undefined;

    const upstream = await fetch(`${TARGET}${req.url}`, {
      method: req.method,
      headers,
      body,
      redirect: 'manual',
    });

    const respHeaders = {};
    upstream.headers.forEach((v, k) => {
      if (k !== 'content-encoding' && k !== 'transfer-encoding') respHeaders[k] = v;
    });
    res.writeHead(upstream.status, respHeaders);
    const buf = Buffer.from(await upstream.arrayBuffer());
    res.end(buf);
  } catch (e) {
    res.writeHead(502, { 'content-type': 'application/json' });
    res.end(JSON.stringify({ error: 'bad_gateway', message: e.message }));
  }
});

const wss = new WebSocketServer({ noServer: true });

server.on('upgrade', (req, socket, head) => {
  const headers = {};
  if (req.headers.authorization) headers.authorization = req.headers.authorization;
  const upstream = new WebSocket(`${WS_TARGET}${req.url}`, { headers });

  const toUpstream = [];
  const toClient = [];
  let upstreamOpen = false;
  let clientWs = null;

  upstream.on('open', () => {
    upstreamOpen = true;
    for (const [d, b] of toUpstream) upstream.send(d, { binary: b });
    toUpstream.length = 0;
  });

  upstream.on('message', (data, isBinary) => {
    if (clientWs && clientWs.readyState === 1) clientWs.send(data, { binary: isBinary });
    else toClient.push([data, isBinary]);
  });

  upstream.on('unexpected-response', (_r, upRes) => {
    if (clientWs && clientWs.readyState <= 1) {
      clientWs.close(1008, `upstream_${upRes.statusCode}`);
    } else if (!socket.destroyed) {
      socket.write(`HTTP/1.1 ${upRes.statusCode} ${upRes.statusMessage}\r\nConnection: close\r\n\r\n`);
      socket.destroy();
    }
  });

  upstream.on('error', () => {
    if (clientWs && clientWs.readyState <= 1) clientWs.close(1011, 'upstream_error');
    else if (!socket.destroyed) socket.destroy();
  });

  upstream.on('close', (code, reason) => {
    if (clientWs && clientWs.readyState <= 1) {
      clientWs.close(code >= 1000 && code < 5000 ? code : 1000, reason?.toString());
    }
  });

  wss.handleUpgrade(req, socket, head, (client) => {
    clientWs = client;
    for (const [d, b] of toClient) {
      if (client.readyState === 1) client.send(d, { binary: b });
    }
    toClient.length = 0;
    client.on('message', (data, isBinary) => {
      if (upstreamOpen && upstream.readyState === 1) upstream.send(data, { binary: isBinary });
      else toUpstream.push([data, isBinary]);
    });
    client.on('close', (code, reason) => {
      if (upstream.readyState <= 1) {
        upstream.close(code >= 1000 && code < 5000 ? code : 1000, reason?.toString());
      }
    });
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`[vela-proxy] listening on 0.0.0.0:${PORT} -> ${TARGET}`);
});
