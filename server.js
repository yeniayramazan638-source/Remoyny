// Rope Penguins - WebSocket relay sunucusu (Railway için)
const http = require('http');
const { WebSocketServer } = require('ws');

const PORT = process.env.PORT || 3000;
const server = http.createServer((req, res) => {
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('Rope Penguins server OK');
});
const wss = new WebSocketServer({ server, perMessageDeflate: false });

// room -> { host: ws, guest: ws|null }
const rooms = new Map();
const send = (ws, obj) => { if (ws && ws.readyState === 1) ws.send(JSON.stringify(obj)); };

function leave(ws) {
  const room = rooms.get(ws.room);
  if (!room) return;
  if (ws.role === 'host') {
    send(room.guest, { type: 'host_left' });
    if (room.guest) { room.guest.room = null; }
    rooms.delete(ws.room);
  } else if (ws.role === 'guest' && room.guest === ws) {
    room.guest = null;
    send(room.host, { type: 'guest_left' });
  }
  ws.room = null;
}

wss.on('connection', ws => {
  ws.isAlive = true;
  try { ws._socket.setNoDelay(true); } catch {}
  ws.on('pong', () => { ws.isAlive = true; });

  ws.on('message', raw => {
    if (raw.length > 65536) return;
    let m;
    try { m = JSON.parse(raw.toString()); } catch { return; }
    if (!m || typeof m !== 'object') return;

    if (m.type === 'create') {
      const code = String(m.room || '');
      if (!/^\d{4}$/.test(code)) return;
      if (ws.room) leave(ws);
      if (rooms.has(code)) return send(ws, { type: 'error', message: 'Oda kodu kullanımda, tekrar dene.' });
      rooms.set(code, { host: ws, guest: null });
      ws.room = code; ws.role = 'host';
      send(ws, { type: 'created', room: code });
    } else if (m.type === 'join') {
      const code = String(m.room || '');
      const room = rooms.get(code);
      if (!room) return send(ws, { type: 'error', message: 'Oda bulunamadı. Kodu kontrol et.' });
      if (room.guest) return send(ws, { type: 'error', message: 'Oda dolu.' });
      if (ws.room) leave(ws);
      room.guest = ws; ws.room = code; ws.role = 'guest';
      send(ws, { type: 'joined', room: code });
      send(room.host, { type: 'guest_joined' });
    } else if (m.type === 'relay') {
      const room = rooms.get(ws.room);
      if (!room) return;
      if (ws.role === 'host') {
        // Misafirin bağlantısı yavaşsa eski durum paketlerini biriktirme (gecikmeyi önler)
        if (room.guest && room.guest.bufferedAmount > 32768) return;
        send(room.guest, { type: 'state', data: m.data });
      }
      else if (ws.role === 'guest') send(room.host, { type: 'input', data: m.data });
    }
  });

  ws.on('close', () => leave(ws));
  ws.on('error', () => {});
});

// Ölü bağlantıları temizle + proxy'nin boşta kesmesini engelle
setInterval(() => {
  wss.clients.forEach(ws => {
    if (!ws.isAlive) return ws.terminate();
    ws.isAlive = false;
    try { ws.ping(); } catch {}
  });
}, 25000);

server.listen(PORT, () => console.log('Listening on ' + PORT));
