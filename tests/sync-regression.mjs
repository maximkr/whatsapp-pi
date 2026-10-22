// Isolated regression suite: no real WhatsApp connection, account or messages.
import test, { after } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, mkdir, readFile, writeFile, rm, readdir } from 'node:fs/promises';
import { tmpdir, homedir } from 'node:os';
import { join } from 'node:path';
import { createJiti } from 'jiti';

const home = await mkdtemp(join(tmpdir(), 'whatsapp-pi-sync-'));
process.env.USERPROFILE = home;
process.env.HOME = home;
assert.equal(homedir(), home, 'Tests must never use real WhatsApp storage');
const jiti = createJiti(import.meta.url, { fsCache: false });
const { WhatsAppService } = await jiti.import('../src/services/whatsapp.service.ts');
const { RecentsService } = await jiti.import('../src/services/recents.service.ts');
const { createStoragePaths } = await jiti.import('../src/services/storage-path.ts');
await mkdir(createStoragePaths().root, { recursive: true });
after(() => rm(home, { recursive: true, force: true }));
const jid = '1234567890@s.whatsapp.net';
const message = (id, extra = {}) => ({ key: { id, remoteJid: jid }, message: { conversation: id }, messageTimestamp: 1700000000, ...extra });
function fixture(allowed = true) {
    const session = { getStatus: () => 'connected', isConversationAllowed: () => allowed, isAllowedGroup: () => allowed, trackIgnoredNumber: async () => {} };
    const service = new WhatsAppService(session), records = [], delivered = [];
    service.setMessageRecorder(batch => { records.push(...batch); });
    service.setMessageCallback(payload => { delivered.push(payload.messages[0].key.id); });
    return { service, session, records, delivered };
}
async function disk(name) {
    const paths = createStoragePaths(join(home, name), join(home, 'no-legacy'));
    const session = { isConversationAllowed: () => false };
    const store = new RecentsService(session, paths);
    await store.ensureInitialized();
    return { store, paths, session };
}
const record = (id, timestamp = 1700000000) => ({ messageId: id, senderNumber: '+1234567890', text: id, direction: 'incoming', timestamp });

test('all notify elements are saved and delivered individually', async () => {
    const f = fixture();
    await f.service.handleIncomingMessages({ type: 'notify', messages: [message('a'), message('b'), message('c')] });
    assert.deepEqual(f.records.map(m => m.id), ['a', 'b', 'c']);
    assert.deepEqual(f.delivered, ['a', 'b', 'c']);
});
test('history, append, resend and unknown type are storage-only', async () => {
    const f = fixture();
    await f.service.handleHistoryMessages({ messages: [message('h')] });
    for (const p of [{ type: 'append' }, { type: 'notify', requestId: 'resend' }, {}]) await f.service.handleIncomingMessages({ ...p, messages: [message('stored')] });
    assert.equal(f.records.length, 4); assert.deepEqual(f.delivered, []); assert.equal(f.service.getLastRemoteJid(), null);
});
test('own messages and duplicate notify cannot prompt the agent twice', async () => {
    const f = fixture();
    await f.service.handleIncomingMessages({ type: 'notify', messages: [message('own', { key: { id: 'own', remoteJid: jid, fromMe: true } }), message('x'), message('x')] });
    assert.equal(f.records[0].fromMe, true); assert.deepEqual(f.delivered, ['x']);
});
test('disallowed messages are stored but never forwarded', async () => {
    const f = fixture(false); await f.service.handleIncomingMessages({ type: 'notify', messages: [message('denied')] });
    assert.equal(f.records.length, 1); assert.deepEqual(f.delivered, []);
});
test('Long timestamps and wrapped content are supported; bad history dates are skipped', async () => {
    const f = fixture();
    await f.service.handleHistoryMessages({ messages: [message('bad', { messageTimestamp: undefined }), message('good', { messageTimestamp: { toNumber: () => 1700000001 }, message: { ephemeralMessage: { message: { conversation: 'wrapped' } } } })] });
    assert.equal(f.records.length, 1); assert.equal(f.records[0].timestamp, 1700000001); assert.equal(f.records[0].text, 'wrapped');
});
test('system/status messages are excluded', async () => {
    const f = fixture(); await f.service.handleHistoryMessages({ messages: [message('sys', { message: { protocolMessage: { type: 5 } } }), message('status', { key: { id: 'status', remoteJid: 'status@broadcast' } })] });
    assert.equal(f.records.length, 0);
});
test('group binding applies to both history and live traffic', async () => {
    const f = fixture(); f.service.setGroupBinding('120363012345@g.us');
    await f.service.handleHistoryMessages({ messages: [message('other'), message('group', { key: { id: 'group', remoteJid: '120363012345@g.us' } })] });
    await f.service.handleIncomingMessages({ type: 'notify', messages: [message('other-live')] });
    assert.deepEqual(f.records.map(m => m.id), ['group']); assert.deepEqual(f.delivered, []);
});
test('socket queue serializes history/live and ignores stale sockets', async () => {
    const f = fixture(), listeners = new Map(), removed = [];
    const socket = { ev: { on: (e, fn) => listeners.set(e, fn), removeAllListeners: e => removed.push(e) }, end() {} };
    f.service.setSocket(socket); f.service.registerSocketListeners(socket, {}, async () => {});
    listeners.get('messaging-history.set')({ messages: [message('h')] }); listeners.get('messages.upsert')({ type: 'notify', messages: [message('n')] }); await f.service.messageQueue;
    f.service.cleanupSocket(); listeners.get('messages.upsert')({ type: 'notify', messages: [message('stale')] }); await f.service.messageQueue;
    assert.deepEqual(f.records.map(m => m.id), ['h', 'n']); assert.ok(removed.includes('messaging-history.set'));
});
test('batch storage sorts/deduplicates, retains newest20, atomic file leaves no temp', async () => {
    const { store, paths } = await disk('batch'); const input = Array.from({ length: 30 }, (_, i) => record(String(i), 1700000000 + i)).reverse();
    await store.recordMessages([...input, input[0]]); const h = await store.getConversationHistory(jid);
    assert.equal(h.length, 20); assert.equal(new Set(h.map(m => m.messageId)).size, 20); assert.equal(h.at(-1).messageId, '29'); assert.deepEqual(await readdir(paths.recentsDir), ['recents.json']);
});
test('reads observe external updates; instances serialize modifications', async () => {
    const { store, paths, session } = await disk('fresh'); await store.recordMessage(record('a'));
    const json = JSON.parse(await readFile(paths.recentsPath, 'utf8')); json.messagesBySender['+1234567890'].push({ ...record('external'), timestamp: 1700000001000 }); await writeFile(paths.recentsPath, JSON.stringify(json));
    assert.equal((await store.getConversationHistory(jid)).at(-1).messageId, 'external');
    const second = new RecentsService(session, paths); await second.ensureInitialized(); await Promise.all(Array.from({ length: 8 }, (_, i) => (i % 2 ? store : second).recordMessage(record('parallel-' + i, 1700000010 + i))));
    assert.equal((await store.getConversationHistory(jid)).length, 10);
});
test('corrupt cache is not erased and failed queue recovers', async () => {
    const { store, paths } = await disk('corrupt'); await writeFile(paths.recentsPath, '{bad'); await assert.rejects(store.recordMessage(record('x'))); assert.equal(await readFile(paths.recentsPath, 'utf8'), '{bad');
    await writeFile(paths.recentsPath, JSON.stringify({ conversations: [], messagesBySender: {} })); await store.recordMessage(record('ok')); assert.equal((await store.getConversationHistory(jid)).length, 1);
});
test('old-ID replay preserves original date/direction correction and actual LID JID', async () => {
    const { store } = await disk('replay'); await store.recordMessage(record('old'));
    await store.recordMessage({ ...record('old', 1800000000), direction: 'outgoing', remoteJid: '1234567890@lid' }); const [m] = await store.getConversationHistory(jid);
    assert.equal(m.timestamp, 1700000000000); assert.equal(m.direction, 'outgoing'); assert.equal(m.remoteJid, '1234567890@lid');
});
test('diagnostics distinguish connection and received history from cached messages', async () => {
    const f = fixture(); assert.equal(f.service.getDiagnostics().status, 'disconnected'); f.service.setSocket({}); assert.equal(f.service.getDiagnostics().status, 'connected');
    await f.service.handleIncomingMessages({ type: 'append', messages: [message('a')] }); assert.equal(f.service.getDiagnostics().lastHistoryAt, 0);
    await f.service.handleHistoryMessages({ messages: [message('h')], progress: 100 }); assert.equal(f.service.getDiagnostics().historyMessages, 1); assert.equal(f.service.getDiagnostics().historyProgress, 100);
});
test('history requests preserve true anchor/JID/milliseconds and report requested, not received', async () => {
    const f = fixture(), calls = []; f.service.setSocket({ fetchMessageHistory: async (...args) => { calls.push(args); return 'request'; } });
    const key = { remoteJid: '1234567890@lid', id: 'real-id', fromMe: true }, timestampMs = 1700000000000;
    const r = await f.service.requestHistory({ anchor: { key, timestampMs } }); assert.equal(r.status, 'requested'); assert.equal(r.coverage, 'older_than_anchor_only'); assert.deepEqual(calls, [[50, key, timestampMs]]); assert.equal(f.service.getDiagnostics().lastHistoryAt, 0);
    await assert.rejects(f.service.requestHistory(), /30 seconds/);
});
test('invalid/fake anchors and disconnected history requests fail closed', async () => {
    const f = fixture(); await assert.rejects(f.service.requestHistory(), /not connected/); f.service.setSocket({});
    await assert.rejects(f.service.requestHistory({ anchor: { key: { id: '', remoteJid: jid }, timestampMs: Date.now() } }), /real message/);
    await assert.rejects(f.service.requestHistory({ anchor: { key: { id: 'x', remoteJid: jid }, timestampMs: 1700000000 } }), /milliseconds/);
});
test('phone full-history request is bounded, and history contact names beat own pushName', async () => {
    const f = fixture(), calls = []; f.service.setSocket({ sendPeerDataOperationMessage: async p => { calls.push(p); return 'full'; } });
    assert.equal((await f.service.requestHistory({ days: 2 })).status, 'requested'); assert.equal(calls[0].fullHistorySyncOnDemandRequest.historySyncConfig.fullSyncSizeMbLimit, 20);
    await f.service.handleHistoryMessages({ contacts: [{ id: jid, name: 'Contact' }], messages: [message('out', { key: { id: 'out', remoteJid: jid, fromMe: true }, pushName: 'Self' })] }); assert.equal(f.records[0].pushName, 'Contact');
});
test('verbose console filtering never exposes SessionEntry private key objects', async () => {
    const { installBaileysConsoleFilter } = await jiti.import('../src/services/baileys-console-filter.ts'); const original = console.log, output = []; console.log = (...args) => output.push(args); const restore = installBaileysConsoleFilter(true);
    try { console.log('Closing session:', { currentRatchet: { privKey: 'fake-test-only' } }); assert.equal(output.length, 0); console.log('safe'); assert.equal(output.length, 1); } finally { restore(); console.log = original; }
});
