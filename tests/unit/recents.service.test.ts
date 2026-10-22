import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mkdtemp, readFile, rm, writeFile, stat } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { RecentsService } from '../../src/services/recents.service.ts';
import { createStoragePaths } from '../../src/services/storage-path.ts';

// Real temporary files exercise atomic rename and read-refresh semantics. A
// writeFile-only mock cannot represent the persistent store after a reload.
describe('RecentsService', () => {
    let root: string;
    let paths: ReturnType<typeof createStoragePaths>;
    let service: RecentsService;
    const sessionManager = { isConversationAllowed: vi.fn((number: string) => number === '+5511999998888') };
    const entry = (id: string, timestamp = 1700000000, extra = {}) => ({ messageId: id, senderNumber: '+5511999998888', text: id, direction: 'incoming' as const, timestamp, ...extra });
    beforeEach(async () => {
        root = await mkdtemp(join(tmpdir(), 'wa-recents-unit-'));
        paths = createStoragePaths(root, join(root, 'no-legacy'));
        service = new RecentsService(sessionManager as any, paths);
        await service.ensureInitialized();
    });
    afterEach(async () => { await rm(root, { recursive: true, force: true }); });

    it('initializes an empty store when no recents file exists', async () => {
        expect(await service.getRecentConversations()).toEqual([]);
        expect((await stat(paths.recentsDir)).isDirectory()).toBe(true);
    });
    it('records normalized messages and conversation summaries', async () => {
        await service.recordMessage(entry('MSG1', 1700000000, { senderNumber: '5511999998888@s.whatsapp.net', senderName: 'Ana', text: '  hello 👋\nthere 😊  ' }));
        const persisted = JSON.parse(await readFile(paths.recentsPath, 'utf8'));
        expect(persisted.messagesBySender['+5511999998888'][0]).toEqual({ messageId: 'MSG1', senderNumber: '+5511999998888', text: 'hello there', direction: 'incoming', timestamp: 1700000000000 });
        expect(await service.getRecentConversations()).toEqual([expect.objectContaining({ senderNumber: '+5511999998888', senderName: 'Ana', lastMessagePreview: 'hello there', isAllowed: true })]);
    });
    it('deduplicates messages by id and keeps history sorted', async () => {
        await service.recordMessage(entry('MSG1', 1700000002, { text: 'first' }));
        await service.recordMessage(entry('MSG1', 1700000000, { text: 'edited' }));
        await service.recordMessage(entry('MSG2', 1700000001, { text: 'second', direction: 'outgoing' }));
        expect(await service.getConversationHistory('+5511999998888')).toEqual([
            expect.objectContaining({ messageId: 'MSG1', text: 'edited', timestamp: 1700000000000 }),
            expect.objectContaining({ messageId: 'MSG2', text: 'second', timestamp: 1700000001000 })
        ]);
    });
    it('ignores messages that become empty after stripping special characters', async () => {
        await service.recordMessage(entry('MSG1', 1700000000, { text: '😀👍' }));
        await expect(stat(paths.recentsPath)).rejects.toMatchObject({ code: 'ENOENT' });
        expect(await service.getRecentConversations()).toEqual([]);
    });
    it('strips emoji components without dropping adjacent text', async () => {
        await service.recordMessage(entry('MSG1', 1700000000, { text: '\u2708\uFE0F Boarding \u{1F1E7}\u{1F1F7} now' }));
        expect((await service.getConversationHistory('+5511999998888'))[0].text).toBe('Boarding now');
    });
    it('orders conversations by their latest message time', async () => {
        await service.recordMessage(entry('MSG1', 1700000000, { senderNumber: '+5511000000001', text: 'older' }));
        await service.recordMessage(entry('MSG2', 1700000001, { senderNumber: '+5511000000002', text: 'newer' }));
        expect(await service.getRecentConversations()).toEqual([
            expect.objectContaining({ senderNumber: '+5511000000002', lastMessagePreview: 'newer' }),
            expect.objectContaining({ senderNumber: '+5511000000001', lastMessagePreview: 'older' })
        ]);
    });
    it('loads and normalizes existing recents from disk', async () => {
        await writeFile(paths.recentsPath, JSON.stringify({ conversations: [{ senderNumber: '+5511999998888', senderName: 'Ana' }], messagesBySender: { '+5511999998888': [entry('bad', 1700000000, { text: '   ' }), entry('MSG1', 1700000000, { text: 'loaded' })] } }));
        expect(await service.getRecentConversations()).toEqual([expect.objectContaining({ senderName: 'Ana', lastMessagePreview: 'loaded', messageCount: 1 })]);
    });
    describe('findMessageById', () => {
        it('finds a message by its ID across all conversations', async () => {
            await service.recordMessages([entry('MSG1'), entry('MSG2', 1700000001, { text: 'Second message', direction: 'outgoing' }), entry('MSG3', 1700000002, { senderNumber: '+5511000000001' })]);
            expect(service.findMessageById('MSG2')).toMatchObject({ messageId: 'MSG2', text: 'Second message', direction: 'outgoing', timestamp: 1700000001000 });
        });
        it('returns undefined when message ID is not found', async () => {
            await service.recordMessage(entry('MSG1')); expect(service.findMessageById('NONEXISTENT')).toBeUndefined();
        });
        it('returns undefined when no messages exist', () => { expect(service.findMessageById('MSG1')).toBeUndefined(); });
        it('finds messages across multiple conversations', async () => {
            await service.recordMessages([entry('MSG1', 1700000000, { text: 'Conversation A' }), entry('MSG2', 1700000001, { senderNumber: '+5511000000001', text: 'Conversation B' }), entry('MSG3', 1700000002, { senderNumber: '+5511000000002', text: 'Conversation C' })]);
            expect(['MSG1', 'MSG2', 'MSG3'].map(id => service.findMessageById(id)?.text)).toEqual(['Conversation A', 'Conversation B', 'Conversation C']);
        });
    });
});
