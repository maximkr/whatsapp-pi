import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { mkdtemp, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { SessionManager } from '../../src/services/session.manager.ts';
import { WhatsAppService } from '../../src/services/whatsapp.service.ts';

// Filtering fixtures represent real Baileys live notify events: ID, timestamp,
// type and awaited async delivery. Every case has a separate temporary profile.
describe('WhatsAppService Filtering', () => {
    let service: WhatsAppService, session: SessionManager, root: string;
    let sequence = 0;
    const originalHome = process.env.HOME, originalProfile = process.env.USERPROFILE;
    const group = '120363012345@g.us', pn = '1234567890@s.whatsapp.net';
    const event = (remoteJid = pn, extra: any = {}) => ({ type: 'notify' as const, messages: [{ key: { remoteJid, id: `TEST-${++sequence}`, ...extra.key }, message: extra.message ?? { conversation: 'Hello' }, messageTimestamp: 1700000000, pushName: 'Test' }] });
    beforeEach(async () => {
        root = await mkdtemp(join(tmpdir(), 'wa-filter-unit-'));
        process.env.HOME = root; process.env.USERPROFILE = root;
        session = new SessionManager(root, root); await session.ensureInitialized();
        service = new WhatsAppService(session);
    });
    afterEach(async () => { process.env.HOME = originalHome; process.env.USERPROFILE = originalProfile; await rm(root, { recursive: true, force: true }); });
    const connected = async () => { await session.setStatus('connected'); };
    const allowGroup = async () => { await connected(); await session.addAllowedGroup(group); };
    const spy = () => { const callback = vi.fn(); service.setMessageCallback(callback); return callback; };

    it('should only process messages if status is connected', async () => {
        const callback = spy(); await session.setStatus('disconnected'); await service.handleIncomingMessages(event()); expect(callback).not.toHaveBeenCalled();
    });
    it('should report disconnected effective status when persisted status is connected but socket is absent', async () => {
        await connected(); expect(service.getStatus()).toBe('connected'); expect(service.getEffectiveStatus()).toBe('disconnected');
    });
    it('should only process messages if sender is in allow list', async () => {
        const callback = spy(); await connected(); await session.addNumber('+1234567890');
        await service.handleIncomingMessages(event()); await service.handleIncomingMessages(event('0987654321@s.whatsapp.net')); expect(callback).toHaveBeenCalledTimes(1);
    });
    it('should resolve an allowed contact JID to its configured send phone number for outbound replies', async () => {
        await session.addNumber('+1234567890'); await session.setContactSendNumber('+1234567890', '+5511999998888'); expect(service.resolveOutboundRecipientJid(pn)).toBe('5511999998888@s.whatsapp.net');
    });
    it('should resolve an allowed contact LID to its configured send phone number for outbound replies', async () => {
        await session.addNumber('1234567890@lid'); await session.setContactSendNumber('1234567890@lid', '+5511999998888'); expect(service.resolveOutboundRecipientJid('1234567890@lid')).toBe('5511999998888@s.whatsapp.net');
    });
    it('should keep group reply targets as group JIDs', () => { expect(service.resolveOutboundRecipientJid(group)).toBe(group); });
    it('should store own fromMe messages without prompting the agent', async () => {
        const callback = spy(), recorder = vi.fn(); service.setMessageRecorder(recorder); await connected(); await session.addNumber('+1234567890');
        await service.handleIncomingMessages(event(pn, { key: { fromMe: true } })); expect(recorder).toHaveBeenCalledTimes(1); expect(callback).not.toHaveBeenCalled();
    });
    it('should ignore own Pi-branded messages for agent delivery', async () => {
        const callback = spy(); await connected(); await session.addNumber('+1234567890'); await service.handleIncomingMessages(event(pn, { key: { fromMe: true }, message: { conversation: 'Testing Pi π' } })); expect(callback).not.toHaveBeenCalled();
    });
    describe('Group Binding', () => {
        it('should process group messages when bound to that group', async () => {
            const callback = spy(); await allowGroup(); service.setGroupBinding(group); await service.handleIncomingMessages(event(group)); expect(callback).toHaveBeenCalledTimes(1);
        });
        it('should ignore group messages from a different group', async () => {
            const callback = spy(); await allowGroup(); service.setGroupBinding(group); await service.handleIncomingMessages(event('120363099999@g.us')); expect(callback).not.toHaveBeenCalled();
        });
        it('should ignore individual messages when a group is bound', async () => {
            const callback = spy(); await allowGroup(); service.setGroupBinding(group); await service.handleIncomingMessages(event(pn)); expect(callback).not.toHaveBeenCalled();
        });
        it('should not process group traffic when no binding and group is not allowed', async () => {
            const callback = spy(); await connected(); await service.handleIncomingMessages(event(group)); expect(callback).not.toHaveBeenCalled();
        });
        it('should process group messages when group JID is allowed with no binding', async () => {
            const callback = spy(); await allowGroup(); await service.handleIncomingMessages(event(group)); expect(callback).toHaveBeenCalledTimes(1);
        });
        it('should set lastRemoteJid to the group JID for group messages', async () => {
            await allowGroup(); service.setGroupBinding(group); await service.handleIncomingMessages(event(group)); expect(service.getLastRemoteJid()).toBe(group);
        });
        it('should require the bound group to be in allowed groups', async () => {
            const callback = spy(); await connected(); service.setGroupBinding(group); await service.handleIncomingMessages(event(group)); expect(callback).not.toHaveBeenCalled();
            await session.addAllowedGroup(group); await service.handleIncomingMessages(event(group)); expect(callback).toHaveBeenCalledTimes(1);
        });
        it('should process allowed group messages without passive gating', async () => {
            const callback = spy(); await allowGroup(); (service as any).socket = { user: { lid: 'bot@lid' }, groupMetadata: vi.fn().mockResolvedValue({ id: group, subject: 'Team', participants: [] }) }; await service.handleIncomingMessages(event(group)); expect(callback).toHaveBeenCalledTimes(1);
        });
        it('should process image captions in allowed groups', async () => {
            const callback = spy(); await allowGroup(); await service.handleIncomingMessages(event(group, { message: { imageMessage: { caption: 'Photo', contextInfo: { mentionedJid: ['bot@s.whatsapp.net'] } } } })); expect(callback).toHaveBeenCalledTimes(1);
        });
        it('should allow group messages even when no agent candidates exist', async () => {
            const callback = spy(); await allowGroup(); (service as any).socket = { user: undefined, groupMetadata: vi.fn().mockResolvedValue({ id: group, subject: 'Team', participants: [] }) }; await service.handleIncomingMessages(event(group, { message: { extendedTextMessage: { text: 'hello', contextInfo: { mentionedJid: [] } } } })); expect(callback).toHaveBeenCalledTimes(1);
        });
    });
});
