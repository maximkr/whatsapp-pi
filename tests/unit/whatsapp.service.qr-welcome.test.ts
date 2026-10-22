import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { resetI18n } from '../../src/i18n.ts';

const SELF_JID_NORMALIZED = '5511999998888@s.whatsapp.net';

const baileysMocks = vi.hoisted(() => {
    const sockets: any[] = [];

    const createSocket = () => {
        const handlers = new Map<string, (event: any) => Promise<void>>();
        const socket = {
            handlers,
            user: { id: '5511999998888:0@s.whatsapp.net' },
            sendMessage: vi.fn().mockResolvedValue(undefined),
            sendPresenceUpdate: vi.fn().mockResolvedValue(undefined),
            groupMetadata: vi.fn(),
            ev: {
                on: vi.fn((event: string, handler: (event: any) => Promise<void>) => {
                    handlers.set(event, handler);
                }),
                removeAllListeners: vi.fn()
            },
            end: vi.fn()
        };
        sockets.push(socket);
        return socket;
    };

    return {
        sockets,
        makeWASocket: vi.fn((_config: any) => createSocket()),
        fetchLatestBaileysVersion: vi.fn().mockResolvedValue({ version: [2, 3000, 0] }),
        fetchLatestWaWebVersion: vi.fn().mockResolvedValue({ version: [2, 3000, 0], isLatest: true }),
        makeCacheableSignalKeyStore: vi.fn((_keys: any, _logger: any) => _keys),
        reset() {
            sockets.length = 0;
            this.makeWASocket.mockReset().mockImplementation((_config: any) => createSocket());
            this.fetchLatestBaileysVersion.mockReset().mockResolvedValue({ version: [2, 3000, 0] });
            this.fetchLatestWaWebVersion.mockReset().mockResolvedValue({ version: [2, 3000, 0], isLatest: true });
            this.makeCacheableSignalKeyStore.mockReset().mockImplementation((_keys: any, _logger: any) => _keys);
        }
    };
});

vi.mock('baileys', () => ({
    makeWASocket: baileysMocks.makeWASocket,
    fetchLatestBaileysVersion: baileysMocks.fetchLatestBaileysVersion,
    fetchLatestWaWebVersion: baileysMocks.fetchLatestWaWebVersion,
    makeCacheableSignalKeyStore: baileysMocks.makeCacheableSignalKeyStore,
    DisconnectReason: {
        loggedOut: 401,
        badSession: 500,
        connectionReplaced: 440
    }
}));

const createSessionManager = () => ({
    getAuthState: vi.fn().mockResolvedValue({
        state: { creds: {}, keys: {} },
        saveCreds: vi.fn().mockResolvedValue(undefined)
    }),
    markAuthStateAvailable: vi.fn().mockResolvedValue(undefined),
    getStatus: vi.fn().mockReturnValue('connected'),
    setStatus: vi.fn().mockResolvedValue(undefined),
    deleteAuthState: vi.fn().mockResolvedValue(undefined),
    isAllowed: vi.fn().mockReturnValue(true),
    isConversationAllowed: vi.fn().mockReturnValue(true),
    getAllowedContact: vi.fn().mockReturnValue(undefined),
    getOperatorJid: vi.fn().mockReturnValue(''),
    setOperatorJid: vi.fn().mockResolvedValue(undefined)
});

describe('WhatsAppService QR welcome message', () => {
    let logSpy: ReturnType<typeof vi.spyOn>;

    beforeEach(() => {
        resetI18n();
        baileysMocks.reset();
        vi.spyOn(console, 'error').mockImplementation(() => {});
        logSpy = vi.spyOn(console, 'log').mockImplementation(() => {});
        vi.spyOn(console, 'warn').mockImplementation(() => {});
    });

    afterEach(() => {
        vi.useRealTimers();
        vi.restoreAllMocks();
    });

    it('sends welcome self-message and logs qrConnected after QR pairing', async () => {
        const { WhatsAppService } = await import('../../src/services/whatsapp.service.ts');
        const sessionManager = createSessionManager();
        const service = new WhatsAppService(sessionManager as any);

        await service.start();
        const socket = baileysMocks.sockets[0];

        await socket.handlers.get('connection.update')!({ qr: 'some-qr-string' });
        await socket.handlers.get('connection.update')!({ connection: 'open' });
        await Promise.resolve();

        expect(logSpy).toHaveBeenCalledWith('WhatsApp connected');
        expect(socket.sendMessage).toHaveBeenCalledOnce();
        expect(socket.sendMessage).toHaveBeenCalledWith(
            SELF_JID_NORMALIZED,
            expect.objectContaining({ text: expect.stringContaining('Send me a message') })
        );

        await service.stop();
    });

    it('does not send welcome when connecting from saved credentials (no QR event)', async () => {
        const { WhatsAppService } = await import('../../src/services/whatsapp.service.ts');
        const sessionManager = createSessionManager();
        const service = new WhatsAppService(sessionManager as any);

        await service.start();
        const socket = baileysMocks.sockets[0];

        await socket.handlers.get('connection.update')!({ connection: 'open' });
        await Promise.resolve();

        expect(socket.sendMessage).not.toHaveBeenCalled();
        const welcomeCalls = logSpy.mock.calls.filter(args => args[0] === 'WhatsApp connected');
        expect(welcomeCalls).toHaveLength(0);

        await service.stop();
    });

    it('provides sent-message retry caching alongside the live Web version', async () => {
        const { WhatsAppService } = await import('../../src/services/whatsapp.service.ts');
        const sessionManager = createSessionManager();
        const service = new WhatsAppService(sessionManager as any);
        const sentMessage = { extendedTextMessage: { text: 'hello π' } };

        await service.start();
        const socket = baileysMocks.sockets[0];
        socket.sendMessage.mockResolvedValue({ key: { id: 'MSG123' }, message: sentMessage });
        await socket.handlers.get('connection.update')!({ connection: 'open' });
        await service.sendMessage('5511999998888@s.whatsapp.net', 'hello');

        const config = baileysMocks.makeWASocket.mock.calls[0][0];
        expect(config.version).toEqual([2, 3000, 0]);
        expect(baileysMocks.fetchLatestBaileysVersion).not.toHaveBeenCalled();
        await expect(config.getMessage({ id: 'MSG123' })).resolves.toBe(sentMessage);
        await expect(config.getMessage({ id: 'MSG123', fromMe: false })).resolves.toBeUndefined();
        await expect(config.getMessage({ id: 'unknown' })).resolves.toBeUndefined();

        await service.stop();
    });

    it('expires sent messages and bounds the retry cache', async () => {
        vi.useFakeTimers();
        vi.setSystemTime(1_000);
        const { WhatsAppService } = await import('../../src/services/whatsapp.service.ts');
        const sessionManager = createSessionManager();
        const service = new WhatsAppService(sessionManager as any);

        await service.start();
        const config = baileysMocks.makeWASocket.mock.calls[0][0];
        for (let index = 0; index <= 500; index++) {
            service.cacheSentMessage(`MSG${index}`, { conversation: `${index}` });
        }

        await expect(config.getMessage({ id: 'MSG0' })).resolves.toBeUndefined();
        await expect(config.getMessage({ id: 'MSG500' })).resolves.toEqual({ conversation: '500' });

        vi.setSystemTime(24 * 60 * 60 * 1000 + 1_001);
        await expect(config.getMessage({ id: 'MSG500' })).resolves.toBeUndefined();

        await service.stop();
    });

    it('invalidates cached group metadata when participants change', async () => {
        const { WhatsAppService } = await import('../../src/services/whatsapp.service.ts');
        const sessionManager = createSessionManager();
        const service = new WhatsAppService(sessionManager as any);
        const groupJid = '120363012345@g.us';

        await service.start();
        const socket = baileysMocks.sockets[0];
        socket.groupMetadata.mockResolvedValue({ id: groupJid, subject: 'Group', participants: [] });
        await socket.handlers.get('connection.update')!({ connection: 'open' });
        await service.prepareGroupSession(groupJid);
        await service.prepareGroupSession(groupJid);
        expect(socket.groupMetadata).toHaveBeenCalledOnce();

        await socket.handlers.get('group-participants.update')!({ id: groupJid });
        await service.prepareGroupSession(groupJid);
        expect(socket.groupMetadata).toHaveBeenCalledTimes(2);

        await service.stop();
    });

    it('does not send welcome on a second connection open after initial QR pairing', async () => {
        const { WhatsAppService } = await import('../../src/services/whatsapp.service.ts');
        const sessionManager = createSessionManager();
        const service = new WhatsAppService(sessionManager as any);

        await service.start();
        const socket = baileysMocks.sockets[0];

        await socket.handlers.get('connection.update')!({ qr: 'some-qr-string' });
        await socket.handlers.get('connection.update')!({ connection: 'open' });
        await Promise.resolve();

        expect(socket.sendMessage).toHaveBeenCalledOnce();

        await socket.handlers.get('connection.update')!({ connection: 'open' });
        await Promise.resolve();

        expect(socket.sendMessage).toHaveBeenCalledOnce();

        await service.stop();
    });

    it('session stays connected when sendQrWelcome send fails', async () => {
        const { WhatsAppService } = await import('../../src/services/whatsapp.service.ts');
        const sessionManager = createSessionManager();
        const service = new WhatsAppService(sessionManager as any);

        await service.start();
        const socket = baileysMocks.sockets[0];
        socket.sendMessage.mockRejectedValue(new Error('network error'));

        await socket.handlers.get('connection.update')!({ qr: 'some-qr-string' });
        await socket.handlers.get('connection.update')!({ connection: 'open' });
        await Promise.resolve();

        expect(sessionManager.setStatus).toHaveBeenCalledWith('connected');

        await service.stop();
    });

    it('propagates status persistence failures before reporting connected', async () => {
        const { WhatsAppService } = await import('../../src/services/whatsapp.service.ts');
        const sessionManager = createSessionManager();
        const statusCallback = vi.fn();
        const service = new WhatsAppService(sessionManager as any);
        service.setStatusCallback(statusCallback);
        sessionManager.setStatus.mockRejectedValueOnce(new Error('write failed'));

        await service.start();
        const socket = baileysMocks.sockets[0];

        await expect(socket.handlers.get('connection.update')!({ connection: 'open' }))
            .rejects.toThrow('write failed');
        expect(statusCallback).not.toHaveBeenCalledWith('| WhatsApp: Connected');

        await service.stop();
    });
});
