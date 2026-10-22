import {
    makeWASocket,
    DisconnectReason,
    fetchLatestBaileysVersion,
    fetchLatestWaWebVersion,
    proto,
    makeCacheableSignalKeyStore
} from 'baileys';
import P from 'pino';
import { SessionManager } from './session.manager.js';
import { IncomingMessage, SessionStatus } from '../models/whatsapp.types.js';
import { MessageSender } from './message.sender.js';
import { installBaileysConsoleFilter } from './baileys-console-filter.js';
import { t } from '../i18n.js';
import { appendFileSync } from 'fs';
import { createStoragePaths } from './storage-path.js';
import { extractIncomingText } from './incoming-message.resolver.js';
import type { RecentsService } from './recents.service.js';

const LOG_FILE = createStoragePaths().logPath;
function fileLog(msg: string) {
    try { appendFileSync(LOG_FILE, `[${new Date().toISOString()}] [WhatsApp-Pi] ${msg}\n`); } catch {
        // File logging is best-effort.
    }
}

export interface WhatsAppStartOptions {
    allowPairingOnAuthFailure?: boolean;
}

interface DisconnectPayload {
    error?: unknown;
}

interface ConnectionUpdateEvent {
    connection?: 'close' | 'open' | string;
    lastDisconnect?: DisconnectPayload;
    qr?: string;
    receivedPendingNotifications?: boolean;
    isOnline?: boolean;
    isNewLogin: boolean | undefined;
}

interface IncomingMessageKey {
    id?: string;
    remoteJid?: string;
    fromMe?: boolean;
    participant?: string;
    remoteJidAlt?: string;
}

interface IncomingMessageContextInfo {
    mentionedJid?: string[];
    quotedMessage?: IncomingMessageContent;
    participant?: string;
    stanzaId?: string;
    remoteJid?: string;
}

interface IncomingMessageWithContext {
    contextInfo?: IncomingMessageContextInfo;
}

interface IncomingMessageContent {
    conversation?: string;
    extendedTextMessage?: {
        text?: string;
        contextInfo?: IncomingMessageContextInfo;
    };
    imageMessage?: IncomingMessageWithContext;
    videoMessage?: IncomingMessageWithContext;
    documentMessage?: IncomingMessageWithContext;
    audioMessage?: IncomingMessageWithContext;
    stickerMessage?: IncomingMessageWithContext;
    buttonsMessage?: IncomingMessageWithContext;
    templateMessage?: IncomingMessageWithContext;
}

interface IncomingMessageLike {
    key: IncomingMessageKey;
    message?: IncomingMessageContent;
    pushName?: string;
    messageTimestamp?: number | string | { toNumber(): number };
}

interface ContactMetadata { id: string; name?: string; notify?: string; lid?: string; phoneNumber?: string; }
interface HistoryPayload { messages?: IncomingMessageLike[]; contacts?: ContactMetadata[]; chats?: Array<{ id: string; name?: string }>; progress?: number | null; syncType?: number | null; }

interface MessagesUpsertEvent {
    messages?: IncomingMessageLike[];
    type?: 'notify' | 'append';
    requestId?: string;
}

interface GroupParticipantsUpdateEvent {
    id: string;
}

interface SentMessageLike {
    key?: { id?: string };
    message?: proto.IMessage | null;
}

interface CachedSentMessage {
    message: proto.IMessage;
    expiresAt: number;
}

interface WhatsAppSocketLike {
    user?: { id?: string; lid?: string };
    ev: {
        on(event: 'connection.update', handler: (update: ConnectionUpdateEvent) => void | Promise<void>): void;
        on(event: 'creds.update', handler: () => void | Promise<void>): void;
        on(event: 'messages.upsert', handler: (payload: MessagesUpsertEvent) => void | Promise<void>): void;
        on(event: 'group-participants.update', handler: (payload: GroupParticipantsUpdateEvent) => void | Promise<void>): void;
        on(event: 'messaging-history.set', handler: (payload: HistoryPayload) => void | Promise<void>): void;
        on(event: 'contacts.upsert' | 'contacts.update', handler: (payload: ContactMetadata[]) => void | Promise<void>): void;
        removeAllListeners(event: 'connection.update' | 'creds.update' | 'messages.upsert' | 'group-participants.update' | 'messaging-history.set' | 'contacts.upsert' | 'contacts.update'): void;
    };
    fetchMessageHistory(count: number, key: IncomingMessageKey, timestampMs: number): Promise<string>;
    sendPeerDataOperationMessage(message: unknown): Promise<string>;
    end(reason?: unknown): void;
    logout(): Promise<void>;
    sendMessage(jid: string, content: { text: string }): Promise<SentMessageLike | undefined>;
    sendPresenceUpdate(presence: 'composing' | 'recording' | 'paused', jid: string): Promise<void>;
    readMessages(messages: Array<{ remoteJid: string; id: string; fromMe: boolean }>): Promise<void>;
    groupMetadata(jid: string): Promise<{ id: string; subject: string; participants: Array<{ id: string }> }>;
    groupFetchAllParticipating(): Promise<Record<string, { id: string; subject: string; participants: Array<{ id: string }> }>>;
}

interface LastDisconnectLike {
    error?: unknown;
}

interface BoomLikeError {
    output?: {
        statusCode?: number;
    };
    message?: string;
}

export class WhatsAppService {
    private static readonly INITIAL_RECONNECT_DELAY_MS = 5_000;
    private static readonly MAX_RECONNECT_DELAY_MS = 120_000;
    private static readonly SENT_MESSAGE_TTL_MS = 24 * 60 * 60 * 1000;
    private static readonly MAX_CACHED_SENT_MESSAGES = 500;

    private socket?: WhatsAppSocketLike;
    private sessionManager: SessionManager;
    private recentsService?: RecentsService;
    private messageSender: MessageSender;
    private isReconnecting = false;
    private reconnectAttempts = 0;
    private verboseMode = false;
    private onMessagesRecorded?: (messages: IncomingMessage[]) => void | Promise<void>;
    private messageQueue: Promise<void> = Promise.resolve();
    private readonly deliveredLiveMessages = new Set<string>();
    private readonly contactNames = new Map<string, string>();
    private diagnostics = {
        revision: 'sync-v2-baileys7', pid: process.pid, loadedAt: Date.now(),
        lastConnectedAt: 0, lastUpsertAt: 0, lastHistoryAt: 0,
        receivedMessages: 0, historyMessages: 0, historyProgress: null as number | null,
        webVersion: [] as number[], versionSource: '', lastDisconnectCode: null as number | null,
        lastHistoryRequest: null as { id: string; at: number; coverage: string } | null
    };

    public getDiagnostics() {
        return { ...this.diagnostics, status: this.getEffectiveStatus(), socketPresent: !!this.socket,
            knownContactNames: this.contactNames.size, note: 'cached messages are not proof of current connection or complete history' };
    }

    public async requestHistory(options: { days?: number; count?: number; anchor?: { key: IncomingMessageKey; timestampMs: number } } = {}) {
        if (this.getEffectiveStatus() !== 'connected' || !this.socket) throw new Error('WhatsApp is not connected. Use /whatsapp to connect first.');
        if (this.diagnostics.lastHistoryRequest && Date.now() - this.diagnostics.lastHistoryRequest.at < 30_000) throw new Error('Wait 30 seconds before another history request.');
        let id: string;
        let coverage: string;
        if (options.anchor) {
            const { key, timestampMs } = options.anchor;
            if (!key.id || !key.remoteJid || !/^\d+(?:-\d+)?@(lid|s\.whatsapp\.net|g\.us)$/.test(key.remoteJid) || !Number.isFinite(timestampMs) || timestampMs < 1_000_000_000_000) throw new Error('A real message key/JID and timestamp in milliseconds are required.');
            id = await this.socket.fetchMessageHistory(Math.min(100, Math.max(1, options.count ?? 50)), key, timestampMs);
            coverage = 'older_than_anchor_only';
        } else {
            const days = Math.min(30, Math.max(1, options.days ?? 2));
            id = await this.socket.sendPeerDataOperationMessage({
                peerDataOperationRequestType: proto.Message.PeerDataOperationRequestType.FULL_HISTORY_SYNC_ON_DEMAND,
                fullHistorySyncOnDemandRequest: { requestMetadata: { requestId: `${process.pid}-${Date.now()}` },
                    historySyncConfig: { fullSyncDaysLimit: days, recentSyncDaysLimit: days, fullSyncSizeMbLimit: 20 } }
            });
            coverage = 'phone_history_request_not_guaranteed';
        }
        this.diagnostics.lastHistoryRequest = { id, at: Date.now(), coverage };
        fileLog(`History requested: ${coverage}`);
        return { status: 'requested', requestId: id, coverage, note: 'Request sent to your own primary phone, not delivered history. Keep phone online; support/approval depends on WhatsApp.' };
    }
    private saveCreds?: () => Promise<void>;
    private startPromise?: Promise<void>;
    private restoreBaileysConsoleFilter?: () => void;
    private reconnectTimeout?: ReturnType<typeof setTimeout>;
    private intentionalStop = false;
    private onQRCode?: (qr: string) => void;
    private onMessage?: (m: MessagesUpsertEvent) => void | Promise<void>;
    private onStatusUpdate?: (status: string) => void;
    private lastRemoteJid: string | null = null;
    private qrWasShown = false;
    private boundGroupJid: string | null = null;
    private groupMetadataCache: Map<string, { id: string; subject: string; participants: Array<{ id: string }> }> = new Map();
    private sentMessageCache = new Map<string, CachedSentMessage>();

    constructor(sessionManager: SessionManager) {
        this.sessionManager = sessionManager;
        this.messageSender = new MessageSender(this);
    }

    public setRecentsService(recentsService: RecentsService) {
        this.recentsService = recentsService;
    }

    public setGroupBinding(groupJid: string) {
        this.boundGroupJid = groupJid;
    }

    public getBoundGroupJid(): string | null {
        return this.boundGroupJid;
    }

    public getStatus(): SessionStatus {
        return this.sessionManager.getStatus();
    }

    public getEffectiveStatus(): SessionStatus {
        const status = this.sessionManager.getStatus();
        if (status === 'connected' && !this.socket) {
            return 'disconnected';
        }

        return status;
    }

    public setIncomingMessageRecorder(callback: (message: IncomingMessage) => void | Promise<void>) {
        this.onMessagesRecorded = async messages => {
            for (const message of messages) await callback(message);
        };
    }

    public setMessageRecorder(callback: (messages: IncomingMessage[]) => void | Promise<void>) {
        this.onMessagesRecorded = callback;
    }

    public getSocket(): WhatsAppSocketLike | undefined {
        return this.socket;
    }

    public cacheSentMessage(id: string | undefined, message: proto.IMessage | null | undefined) {
        if (!id || !message) return;

        const now = Date.now();
        this.pruneSentMessageCache(now);
        this.sentMessageCache.delete(id);
        this.sentMessageCache.set(id, {
            message,
            expiresAt: now + WhatsAppService.SENT_MESSAGE_TTL_MS
        });

        while (this.sentMessageCache.size > WhatsAppService.MAX_CACHED_SENT_MESSAGES) {
            const oldestId = this.sentMessageCache.keys().next().value;
            if (!oldestId) break;
            this.sentMessageCache.delete(oldestId);
        }
    }

    private getCachedSentMessage(id: string | null | undefined): proto.IMessage | undefined {
        if (!id) return undefined;

        const now = Date.now();
        this.pruneSentMessageCache(now);
        return this.sentMessageCache.get(id)?.message;
    }

    private pruneSentMessageCache(now: number) {
        for (const [id, cached] of this.sentMessageCache) {
            if (cached.expiresAt > now) continue;
            this.sentMessageCache.delete(id);
        }
    }

    public isVerbose(): boolean {
        return this.verboseMode;
    }

    public setVerboseMode(verbose: boolean) {
        this.verboseMode = verbose;
        if (verbose) {
            this.restoreBaileysConsoleFilter?.();
            this.restoreBaileysConsoleFilter = undefined;
        }
    }

    private normalizeContactNumber(value: string): string {
        if (value.startsWith('+')) {
            return value;
        }

        if (/^\d+$/.test(value)) {
            return `+${value}`;
        }

        return value;
    }

    private normalizeRecipientJid(jid: string): string {
        if (jid.includes('@')) return jid;
        const digits = jid.startsWith('+') ? jid.slice(1) : jid;
        return `${digits}@s.whatsapp.net`;
    }

    public resolveOutboundRecipientJid(recipient: string): string {
        if (SessionManager.isGroupJid(recipient)) {
            return recipient;
        }

        const senderNumber = this.normalizeContactNumber(recipient.split('@')[0]);
        const allowedContact = this.sessionManager.getAllowedContact(recipient)
            ?? this.sessionManager.getAllowedContact(senderNumber);

        if (allowedContact?.sendNumber) {
            return this.normalizeRecipientJid(allowedContact.sendNumber);
        }

        return this.normalizeRecipientJid(recipient);
    }

    private normalizeJidForComparison(jid: string): string {
        const [localPart, domain = ''] = jid.split('@');
        const normalizedLocal = localPart.split(':')[0];
        return domain ? `${normalizedLocal}@${domain}` : normalizedLocal;
    }

    private normalizeJidIdentity(jid: string): string {
        return this.normalizeJidForComparison(jid).split('@')[0];
    }

    private getAgentJidCandidates(): string[] {
        const user = this.socket?.user;
        const rawJids = [user?.id, user?.lid].filter((jid): jid is string => Boolean(jid));
        const candidates = new Set<string>();

        for (const jid of rawJids) {
            const normalized = this.normalizeJidForComparison(jid);
            candidates.add(normalized);
            candidates.add(this.normalizeJidIdentity(jid));
        }

        return [...candidates];
    }

    private getDisconnectStatusCode(error: unknown): number | undefined {
        if (!error || typeof error !== 'object') {
            return undefined;
        }

        const candidate = error as BoomLikeError;
        return candidate.output?.statusCode;
    }

    private getErrorMessage(error: unknown): string {
        if (error instanceof Error) {
            return error.message;
        }

        if (typeof error === 'object' && error !== null && 'message' in error) {
            const candidate = error as { message?: unknown };
            return typeof candidate.message === 'string' ? candidate.message : '';
        }

        return '';
    }

    private clearReconnectTimeout() {
        if (this.reconnectTimeout) {
            clearTimeout(this.reconnectTimeout);
            this.reconnectTimeout = undefined;
        }
    }

    private getReconnectDelayMs(): number {
        const delay = WhatsAppService.INITIAL_RECONNECT_DELAY_MS * (2 ** Math.max(0, this.reconnectAttempts - 1));
        return Math.min(delay, WhatsAppService.MAX_RECONNECT_DELAY_MS);
    }

    private scheduleReconnect(options: WhatsAppStartOptions) {
        if (this.intentionalStop) {
            fileLog('[scheduleReconnect] Skipping - intentional stop');
            return;
        }

        if (this.isReconnecting) {
            fileLog('[scheduleReconnect] Already reconnecting, skipping duplicate schedule');
            return;
        }

        this.isReconnecting = true;
        this.reconnectAttempts++;
        const delay = this.getReconnectDelayMs();
        
        fileLog(`[scheduleReconnect] Scheduling reconnect attempt #${this.reconnectAttempts} in ${delay}ms`);
        
        
        
        this.onStatusUpdate?.(t('service.whatsapp.reconnecting'));
        this.clearReconnectTimeout();
        this.reconnectTimeout = setTimeout(async () => {
            fileLog(`[scheduleReconnect] Executing reconnect attempt #${this.reconnectAttempts}`);
            this.isReconnecting = false;
            
            if (this.intentionalStop) {
                fileLog('[scheduleReconnect] Aborting - intentional stop detected');
                return;
            }
            
            try {
                await this.start(options);
            } catch (error) {
                fileLog(`[scheduleReconnect] Reconnect failed: ${error instanceof Error ? error.message : String(error)}`);
                if (!this.intentionalStop) {
                    this.scheduleReconnect(options);
                }
            }
        }, delay);
    }

    private cleanupSocket() {
        this.clearReconnectTimeout();
        this.groupMetadataCache.clear();

        if (!this.socket) {
            return;
        }

        this.restoreBaileysConsoleFilter?.();
        this.restoreBaileysConsoleFilter = undefined;
        this.socket.ev.removeAllListeners('connection.update');
        this.socket.ev.removeAllListeners('creds.update');
        this.socket.ev.removeAllListeners('messages.upsert');
        this.socket.ev.removeAllListeners('group-participants.update');
        this.socket.ev.removeAllListeners('messaging-history.set');
        this.socket.ev.removeAllListeners('contacts.upsert');
        this.socket.ev.removeAllListeners('contacts.update');

        try {
            this.socket.end(undefined);
        } catch {
            // Best-effort cleanup
        }

        this.socket = undefined;
    }

    private setSocket(socket: WhatsAppSocketLike) {
        this.socket = socket;
    }

    private registerSocketListeners(socket: WhatsAppSocketLike, options: WhatsAppStartOptions, saveCreds: () => Promise<void>) {
        socket.ev.on('creds.update', async () => {
            await saveCreds();
            await this.sessionManager.markAuthStateAvailable();
        });

        socket.ev.on('connection.update', async (update) => {
            await this.handleConnectionUpdate(update, options);
        });

        const enqueue = (work: () => Promise<void>) => {
            this.messageQueue = this.messageQueue.then(async () => {
                if (this.socket === socket) await work();
            }).catch(error => {
                fileLog(`Message import failed: ${this.getErrorMessage(error)}`);
            });
        };
        socket.ev.on('messages.upsert', payload => {
            enqueue(() => this.handleIncomingMessages(payload));
        });
        socket.ev.on('messaging-history.set', payload => {
            enqueue(() => this.handleHistoryMessages(payload));
        });
        socket.ev.on('group-participants.update', ({ id }) => {
            this.groupMetadataCache.delete(id);
        });
        for (const event of ['contacts.upsert', 'contacts.update'] as const) {
            socket.ev.on(event, contacts => {
                for (const contact of contacts) {
                    const name = contact.name || contact.notify;
                    if (contact.id && name) this.contactNames.set(contact.id, name);
                }
            });
        }
    }

    private async createSocket(): Promise<WhatsAppSocketLike> {
        const { state, saveCreds } = await this.sessionManager.getAuthState();
        this.saveCreds = saveCreds;
        let resolved = await fetchLatestWaWebVersion({ signal: AbortSignal.timeout(10_000) });
        this.diagnostics.versionSource = 'whatsapp-web';
        if (!resolved.isLatest) {
            resolved = await fetchLatestBaileysVersion({ signal: AbortSignal.timeout(10_000) });
            this.diagnostics.versionSource = 'baileys-fallback';
        }
        const { version } = resolved;
        this.diagnostics.webVersion = [...version];
        fileLog(`Socket start: sync-v2-baileys7 pid=${process.pid} version=${version.join('.')} source=${this.diagnostics.versionSource}`);

        const logger = P({ level: this.verboseMode ? 'trace' : 'silent' });

        const groupMetadataCache = this.groupMetadataCache;

        const socket = makeWASocket({
            version,
            printQRInTerminal: false,
            auth: {
                creds: state.creds,
                keys: makeCacheableSignalKeyStore(state.keys, logger)
            },
            // Accept history supplied by the phone. Reconnect does not guarantee a full replay.
            syncFullHistory: true,
            // Preserve the default Chrome profile; forcing Desktop caused 428 in live probes.
            markOnlineOnConnect: false,
            logger,
            getMessage: async key => key.fromMe === false ? undefined : this.getCachedSentMessage(key.id),
            cachedGroupMetadata: async (jid: string) => {
                return groupMetadataCache.get(jid) as any;
            }
        }) as WhatsAppSocketLike;

        return socket;
    }

    async start(options: WhatsAppStartOptions = {}) {
        if (this.startPromise) return this.startPromise;

        const startPromise = this.startSocket(options);
        this.startPromise = startPromise;
        try {
            await startPromise;
        } finally {
            if (this.startPromise === startPromise) {
                this.startPromise = undefined;
            }
        }
    }

    private async startSocket(options: WhatsAppStartOptions) {
        fileLog(`[start] Starting WhatsApp service, isReconnecting=${this.isReconnecting}`);
        this.intentionalStop = false;
        if (this.isReconnecting) {
            fileLog('[start] Skipping - reconnect already in progress');
            return;
        }
        this.onStatusUpdate?.(t('service.whatsapp.connecting'));

        this.cleanupSocket();

        const originalConsoleLog = console.log;
        const originalConsoleWarn = console.warn;
        const originalConsoleError = console.error;
        let socketInitialized = false;

        if (!this.verboseMode) {
            console.log = () => {};
            console.warn = () => {};
            console.error = () => {};
        }

        try {
            const socket = await this.createSocket();
            this.setSocket(socket);
            this.registerSocketListeners(socket, options, this.saveCreds ?? (async () => {}));
            socketInitialized = true;
        } catch (error) {
            if (!this.verboseMode) {
                console.log = originalConsoleLog;
                console.warn = originalConsoleWarn;
                console.error = originalConsoleError;
            }
            throw error;
        } finally {
            if (!this.verboseMode) {
                console.log = originalConsoleLog;
                console.warn = originalConsoleWarn;
                console.error = originalConsoleError;
            }
            if (socketInitialized) {
                this.restoreBaileysConsoleFilter = installBaileysConsoleFilter(this.verboseMode);
            }
        }
    }

    private async handleConnectionUpdate(update: ConnectionUpdateEvent, options: WhatsAppStartOptions) {
        const { connection, lastDisconnect, qr } = update;
        const allowPairingOnAuthFailure = options.allowPairingOnAuthFailure ?? false;

        if (this.verboseMode) {
            fileLog(`[connection.update] connection=${connection}, hasDisconnect=${!!lastDisconnect}, qr=${!!qr}, isReconnecting=${this.isReconnecting}`);
        }

        if (qr) {
            await this.handlePairingQr(qr);
            return
        }

        if (connection === 'open') {
            await this.handleConnectionOpen();
            return
        }

        if (connection === 'connecting' ||
            update.receivedPendingNotifications ||
            update.isOnline ||
            update.isNewLogin) {
            return
        }

        if (connection === 'close') {
            await this.handleConnectionClosed(lastDisconnect, allowPairingOnAuthFailure, options);
            return;
        }

        if (this.verboseMode && connection !== undefined) {
            fileLog(`[connection.update] Ignoring unexpected connection state: ${connection}`);
        }
    }

    private async handlePairingQr(qr: string) {
        await this.sessionManager.setStatus('pairing');
        this.onQRCode?.(qr);
        this.onStatusUpdate?.(t('service.whatsapp.typeToConnect'));
        this.qrWasShown = true;
    }

    private async handleConnectionOpen() {
        fileLog('[handleConnectionOpen] Connection established successfully');
        
        
        if (this.verboseMode) {
            console.log(t('service.whatsapp.connectionOpened'));
        }

        this.isReconnecting = false;
        this.reconnectAttempts = 0;
        this.clearReconnectTimeout();
        await this.saveCreds?.();
        await this.sessionManager.markAuthStateAvailable();
        await this.sessionManager.setStatus('connected');
        this.diagnostics.lastConnectedAt = Date.now();
        this.diagnostics.lastDisconnectCode = null;
        fileLog(`Connected: sync-v2-baileys7 pid=${process.pid}`);
        this.onStatusUpdate?.(t('service.whatsapp.connected'));

        if (this.qrWasShown) {
            this.qrWasShown = false;
            console.log(t('service.whatsapp.qrConnected'));
            console.log(t('service.whatsapp.qrWelcomeMessage'));
            void this.sendQrWelcome();
        }
    }

    private async sendQrWelcome(): Promise<void> {
        const rawId = this.socket?.user?.id;
        if (!rawId) return;
        const selfJid = this.normalizeJidForComparison(rawId);
        await this.sessionManager.setOperatorJid(selfJid);
        try {
            await this.socket?.sendMessage(selfJid, { text: t('service.whatsapp.qrWelcomeMessage') });
        } catch {
            // Best-effort — welcome send failure must not abort the session.
        }
    }

    public getOperatorJid(): string {
        return this.sessionManager.getOperatorJid();
    }

    private isBadMacError(errorMessage: string): boolean {
        return errorMessage.includes('Bad MAC');
    }

    private isAuthRejected(statusCode: number | undefined, errorMessage: string): boolean {
        return errorMessage.includes('bad-request')
            || statusCode === 400
            || statusCode === 401
            || statusCode === DisconnectReason.loggedOut;
    }

    private async handleConnectionClosed(
        lastDisconnect: LastDisconnectLike | undefined,
        allowPairingOnAuthFailure: boolean,
        options: WhatsAppStartOptions
    ) {
        const statusCode = this.getDisconnectStatusCode(lastDisconnect?.error);
        this.diagnostics.lastDisconnectCode = statusCode ?? null;
        fileLog(`Connection closed: pid=${process.pid} code=${statusCode ?? 'unknown'} intentional=${this.intentionalStop}`);
        const errorMessage = this.getErrorMessage(lastDisconnect?.error);
        const shouldReconnect = statusCode !== DisconnectReason.loggedOut;
        const isBadMac = this.isBadMacError(errorMessage);
        const isAuthRejected = this.isAuthRejected(statusCode, errorMessage);

        fileLog(`[handleConnectionClosed] statusCode=${statusCode}, errorMessage="${errorMessage}", shouldReconnect=${shouldReconnect}, isBadMac=${isBadMac}, isAuthRejected=${isAuthRejected}, intentionalStop=${this.intentionalStop}, isReconnecting=${this.isReconnecting}`);


        if (this.intentionalStop) {
            if (this.verboseMode) {
                fileLog('[handleConnectionClosed] Skipping - intentional stop');
            }
            return;
        }

        if (this.verboseMode) {
            console.error(t('service.whatsapp.connectionClosed', { statusCode: statusCode ?? 'unknown', shouldReconnect: String(shouldReconnect) }));
        }

        if (isBadMac) {
            if (this.verboseMode) {
                console.error(t('service.whatsapp.badMacDetected'));
                console.error(t('service.whatsapp.runClearAuth'));
            }
            this.cleanupSocket();
            this.isReconnecting = false;
            this.reconnectAttempts = 0;
            this.onStatusUpdate?.(t('service.whatsapp.sessionErrorBadMac'));
            await this.sessionManager.setStatus('disconnected');
            this.onStatusUpdate?.(t('service.whatsapp.disconnected'));
            return;
        }

        if (isAuthRejected) {
            this.cleanupSocket();
            this.isReconnecting = false;
            this.reconnectAttempts = 0;

            if (allowPairingOnAuthFailure) {
                if (this.verboseMode) {
                    console.error(t('service.whatsapp.sessionRejected', { statusCode: statusCode ?? 'unknown' }));
                }
                this.onStatusUpdate?.(t('service.whatsapp.sessionRejected', { statusCode: statusCode ?? 'unknown' }));
                await this.sessionManager.deleteAuthState();
                await this.start({ ...options, allowPairingOnAuthFailure: false });
                return;
            }

            if (this.verboseMode) {
                console.error(t('service.whatsapp.sessionInvalidOrLoggedOut', { statusCode: statusCode ?? 'unknown' }));
            }
            this.onStatusUpdate?.(t('service.whatsapp.sessionInvalidOrLoggedOut', { statusCode: statusCode ?? 'unknown' }));
            await this.sessionManager.setStatus('logged-out');
            this.onStatusUpdate?.(t('service.whatsapp.disconnected'));
            return;
        }

        if (statusCode === DisconnectReason.connectionReplaced) {
            if (this.verboseMode) {
                console.error(t('service.whatsapp.connectionReplaced'));
            }
            this.cleanupSocket();
            this.isReconnecting = false;
            this.reconnectAttempts = 0;
            await this.sessionManager.setStatus('disconnected');
            this.onStatusUpdate?.(t('service.whatsapp.conflict'));
            return;
        }

        if (shouldReconnect) {
            if (this.isReconnecting) {
                fileLog('[handleConnectionClosed] Reconnect already in progress, skipping duplicate');
            } else {
                fileLog('[handleConnectionClosed] Initiating reconnect sequence');
                await this.saveCreds?.();
                this.cleanupSocket();
                this.scheduleReconnect(options);
            }
        } else {
            fileLog('[handleConnectionClosed] Not reconnecting - logged out or permanent disconnect');
            this.reconnectAttempts = 0;
            await this.sessionManager.setStatus('logged-out');
            this.onStatusUpdate?.(t('service.whatsapp.disconnected'));
        }
    }

    private extractText(message: IncomingMessageContent | undefined): string {
        if (!message) return '';
        const resolved = extractIncomingText(message);
        return ['system', 'unsupported', 'reaction'].includes(resolved.kind) ? '' : resolved.text;
    }

    private isPiGeneratedMessage(text: string): boolean {
        return text.endsWith('π');
    }

    private getIncomingTimestamp(timestamp: IncomingMessageLike['messageTimestamp']): number | undefined {
        const value = typeof timestamp === 'object' ? timestamp?.toNumber() : Number(timestamp);
        return Number.isFinite(value) && value! > 0 ? value : undefined;
    }

    private async recordMessageBatch(messages: IncomingMessageLike[]): Promise<void> {
        const records: IncomingMessage[] = [];
        for (const message of messages) {
            const remoteJid = message.key?.remoteJid;
            if (!remoteJid || !message.key.id) continue;
            if (!/@(s\.whatsapp\.net|lid|g\.us)$/.test(remoteJid)) continue;
            if (this.boundGroupJid && remoteJid !== this.boundGroupJid) continue;
            const text = this.extractText(message.message);
            const timestamp = this.getIncomingTimestamp(message.messageTimestamp);
            // Never date old history as "now" when the timestamp is missing/invalid.
            if (!text.trim() || timestamp === undefined) continue;
            const resolved = extractIncomingText(message.message, this.recentsService);
            records.push({ id: message.key.id, remoteJid, fromMe: !!message.key.fromMe,
                pushName: this.contactNames.get(remoteJid) || (message.key.fromMe ? undefined : message.pushName || undefined),
                text, timestamp, quotedMessage: 'quotedMessage' in resolved ? resolved.quotedMessage : undefined });
        }
        if (records.length) await this.onMessagesRecorded?.(records);
    }

    public async handleHistoryMessages(payload: HistoryPayload): Promise<void> {
        // History is storage-only: no read receipts, typing indicators or agent callbacks.
        this.diagnostics.lastHistoryAt = Date.now();
        this.diagnostics.historyMessages += payload.messages?.length ?? 0;
        this.diagnostics.historyProgress = payload.progress ?? null;
        for (const contact of [...(payload.contacts ?? []), ...(payload.chats ?? [])]) {
            const name = contact.name || ('notify' in contact && typeof contact.notify === 'string' ? contact.notify : undefined);
            if (contact.id && name) this.contactNames.set(contact.id, name);
        }
        await this.recordMessageBatch(payload.messages ?? []);
        fileLog(`History batch processed: ${(payload.messages ?? []).length} received`);
    }

    public async handleIncomingMessages(payload: MessagesUpsertEvent) {
        const messages = payload.messages ?? [];
        this.diagnostics.lastUpsertAt = Date.now();
        this.diagnostics.receivedMessages += messages.length;
        await this.recordMessageBatch(messages);
        // append and phone-resend batches must never replay old prompts into the agent.
        if (payload.type !== 'notify' || payload.requestId || this.sessionManager.getStatus() !== 'connected') return;
        for (const message of messages) {
            const remoteJid = message.key?.remoteJid;
            if (!remoteJid || !message.key.id || message.key.fromMe) continue;
            if (!/@(s\.whatsapp\.net|lid|g\.us)$/.test(remoteJid)) continue;
            if (this.boundGroupJid && remoteJid !== this.boundGroupJid) continue;
            const resolved = extractIncomingText(message.message, this.recentsService);
            const text = resolved.kind === 'system' || resolved.kind === 'unsupported' ? '' : resolved.text;
            if (!text || this.isPiGeneratedMessage(text)) continue;
            const deliveryKey = `${remoteJid}:${message.key.id}`;
            if (this.deliveredLiveMessages.has(deliveryKey)) continue;
            this.deliveredLiveMessages.add(deliveryKey);
            if (this.deliveredLiveMessages.size > 2000) {
                this.deliveredLiveMessages.delete(this.deliveredLiveMessages.values().next().value!);
            }
            const isGroup = remoteJid.endsWith('@g.us');
            const senderJid = isGroup ? remoteJid : this.normalizeContactNumber(remoteJid.split('@')[0]);
            const allowed = this.boundGroupJid
                ? this.sessionManager.isAllowedGroup(this.boundGroupJid)
                : this.sessionManager.isConversationAllowed(senderJid);
            if (!allowed) {
                await this.sessionManager.trackIgnoredNumber(senderJid, message.pushName || undefined);
                continue;
            }
            if (isGroup) void this.prepareGroupSession(remoteJid);
            this.lastRemoteJid = remoteJid;
            // The consumer expects one message, so deliver each element separately.
            await this.onMessage?.({ ...payload, messages: [message] });
        }
    }

    setQRCodeCallback(callback: (qr: string) => void) {
        this.onQRCode = callback;
    }

    setMessageCallback(callback: (m: MessagesUpsertEvent) => void | Promise<void>) {
        this.onMessage = callback;
    }

    setStatusCallback(callback: (status: string) => void) {
        this.onStatusUpdate = callback;
    }

    public getLastRemoteJid(): string | null {
        return this.lastRemoteJid;
    }

    private getActiveSocket(): WhatsAppSocketLike | null {
        if (!this.socket || this.getStatus() !== 'connected') {
            return null;
        }

        return this.socket;
    }

    /**
     * Pre-loads group metadata into the cache for Baileys' cachedGroupMetadata.
     * This ensures Baileys can resolve group participants for Signal
     * sender-key encryption, preventing "No sessions" errors.
     */
    public async prepareGroupSession(jid: string): Promise<void> {
        if (!jid.endsWith('@g.us')) return;
        if (this.groupMetadataCache.has(jid)) {
            fileLog(`Group metadata cache HIT for ${jid}`);
            return;
        }
        const socket = this.getActiveSocket();
        if (!socket) return;
        try {
            fileLog(`Fetching group metadata for ${jid}...`);
            const metadata = await socket.groupMetadata(jid);
            this.groupMetadataCache.set(jid, metadata);
            fileLog(`Cached group metadata for ${jid} (${metadata.participants?.length ?? 0} participants)`);
        } catch (error) {
            fileLog(`FAILED to fetch group metadata for ${jid}: ${error instanceof Error ? error.message : String(error)}`);
        }
    }

    async sendMessage(jid: string, text: string) {
        const recipientJid = this.resolveOutboundRecipientJid(jid);

        // Ensure we show the typing indicator before sending
        await this.sendPresence(recipientJid, 'composing');

        const result = await this.messageSender.send({
            recipientJid,
            text: text
        });

        // After sending, we can stop the typing indicator
        await this.sendPresence(recipientJid, 'paused');

        if (!result.success) {
            console.error(t('service.whatsapp.failedSendMessage', { jid: recipientJid, error: result.error ?? t('message.sender.unknownError') }));
        }

        return result;
    }

    async sendMenuMessage(jid: string, text: string) {
        const normalizedJid = this.resolveOutboundRecipientJid(jid);
        const socket = this.getActiveSocket();

        if (!socket) {
            return {
                success: false,
                error: t('service.whatsapp.notConnected'),
                attempts: 0
            };
        }

        try {
            await this.sendPresence(normalizedJid, 'composing');
            const response = await socket.sendMessage(normalizedJid, { text });
            this.cacheSentMessage(response?.key?.id, response?.message);
            await this.sendPresence(normalizedJid, 'paused');

            return {
                success: true,
                messageId: response?.key?.id,
                attempts: 1
            };
        } catch (error: unknown) {
            await this.sendPresence(normalizedJid, 'paused');
            console.error(t('service.whatsapp.failedSendMenuMessage', { jid: normalizedJid }), error);
            return {
                success: false,
                error: error instanceof Error ? error.message : 'Unknown error',
                attempts: 1
            };
        }
    }

    async sendPresence(jid: string, presence: 'composing' | 'recording' | 'paused') {
        const socket = this.getActiveSocket();
        if (!socket) return;
        try {
            await socket.sendPresenceUpdate(presence, jid);
        } catch (error) {
            if (this.verboseMode) {
                console.error(t('service.whatsapp.failedPresenceUpdate', { jid }), error);
            }
        }
    }

    async markRead(jid: string, messageId: string, fromMe: boolean = false) {
        const socket = this.getActiveSocket();
        if (!socket) return;
        try {
            await socket.readMessages([{ remoteJid: jid, id: messageId, fromMe }]);
        } catch (error) {
            if (this.verboseMode) {
                console.error(t('service.whatsapp.failedMarkRead'), error);
            }
        }
    }

    async logout() {
        fileLog('[logout] Logging out - setting intentional stop');
        this.intentionalStop = true;
        await this.socket?.logout();
        this.cleanupSocket();
        this.isReconnecting = false;
        await this.sessionManager.deleteAuthState();
    }

    async stop() {
        fileLog('[stop] Stopping WhatsApp service - setting intentional stop');
        this.intentionalStop = true;
        try {
            await this.saveCreds?.();
        } catch (error) {
            if (this.verboseMode) {
                console.error(t('service.whatsapp.failedPersistAuthState'), error);
            }
            fileLog(`[stop] Failed to save credentials: ${error instanceof Error ? error.message : String(error)}`);
        }

        this.cleanupSocket();
        this.isReconnecting = false;
        await this.sessionManager.setStatus('disconnected');
        this.onStatusUpdate?.(t('service.whatsapp.disconnected'));
    }
}
