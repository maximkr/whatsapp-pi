type ConsoleMethod = 'log' | 'info' | 'warn' | 'error';

import { t } from '../i18n.js';

const noisyBaileysPatterns = [
    t('baileys.filter.failedDecrypt'),
    t('baileys.filter.sessionError'),
    t('baileys.filter.badMac'),
    t('baileys.filter.closingOpenSession'),
    t('baileys.filter.closingSession'),
    // libsignal logs are English regardless of the selected extension locale.
    'Failed to decrypt message with any known session', 'Session error:',
    'Closing open session', 'Closing session:', 'Removing old closed session'
];

const stringifyConsolePart = (part: unknown): string => {
    if (part instanceof Error) {
        return `${part.name}: ${part.message}\n${part.stack ?? ''}`;
    }

    if (typeof part === 'string') {
        return part;
    }

    try {
        return JSON.stringify(part);
    } catch {
        return String(part);
    }
};

export const shouldSuppressBaileysConsoleMessage = (args: unknown[]): boolean => {
    const message = args.map(stringifyConsolePart).join(' ');
    return noisyBaileysPatterns.some(pattern => message.includes(pattern));
};

export const installBaileysConsoleFilter = (verbose: boolean): (() => void) => {
    const methods: ConsoleMethod[] = ['log', 'info', 'warn', 'error'];
    const originals = new Map<ConsoleMethod, (...args: any[]) => void>();
    const patched = new Map<ConsoleMethod, (...args: any[]) => void>();

    for (const method of methods) {
        const original = console[method].bind(console);
        originals.set(method, original);

        const replacement = (...args: unknown[]) => {
            // Never expose SessionEntry/key material, even in verbose mode.
            const sensitiveDump = args.some(arg => {
                if (typeof arg === 'string') return /Closing (?:open )?session|SessionEntry|Removing old closed session/.test(arg);
                return !!arg && typeof arg === 'object' && ('currentRatchet' in arg || 'privKey' in arg || 'rootKey' in arg);
            });
            if (sensitiveDump || (!verbose && shouldSuppressBaileysConsoleMessage(args))) return;

            original(...args);
        };

        patched.set(method, replacement);
        console[method] = replacement as typeof console[typeof method];
    }

    return () => {
        for (const method of methods) {
            const replacement = patched.get(method);
            const original = originals.get(method);
            if (replacement && original && console[method] === replacement) {
                console[method] = original as typeof console[typeof method];
            }
        }
    };
};
