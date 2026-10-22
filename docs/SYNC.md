# Sync reliability fork

This fork preserves the upstream project and its MIT license declaration. It is not affiliated with WhatsApp/Meta. It uses an unofficial companion-device client; WhatsApp can change protocol behavior or restrict clients. Do not promise complete message recovery.

## What changes

- Pin Baileys7.0.0-rc14 and resolve the current WhatsApp Web version, with a fallback.
- Process every message in an upsert batch; preserve incoming/outgoing direction and real remote JIDs, including LIDs.
- Persist `messaging-history.set`, offline append and phone-resend traffic without triggering the agent, read receipts or typing indicators.
- Keep only live allowed incoming notify traffic eligible for agent prompts. Duplicate notify IDs within a runtime are not delivered twice.
- Write history batches atomically and serialize cache operations within one process. Reads refresh from disk. Corrupt/unreadable JSON is not silently overwritten.
- Preserve the earliest known timestamp for an existing message ID. A phone replay carrying today's delivery timestamp is not evidence that an old message is new.
- Keep upstream sent-message retry caching, group-metadata invalidation, quoted-message parsing and live reaction context.
- Suppress raw libsignal session/key dumps even with verbose logging. Do not deliberately print credentials or QR payload strings for debugging.

## Connection versus cached history

Use `/whatsapp status` or the `get_wa_status` tool. They report the loaded revision, process ID, real socket state, last connection/event times, message counters, history progress and last history request. A successful history query only proves access to the local cache, not an active connection or complete coverage.

Only one process should actively use a given auth directory. The file-operation queue is in-process, not a cross-process lock. Concurrent clients can replace one another or race on encryption state.

## Request history

Keep the primary phone online and use `/whatsapp sync` or `request_wa_history`.

- Without an anchor, a bounded recent/full-history request is sent to your own primary device. WhatsApp may decline it or not support it for this account.
- With an anchor, supply the actual message ID, exact chat JID, fromMe flag and timestamp in **milliseconds**. The request fetches only messages older than that real anchor. It cannot recover newer messages by inventing an empty ID or reinterpreting LID digits as a phone number.
- `status: requested` is NOT `history received`. Check status/event counters and the actual returned messages afterwards.
- Requests have a cooldown. Do not repeatedly reconnect or flood the phone hoping to force sync.

## Missing initial history

An already linked device may not replay its previously processed initial history after a reconnect. Correcting the event handler cannot force recovery of data it never receives. If normal requests do not deliver the needed range, a separately approved new QR linking may be required to receive initial history.

Never delete auth automatically to test this. Preserve the existing directory privately, use a separate auth directory for a passive verification client, verify the actual target conversation, stop that client, then deliberately activate the verified credentials. A successful QR scan may require a normal515 restart; a401 means that attempt was rejected, not that history was synced.

A privacy-preserving passive migration check confirmed current messages after new linking. This does not guarantee full-history recovery for other accounts, platforms or existing links.

## Storage and privacy

Runtime data stays outside the source checkout, under `~/.pi/agent/extensions/whatsapp-pi/`, with upstream legacy migration support. The recent cache retains the original limits:20 messages per conversation and20 conversation summaries. It is not a full WhatsApp backup.

Never commit or upload:

- auth directories, credential/key files, config containing API keys;
- conversation databases, media, QR payloads/images/pages;
- debug logs, diagnostic snapshots containing personal data, credential backups;
- node_modules or generated archives containing runtime state.

`.gitignore` is defense-in-depth, not a substitute for reviewing the exact staged files and package contents. Tests use synthetic IDs and a temporary HOME; they do not connect to WhatsApp or use your real account.

## Development and updates

```sh
npm install
npm run test:sync
npm run typecheck
npm test
```

The sync suite is a Node test runner using jiti and a temporary HOME/USERPROFILE.
Host Pi packages are peer dependencies. Do not bundle a second host runtime.

Use git tags or full commit IDs in Pi settings:

```sh
pi install git:github.com/maximkr/whatsapp-pi@v1.0.72-sync.2
```

A full commit ID is the strongest pin. Ordinary package updates reconcile that checkout without advancing its configured ref. New fixes should be committed and tested in this fork, released under a new tag, then installed intentionally. Switching between npm and git sources must leave exactly one active WhatsApp-Pi package entry.
