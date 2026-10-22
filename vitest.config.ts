import { defineConfig } from 'vitest/config';
import { mkdtempSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';

// A number of upstream unit tests instantiate the real SessionManager. Never
// allow those tests to write into the developer's actual WhatsApp user profile.
const testHome = mkdtempSync(join(tmpdir(), 'whatsapp-pi-vitest-'));
process.env.HOME = testHome;
process.env.USERPROFILE = testHome;
process.on('exit', () => {
    try { rmSync(testHome, { recursive: true, force: true }); } catch { /* Windows may still hold a test file. */ }
});

function resolveLocalJsImports() {
    return {
        name: 'resolve-local-js-imports',
        async resolveId(source: string, importer: string | undefined) {
            if (!importer || !source.startsWith('.') || !source.endsWith('.js')) {
                return null;
            }

            const tsSource = `${source.slice(0, -3)}.ts`;
            const resolved = await this.resolve(tsSource, importer, { skipSelf: true });
            return resolved?.id ?? null;
        }
    };
}

export default defineConfig({
    plugins: [resolveLocalJsImports()]
});
