/**
 * ESM wrapper around setrecon.js
 *
 * The library itself is CommonJS - one file, no build step. This wrapper
 * re-exports it so ES modules get both the default and the named internals,
 * which a bare `require` interop cannot do on its own.
 */

import setrecon from './setrecon.js';

export default setrecon;

export const core = setrecon.core;
export const sketch = setrecon.sketch;
export const gf64 = setrecon.gf64;
