/**
 * ESM type declarations. Re-exports from the CommonJS types so both
 *   import setrecon from 'setrecon'
 * and
 *   import { core, sketch, gf64 } from 'setrecon'
 * are typed correctly.
 */
import setrecon = require('./setrecon.js');

export default setrecon;
export const core: typeof setrecon.core;
export const sketch: typeof setrecon.sketch;
export const gf64: typeof setrecon.gf64;

// Re-export all the public types so ESM users can import them by name.
export type Options = setrecon.Options;
export type Session = setrecon.Session;
export type FinishReason = setrecon.FinishReason;
export type FinishStats = setrecon.FinishStats;
export type Algorithm = setrecon.Algorithm;
export type NeedHint = setrecon.NeedHint;
export type MessageData = setrecon.MessageData;
export type Sketch = setrecon.Sketch;
