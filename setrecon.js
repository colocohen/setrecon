/**
 * setrecon  -  set reconciliation, with no transport
 *
 * Two peers each hold a list of numeric IDs up to 2^53. This works out which
 * IDs each is missing while moving as few bytes as it can, and never touches
 * a socket:
 *
 *     var s = setrecon({ items: [1001, 1002, 8871] });
 *     s.on('send', function (type, data) { net.send(peer, type, encode(data)); });
 *     s.on('finish', function (reason, mine, theirs) { ... });
 *     s.start();
 *
 * ==================== WHEN THIS IS THE WRONG TOOL ====================
 *
 * If one side already knows what the other is missing, send it. A server with
 * an ordered log and a version number per client needs none of this - "I am
 * at 4231" and "here are the next twelve" is cheaper and simpler, and it is
 * what WhatsApp's app state sync does.
 *
 * This is for the case where NOBODY holds the authoritative order: two peers
 * meeting after both changed, a mempool, a device coming back from offline
 * into a network with no central copy.
 *
 * ==================== WHY THE TRANSPORT IS ABSENT ====================
 *
 * Because the interesting question is which ALGORITHM to use, and answering
 * it means running thousands of trials against synthetic peers - which is
 * only possible with no socket involved. With a transport baked in, a
 * simulation measures the network instead of the decision.
 *
 * The same absence makes it work over WebRTC, WebSockets, a relay, or two
 * objects in one process, without knowing which.
 *
 * ==================== IT DOES NOT SERIALISE ====================
 *
 * 'send' hands over a type string and a plain object of number arrays. The
 * caller encodes it however the rest of their system does and hands the
 * decoded object back to feed(). Anything else would put a second wire
 * format inside a system that already has one.
 *
 * The cost is that byte counts here are ESTIMATES - the library predicts what
 * its messages weigh in order to choose between algorithms, and cannot know
 * what a caller's encoder produces. `overhead` corrects a consistent
 * difference; a caller who counts real bytes in 'send' can compare.
 *
 * ==================== NO ORDER IS ASSUMED ====================
 *
 * The transport has to deliver reliably, without duplicates, and reassemble
 * long messages. It does NOT have to preserve order, and this does not assume
 * it does: a small message really can overtake a large one from the same
 * sender, so anything arriving before its prerequisite is held and replayed.
 *
 * Loss is the one thing that is not handled - there is no retransmission
 * here, so a dropped message stalls the session until its timeout.
 *
 * ==================== ONE SESSION, ONE PEER ====================
 *
 * A session has exactly one counterparty, so no peer identifier appears
 * anywhere in this file. Which peer a session belongs to is the caller's
 * bookkeeping, and giving the library an ID it would only echo back is an
 * invitation to make it mean something later.
 *
 * ======================== WHAT IS IN HERE ========================
 *
 *   1. Hashing and fingerprints
 *   2. IBLT            - invertible Bloom lookup table
 *   3. Strata          - estimates the difference before sizing an IBLT
 *   4. Buckets         - hash-bucket comparison, two phases
 *   5. The decider     - picks between them on predicted bytes
 *   6. The session     - who sends what and when
 *   7. GF(2^64)        - finite field, for the sketch below
 *   8. BCH sketch      - optional: far fewer bytes, far more CPU
 *
 * Parts 7 and 8 are only reached through sketchCapacity, and a caller who
 * never sets it never runs them.
 */

'use strict';


/* =======================================================================
 * 1-5. Algorithms: hashing, IBLT, Strata, buckets, and the decider
 * ======================================================================= */

// =========================================================================
// Constants — algorithm sizing (calibrated in list_sync_sim.js)
// =========================================================================

var DEFAULT_TIMEOUT = 30000;   // ms — overall sync deadline

/*
 * WAITING ON A SLOW APPLICATION.
 *
 * When the list is loaded lazily, the application may take a long time to
 * answer a 'need'. There is no separate application-response timeout: the
 * ONE deadline above governs, and a slow load is covered by extending it.
 *
 * After HOLD_GRACE of silence this side starts sending 'hold', which asks
 * the far side to extend its own deadline by HOLD_EXTRA_MS. Extensions are
 * bounded by HOLD_MAX_TOTAL in both directions, so an application that never
 * answers - and a peer that holds forever - still fail rather than hang.
 *
 * (An APP_RESPONSE_TIMEOUT constant used to be declared here and described
 * at length, but nothing ever read it. The bound that actually applies is
 * HOLD_MAX_TOTAL, so the dead constant is gone and this says what happens.)
 */
var HOLD_GRACE     = 2000;     // ms — start sending holds after this
var HOLD_EXTRA_MS  = 10000;    // ms — each hold asks for this much extra
var HOLD_MAX_TOTAL = 60000;    // ms — ceiling on total extension, per side

/*
 * How many items a peer asks for at a time.
 *
 * FOUR THOUSAND, which is about 32 KB of identifiers - the same size as the
 * largest message the library already sends and cannot split, the Strata
 * estimate at 30 KB. Splitting below that would only mean more round trips
 * for a message that was never the biggest thing on the wire.
 *
 * Counted in items rather than bytes, because this library does not
 * serialise and has no idea what a caller's encoder makes of a number. A
 * fatter encoding means a proportionally bigger message, and a caller with a
 * transport limit lowers this themselves.
 */
var DEFAULT_CHUNK = 4000;            // items — not ms; see the note above

// IBLT: cells = max(24, 2·d + 16), k = 4 for d≤200, k = 3 for d>200
var IBLT_HEDGE       = 2.0;
var IBLT_PAD         = 16;
var IBLT_MIN_CELLS   = 24;
// Note: earlier design had an IBLT_MAX_RETRIES that would double the cell
// count on decode failure. That's not workable here because the responder
// (not the initiator) sizes the IBLT — the initiator can't retry without
// a full round-trip. Instead the initiator falls back to FULL_DUMP on
// decode failure (see the LIST_IBLT handler); always converges.

// Strata Estimator
var STRATA_CELLS       = 80;
var STRATA_K           = 4;
var STRATA_CORRECTION  = 1.39;
var STRATA_MIN_LEVELS  = 8;
var STRATA_MAX_LEVELS  = 25;

// Threshold: Strata's fixed cost is worth paying when the potential
// full-dump savings exceed 4× it.
var STRATA_BUDGET_FRACTION = 0.25;

/*
 * Seeds live with the 32-bit hash, as S32_* below — they must match on both
 * sides, and there is exactly one set of them.
 *
 * A parallel set of BigInt seeds (SEED_ROOT, SEED_IBLT_IX, SEED_IBLT_FP,
 * SEED_STRATA) used to be declared here, left over from when hashing went
 * through splitmix64. Nothing read them once hash32 took over, and keeping
 * two sets of seeds where one is live is how a seed mismatch gets shipped.
 */


// =========================================================================
// Hashing — splitmix64 (BigInt) + Number-safe fingerprint
// =========================================================================
//
// splitmix64 is a fast, high-quality 64-bit mixer. We use it as both:
//   - the item hash (XOR-combined into a fingerprint)
//   - the IBLT/Strata bucket assignment function
//
// Everything user-facing (items, fingerprint, wire types) is 53-bit Number.
// Internal math uses BigInt for 64-bit correctness — a plain XOR of two
// Numbers > 32 bits silently coerces to int32 in JavaScript.
//
// Fingerprint model (app-visible, spelled out in the header docstring):
//   fingerprint = XOR over all items of h(item, SEED_ROOT)
//   truncated to 53 bits.
// Self-inverse under XOR, so the app can maintain it in O(1) per add/remove:
//   addItem(x)    → fp ^= h(x, SEED_ROOT)
//   removeItem(x) → fp ^= h(x, SEED_ROOT)   // same op undoes it
// =========================================================================

var MASK53 = 0x1FFFFFFFFFFFFFn;   // 53-bit mask
var MASK64 = 0xFFFFFFFFFFFFFFFFn;
var GOLDEN = 0x9E3779B97F4A7C15n;
var MIX1   = 0xBF58476D1CE4E5B9n;
var MIX2   = 0x94D049BB133111EBn;

function splitmix64(x) {
    var z = (x + GOLDEN) & MASK64;
    z = ((z ^ (z >> 30n)) * MIX1) & MASK64;
    z = ((z ^ (z >> 27n)) * MIX2) & MASK64;
    return z ^ (z >> 31n);
}

/* =========================================================================
 * The 32-bit hash
 *
 * splitmix64 needs 64-bit arithmetic, which in JavaScript means BigInt, which
 * means an allocation per operation - about thirteen per call. Hashing was
 * the single largest cost of a large sync, and 8 peers reconciling 50,000
 * items every two seconds did not fit in one core because of it.
 *
 * Items are at most 2^53, so they split into a 32-bit low half and a 21-bit
 * high half with nothing lost. Each half goes through a murmur3 finalizer and
 * the two are folded into each other three times. Two folds leave the halves
 * partly independent, which shows up immediately as correlation between the
 * cell indices drawn from them - and correlated indices are the failure that
 * matters here, because an IBLT decodes by finding cells holding exactly one
 * item and a hash that clusters leaves none.
 *
 * That failure is silent: nothing throws, the sync just falls back to
 * something more expensive. So the replacement was checked against the old
 * one on every property its callers depend on before it went in - avalanche
 * (31.99 against an ideal 32.00), independence of the four 16-bit windows
 * (1.26% agreement against 1.25% expected), cell spread (2.03% against
 * 2.35%), trailing-zero distribution across ten strata levels (worst 0.77%),
 * and fingerprint collisions at 400,000 items (none). It is 9.6x faster.
 * ======================================================================= */

function mix32(h) {
    h ^= h >>> 16;
    h = Math.imul(h, 0x85ebca6b);
    h ^= h >>> 13;
    h = Math.imul(h, 0xc2b2ae35);
    h ^= h >>> 16;
    return h >>> 0;
}

/** Returns [lo, hi] as unsigned 32-bit words. */
function hash32(item, seedLo, seedHi) {
    var lo = (item % 4294967296) >>> 0;
    var hi = Math.floor(item / 4294967296) >>> 0;
    var a = (lo ^ seedLo) >>> 0;
    var b = (hi ^ (seedHi || 0) ^ 0x9e3779b9) >>> 0;
    a = mix32(a ^ b);
    b = mix32(b ^ a);
    a = mix32(a ^ b);
    return [a >>> 0, b >>> 0];
}

/* Seeds, as 32-bit pairs. Distinct per purpose for the same reason the
   BigInt seeds were: the IBLT's cell choice and its fingerprint must not
   agree, or a cell would verify itself. */
var S32_ROOT   = [0x00000001, 0x00000000];
var S32_IBLT_IX = [0x1B17C0DE, 0x00000000];
var S32_IBLT_FP = [0xC4B5A697, 0xF1A6E2D3];
var S32_STRATA = [0x00000005, 0x00000000];
var S32_BUCKET = [0x9E3779B9, 0x00000000];

/*
 * The splitmix64 item hash, keyed by a BigInt seed. Returns a BigInt in
 * [0, 2^64).
 *
 * NOTHING INSIDE THE LIBRARY USES THIS ANY MORE. Every live hash - the
 * fingerprint, the IBLT indices, the strata levels, the bucket assignment -
 * goes through hash32, which is 9.6x faster. This is kept only because it was
 * exported and something may still call it, and because gf64's reference
 * implementation is checked against the same BigInt arithmetic.
 *
 * It is NOT interchangeable with hashItem53: they are different functions
 * over different seeds and do not agree. To keep a fingerprint current, use
 * hashItem53, which is the one computeFingerprint is built from.
 */
function hashItem64(item, seedBig) {
    return splitmix64(BigInt(item) ^ seedBig);
}

// 53-bit Number version — safe to store in caller's state and pass through
// JSON / DB / wire without BigInt conversion pain.

// Compute the fingerprint the app would compute — used when the caller
// didn't supply one. O(N), one splitmix per item, no allocations beyond the
// running BigInt accumulator.
/**
 * The fingerprint: an XOR of per-item hashes.
 *
 * Order-independent, and updatable in place - adding an item and removing it
 * are the same line - which is what lets a caller keep one current without
 * ever recomputing it.
 *
 * A DUPLICATE ITEM CANCELS ITSELF, and that is not a quirk to shrug at.
 * [1,2,3,3] and [1,2,5,5] have the same length and the same fingerprint, so
 * they look identical and the handshake ends reporting no differences - a
 * silently wrong answer, not a failure. This takes SETS; a list with
 * repeats is not one.
 *
 * The check below costs a hash table the size of the input. It is worth it,
 * because the alternative is a wrong answer that nothing downstream can
 * detect. A caller who has already guaranteed uniqueness can pass their own
 * fingerprint and skip this entirely.
 */
function computeFingerprint(items) {
    var lo = 0, hi = 0;
    var seen = Object.create(null);
    for (var i = 0; i < items.length; i++) {
        if (seen[items[i]]) {
            throw new Error('setrecon: duplicate item ' + items[i] +
                            ' - this takes sets, and a repeat cancels itself ' +
                            'in the fingerprint');
        }
        seen[items[i]] = 1;
        var h = hash32(items[i], S32_ROOT[0], S32_ROOT[1]);
        lo = (lo ^ h[0]) >>> 0;
        hi = (hi ^ h[1]) >>> 0;
    }
    return fp53(lo, hi);
}

/*
 * WHAT COUNTS AS AN ITEM, checked once on the way in.
 *
 * An item is split into a 32-bit low half and a 21-bit high half wherever it
 * is stored in a cell - see cellItem - and rebuilt from those halves when it
 * comes back off the wire. Both halves are taken with `>>> 0`, which discards
 * a fractional part and wraps anything at or above 2^53.
 *
 * So an item outside the contract does not fail: it comes back CHANGED. A
 * sync of two 20,000-item lists differing by {1.5, 2.5} reported the
 * difference as {1, 2}, with reason 'complete' and the right count - a wrong
 * answer wearing a success. FULL_DUMP and BUCKETS hid it, because both ship
 * items verbatim and never rebuild one from halves; it only appears once the
 * lists are large enough for the IBLT to be chosen, which is exactly when a
 * caller has stopped watching.
 *
 * The ceiling is 2^53 - 1 rather than 2^53, because 2^53 is the first value
 * whose high half needs a 22nd bit and is silently masked to zero.
 *
 * This is a bare numeric loop - no allocation, no hashing - so it costs a few
 * milliseconds on a million items. The duplicate check is NOT folded in here:
 * it needs a hash table the size of the input, and a caller supplying their
 * own fingerprint is deliberately skipping that scan. Pass `validate: false`
 * to skip this one too, once your ids are known to be in range.
 */
var MAX_ITEM = 9007199254740991;        /* 2^53 - 1 */

/*
 * Zero cannot be carried by the BCH sketch - every power of it is zero, so
 * it contributes nothing to any syndrome and the item simply vanishes.
 * Returns the offending index, or -1.
 */
function findZero(items) {
    for (var i = 0; i < items.length; i++) { if (items[i] === 0) { return i; } }
    return -1;
}

function rejectZero(items) {
    if (findZero(items) !== -1) {
        throw new Error('setrecon: zero is not a valid item when the sketch ' +
                        'is enabled - the sketch cannot carry it. Offset your ' +
                        'ids by one, or drop `sketch`.');
    }
    return items;
}

function validateItems(items) {
    for (var i = 0; i < items.length; i++) {
        var v = items[i];
        if (typeof v !== 'number' || !isFinite(v) ||
            Math.floor(v) !== v || v < 0 || v > MAX_ITEM) {
            throw new Error('setrecon: item at index ' + i + ' is ' +
                            (typeof v === 'number' ? v : typeof v) +
                            ' - items must be integers from 0 to 2^53-1. ' +
                            'Hash strings and UUIDs into that range first; ' +
                            'anything else is silently altered in transit.');
        }
    }
    return items;
}

/*
 * Pack two 32-bit words into the 53 bits a JavaScript number holds exactly:
 * all of the low word, and 21 bits of the high one. XOR commutes with the
 * mask, so a fingerprint stays updatable in place.
 */
function fp53(lo, hi) {
    return (lo >>> 0) + ((hi & 0x1fffff) >>> 0) * 4294967296;
}

/**
 * The per-item contribution, for callers keeping a fingerprint current.
 *
 * The seed is a [lo, hi] pair. A seed of the wrong SHAPE used to be accepted
 * silently - seed[0] and seed[1] came back undefined, hash32 coerced them to
 * zero, and the result was a plausible-looking number that did not match the
 * fingerprint. Since the only thing a caller does with this value is XOR it
 * into a fingerprint, a wrong one is undetectable downstream, so it throws.
 */
function hashItem53(item, seed) {
    var s = seed || S32_ROOT;
    if (!s.length || typeof s[0] !== 'number' || typeof s[1] !== 'number') {
        throw new Error('setrecon: seed must be a [lo, hi] pair of numbers, ' +
                        'e.g. setrecon.core.SEED_ROOT');
    }
    var h = hash32(item, s[0], s[1]);
    return fp53(h[0], h[1]);
}

// XOR two 53-bit Numbers safely (JS `^` on Numbers > 32 bits is broken).
// Used for BUCKETS hash accumulation.
function xor53(a, b) {
    return Number((BigInt(a) ^ BigInt(b)) & MASK53);
}


// =========================================================================
// IBLT — Invertible Bloom Lookup Table                     [EGUV11 §3.1]
// =========================================================================
//
// Each item is inserted into k distinct cells chosen by hashing (see
// ibltIndices). A cell tracks (count, keySum, hashSum). Two tables A, B
// over overlapping sets can be subtracted cell-wise to produce a table
// of their symmetric difference. If the difference is small enough for
// the table size, ibltDecode peels it: whenever a cell has count=±1 and
// its hashSum matches the fingerprint of its keySum, that cell is "pure"
// and we've recovered one item. Remove it from all k cells it touched,
// look for newly-pure cells, repeat.
//
// Failure mode: peel gets stuck when all remaining cells have count 0,
// ±2, or count=±1 but hashSum lies. Then success=false and the caller
// enlarges the table (see IBLT_MAX_RETRIES). Correctness is never at
// risk — a decode that returns success=true is exact.
//
// Sizing rule (calibrated in list_sync_sim.js --calibrate):
//   cells = max(24, ceil(2·d) + 16), k = 4 for d≤200, k = 3 for d>200.
// =========================================================================

// k distinct cell indices for `item`. splitmix64 chain until we've picked
// k unique cells — for uniform k/cells ratios far below 1 this converges
// in ~k iterations. The guard is a belt-and-braces fallback for pathological
// cell counts (only reachable if cells < k, which we prevent).
/*
 * ONE HASH YIELDS FOUR INDICES, not four hashes yielding one each.
 *
 * A 64-bit splitmix output has four 16-bit windows, and each is enough to
 * pick among a few hundred cells. Reusing them makes this three and a half
 * times faster - and this was the hottest function in the whole library,
 * about half the time of a Strata encode, which is itself the largest cost
 * of a large sync.
 *
 * ON COLLISION THE HASH IS ADVANCED, not filled in from zero. Filling
 * linearly was tried first and looked fine on a stopwatch, but it pushed
 * every collision onto the lowest cells: the spread got seven times worse,
 * and an IBLT with a lopsided load decodes less often. Rehashing keeps the
 * distribution at least as even as the four-hash version measured over
 * 60,000 keys, and collisions are rare enough that the extra hash almost
 * never runs.
 *
 * The result is written into a module-level buffer to avoid allocating an
 * array per item; callers read the first `n` entries and must not hold on
 * to it.
 */
var IBLT_PICKS = new Int32Array(16);

function ibltIndices(item, cells, k) {
    var h = hash32(item, S32_IBLT_IX[0], S32_IBLT_IX[1]);
    var lo = h[0], hi = h[1];
    var n = 0, guard = 0, i;
    for (;;) {
        var w0 = lo & 0xffff, w1 = (lo >>> 16) & 0xffff;
        var w2 = hi & 0xffff, w3 = (hi >>> 16) & 0xffff;

        for (var s = 0; s < 4; s++) {
            var cand = (s === 0 ? w0 : s === 1 ? w1 : s === 2 ? w2 : w3) % cells;
            var dup = false;
            for (i = 0; i < n; i++) { if (IBLT_PICKS[i] === cand) { dup = true; break; } }
            if (!dup) {
                IBLT_PICKS[n++] = cand;
                if (n === k) { return IBLT_PICKS; }
            }
        }

        /* nothing new in this round of windows - rehash and try again */
        lo = mix32(lo ^ hi); hi = mix32(hi ^ lo);

        /* a table smaller than k cells can never fill; give it what exists */
        if (++guard > 200) {
            for (var f = 0; n < k && f < cells; f++) {
                var d2 = false;
                for (i = 0; i < n; i++) { if (IBLT_PICKS[i] === f) { d2 = true; break; } }
                if (!d2) { IBLT_PICKS[n++] = f; }
            }
            return IBLT_PICKS;
        }
    }
}

// Fingerprint stored in each cell's hashSum slot. Distinct seed from
// ibltIndices so the two are statistically independent.
// Truncated to 53 bits to match the wire format — an empirical check
// (1000-trial sweep) shows this doesn't degrade decode success from the
// 64-bit variant. Keeping the whole pipeline within 53 bits means every
// cell field fits in a JS Number, and wire encoding stays trivial.
/*
 * The item a cell's key sums represent. Only meaningful when the cell holds
 * exactly one - that is what ibltCellIsPure checks before anyone asks.
 *
 * The high half is masked to 21 bits, matching the wire format: a cell that
 * has been serialised and read back cannot carry more, so keeping the
 * in-memory form wider would let the two disagree.
 */
function cellItem(c) {
    return (c.kLo >>> 0) + ((c.kHi & 0x1fffff) >>> 0) * 4294967296;
}

/*
 * A cell's fingerprint, MASKED TO 53 BITS to match what the wire carries.
 *
 * The serialised form packs each sum into one JavaScript number, which holds
 * 53 bits exactly - so the high word loses eleven. Leaving the in-memory
 * fingerprint wider made a cell that had crossed the wire compare unequal to
 * its own recomputed fingerprint: it never looked pure, and every decode
 * after a round trip failed while decodes in the same process passed.
 *
 * Masking here rather than at serialisation keeps the two forms identical by
 * construction, and 53 bits is what the BigInt version carried anyway.
 */
function ibltFingerprint(item) {
    var h = hash32(item, S32_IBLT_FP[0], S32_IBLT_FP[1]);
    return [h[0], (h[1] & 0x1fffff) >>> 0];
}

/*
 * A cell holds its key and fingerprint sums as PAIRS OF 32-BIT WORDS rather
 * than BigInts. Items are at most 2^53 and XOR is bitwise, so nothing is
 * lost - and the BigInt version allocated on every one of the four cell
 * updates each item triggers, which measured nine times slower.
 */
function ibltNew(cells, k) {
    var t = { cells: new Array(cells), k: k };
    for (var i = 0; i < cells; i++) {
        t.cells[i] = { count: 0, kLo: 0, kHi: 0, fLo: 0, fHi: 0 };
    }
    return t;
}

function ibltInsert(t, item) {
    var kLo = (item % 4294967296) >>> 0;
    var kHi = Math.floor(item / 4294967296) >>> 0;
    var fp  = ibltFingerprint(item);
    var picks = ibltIndices(item, t.cells.length, t.k);
    for (var i = 0; i < t.k; i++) {
        var c = t.cells[picks[i]];
        c.count++;
        /*
         * `>>> 0` after every XOR, because ^= yields a SIGNED 32-bit result
         * and anything with the top bit set comes back negative. The sums
         * then compare unequal to the unsigned fingerprints they should
         * match, no cell ever looks pure, and decoding fails on every input
         * with a high bit - which is most of them.
         */
        c.kLo = (c.kLo ^ kLo) >>> 0;
        c.kHi = (c.kHi ^ kHi) >>> 0;
        c.fLo = (c.fLo ^ fp[0]) >>> 0;
        c.fHi = (c.fHi ^ fp[1]) >>> 0;
    }
}

// Cell-wise A − B: counts subtract, sums XOR (XOR is self-inverse), leaving
// an IBLT of the symmetric difference.                     [EGUV11 Algorithm 2]
function ibltSubtract(A, B) {
    var out = ibltNew(A.cells.length, A.k);
    for (var i = 0; i < A.cells.length; i++) {
        out.cells[i].count   = A.cells[i].count - B.cells[i].count;
        out.cells[i].kLo = (A.cells[i].kLo ^ B.cells[i].kLo) >>> 0;
        out.cells[i].kHi = (A.cells[i].kHi ^ B.cells[i].kHi) >>> 0;
        out.cells[i].fLo = (A.cells[i].fLo ^ B.cells[i].fLo) >>> 0;
        out.cells[i].fHi = (A.cells[i].fHi ^ B.cells[i].fHi) >>> 0;
    }
    return out;
}

function ibltCellIsPure(c) {
    return (c.count === 1 || c.count === -1) &&
           (function () {
               var fp = ibltFingerprint(cellItem(c));
               return c.fLo === fp[0] && c.fHi === fp[1];
           })();
}

// Queue-based peel ([EGUV11] Algorithm 3). Returns
//   { aOnly: [Number], bOnly: [Number], success: bool }
// where aOnly = items only in the minuend (count +1), bOnly = only in the
// subtrahend (count −1). success === every cell fully zeroed at end.
function ibltDecode(t) {
    var n     = t.cells.length;
    var aOnly = [];
    var bOnly = [];
    var queue = [];
    for (var i = 0; i < n; i++) {
        if (ibltCellIsPure(t.cells[i])) queue.push(i);
    }

    while (queue.length > 0) {
        var idx = queue.pop();
        var c   = t.cells[idx];
        if (!ibltCellIsPure(c)) continue;   // may have become impure since queued

        var key  = cellItem(c);             // pure ⇒ the sums ARE the item
        var sign = c.count;                 // capture ONCE before mutation
        var fp   = ibltFingerprint(key);
        if (sign === 1) aOnly.push(key);
        else            bOnly.push(key);

        // Remove the item from every cell it hashed to — including this one.
        /* the buffer is shared and reused - read t.k entries, and do not
           call ibltIndices again before finishing with them */
        var picks = ibltIndices(key, n, t.k);
        for (var j = 0; j < t.k; j++) {
            var cc = t.cells[picks[j]];
            cc.count -= sign;
            cc.kLo = (cc.kLo ^ ((key % 4294967296) >>> 0)) >>> 0;
            cc.kHi = (cc.kHi ^ (Math.floor(key / 4294967296) >>> 0)) >>> 0;
            cc.fLo = (cc.fLo ^ fp[0]) >>> 0;
            cc.fHi = (cc.fHi ^ fp[1]) >>> 0;
            if (ibltCellIsPure(cc)) queue.push(picks[j]);
        }
    }

    var success = true;
    for (var k2 = 0; k2 < n; k2++) {
        var cell = t.cells[k2];
        if (cell.count !== 0 || cell.kLo !== 0 || cell.kHi !== 0 ||
            cell.fLo !== 0 || cell.fHi !== 0) {
            success = false;
            break;
        }
    }
    return { aOnly: aOnly, bOnly: bOnly, success: success };
}

// Sizing rule — calibrated in list_sync_sim.js (see --calibrate).
// hedge 2.0 and the k switch at d=200 are [EGUV11 §6.2]'s guideline; the
// PAD of 16 covers small-d finite-size variance (without it, success dips
// to ~97.8% around d ≈ 15).
function ibltParamsForDiff(d) {
    var dd = Math.max(1, Math.ceil(d));
    return {
        k:     dd > 200 ? 3 : 4,
        cells: Math.max(IBLT_MIN_CELLS, Math.ceil(IBLT_HEDGE * dd) + IBLT_PAD)
    };
}

// -------------------------------------------------------------------------
// Flatten an IBLT into plain number arrays, and rebuild it from them.
//
// This is not serialisation - nothing here turns anything into bytes. It is
// a reshape into the only structure the 'send' event carries: an object of
// numbers and arrays of numbers, which the caller then encodes however the
// rest of their system does.
// -------------------------------------------------------------------------

function ibltToWire(t) {
    var n      = t.cells.length;
    var counts = new Array(n);
    var keys   = new Array(n);
    var hashes = new Array(n);
    for (var i = 0; i < n; i++) {
        counts[i] = t.cells[i].count;
        // BigInt sums → Number (53-bit) for wire. IBLT math needs BigInt
        // to add/xor correctly, but any single sum still fits in 53 bits
        // because it's a XOR chain of 53-bit-masked items.
        keys[i]   = cellItem(t.cells[i]);
        hashes[i] = fp53(t.cells[i].fLo, t.cells[i].fHi);
    }
    return { cells: n, k: t.k, counts: counts, keys: keys, hashes: hashes };
}

function ibltFromWire(w) {
    var t = ibltNew(w.cells, w.k);
    for (var i = 0; i < w.cells; i++) {
        t.cells[i].count   = w.counts[i];
        t.cells[i].kLo = (w.keys[i] % 4294967296) >>> 0;
        t.cells[i].kHi = Math.floor(w.keys[i] / 4294967296) >>> 0;
        t.cells[i].fLo = (w.hashes[i] % 4294967296) >>> 0;
        t.cells[i].fHi = Math.floor(w.hashes[i] / 4294967296) >>> 0;
    }
    return t;
}


// =========================================================================
// Strata Estimator                                          [EGUV11 §3.2]
// =========================================================================
//
// L strata, each a small fixed-size IBLT (80 cells, k=4 — the paper's
// configuration; Fig. 8 shows 80 cells is the knee of the accuracy curve).
// Item goes to stratum min(trailingZeros(h(item, SEED_STRATA)), L−1), so
// stratum i samples the set with probability 2^-(i+1). The receiver
// subtracts its own strata cell-wise and decodes from the sparsest
// stratum (highest index) down; at the first stratum that fails, we know
// d̂ = 2^(i+1) × (items recovered above stratum i). If every stratum
// decodes, the recovered count is exact.
//
// Estimates are scaled by 1.39 so d̂ ≥ d 99% of the time (Fig. 8).
// =========================================================================

// Trailing zeros of a 64-bit BigInt — the stratum-assignment function.
// Average ~2 iterations per call; hot enough that we cache the bit shifts.
/*
 * How many trailing zeros the item's strata hash has, which is the level it
 * belongs to. Half the items land on level 0, a quarter on level 1, and so
 * on - that halving is what lets the estimator read a difference off the
 * deepest level that still decodes.
 */
function trailingZeros32(item) {
    var h = hash32(item, S32_STRATA[0], S32_STRATA[1]);
    var lo = h[0];
    if (lo !== 0) { return 31 - Math.clz32(lo & -lo); }
    var hi = h[1];
    if (hi !== 0) { return 63 - Math.clz32(hi & -hi); }
    return 64;
}

function strataLevelsFor(nA, nB) {
    // d can never exceed nA + nB, so ⌊log2(nA+nB)⌋+1 strata always suffice.
    var dmax = Math.max(2, nA + nB);
    var L = Math.floor(Math.log2(dmax)) + 1;
    return Math.max(STRATA_MIN_LEVELS, Math.min(STRATA_MAX_LEVELS, L));
}

function strataEncode(items, L) {
    var strata = new Array(L);
    for (var i = 0; i < L; i++) strata[i] = ibltNew(STRATA_CELLS, STRATA_K);
    for (var j = 0; j < items.length; j++) {
        var lvl = Math.min(trailingZeros32(items[j]), L - 1);
        ibltInsert(strata[lvl], items[j]);
    }
    return strata;
}

// Returns { dRaw, dScaled, exact }. dRaw of 0 is clamped to 1 by callers
// that only reach the estimator when the fingerprints already differ.
function strataEstimate(mine, theirs) {
    var L = mine.length;
    var recovered = 0;
    for (var i = L - 1; i >= 0; i--) {
        var dec = ibltDecode(ibltSubtract(mine[i], theirs[i]));
        if (!dec.success) {
            var dRaw = Math.pow(2, i + 1) * recovered;
            return {
                dRaw:    dRaw,
                dScaled: Math.max(1, Math.ceil(STRATA_CORRECTION * dRaw)),
                exact:   false
            };
        }
        recovered += dec.aOnly.length + dec.bOnly.length;
    }
    return { dRaw: recovered, dScaled: Math.max(1, recovered), exact: true };
}

// Wire serialization — flatten L strata × STRATA_CELLS into three parallel
// arrays (counts, keys, hashes) of length L × STRATA_CELLS.
function strataToWire(strata) {
    var L      = strata.length;
    var total  = L * STRATA_CELLS;
    var counts = new Array(total);
    var keys   = new Array(total);
    var hashes = new Array(total);
    var o      = 0;
    for (var lvl = 0; lvl < L; lvl++) {
        for (var c = 0; c < STRATA_CELLS; c++) {
            counts[o] = strata[lvl].cells[c].count;
            keys[o]   = cellItem(strata[lvl].cells[c]);
            hashes[o] = fp53(strata[lvl].cells[c].fLo, strata[lvl].cells[c].fHi);
            o++;
        }
    }
    return { levels: L, counts: counts, keys: keys, hashes: hashes };
}

function strataFromWire(w) {
    var L      = w.levels;
    var strata = new Array(L);
    var o      = 0;
    for (var lvl = 0; lvl < L; lvl++) {
        strata[lvl] = ibltNew(STRATA_CELLS, STRATA_K);
        for (var c = 0; c < STRATA_CELLS; c++) {
            strata[lvl].cells[c].count   = w.counts[o];
            strata[lvl].cells[c].kLo = (w.keys[o] % 4294967296) >>> 0;
            strata[lvl].cells[c].kHi = Math.floor(w.keys[o] / 4294967296) >>> 0;
            strata[lvl].cells[c].fLo = (w.hashes[o] % 4294967296) >>> 0;
            strata[lvl].cells[c].fHi = Math.floor(w.hashes[o] / 4294967296) >>> 0;
            o++;
        }
    }
    return strata;
}


// =========================================================================
// FULL_DUMP — one side ships its whole list; the other diffs locally
// against its own set. 1 phase per direction; the decider chooses whether
// only one side dumps (e.g. one side empty) or both. In the both-dump
// case the misses are computed by set-diff on each side.
// =========================================================================

// Local FULL_DUMP miss computation: given my items and the peer's items
// (both received), report what I'm missing (theirs \ mine) and what they
// were missing (mine \ theirs).
function fullDumpDiff(myItems, peerItems) {
    var mySet   = new Set(myItems);
    var peerSet = new Set(peerItems);
    var myMissing   = [];
    var peerMissing = [];
    for (var i = 0; i < peerItems.length; i++) {
        if (!mySet.has(peerItems[i])) myMissing.push(peerItems[i]);
    }
    for (var j = 0; j < myItems.length; j++) {
        if (!peerSet.has(myItems[j])) peerMissing.push(myItems[j]);
    }
    return { myMissing: myMissing, peerMissing: peerMissing };
}


// =========================================================================
// BUCKETS — deterministic bucketing, XOR bucket hashes, exchange
// mismatched bucket contents.
// =========================================================================
//
// Both sides bucketize their items using the same function (splitmix64 %
// B). Each bucket accumulates an XOR of the items' SEED_ROOT hashes plus
// the list of items themselves. The two sides swap:
//   (bucket hashes) + (all local items with an in-message bucket key)
// The receiver bucketizes its own list, compares hashes, and for every
// mismatched bucket takes the sender's items in that bucket and computes
// its local miss set.
//
// One send per direction — 2 message flow (both sides in parallel).
//
// B = max(16, sqrt(maxN)). Tried sqrt(2N): shaves ~15% off BUCKETS in
// the medium-diff range when d is moderate, but the extra hashes hurt
// at very small d and the scenarios where the improvement matters are
// rare. sqrt(N) is robust across all d.
// =========================================================================

function bucketCountFor(maxN) {
    return Math.max(16, Math.ceil(Math.sqrt(Math.max(1, maxN))));
}

function bucketOf(item, B) {
    return hash32(item, S32_BUCKET[0], S32_BUCKET[1])[0] % B;
}

// Build B XOR-hashes over the caller's items. Returns { hashes: [Number],
// bucketedItems: [ [items in bucket 0], [items in bucket 1], ... ] }.
// The bucketedItems is used both for the wire payload (flatten with
// per-item bucket key) and for local miss computation on receive.
function buildBuckets(items, B) {
    var buckets = new Array(B);
    for (var i = 0; i < B; i++) buckets[i] = { hashLo: 0, hashHi: 0, items: [] };
    for (var j = 0; j < items.length; j++) {
        var bIdx = bucketOf(items[j], B);
        var bh = hash32(items[j], S32_ROOT[0], S32_ROOT[1]);
        buckets[bIdx].hashLo = (buckets[bIdx].hashLo ^ bh[0]) >>> 0;
        buckets[bIdx].hashHi = (buckets[bIdx].hashHi ^ bh[1]) >>> 0;
        buckets[bIdx].items.push(items[j]);
    }
    var hashes = new Array(B);
    var bucketedItems = new Array(B);
    for (var i2 = 0; i2 < B; i2++) {
        hashes[i2]        = fp53(buckets[i2].hashLo, buckets[i2].hashHi);
        bucketedItems[i2] = buckets[i2].items;
    }
    return { hashes: hashes, bucketedItems: bucketedItems };
}

// (bucketsDiff removed — the 2-phase protocol handles reconciliation
// inline in the LIST_BUCKETS_ITEMS handler, using the mismatched-bucket
// list computed in Phase 1.)


// =========================================================================
// Cost model — projected bytes per algorithm given (nA, nB, dHat).
// Used by the decider. HELLO/STRATA overhead is common to compared paths
// and excluded from the comparison.
// =========================================================================
//
// Byte estimates, used only to compare algorithms against each other. They
// assume a varint-ish encoding and land within a few bytes of one; a caller
// whose encoder differs corrects with `overhead`.
//   item / hash / fingerprint (≤ 53 bits) → 8 bytes
//   count / small param                    → 2-4 bytes
//   IBLT cell = keySum(8) + hashSum(8) + count(4) = 20 bytes
//   Message tag + syncId overhead                = ~10 bytes
// =========================================================================

var BYTES_ITEM         = 8;
var BYTES_HASH         = 8;
var BYTES_IBLT_CELL    = 20;
var BYTES_STRATA_CELL  = 20;
var BYTES_MSG_OVERHEAD = 10;

function predictFullDumpBytes(nA, nB) {
    return BYTES_MSG_OVERHEAD * 2 + (nA + nB) * BYTES_ITEM;
}

function predictBucketsBytes(nA, nB, dHat) {
    var B = bucketCountFor(Math.max(nA, nB));
    // A diff item lands in a uniform random bucket; expected mismatched
    // buckets = B·(1 − (1 − 1/B)^d). Each mismatched bucket ships its full
    // contents from both sides ≈ (nA+nB)/B items.
    var pMis   = 1 - Math.pow(1 - 1 / B, Math.max(1, dHat));
    var expMis = B * pMis;
    var perBucketItems = (nA + nB) / B;
    return BYTES_MSG_OVERHEAD * 2 +
           B * BYTES_HASH * 2 +                                // hashes both dirs
           Math.ceil(expMis * perBucketItems) * BYTES_ITEM;
}

function predictIbltBytes(dHat) {
    var p = ibltParamsForDiff(dHat);
    // IBLT itself + expected miss-item list back (dHat/2 items on avg)
    return BYTES_MSG_OVERHEAD * 2 +
           p.cells * BYTES_IBLT_CELL +
           Math.ceil(dHat / 2) * BYTES_ITEM;
}

function predictStrataBytes(nA, nB) {
    var L = strataLevelsFor(nA, nB);
    return BYTES_MSG_OVERHEAD + L * STRATA_CELLS * BYTES_STRATA_CELL;
}


// =========================================================================
// Decider — pure function; same inputs → same output on both sides.
// =========================================================================
//
// Called by both peers with the same (nA, nB) after HELLO exchange, then
// again by the responder after decoding STRATA (with dHat known). Because
// both sides run the identical decider on identical inputs, they agree
// on the algorithm without needing a wire STRATEGY message — the MSG
// type of the next payload announces the pick implicitly.
//
// Returns { algo, params } where algo names the algorithm and params
// carries algorithm-specific hints (e.g. B for BUCKETS, cells for IBLT).
//
// Phases:
//   Phase 'preStrata': called with just (nA, nB) after HELLO. Result is
//     one of: IDENTICAL, FULL_DUMP, BUCKETS, STRATA.
//     If STRATA, the caller runs Strata and calls again with dHat.
//   Phase 'postStrata': called with (nA, nB, dHat, dRaw). Result is
//     one of: FULL_DUMP, BUCKETS, IBLT.
// =========================================================================

// preStrata: rootA === rootB is caller's business (they've already checked
// the HELLO fingerprints match). This function is called only when they
// don't match, so we never return IDENTICAL here.
/**
 * The decider, WITH an optional estimate of d.
 *
 * The original took only the two sizes, which means it cannot tell a pair of
 * 100,000-item lists differing by 2 from a pair differing by 20,000 - and it
 * pays for Strata in both. The simulation showed that costing 29 KB where
 * BUCKETS would have cost 4.
 *
 * expectedDiff is what the CALLER already knows: how long since the last successful
 * sync with this peer, times the rate items arrive. Bitcoin's Erlay sizes its
 * sketches from exactly that, and it needs no extra round trip.
 *
 * WHEN expectedDiff IS ABSENT the behaviour is unchanged, so this is additive.
 *
 * @param {number} [expectedDiff]  a guess at the difference, or undefined
 */
/*
 * ABOVE THIS DIFFERENCE THE SKETCH IS NOT WORTH IT.
 *
 * Its bytes are unbeatable - a few hundred where the others need tens of
 * kilobytes - but decoding costs O(d^1.5) or worse in field operations, and
 * measured here that is 1.3 seconds at d=20 and 168 seconds at d=500. A
 * protocol that syncs every couple of seconds cannot spend either.
 *
 * 20 is where the trade stops paying: below it the decode is under a second
 * and the saving is 40-160x; above it the saving shrinks and the cost
 * explodes.
 */
var SKETCH_MAX_D = 20;

/*
 * ROUND TRIPS EACH ALGORITHM COSTS, counted from the message flow.
 *
 * A dump crosses once; buckets need hashes out and items back; Strata needs
 * the estimate out and the IBLT back. The sketch is one crossing plus its
 * result, which is the same shape as a dump.
 */
var ROUND_TRIPS = { FULL_DUMP: 1, BUCKETS: 2, STRATA: 2, SKETCH: 1 };

/**
 * Turn a byte count into a time, when the link is known.
 *
 * WITHOUT A LINK MODEL, BYTES ARE THE ONLY YARDSTICK - and that quietly
 * assumes bytes are the scarce thing. On a fast link with real latency they
 * are not: sending twenty thousand items outright beat a Strata exchange by
 * 86ms in the simulator, because the two extra round trips cost more than
 * the 280 kilobytes saved. The same pair reverses below about 4 Mbps, where
 * the same 280 kilobytes cost more than a second.
 *
 * So an algorithm is scored on what it actually costs the caller:
 *
 *     trips * rtt  +  bytes / bandwidth
 *
 * With neither figure supplied this returns the byte count unchanged, which
 * keeps the old behaviour exactly.
 *
 * AN RTT WITHOUT A BANDWIDTH IS NOT A LINK MODEL. The formula needs the
 * bandwidth to turn bytes into time; without one the bytes term was zero, so
 * the score was round trips alone and the dump - one crossing - won at every
 * size: fifteen megabytes for two lists of a million that Strata settles in
 * forty kilobytes. So an rtt is used only with a bandwidth, and bytes decide
 * otherwise. A bandwidth alone is kept: it ranks exactly as bytes do.
 *
 * IT OPTIMISES TIME, AND THAT IS NOT ALWAYS WHAT YOU WANT. Across 81
 * combinations of size, difference, latency and bandwidth it was faster in 45
 * and slower in one - and that one traded fifteen milliseconds for ten times
 * the bytes. On a link the caller pays for, or one shared with eight other
 * peers, that is the wrong trade. Supplying a link model says latency is the
 * scarce thing; leaving it out says bytes are.
 */
function hasLinkModel(link) { return !!(link && link.bandwidth > 0); }

function costOf(algo, bytes, link) {
    if (!hasLinkModel(link)) { return bytes; }
    var trips = ROUND_TRIPS[algo] || 2;
    var rtt = link.rtt || 0;
    var bw = link.bandwidth || 0;
    /* bytes to milliseconds: bits over bits-per-second, times a thousand */
    var txMs = bw > 0 ? (bytes * 8) / (bw * 1e6) * 1000 : 0;
    return trips * rtt + txMs;
}

/**
 * @param {object} [link]  { rtt, bandwidth } - measured, not guessed. rtt in
 *        milliseconds, bandwidth in Mbps. rtt counts only with a bandwidth
 *        (see costOf); a bandwidth alone ranks as bytes do.
 */
function decidePreStrata(nA, nB, expectedDiff, allowSketch, link) {
    var maxN = Math.max(nA, nB);
    var minN = Math.min(nA, nB);
    var skew = Math.abs(nA - nB);

    // Very small or lopsided → just dump both sides.
    if (maxN < 20 || minN === 0 || skew >= 0.5 * maxN) {
        return { algo: 'FULL_DUMP', params: {} };
    }

    // Is Strata's fixed cost worth paying? It's ~L * 80 * 20 bytes.
    // Payoff comes if actual d turns out to allow IBLT well below the
    // full-dump cost. STRATA_BUDGET_FRACTION calibrated in the sim.
    var strataBytes = predictStrataBytes(nA, nB);
    var strataCost = strataBytes;

    /*
     * IF d IS ALREADY KNOWN, MEASURING IT IS WASTE. Strata exists to answer
     * "how different are we"; a caller who can guess from elapsed time has
     * the answer for free, and paying 12 KB to confirm it is the mistake the
     * simulation found.
     *
     * The guess is compared against the alternatives on predicted bytes -
     * not trusted blindly - so a bad hint costs a suboptimal choice rather
     * than a failure. And IBLT decoding fails safe: a hint that was far too
     * low falls back to a full dump.
     */
    if (expectedDiff !== undefined && expectedDiff !== null) {
        var withHint = [
            { algo: 'BUCKETS', bytes: predictBucketsBytes(nA, nB, expectedDiff),
              params: { B: bucketCountFor(maxN) } },
            { algo: 'STRATA', bytes: strataCost + predictIbltBytes(expectedDiff),
              params: { levels: strataLevelsFor(nA, nB) } },
            { algo: 'FULL_DUMP', bytes: predictFullDumpBytes(nA, nB), params: {} }
        ];
        /*
         * The sketch only competes when the caller asked for it AND the hint
         * says the difference is small. Without a hint it cannot be chosen at
         * all - sizing it needs a number, and guessing wrong means it decodes
         * nothing rather than something partial.
         */
        if (allowSketch && expectedDiff > 0 && expectedDiff <= SKETCH_MAX_D) {
            /*
             * A margin over the estimate, because the estimate is one. A
             * sketch sized exactly to a guess that came in low decodes
             * nothing - it fails all at once rather than partially - so
             * paying a few extra syndromes is cheaper than the round trip a
             * failure costs.
             */
            var cap = Math.max(4, Math.ceil(expectedDiff * 1.25) + 1);
            withHint.push({ algo: 'SKETCH',
                            bytes: 40 + cap * 8 + expectedDiff * 8,
                            params: { capacity: cap } });
        }

        var best = withHint[0];
        var bestCost = costOf(best.algo, best.bytes, link);
        for (var wi = 1; wi < withHint.length; wi++) {
            var c = costOf(withHint[wi].algo, withHint[wi].bytes, link);
            if (c < bestCost) { best = withHint[wi]; bestCost = c; }
        }
        return { algo: best.algo, params: best.params };
    }

    /*
     * With a link model the three are compared on TIME, which can reverse the
     * budget rule entirely - a dump that looks extravagant in bytes is often
     * the fastest thing on a quick link, because it crosses once.
     */
    if (hasLinkModel(link)) {
        var opts = [
            { algo: 'FULL_DUMP', bytes: predictFullDumpBytes(nA, nB), params: {} },
            { algo: 'BUCKETS', bytes: predictBucketsBytes(nA, nB, Math.sqrt(maxN)),
              params: { B: bucketCountFor(maxN) } },
            { algo: 'STRATA', bytes: strataBytes + predictIbltBytes(Math.sqrt(maxN)),
              params: { levels: strataLevelsFor(nA, nB) } }
        ];
        var pick = opts[0], pickCost = costOf(pick.algo, pick.bytes, link);
        for (var oi = 1; oi < opts.length; oi++) {
            var oc = costOf(opts[oi].algo, opts[oi].bytes, link);
            if (oc < pickCost) { pick = opts[oi]; pickCost = oc; }
        }
        return { algo: pick.algo, params: pick.params };
    }

    if (strataCost <= STRATA_BUDGET_FRACTION * predictFullDumpBytes(nA, nB)) {
        return { algo: 'STRATA', params: { levels: strataLevelsFor(nA, nB) } };
    }

    // Small/medium: BUCKETS handles random-distribution items well and
    // degrades gracefully (worst case ≈ full-dump + 2·B·8 bytes).
    return { algo: 'BUCKETS', params: { B: bucketCountFor(maxN) } };
}

// postStrata: caller has run Strata and obtained { dRaw, dScaled, exact }.
// dHat is the conservative (scaled) estimate; dRaw is what to feed IBLT
// sizing (the 2× hedge inside ibltParamsForDiff already covers estimator
// error, so raw is right there).
function decidePostStrata(nA, nB, dHat, dRaw) {
    var cFull    = predictFullDumpBytes(nA, nB);
    var cBuckets = predictBucketsBytes(nA, nB, dHat);
    var cIblt    = predictIbltBytes(Math.max(1, dRaw));

    if (cFull <= cBuckets && cFull <= cIblt) {
        return { algo: 'FULL_DUMP', params: {} };
    }
    if (cBuckets <= cIblt) {
        return { algo: 'BUCKETS', params: { B: bucketCountFor(Math.max(nA, nB)) } };
    }
    // IBLT sized by the paper's rule of thumb with our calibrated pad.
    // The 2× hedge inside ibltParamsForDiff covers estimator error.
    var p = ibltParamsForDiff(Math.max(1, dRaw));
    return { algo: 'IBLT', params: { cells: p.cells, k: p.k, dRaw: dRaw } };
}


// =========================================================================
// Per-sync state
// =========================================================================
//
// activeSyncs[syncId] holds the transient sync state. Sample shape:
//   {
//     syncId, target: { peerId }, timeout,
//     role: 'initiator' | 'responder',
//     items:       [Number] | null,      // null when caller registered lazily
//     fingerprint: Number  | null,       // 53-bit
//     count:       Number  | null,       // items.length when items present
//     peerCount, peerFingerprint,        // from HELLO
//     algorithm:   string | null,        // set when decider fires
//     phase:       'awaiting_hello' | 'awaiting_items' | 'awaiting_full_dump'


/* ------------------------------------------------------------------------
 * Exports.
 *
 * The byte predictors are exported alongside the algorithms because a
 * simulation's whole purpose is comparing PREDICTED cost against ACTUAL -
 * and a decider that predicts badly is the failure mode worth finding.
 * ---------------------------------------------------------------------- */



/* =======================================================================
 * 6. The session: who sends what, and when
 * ======================================================================= */





/* ---------------- defaults ---------------- */
/*
 * DEFAULT_TIMEOUT, HOLD_GRACE, HOLD_EXTRA_MS, HOLD_MAX_TOTAL and
 * DEFAULT_CHUNK are declared once, at the top of the file.
 *
 * They used to be declared a second time here, with the same values. `var`
 * makes that silent, and two declarations of a tuning constant is a standing
 * invitation to change one of them.
 */

/*
 * A sketch's syndromes are pairs of 32-bit halves. They cross the wire as a
 * flat array of numbers, because that is what every other message here is and
 * the caller's encoder should not have to learn a second shape.
 */
function flattenSketch(syn) {
    var out = new Array(syn.length * 2);
    for (var i = 0; i < syn.length; i++) {
        out[i * 2] = syn[i][0];
        out[i * 2 + 1] = syn[i][1];
    }
    return out;
}

function unflattenSketch(flat) {
    var out = new Array(flat.length >> 1);
    for (var i = 0; i < out.length; i++) {
        out[i] = [flat[i * 2] >>> 0, flat[i * 2 + 1] >>> 0];
    }
    return out;
}

/* ---------------- agreeing on the decision inputs ---------------- */

function present(v) { return typeof v === 'number' && isFinite(v); }

/*
 * The link both sides decide on, from what each measured. Either side's
 * figure is used when only one gave it; when both did, the slower reading:
 * the higher rtt and the lower bandwidth, which is the link the exchange will
 * actually get in its worse direction. An rtt with no bandwidth anywhere is no
 * link model at all (see costOf).
 */
function mergeLink(rtt1, bw1, rtt2, bw2) {
    var bw = present(bw1) && bw1 > 0 ? bw1 : undefined;
    if (present(bw2) && bw2 > 0) { bw = (bw === undefined) ? bw2 : Math.min(bw, bw2); }
    if (bw === undefined) { return null; }
    var rtt = present(rtt1) && rtt1 > 0 ? rtt1 : 0;
    if (present(rtt2) && rtt2 > rtt) { rtt = rtt2; }
    return { rtt: rtt, bandwidth: bw };
}

/**
 * Combine the two sides' decision inputs. Symmetric in its two halves - swap
 * them and the answer is the same - which is what lets both sides compute it
 * independently and agree without another message.
 *
 *   expectedDiff  either side's; both → the higher. A hint that is too low
 *                 sizes a sketch or an IBLT that fails to decode and costs a
 *                 fallback; one that is too high costs a few bytes.
 *   sketch        both sides must allow it: decoding it costs real CPU (about
 *                 a second at the cap), and one side cannot spend it for the
 *                 other.
 *   link          see mergeLink.
 */
function mergeInputs(hint1, sketch1, rtt1, bw1, hint2, sketch2, rtt2, bw2) {
    var h = present(hint1) ? hint1 : undefined;
    if (present(hint2)) { h = (h === undefined) ? hint2 : Math.max(h, hint2); }
    return {
        expectedDiff: h,
        sketch: !!(sketch1 && sketch2),
        link: mergeLink(rtt1, bw1, rtt2, bw2)
    };
}

/* ---------------- a minimal emitter ---------------- */

function Emitter() { this._h = Object.create(null); }

Emitter.prototype.on = function (name, fn) {
    (this._h[name] || (this._h[name] = [])).push(fn);
    return this;
};

Emitter.prototype._fire = function (name, a, b, c, d) {
    var l = this._h[name];
    if (!l) { return false; }
    for (var i = 0; i < l.length; i++) { l[i](a, b, c, d); }
    return true;
};

/* ---------------- the session ---------------- */

/**
 * @param {object} opts
 *   items        {Array<number>}  the list, if it is in memory
 *   count        {number}         how many, when items are loaded lazily
 *   fingerprint  {number}         precomputed - optional, but keeping one is
 *                                 what makes an unchanged sync cost 56 bytes
 *                                 and no read at all
 *   expectedDiff {number}         how many you expect to differ - see below
 *   rtt          {number}         measured round-trip, ms - lets the decider
 *                                 weigh round trips against bytes
 *   bandwidth    {number}         Mbps, even roughly
 *   limit        {number}         stop once this many missing items have been
 *                                 found; the rest are left for another time
 *   chunkSize    {number}         how many items to ask for at a time; 0
 *                                 disables splitting. Default 4000
 *   pull         {boolean}        only I need the answer - halves the traffic
 *                                 on the symmetric algorithms
 *   sketch       {boolean}        allow the BCH sketch when expectedDiff is small
 *   overhead     {number}         multiplier if your encoder is fatter
 *   timeout      {number}         overall deadline, ms
 *   validate     {boolean}        false skips the item range check; only do
 *                                 this once the ids are known to be integers
 *                                 in [0, 2^53-1], because out-of-range items
 *                                 come back CHANGED rather than rejected
 *   setTimeout / clearTimeout     for a simulation with virtual time
 *   now          {function}       clock to match them, for the same
 *
 * ON ITEMS: integers from 0 to 2^53-1, and unique. Out of that range they are
 * rebuilt wrongly from their 32/21-bit halves, which is why `validate` is on
 * by default. Zero is fine everywhere EXCEPT the sketch, which cannot carry
 * it; enabling `sketch` with a zero in the list throws here rather than
 * further in.
 *
 * ON expectedDiff: it is worth giving ONLY when it comes from measurement -
 * time since the last successful sync times the observed rate of new items. A
 * guess within a factor of two saves 2-6x; one that is ten times off is worse
 * than none, because it makes the decider confident and wrong. When there is
 * no history, leave it out and let Strata measure.
 *
 * The name says `expected` rather than `diff` on purpose: `diff` is what
 * comes OUT of a sync, and a parameter that reads like a fact invites callers
 * to supply one they do not have.
 */
function setrecon(opts) {
    var o = opts || {};
    var s = new Emitter();

    var checkItems = (o.validate !== false);

    var items = o.items || null;
    var count = items ? items.length : (o.count || 0);

    if (items && checkItems) {
        validateItems(items);
        /*
         * Zero is rejected here, not deeper in, BECAUSE THE SKETCH THROWS ON
         * IT. That throw used to surface from inside feed() - out of the
         * message handler and into the caller's transport callback, where
         * nothing expects an exception - and it left both sides waiting out
         * their timeouts. Throwing from the constructor puts the error in the
         * caller's own stack frame, where a bad configuration belongs.
         *
         * Only when the sketch is asked for: zero is a perfectly good item on
         * every other path, and rejecting it outright would break callers
         * whose ids start at zero and who never enable the sketch.
         */
        if (o.sketch) { rejectZero(items); }
    }

    var fingerprint = o.fingerprint;
    if (fingerprint === undefined && items) {
        fingerprint = computeFingerprint(items);
    }

    /*
     * A COUNT WITHOUT A FINGERPRINT IS ALLOWED, and costs one early load.
     *
     * A caller who can answer "how many" cheaply - a COUNT query - but does
     * not keep a running fingerprint should not be turned away. The items are
     * fetched once before the handshake and the fingerprint computed from
     * them, which is strictly better than being unable to start.
     *
     * It does give up the best case though: two sides that are already
     * identical settle in 56 bytes WITHOUT TOUCHING THE LIST, and that only
     * works when the fingerprint was known in advance. Keeping one is cheap -
     * it is an XOR per change, in either direction - and in a system that
     * syncs regularly it is the difference between the common case costing
     * nothing and costing a full read.
     */
    var needsFingerprint = (fingerprint === undefined);

    var overhead = o.overhead || 1;
    var setT = o.setTimeout || setTimeout;
    var clearT = o.clearTimeout || clearTimeout;
    /*
     * Extending a deadline means knowing how much of it is left, which needs
     * a clock as well as a timer. A simulation running on virtual time
     * already supplies setTimeout/clearTimeout and should supply `now` with
     * them; without it the extension arithmetic would read a wall clock that
     * its timers are not on.
     */
    var nowFn = o.now || Date.now;

    /* session state */
    var role = null;              /* 'initiator' | 'responder' */
    var phase = 'idle';
    var algo = null;
    var peer = null;              /* { count, fingerprint } */
    var finished = false;
    var messages = 0;
    var bytes = 0;

    /* messages that arrived before hello, replayed once it does */
    var held = [];

    /* cached work */
    var myBuckets = null;
    var myStrata = null;
    var pendingBucketIds = null;
    var sentPhase2 = false;   /* our bucket_items has gone out */
    var sketchCap = 0;

    /*
     * The decision inputs: this side's, as its hello will carry them, and the
     * ones agreed with the peer once its hello is in (see mergeInputs).
     *
     * expectedDiff and rtt are whole numbers on the wire for any sane encoder,
     * so they are rounded here, before they are compared with the peer's copy
     * of them: the two sides must compute from identical values, and a 100.4
     * here that arrives as 100 there is two different inputs.
     */
    var myHint = present(o.expectedDiff) ? Math.max(0, Math.round(o.expectedDiff)) : undefined;
    var myRtt  = (present(o.rtt) && o.rtt > 0) ? Math.round(o.rtt) : undefined;
    var myBw   = (present(o.bandwidth) && o.bandwidth > 0) ? o.bandwidth : undefined;
    var effectiveHint = myHint;
    var allowSketch = !!o.sketch;
    var link = mergeLink(myRtt, myBw, undefined, undefined);

    /*
     * PULL MODE: only one side needs the answer.
     *
     * FULL_DUMP and BUCKETS are symmetric because both sides normally learn
     * what they are missing, and each needs the other's contents to work it
     * out. When only one side cares - a client catching up from a server, a
     * read-only replica - the other half of that traffic is wasted, and it is
     * exactly half.
     *
     * STRATA and SKETCH are already one-directional, so this changes nothing
     * for them.
     *
     * The puller announces itself in the handshake; the other side answers
     * and stays silent about its own gaps, finishing with two empty lists.
     */
    var pulling = !!o.pull;
    var peerPulling = false;
    /*
     * Set when THIS side lost the two-puller tie-break above.
     *
     * The losing side answers normally and learns nothing, so it finishes
     * 'complete' with two empty lists - which is byte for byte what a session
     * between two identical peers reports. A caller who asked to pull cannot
     * tell "there was nothing to fetch" from "my request was overruled and I
     * fetched nothing", and those call for opposite next moves.
     *
     * It is reported in the finish stats rather than as a different reason,
     * because the sync did complete and existing reason-handling should not
     * have to change to keep working.
     */
    var pullOverridden = false;

    /*
     * ASKING BEATS SENDING BLIND.
     *
     * The bucket exchange used to answer the hashes with every disagreeing
     * bucket at once. That works, but it gives the receiver no say: it cannot
     * stop once it has enough, and a large difference arrives as one very
     * large message.
     *
     * Now the side that wants the items asks for them, a chunk at a time. It
     * knows which buckets disagree - it computed that from the hashes - and
     * it knows how many new items it has collected, because it holds its own
     * list. Nothing has to be estimated, which is what sank an earlier
     * attempt at the same feature from the sending side.
     */
    /*
     * A LIMIT IS A STOPPING CONDITION, NOT AN ESTIMATE.
     *
     * An earlier attempt at this worked from the sending side, guessing how
     * many buckets would yield the requested number of items. It needed to
     * know how thinly the difference was spread, which is not knowable before
     * the buckets are exchanged, and every formula was badly wrong somewhere:
     * fifty asked for returned four in one case and five hundred in another.
     *
     * With the asking loop in place there is nothing to guess. The side that
     * wants the items counts what has arrived - it holds its own list, so it
     * knows exactly which are new - and stops asking. The far side learns
     * from the `done` that follows.
     */
    var limit = (typeof o.limit === 'number' && o.limit > 0) ? o.limit : 0;
    var chunkSize = (typeof o.chunkSize === 'number') ? o.chunkSize : DEFAULT_CHUNK;
    var peerChunk = DEFAULT_CHUNK;      /* what the far side asked for */

    /* what this side still wants, and what it has collected so far */
    var pendingWants = null;            /* bucket ids not yet requested */
    var collectedFrom = null;           /* bucket ids already accounted for */
    var collectedMine = [];
    var collectedTheirs = [];
    var dumpOffset = 0;                 /* how far through a chunked dump */
    var iAmDone = false, peerIsDone = false, endReason = null;
    var peerDumpCount = 0;
    var dumpParts = [];


    /* timers */
    var deadline = null;
    var deadlineAt = 0;          /* when the deadline lands, on nowFn's clock */
    var grantedTotal = 0;        /* extension handed out so far, both causes */
    var holdTimer = null;
    var holdTotal = 0;

    /* ---------------- sending ---------------- */

    /*
     * OUTGOING MESSAGES ARE QUEUED, NOT FIRED IMMEDIATELY.
     *
     * A caller who delivers synchronously - which every simulation does, and
     * which two objects in one process do - would otherwise re-enter this
     * session in the middle of handling a message. That really happened: the
     * responder sent its bucket hashes, the initiator answered with its
     * items, the responder finished on that answer, and then reached its own
     * send() and found the session already closed. The message was silently
     * dropped and the initiator hung until timeout.
     *
     * Queueing and flushing at the end of feed() removes the whole class
     * of problem, and costs one array.
     */
    var outbox = [];
    var inHandler = 0;

    /* the far side knows this session is over: it ended it, or we said so */
    var told = false;

    function send(type, data, predicted) {
        if (finished) { return; }
        if (type === 'abort' || type === 'reject') { told = true; }
        messages++;
        bytes += Math.round((predicted || 0) * overhead);
        outbox.push([type, data]);
        /*
         * A send from OUTSIDE a handler - the hold timer, most importantly -
         * has nothing that will flush it later. Queuing without flushing is
         * how the first version silently swallowed every hold and let both
         * sides time out while each waited for the other.
         */
        if (!inHandler) { flush(); }
    }

    function flush() {
        while (outbox.length) {
            var m = outbox.shift();
            s._fire('send', m[0], m[1]);
        }
    }

    /*
     * ONE ENDING, WITH A REASON.
     *
     * A session can end well, end badly, or end early - and the third is
     * neither of the first two. Stopping because enough items were found is
     * not a failure and is not the complete answer either; forcing it into a
     * done/fail pair means one of them lies.
     *
     * Every ending carries the results, because an early stop has them and a
     * timeout's empty arrays cost nothing. And a caller cleaning up a session
     * map writes one handler instead of two that have to agree.
     */
    function finish(reason, myMissing, peerMissing) {
        if (finished) { return; }
        /*
         * AN ENDING THE FAR SIDE CANNOT SEE IS SENT TO IT.
         *
         * Most endings are shared: both sides finish on the same message, or
         * one side aborts or rejects and says so. The rest happen here only -
         * our deadline passed, the caller never provided its items, a message
         * from the peer was malformed - and the far side, hearing nothing,
         * waited out its whole deadline, a minute and more with holds, for a
         * reply that was never coming. It looked like a dead peer. So the
         * reason goes out as an abort, once, before the session closes.
         *
         * Not before the session began: a peer that never heard from us has
         * nothing to end.
         */
        if (!told && role && reason !== 'complete' && reason !== 'limit') {
            send('abort', { reason: reason }, 12);
        }
        finished = true;
        clearTimers();
        s._fire('finish', reason, myMissing || [], peerMissing || [], {
            algorithm: algo,
            messages: messages,
            predictedBytes: bytes,
            role: role,
            /* true only when this side asked to pull and was overruled */
            pullOverridden: pullOverridden
        });
    }

    /* An ending with nothing to show for it. */
    function fail(reason) { finish(reason, [], []); }

    function clearTimers() {
        if (deadline) { clearT(deadline); deadline = null; }
        if (holdTimer) { clearT(holdTimer); holdTimer = null; }
    }

    /* ---------------- lazy item loading ---------------- */

    /**
     * Ask the caller for items, and cover for them if they are slow.
     *
     * A caller paging a million rows out of a database will not answer within
     * a normal timeout, and the far side has no way to tell that from a dead
     * peer. So after HOLD_GRACE this starts sending 'hold', which asks the
     * other side for more time - bounded by HOLD_MAX_TOTAL so that a caller
     * who never answers still fails rather than hanging both ends forever.
     */
    var loadWaiters = null;      /* callbacks waiting on an in-flight load */

    function needItems(hint, then) {
        /*
         * ITEMS ARE LOADED AT MOST ONCE PER SESSION.
         *
         * Several messages need the list, and an early version called out to
         * the application for each of them - which meant several overlapping
         * loads, several hold timers fighting over one variable, and a
         * session that never finished because answering the latest request
         * left the earlier ones pending forever.
         *
         * The list cannot change mid-sync anyway - the fingerprint sent in
         * the hello already committed to it - so caching is not just an
         * optimisation, it is what makes the commitment true.
         */
        if (items) { then(items); return; }

        if (loadWaiters) { loadWaiters.push(then); return; }
        loadWaiters = [then];

        if (!s._h['need']) { fail('no_item_source'); return; }

        /*
         * A caller paging a million rows will not answer inside a normal
         * timeout, and the far side cannot tell that from a dead peer. After
         * HOLD_GRACE this starts asking for extensions - bounded, so a caller
         * that never answers fails rather than hanging both ends.
         */
        holdTimer = setT(function sendHold() {
            if (items || finished) { return; }
            if (holdTotal >= HOLD_MAX_TOTAL) { fail('app_timeout'); return; }
            holdTotal += HOLD_EXTRA_MS;
            send('hold', { extraMs: HOLD_EXTRA_MS }, 12);
            bumpDeadline(HOLD_EXTRA_MS);
            holdTimer = setT(sendHold, HOLD_EXTRA_MS / 2);
        }, HOLD_GRACE);

        s._provide = function (got) {
            if (items || finished) { return; }
            /*
             * The lazy path gets the same check as the eager one. A list
             * arriving from a database is MORE likely to carry a surprise -
             * a float from a numeric column, a bigint driver returning a
             * string - not less, and by this point the caller's stack is
             * gone, so an exception here would land in a timer.
             */
            if (checkItems) {
                try { validateItems(got); }
                catch (e) { fail('bad_items'); s._fire('error', e); return; }
            }
            items = got;
            count = got.length;
            if (holdTimer) { clearT(holdTimer); holdTimer = null; }
            var waiting = loadWaiters;
            loadWaiters = null;
            for (var i = 0; i < waiting.length; i++) { waiting[i](items); }
        };

        s._fire('need', hint);
    }

    /*
     * Arm the one overall deadline, `ms` from now.
     *
     * deadlineAt is tracked alongside the timer because a timer handle does
     * not say how much of it is left, and an extension has to know.
     */
    function armDeadline(ms) {
        if (deadline) { clearT(deadline); }
        deadlineAt = nowFn() + ms;
        deadline = setT(function () { fail('timeout'); }, ms);
    }

    /**
     * Extend the deadline. THIS ADDS TO WHAT IS LEFT; it never shortens it.
     *
     * It used to REPLACE the deadline with `extra` milliseconds, which
     * inverted the meaning of the one message whose entire job is to buy
     * time: a session with a 30 second budget that received a hold two
     * seconds in came out with twelve seconds instead of twenty-eight, and a
     * hold asking for anything under the remaining budget actively shortened
     * it. Measured on the original: `timeout: 30000` plus a hold of 1200ms
     * died after 1202ms.
     *
     * `extra` ARRIVES FROM THE NETWORK, so it is not trusted. A peer sending
     * extraMs: 5 used to close the session in six milliseconds, and a
     * negative value closed it on the next tick; both now fall back to the
     * default. Grants are capped in total so a peer cannot hold a session
     * open indefinitely by repeating them.
     */
    function bumpDeadline(extra) {
        var e = (typeof extra === 'number' && isFinite(extra) && extra > 0)
              ? extra : HOLD_EXTRA_MS;

        if (grantedTotal >= HOLD_MAX_TOTAL) { return; }
        e = Math.min(e, HOLD_MAX_TOTAL - grantedTotal);
        grantedTotal += e;

        var remaining = Math.max(0, deadlineAt - nowFn());
        armDeadline(remaining + e);
    }

    /* ---------------- the algorithms ---------------- */

    function runDump() {
        algo = 'FULL_DUMP';

        /*
         * A DUMP IS ASKED FOR TOO, once it is bigger than a chunk.
         *
         * Everything else respects chunkSize; a dump ignoring it made the
         * setting a half-promise - a caller asking for hundred-item messages
         * got 1.1 KB from a bucket exchange and 781 KB from a dump of the
         * same list. Offering the count and letting the other side ask for
         * ranges of it costs one extra round trip and keeps the guarantee.
         *
         * Below a chunk there is nothing to split, so the list goes as it
         * always did and no round trip is spent.
         */
        if (pulling) {
            /* a puller offers nothing, but still wants to be asked to */
            send('dump_offer', { count: count }, 12);
            return;
        }
        needItems({ all: true }, function (all) {
            if (sentPhase2) { return; }
            if (chunkSize > 0 && all.length > chunkSize) {
                sentPhase2 = true;
                send('dump_offer', { count: all.length }, 12);
                return;
            }
            sentPhase2 = true;
            send('dump', { items: all }, predictFullDumpBytes(count, peer.count));
            /*
             * Answering a puller leaves nothing to wait for - that is the
             * whole point - so this side is done. It still answers anything
             * the puller asks for; the session ends when both have said so.
             */
            if (peerPulling) { sayDone('complete'); }
        });
    }

    function runBuckets() {
        algo = 'BUCKETS';
        var B = bucketCountFor(Math.max(count, peer.count));
        needItems({ all: true }, function (all) {
            myBuckets = buildBuckets(all, B);
            send('buckets', { B: B, hashes: myBuckets.hashes }, B * 8 + 10);
        });
    }

    /*
     * THE STRATA PATH IS ASYMMETRIC, ON PURPOSE.
     *
     * Only the initiator sends its Strata; only the responder answers with an
     * IBLT; only the initiator decodes and reports back. One IBLT crosses the
     * wire instead of two, which is most of the cost.
     *
     * A symmetric version was tried and it was wrong in a way that only
     * showed up under a size-aware network model: both sides sent an IBLT,
     * and the small 'iblt_misses' reply overtook the large IBLT still in
     * flight - so the initiator finished on partial information while its own
     * decode input was still on the wire. Synchronous delivery hid it
     * completely, because nothing can overtake anything when every message
     * arrives instantly.
     */
    /*
     * WHO DRIVES AN ASYMMETRIC PATH IS DECIDED BY FINGERPRINT, NOT BY ROLE.
     *
     * When both peers call start() they are both initiators, and keying on
     * that would have both send a Strata and both answer with an IBLT -
     * twice the bytes and twice the decoding for the same answer.
     *
     * Comparing fingerprints picks one side deterministically, and both
     * arrive at the same choice because each holds both values by then. They
     * cannot tie: identical fingerprints mean identical sets, which ended at
     * the handshake.
     */
    function drives() {
        return fingerprint < peer.fingerprint;
    }

    function runStrata() {
        algo = 'STRATA_IBLT';
        if (!drives()) { return; }      /* the other side sends the Strata */
        var L = strataLevelsFor(count, peer.count);
        needItems({ all: true }, function (all) {
            myStrata = strataEncode(all, L);
            send('strata', strataToWire(myStrata),
                 predictStrataBytes(count, peer.count));
        });
    }

    /*
     * BOTH SIDES CHOOSE INDEPENDENTLY, and both send their own phase-1
     * message. That is the point of the decider being a pure function of
     * values both sides hold: no message announces the algorithm, and
     * nothing has to be echoed back. An earlier version had the responder
     * echo, which sent everything twice.
     */
    /*
     * THE SKETCH PATH IS ASYMMETRIC, like Strata's: only the initiator sends
     * a sketch, only the responder decodes, and only one result comes back.
     * Both sides decoding would double the CPU for no extra information -
     * and decoding is the expensive half here by a wide margin.
     */
    /*
     * A ZERO CAN STILL REACH THE SKETCH.
     *
     * Both sides must allow the sketch, and a side that allows it has its
     * zeros rejected in the constructor - but only when it validates: with
     * `validate: false`, or with items that arrived later through provide(),
     * a zero gets this far, and building the sketch throws.
     *
     * So both sketch sites check first and end the session cleanly, with a
     * reason that names the cause, rather than throwing out of a message
     * handler and leaving the far side to time out.
     */
    function sketchBlockedByZero(all) {
        if (findZero(all) === -1) { return false; }
        send('abort', { reason: 'zero_item' }, 12);
        fail('zero_item');
        return true;
    }

    function runSketch(capacity) {
        algo = 'SKETCH';
        sketchCap = capacity;
        if (!drives()) { return; }
        needItems({ all: true }, function (all) {
            if (sketchBlockedByZero(all)) { return; }
            var sk = sketch(capacity);
            for (var i = 0; i < all.length; i++) { sk.add(all[i]); }
            send('sketch', { capacity: capacity, syndromes: flattenSketch(sk.raw()) },
                 40 + capacity * 8);
        });
    }

    /*
     * Ask for the next chunk of buckets, or finish if there is nothing left
     * to ask for. Called once after the hashes arrive and again after every
     * answer, which is what makes the exchange a loop rather than a single
     * shot.
     */
    /* This side has nothing more to ask; end once the other agrees. */
    function sayDone(reason) {
        if (iAmDone) { return; }
        iAmDone = true;
        endReason = reason;
        send('done', {}, 8);
        if (peerIsDone) { endSession(); }
    }

    function endSession() {
        finish(endReason || 'complete', collectedMine, collectedTheirs);
    }

    /*
     * The dump path finishes as soon as it has the answer, but a peer that
     * asked for its own chunks is still mid-flight. Routing both paths
     * through sayDone/endSession keeps one rule: nobody stops listening
     * until both have stopped asking.
     */

    /* Ask for the next slice of a chunked dump. */
    function askForDump() {
        var take = chunkSize > 0 ? chunkSize : peerDumpCount;
        send('want_dump', { from: dumpOffset, take: take }, 20);
    }

    function askForBuckets() {
        /* enough is enough - the rest keeps until next time */
        if (limit > 0 && collectedMine.length >= limit) {
            sayDone('limit');
            return;
        }
        if (!pendingWants || !pendingWants.length) {
            sayDone('complete');
            return;
        }
        /*
         * With a limit, ask in units no larger than what is still wanted -
         * a chunk is taken whole, so a large one overshoots by however much
         * it carried past the target.
         */
        var effChunk = limit > 0
            ? Math.min(chunkSize || DEFAULT_CHUNK, Math.max(1, limit - collectedMine.length))
            : chunkSize;
        var n = effChunk > 0 ? bucketsForChunk(effChunk) : pendingWants.length;
        var batch = pendingWants.splice(0, Math.max(1, n));
        send('want_buckets', { buckets: batch }, batch.length * 4 + 10);
    }

    /*
     * How many buckets to ask for so the answer lands near chunkSize items.
     * A bucket holds roughly count/B items, and asking for too few costs a
     * round trip while asking for too many costs nothing but a bigger
     * message - so this rounds up.
     */
    function bucketsForChunk(size) {
        var B = myBuckets ? myBuckets.hashes.length : 1;
        var perBucket = Math.max(1, Math.ceil(Math.max(count, peer.count) / B));
        return Math.max(1, Math.ceil(size / perBucket));
    }

    function choose() {
        var d = decidePreStrata(count, peer.count, effectiveHint, allowSketch, link);
        if (d.algo === 'FULL_DUMP') { runDump(); }
        else if (d.algo === 'BUCKETS') { runBuckets(); }
        else if (d.algo === 'SKETCH') { runSketch(d.params.capacity); }
        else { runStrata(); }
    }

    /* ---------------- public ---------------- */

    /**
     * Open the exchange.
     *
     * SAFE TO CALL WHEN THE OTHER SIDE ALREADY DID. Two peers deciding to
     * sync at the same moment is normal - a timer on each fires, or both
     * notice the same change - and so is deciding to start just as the far
     * side's hello lands. An earlier version threw on both, sending an
     * exception into the caller's transport handler for a situation the
     * caller could not have avoided.
     *
     * Whichever side spoke first, the sync is under way; this returns quietly
     * rather than starting a second one.
     */
    s.start = function () {
        if (role || finished) { return s; }
        role = 'initiator';
        phase = 'hello';
        armDeadline(o.timeout || DEFAULT_TIMEOUT);
        withFingerprint(function () {
            send('hello', {
                count: count, fingerprint: fingerprint,
                expectedDiff: myHint, sketch: !!o.sketch,
                rtt: myRtt, bandwidth: myBw, pull: !!o.pull,
                chunkSize: chunkSize
            }, 24);
        });
        return s;
    };

    /* Load the items first if the fingerprint has to be derived from them. */
    function withFingerprint(then) {
        if (!needsFingerprint) { then(); return; }
        needItems({ all: true }, function (all) {
            fingerprint = computeFingerprint(all);
            needsFingerprint = false;
            then();
        });
    }

    /** Answer a 'need' event. */
    s.provide = function (got) {
        if (!s._provide) { return s; }
        inHandler++;
        try { s._provide(got); } finally { inHandler--; }
        flush();
        return s;
    };

    s.abort = function (reason) {
        if (finished) { return s; }
        send('abort', { reason: reason || 'aborted' }, 12);
        var pending = outbox.slice();
        outbox.length = 0;
        fail(reason || 'aborted');
        /* the abort itself still goes out, even though the session is closed */
        for (var i = 0; i < pending.length; i++) { s._fire('send', pending[i][0], pending[i][1]); }
        return s;
    };

    /**
     * Feed in a message from the far side - the same (type, data) that 'send'
     * produced there, after a round trip through the caller's encoder.
     */
    s.feed = function (type, data) {
        if (finished) { return s; }
        inHandler++;
        try { handle(type, data); } finally { inHandler--; }
        flush();
        return s;
    };

    function handle(type, data) {
        messages++;

        /* the far side asked for more time; extend and wait */
        if (type === 'hold') {
            bumpDeadline(data.extraMs || HOLD_EXTRA_MS);
            return;
        }
        if (type === 'abort') { told = true; fail(data && data.reason || 'aborted'); return; }

        /*
         * `done` MEANS "I HAVE NO MORE REQUESTS", NOT "STOP".
         *
         * Both sides ask independently, and they finish at different moments:
         * a peer missing nothing runs out of buckets to ask for while the
         * other is still collecting. Treating the first `done` as the end of
         * the session cut the other side off mid-collection - it asked for a
         * thousand items and got nine hundred and sixty-nine, reported as a
         * clean finish.
         *
         * So the session ends when BOTH have said it. Until then this side
         * keeps asking and keeps answering.
         */
        if (type === 'done') {
            peerIsDone = true;
            if (iAmDone) { endSession(); }
            return;
        }
        if (type === 'reject') { told = true; fail('rejected'); return; }

        /* first contact: we are the responder */
        if (!role) {
            role = 'responder';
            armDeadline(o.timeout || DEFAULT_TIMEOUT);
        }

        /*
         * ANYTHING ARRIVING BEFORE hello IS HELD, NOT REJECTED.
         *
         * The transport this runs over is not assumed to preserve order -
         * reliable delivery, no duplicates and whole messages are all it has
         * to provide. So a peer's second message can outrun its first, and
         * every message but hello needs peer.count to be meaningful.
         *
         * An earlier version failed on these, which turned a reordering into
         * a dead session; the version before that threw a TypeError out of
         * feed() and into the caller's transport handler. Holding them and
         * replaying after hello costs one small array and makes the whole
         * class of problem disappear.
         *
         * The queue is bounded: a peer that floods without ever sending hello
         * is refused rather than allowed to grow it without limit.
         */
        if (type !== 'hello' && !peer) {
            if (held.length >= 16) { fail('too_many_early'); return; }
            held.push([type, data]);
            return;
        }

        switch (type) {

        case 'hello':
            if (!data || typeof data.count !== 'number' ||
                typeof data.fingerprint !== 'number') {
                fail('bad_message');
                return;
            }
            peer = { count: data.count, fingerprint: data.fingerprint };
            peerPulling = !!data.pull;
            if (typeof data.chunkSize === 'number') { peerChunk = data.chunkSize; }

            /*
             * TWO PULLERS WOULD DEADLOCK, so one of them gives.
             *
             * `pull` says "send me yours, I am keeping mine". When both sides
             * say it, neither sends anything and both sit out the timeout -
             * which really happened, and looked exactly like a network fault
             * rather than a configuration mistake.
             *
             * The side with the higher fingerprint drops its claim and answers
             * normally. Both compute the same comparison, so they agree
             * without another message, and the caller who set the flag on
             * both sides gets a working sync instead of a stall. It costs the
             * losing side the bandwidth it hoped to save, which is the right
             * failure: the sync completes.
             */
            if (pulling && peerPulling) {
                if (fingerprint > data.fingerprint) { pulling = false; pullOverridden = true; }
                else { peerPulling = false; }
            }

            /*
             * THE INPUTS TRAVEL IN THE HELLO, AND BOTH SIDES COMBINE THEM THE
             * SAME WAY.
             *
             * The decider is only leaderless because it is a pure function of
             * values both sides hold - and expectedDiff, the sketch flag and
             * the link are not among them unless they cross. Without that the
             * two sides really did diverge: one picked the sketch while the
             * other picked buckets, and each answered a protocol the other was
             * not running.
             *
             * They used to be taken whole from the side with the lower
             * fingerprint, which is an agreement but throws information away:
             * a hint given on one side was used only when that side happened
             * to have the lower fingerprint - and twelve times in twenty it did
             * not, so a sync the sketch settles in 330 bytes took 24 KB. Now
             * whatever either side gave is used, and what both gave is
             * combined by rules that read the same from either end (see
             * mergeInputs). Equal fingerprints no longer diverge either: under
             * the old rule each side took the other's.
             */
            var merged = mergeInputs(myHint, !!o.sketch, myRtt, myBw,
                                     data.expectedDiff, !!data.sketch, data.rtt, data.bandwidth);
            effectiveHint = merged.expectedDiff;
            allowSketch = merged.sketch;
            link = merged.link;

            /*
             * The responder always answers, INCLUDING when the fingerprints
             * already match - otherwise the initiator learns nothing and
             * waits out its timeout. Sending first and deciding second is
             * the ordering that avoids it.
             */
            if (role === 'responder' && !needsFingerprint) {
                send('hello', {
                    count: count, fingerprint: fingerprint,
                    expectedDiff: myHint, sketch: !!o.sketch,
                rtt: myRtt, bandwidth: myBw, pull: !!o.pull,
                chunkSize: chunkSize
                }, 24);
            }

            if (needsFingerprint) {
                /* answer once the list is in hand, then carry on as usual */
                var d0 = data;
                withFingerprint(function () {
                    send('hello', {
                        count: count, fingerprint: fingerprint,
                        expectedDiff: myHint, sketch: !!o.sketch,
                rtt: myRtt, bandwidth: myBw, pull: !!o.pull,
                chunkSize: chunkSize
                    }, 24);
                    if (fingerprint === d0.fingerprint && count === d0.count) {
                        finish('complete', [], []);
                        return;
                    }
                    choose();
                });
                return;
            }

            /*
             * IDENTICAL FINGERPRINTS END IT HERE. Nothing more needs to
             * cross, and in a system that syncs regularly this is the common
             * case - worth the check even though it looks special.
             */
            if (peer.fingerprint === fingerprint && peer.count === count) {
                finish('complete', [], []);
                return;
            }

            /* let the caller refuse before any real work happens */
            var refused = false;
            s._fire('hello', peer, function (ok) { if (!ok) { refused = true; } });
            if (refused) { send('reject', {}, 8); fail('rejected'); return; }

            choose();

            /* whatever outran the handshake can be processed now */
            if (held.length) {
                var queued = held;
                held = [];
                for (var hi = 0; hi < queued.length && !finished; hi++) {
                    handle(queued[hi][0], queued[hi][1]);
                }
            }
            break;

        case 'dump_offer':
            if (!data || typeof data.count !== 'number') { fail('bad_message'); return; }
            algo = 'FULL_DUMP';
            peerDumpCount = data.count;
            dumpOffset = 0;
            askForDump();
            break;

        case 'want_dump':
            if (!data || typeof data.from !== 'number' ||
                typeof data.take !== 'number') { fail('bad_message'); return; }
            if (pulling) { return; }
            /*
             * Answered even after this side has said `done`. Saying done
             * means "I have no more questions", not "I have stopped
             * listening" - and the far side is still working through its own.
             */
            needItems({ all: true }, function (all) {
                var slice = all.slice(data.from, data.from + data.take);
                send('dump', { items: slice, from: data.from,
                               more: (data.from + data.take) < all.length },
                     slice.length * 8 + 16);
            });
            break;

        case 'dump':
            if (!data || !Array.isArray(data.items)) { fail('bad_message'); return; }
            algo = algo || 'FULL_DUMP';
            needItems({ all: true }, function (all) {
                /*
                 * Send ours first if choose() has not already - a dump that
                 * outran the handshake would otherwise finish us before we
                 * had answered, and the far side would wait forever.
                 */
                if (!sentPhase2 && !pulling) {
                    sentPhase2 = true;
                    if (chunkSize > 0 && all.length > chunkSize) {
                        send('dump_offer', { count: all.length }, 12);
                    } else {
                        send('dump', { items: all },
                             predictFullDumpBytes(count, all.length));
                    }
                }

                /* a chunked dump arrives in pieces; hold them until the last */
                if (typeof data.from === 'number') {
                    collectedTheirs = collectedTheirs.concat([]);
                    dumpParts = dumpParts.concat(data.items);
                    dumpOffset = data.from + data.items.length;
                    if (data.more) { askForDump(); return; }
                    var dAll = fullDumpDiff(all, dumpParts);
                    collectedMine = peerPulling ? [] : dAll.myMissing;
                    collectedTheirs = dAll.peerMissing;
                    sayDone('complete');
                    return;
                }

                var d = fullDumpDiff(all, data.items);
                collectedMine = peerPulling ? [] : d.myMissing;
                collectedTheirs = d.peerMissing;
                sayDone('complete');
            });
            break;

        case 'buckets':
            if (!data || !data.hashes || typeof data.B !== 'number') {
                fail('bad_message'); return;
            }
            algo = 'BUCKETS';
            needItems({ all: true }, function (all) {
                if (!myBuckets) { myBuckets = buildBuckets(all, data.B); }

                /* which buckets disagree - both sides compute the same set */
                var differing = [];
                for (var i = 0; i < data.hashes.length; i++) {
                    if (myBuckets.hashes[i] !== data.hashes[i]) { differing.push(i); }
                }

                if (!differing.length) { sayDone('complete'); return; }

                /*
                 * A PULLER ASKS BUT DOES NOT ANSWER, and the side it pulls
                 * from does the reverse.
                 *
                 * Under the old one-shot exchange `pull` meant "do not send
                 * my items". Now nothing is sent unless it is asked for, so
                 * the flag has to move to the asking: the puller asks and
                 * stays quiet, the other side answers and asks for nothing.
                 * Without this the restructure silently made `pull` a no-op -
                 * both sides asked, both answered, and the traffic was
                 * identical with and without it.
                 */
                if (peerPulling) { sayDone('complete'); return; }

                /*
                 * A puller wants the contents and is not offering its own, so
                 * it asks and waits. Everyone else asks too - the difference
                 * is that they will also answer when asked.
                 */
                pendingWants = differing;
                askForBuckets();
            });
            break;

        case 'want_buckets':
            if (!data || !Array.isArray(data.buckets)) { fail('bad_message'); return; }
            /* a puller offers nothing; it said so in the handshake */
            if (pulling) { return; }
            /* answered even after saying done - see want_dump */
            needItems({ buckets: data.buckets }, function (all) {
                if (!myBuckets) {
                    myBuckets = buildBuckets(all,
                        bucketCountFor(Math.max(count, peer.count)));
                }
                var give = [];
                for (var i = 0; i < data.buckets.length; i++) {
                    var b = myBuckets.bucketedItems[data.buckets[i]];
                    if (b) { give = give.concat(b); }
                }
                send('bucket_items', { buckets: data.buckets, items: give },
                     give.length * 8 + data.buckets.length * 4 + 10);
            });
            break;

        case 'bucket_items':
            if (!data || !Array.isArray(data.items) || !Array.isArray(data.buckets)) {
                fail('bad_message'); return;
            }
            /*
             * Only the items inside the buckets that were asked for get
             * compared - everything else matched by hash. The comparison is
             * local, so what counts as "new" is known immediately and the
             * next chunk can be asked for without anyone estimating anything.
             */
            needItems({ buckets: data.buckets }, function (all) {
                if (!myBuckets) {
                    myBuckets = buildBuckets(all,
                        bucketCountFor(Math.max(count, peer.count)));
                }

                /*
                 * A REPEATED ANSWER MUST NOT COUNT TWICE.
                 *
                 * Results accumulate across chunks now, so a duplicated
                 * message adds the same items again and the totals come out
                 * doubled - which is a wrong answer, not a wasted message.
                 * The single-shot version had nothing to accumulate into and
                 * was immune; this is the cost of the loop.
                 *
                 * Bucket ids identify what a chunk carried, so remembering
                 * them is enough. Anything already accounted for is dropped
                 * without asking for the next chunk, because the answer to
                 * that request is already on its way.
                 */
                if (!collectedFrom) { collectedFrom = Object.create(null); }
                var fresh = [];
                for (var q = 0; q < data.buckets.length; q++) {
                    if (!collectedFrom[data.buckets[q]]) {
                        collectedFrom[data.buckets[q]] = 1;
                        fresh.push(data.buckets[q]);
                    }
                }
                if (!fresh.length) { return; }

                var mine = [];
                for (var i = 0; i < fresh.length; i++) {
                    var b = myBuckets.bucketedItems[fresh[i]];
                    if (b) { mine = mine.concat(b); }
                }
                /* compare only against the items from the buckets we kept */
                var theirs = [];
                var keep = Object.create(null);
                for (i = 0; i < fresh.length; i++) { keep[fresh[i]] = 1; }
                var B2 = myBuckets.hashes.length;
                for (i = 0; i < data.items.length; i++) {
                    if (keep[bucketOf(data.items[i], B2)]) { theirs.push(data.items[i]); }
                }

                var d = fullDumpDiff(mine, theirs);
                collectedMine = collectedMine.concat(d.myMissing);
                collectedTheirs = collectedTheirs.concat(d.peerMissing);
                askForBuckets();
            });
            break;

        case 'strata':
            if (!data || typeof data.levels !== 'number' || !data.counts) {
                fail('bad_message'); return;
            }
            algo = 'STRATA_IBLT';
            needItems({ all: true }, function (all) {
                var theirs = strataFromWire(data);
                if (!myStrata) {
                    myStrata = strataEncode(all, data.levels);
                }
                var est = strataEstimate(myStrata, theirs);

                /*
                 * The responder sizes the IBLT, and the initiator cannot
                 * retry a table that is too small without a full round trip -
                 * so the hedge in ibltParamsForDiff is doing real work here,
                 * and a decode failure falls back to a dump rather than
                 * resizing.
                 */
                var p = ibltParamsForDiff(est.dScaled);
                var t = ibltNew(p.cells, p.k);
                for (var i = 0; i < all.length; i++) { ibltInsert(t, all[i]); }
                send('iblt', ibltToWire(t), predictIbltBytes(est.dScaled));
            });
            break;

        case 'iblt':
            if (!data || typeof data.cells !== 'number' || !data.counts) {
                fail('bad_message'); return;
            }
            needItems({ all: true }, function (all) {
                var theirs = ibltFromWire(data);
                var mine = ibltNew(data.cells, data.k);
                for (var i = 0; i < all.length; i++) { ibltInsert(mine, all[i]); }

                var dec = ibltDecode(ibltSubtract(mine, theirs));
                if (!dec.success) {
                    /* the estimate was too low; dumping always converges */
                    algo = 'STRATA_IBLT→DUMP';
                    send('dump', { items: all },
                         predictFullDumpBytes(count, peer.count));
                    return;
                }
                /*
                 * ORIENTATION MATTERS AND IS EASY TO INVERT. The subtraction
                 * was mine - theirs, so aOnly holds what I have and they do
                 * not: that is what THEY are missing. bOnly is the reverse.
                 * Getting this backwards produces a result that looks
                 * plausible and is exactly wrong.
                 */
                /*
                 * ORIENTATION MATTERS AND IS EASY TO INVERT. The subtraction
                 * was mine - theirs, so aOnly holds what I have and they do
                 * not: that is what THEY are missing. bOnly is the reverse.
                 * Getting this backwards produces a result that looks
                 * plausible and is exactly wrong.
                 */
                var theyLack = dec.aOnly || [];
                var iLack = dec.bOnly || [];
                /*
                 * BOTH LISTS GO BACK, not just theirs. The responder never
                 * decodes, so without being told it would have to report
                 * "they are missing nothing" - which is a lie rather than an
                 * absence. And knowing what the initiator lacks lets it push
                 * those items instead of waiting to be asked.
                 */
                send('iblt_misses', { items: theyLack, mine: iLack },
                     (theyLack.length + iLack.length) * 8 + 12);
                finish('complete', iLack, theyLack);
            });
            break;

        case 'sketch':
            if (!data || typeof data.capacity !== 'number' || !Array.isArray(data.syndromes)) {
                fail('bad_message'); return;
            }
            algo = 'SKETCH';
            needItems({ all: true }, function (all) {
                if (sketchBlockedByZero(all)) { return; }
                var mine = sketch(data.capacity);
                for (var i = 0; i < all.length; i++) { mine.add(all[i]); }
                mine.merge(unflattenSketch(data.syndromes));

                var diff = mine.decode();
                if (diff === null) {
                    /*
                     * The difference exceeded the capacity, and a sketch that
                     * cannot decode gives NOTHING - not a subset. Falling back
                     * to a dump always converges, which is the same escape the
                     * IBLT path uses.
                     */
                    algo = 'SKETCH→DUMP';
                    send('dump', { items: all },
                         predictFullDumpBytes(count, peer.count));
                    return;
                }

                /*
                 * The decode gives the symmetric difference as one list, with
                 * no indication of which side each item came from - so it is
                 * classified here, where one side's contents are known: an
                 * item we hold is one THEY lack, and the rest are ours to
                 * fetch.
                 */
                var have = Object.create(null);
                for (i = 0; i < all.length; i++) { have[all[i]] = 1; }
                var theyLack = [], iLack = [];
                for (i = 0; i < diff.length; i++) {
                    if (have[diff[i]]) { theyLack.push(diff[i]); }
                    else { iLack.push(diff[i]); }
                }

                /*
                 * CHECK THE ANSWER AGAINST THE FINGERPRINTS, WHICH THE
                 * DECODE NEVER TOUCHED.
                 *
                 * A sketch given less capacity than the real difference does
                 * not fail - it returns a different, smaller set that
                 * reproduces those syndromes perfectly. Nothing inside the
                 * sketch can tell the two apart, because the syndromes are
                 * all it has and the wrong answer satisfies them by
                 * construction. Requiring the degree to fall below the
                 * capacity helps and is not enough; a wrong answer of low
                 * degree still slips through.
                 *
                 * The fingerprints are independent evidence. They crossed in
                 * the handshake, before any of this, and they are an XOR of
                 * per-item hashes - so applying the recovered difference to
                 * mine must land exactly on theirs. It does when the answer
                 * is right and does not when it is not, and it costs one hash
                 * per recovered item.
                 */
                var fpCheck = fingerprint;
                for (i = 0; i < theyLack.length; i++) {
                    fpCheck = xor53(fpCheck, hashItem53(theyLack[i]));
                }
                for (i = 0; i < iLack.length; i++) {
                    fpCheck = xor53(fpCheck, hashItem53(iLack[i]));
                }
                if (fpCheck !== peer.fingerprint) {
                    algo = 'SKETCH→DUMP';
                    send('dump', { items: all },
                         predictFullDumpBytes(count, peer.count));
                    return;
                }
                send('sketch_result', { items: theyLack, mine: iLack },
                     (theyLack.length + iLack.length) * 8 + 12);
                finish('complete', iLack, theyLack);
            });
            break;

        case 'sketch_result':
            if (!data || !Array.isArray(data.items)) { fail('bad_message'); return; }
            finish('complete', data.items, data.mine || []);
            break;

        case 'iblt_misses':
            if (!data || !Array.isArray(data.items)) { fail('bad_message'); return; }
            /*
             * The initiator did the decode and is reporting both sides of it.
             * Only one side pays for decoding, which is the whole reason the
             * strata path is asymmetric.
             */
            finish('complete', data.items, data.mine || []);
            break;

        default:
            fail('unknown_message');
        }
        return;
    }

    /** For a caller that wants the fingerprint without starting anything. */
    s.fingerprint = function () { return fingerprint; };


    return s;
}



/* =======================================================================
 * 7. GF(2^64), for the sketch below
 * ======================================================================= */



/* the reduction polynomial below x^64: x^4 + x^3 + x + 1 */
var gf_POLY_LOW = 0x1b;

/*
 * Carry-less multiply of two 32-bit values, returning [hi, lo].
 *
 * FOUR BITS AT A TIME, from a sixteen-entry table of small multiples of b.
 * The table is built incrementally - t[2i] is t[i] shifted, t[2i+1] is that
 * XOR b - so it costs fifteen shift-and-xor pairs, against the 32 iterations
 * the bit-at-a-time form needs. With eight nibbles to walk afterwards it is
 * about three times faster, and this is the single hottest function in the
 * sketch.
 */
var gf_TL = new Int32Array(16), gf_TH = new Int32Array(16);

function gf_clmul32(a, b) {
    a = a >>> 0;
    var bl = b >>> 0;

    gf_TL[0] = 0; gf_TH[0] = 0;
    gf_TL[1] = bl; gf_TH[1] = 0;
    for (var i = 2; i < 16; i += 2) {
        var h = i >>> 1;
        gf_TL[i] = (gf_TL[h] << 1) >>> 0;
        gf_TH[i] = ((gf_TH[h] << 1) | (gf_TL[h] >>> 31)) >>> 0;
        gf_TL[i + 1] = (gf_TL[i] ^ bl) >>> 0;
        gf_TH[i + 1] = gf_TH[i];
    }

    var lo = 0, hi = 0;
    for (var s = 0; s < 32; s += 4) {
        var nib = (a >>> s) & 15;
        if (nib === 0) { continue; }
        var tl = gf_TL[nib] >>> 0, th = gf_TH[nib] >>> 0;
        if (s === 0) { lo ^= tl; hi ^= th; }
        else {
            lo = (lo ^ (tl << s)) >>> 0;
            hi = (hi ^ ((th << s) | (tl >>> (32 - s)))) >>> 0;
        }
    }
    return [hi >>> 0, lo >>> 0];
}

/**
 * Multiply in GF(2^64).
 *
 * Schoolbook: four 32x32 carry-less products assembled into 128 bits, then
 * folded back down. Karatsuba saves one product and costs more bookkeeping
 * than it saves at this width.
 */
function gf_gmul(a, b) {
    var ll = gf_clmul32(a[1], b[1]);
    var hh = gf_clmul32(a[0], b[0]);
    var lh = gf_clmul32(a[1], b[0]);
    var hl = gf_clmul32(a[0], b[1]);

    var mh = (lh[0] ^ hl[0]) >>> 0;
    var ml = (lh[1] ^ hl[1]) >>> 0;

    /* 128 bits as four words, least significant first */
    return gf_reduce128([
        ll[1] >>> 0,
        (ll[0] ^ ml) >>> 0,
        (hh[1] ^ mh) >>> 0,
        hh[0] >>> 0
    ]);
}

/**
 * Fold a 128-bit value down to 64.
 *
 * WORD AT A TIME. Multiplying by the reduction polynomial x^4 + x^3 + x + 1
 * is three shifts and three XORs, and a whole 32-bit word goes through them
 * at once:
 *
 *     H * (1 + x + x^3 + x^4)  =  H ^ (H<<1) ^ (H<<3) ^ (H<<4)
 *
 * with whatever is pushed off the top carried into the next word. Two passes
 * settle it, because the fold can push a few bits back over the boundary.
 *
 * The bit-at-a-time version this replaces was written to be obviously
 * correct - and was - but it ran 64 iterations per multiply and turned out
 * to be nearly the entire cost of the field, which is nearly the entire cost
 * of the sketch. This agrees with it on 5,000 random 128-bit inputs and with
 * the BigInt reference on every multiply.
 */
function gf_fold32(H) {
    return [
        (H ^ (H << 1) ^ (H << 3) ^ (H << 4)) >>> 0,
        ((H >>> 31) ^ (H >>> 29) ^ (H >>> 28)) >>> 0
    ];
}

function gf_reduce128(w) {
    var w0 = w[0] >>> 0, w1 = w[1] >>> 0, w2 = w[2] >>> 0, w3 = w[3] >>> 0;
    for (var pass = 0; pass < 3 && (w2 !== 0 || w3 !== 0); pass++) {
        var H2 = w2, H3 = w3;
        w2 = 0; w3 = 0;
        var f = gf_fold32(H2);         /* bits 64-95 fold down to 0-31 */
        w0 = (w0 ^ f[0]) >>> 0;
        w1 = (w1 ^ f[1]) >>> 0;
        f = gf_fold32(H3);             /* bits 96-127 fold down to 32-63 */
        w1 = (w1 ^ f[0]) >>> 0;
        w2 = (w2 ^ f[1]) >>> 0;
    }
    return [w1 >>> 0, w0 >>> 0];
}

var gf_ZERO = [0, 0];
var gf_ONE = [0, 1];

function gf_gadd(a, b) { return [(a[0] ^ b[0]) >>> 0, (a[1] ^ b[1]) >>> 0]; }
function gf_gsqr(a) { return gf_gmul(a, a); }
function gf_gzero(a) { return a[0] === 0 && a[1] === 0; }
function gf_geq(a, b) { return a[0] === b[0] && a[1] === b[1]; }

/**
 * The multiplicative inverse, as a^(2^64 - 2).
 *
 * That exponent is all ones except the lowest bit, so the chain is 63
 * squarings each followed by a multiply - no exponent bits to test.
 */
function gf_ginv(a) {
    if (gf_gzero(a)) { throw new Error('gf64: no inverse of zero'); }
    var result = gf_ONE, base = a;
    for (var i = 1; i < 64; i++) {
        base = gf_gsqr(base);
        result = gf_gmul(result, base);
    }
    return result;
}

/** A number up to 2^53 as a field element, and back. */
function gf_fromNumber(n) {
    return [Math.floor(n / 4294967296) >>> 0, (n % 4294967296) >>> 0];
}
function gf_toNumber(a) { return a[0] * 4294967296 + (a[1] >>> 0); }

/* =========================================================================
 * Reference implementation, in BigInt
 *
 * Never used in anger. It exists so the fast path has something independent
 * to disagree with, and it is written to be read rather than to be quick.
 * ======================================================================= */

var gf_REF_POLY = (1n << 64n) | 0x1bn;

function gf_refMul(a, b) {
    var r = 0n, x = a, y = b;
    while (y > 0n) {
        if (y & 1n) { r ^= x; }
        y >>= 1n;
        x <<= 1n;
    }
    for (var bit = 127; bit >= 64; bit--) {
        if ((r >> BigInt(bit)) & 1n) { r ^= gf_REF_POLY << BigInt(bit - 64); }
    }
    return r;
}

function gf_toBig(a) { return (BigInt(a[0] >>> 0) << 32n) | BigInt(a[1] >>> 0); }
function gf_fromBig(v) {
    return [Number((v >> 32n) & 0xffffffffn) >>> 0, Number(v & 0xffffffffn) >>> 0];
}



/* =======================================================================
 * 8. BCH sketch: fewest bytes, most CPU
 * ======================================================================= */





/* ---------------- polynomials over GF(2^64) ----------------
 *
 * A polynomial is an array of coefficients, lowest degree first. The array is
 * kept trimmed so that its length is always the degree plus one, which makes
 * degree comparisons cheap and gcd terminate on length alone.
 */

function polyTrim(p) {
    var i = p.length - 1;
    while (i > 0 && gf_gzero(p[i])) { i--; }
    return p.slice(0, i + 1);
}

function polyAdd(a, b) {
    var n = Math.max(a.length, b.length), out = new Array(n);
    for (var i = 0; i < n; i++) {
        var x = i < a.length ? a[i] : gf_ZERO;
        var y = i < b.length ? b[i] : gf_ZERO;
        out[i] = gf_gadd(x, y);
    }
    return polyTrim(out);
}

function polyMul(a, b) {
    if ((a.length === 1 && gf_gzero(a[0])) || (b.length === 1 && gf_gzero(b[0]))) {
        return [gf_ZERO];
    }
    var out = new Array(a.length + b.length - 1);
    for (var i = 0; i < out.length; i++) { out[i] = gf_ZERO; }
    for (i = 0; i < a.length; i++) {
        if (gf_gzero(a[i])) { continue; }
        for (var j = 0; j < b.length; j++) {
            if (gf_gzero(b[j])) { continue; }
            out[i + j] = gf_gadd(out[i + j], gf_gmul(a[i], b[j]));
        }
    }
    return polyTrim(out);
}

/** Remainder of a divided by b. b must not be zero. */
function polyMod(a, b) {
    var r = a.slice();
    var db = b.length - 1;
    if (db === 0) { return [gf_ZERO]; }
    var binv = gf_ginv(b[db]);
    for (var i = r.length - 1; i >= db; i--) {
        if (gf_gzero(r[i])) { continue; }
        var factor = gf_gmul(r[i], binv);
        for (var j = 0; j <= db; j++) {
            r[i - db + j] = gf_gadd(r[i - db + j], gf_gmul(factor, b[j]));
        }
    }
    return polyTrim(r.slice(0, db));
}

function polyGcd(a, b) {
    var x = polyTrim(a), y = polyTrim(b);
    while (!(y.length === 1 && gf_gzero(y[0]))) {
        var t = polyMod(x, y);
        x = y; y = t;
    }
    return polyMonic(x);
}

function polyMonic(p) {
    var lead = p[p.length - 1];
    if (gf_gzero(lead) || gf_geq(lead, gf_ONE)) { return p; }
    var inv = gf_ginv(lead), out = new Array(p.length);
    for (var i = 0; i < p.length; i++) { out[i] = gf_gmul(p[i], inv); }
    return out;
}

/** x^2 mod m, repeated - used to build the trace map. */
function polySqrMod(p, m) { return polyMod(polyMul(p, p), m); }

/* ---------------- Berlekamp-Massey ----------------
 *
 * Turns the syndromes into the shortest linear recurrence that generates
 * them. Its coefficients are the error locator polynomial, and the reciprocals
 * of its roots are the items.
 *
 * Working in characteristic two removes the sign bookkeeping that clutters
 * the general form - subtraction is addition, and negation does nothing.
 */
function berlekampMassey(syndromes) {
    var C = [gf_ONE], B = [gf_ONE];
    var L = 0, m = 1, b = gf_ONE;

    for (var n = 0; n < syndromes.length; n++) {
        /* discrepancy: how far C fails to predict syndromes[n] */
        var d = syndromes[n];
        for (var i = 1; i <= L; i++) {
            if (i < C.length) { d = gf_gadd(d, gf_gmul(C[i], syndromes[n - i])); }
        }

        if (gf_gzero(d)) { m++; continue; }

        var coef = gf_gmul(d, gf_ginv(b));
        var shifted = new Array(m + B.length);
        for (i = 0; i < m; i++) { shifted[i] = gf_ZERO; }
        for (i = 0; i < B.length; i++) { shifted[m + i] = gf_gmul(coef, B[i]); }
        var T = C;
        C = polyAdd(C, shifted);

        if (2 * L <= n) { L = n + 1 - L; B = T; b = d; m = 1; }
        else { m++; }
    }
    return { poly: polyTrim(C), degree: L };
}

/* ---------------- root finding ----------------
 *
 * Chien search - trying every field element - is out of the question at 2^64.
 * Instead the polynomial is split with the BERLEKAMP TRACE ALGORITHM:
 *
 *     Tr(z) = z + z^2 + z^4 + ... + z^(2^63)
 *
 * maps every element of GF(2^64) to 0 or 1, so gcd(f, Tr(bz)) splits f into
 * the roots where the trace is zero and the rest. A different b splits it
 * differently, and recursion on both halves reaches linear factors.
 *
 * Some b values split nothing - the whole polynomial lands on one side - so
 * the search walks through them until one separates.
 */
function traceMod(beta, m) {
    /* Tr(beta*z) mod m, by repeated squaring */
    var t = polyMod([gf_ZERO, beta], m);   /* beta*z */
    var acc = t;
    for (var i = 1; i < 64; i++) {
        t = polySqrMod(t, m);
        acc = polyAdd(acc, t);
    }
    return acc;
}

function findRoots(poly, out, depth) {
    var p = polyMonic(polyTrim(poly));
    var deg = p.length - 1;

    if (deg <= 0) { return true; }
    if (deg === 1) {
        /* p = z + c, so the root is c */
        out.push(p[0]);
        return true;
    }
    if (depth > 80) { return false; }

    /*
     * WALK CANDIDATE BETAS UNTIL ONE SPLITS.
     *
     * They have to span the WHOLE field. An earlier version built them from
     * fromNumber(), which only ever reaches 53 bits - and on structured
     * inputs none of the first two hundred separated anything, so decoding
     * failed on exactly the data a real caller has. Random-looking items hid
     * it completely.
     */
    for (var k = 1; k <= 400; k++) {
        var beta = [(k * 2654435761) >>> 0, (k * 40503 + 0x9e3779b9) >>> 0];
        if (gf_gzero(beta)) { continue; }

        var tr = traceMod(beta, p);
        var g = polyGcd(p, tr);
        var dg = g.length - 1;
        if (dg === 0 || dg === deg) { continue; }   /* no split */

        /* the other half is p divided by g */
        var h = polyDiv(p, g);
        return findRoots(g, out, depth + 1) && findRoots(h, out, depth + 1);
    }
    return false;
}

function polyDiv(a, b) {
    var r = a.slice(), db = b.length - 1;
    var q = new Array(Math.max(1, a.length - db));
    for (var i = 0; i < q.length; i++) { q[i] = gf_ZERO; }
    var binv = gf_ginv(b[db]);
    for (i = r.length - 1; i >= db; i--) {
        if (gf_gzero(r[i])) { continue; }
        var factor = gf_gmul(r[i], binv);
        q[i - db] = factor;
        for (var j = 0; j <= db; j++) {
            r[i - db + j] = gf_gadd(r[i - db + j], gf_gmul(factor, b[j]));
        }
    }
    return polyTrim(q);
}

/* ---------------- the sketch ---------------- */

/**
 * @param {number} capacity  the largest difference this can recover
 */
function sketch(capacity) {
    var syn = new Array(capacity);
    for (var i = 0; i < capacity; i++) { syn[i] = gf_ZERO; }

    var api = {};

    /**
     * Fold an item in - or out, which is the same operation.
     *
     * Adding the same item twice removes it, because addition is XOR. That is
     * what makes merge() work and what makes the whole scheme symmetric.
     */
    api.add = function (item) {
        var x = typeof item === 'number' ? gf_fromNumber(item) : item;
        if (gf_gzero(x)) { throw new Error('sketch: zero is not a valid item'); }
        var sq = gf_gsqr(x);
        var p = x;                       /* x^1, then x^3, x^5, ... */
        for (var k = 0; k < capacity; k++) {
            syn[k] = gf_gadd(syn[k], p);
            p = gf_gmul(p, sq);
        }
        return api;
    };

    /** XOR another sketch in. Shared items cancel; the difference remains. */
    api.merge = function (other) {
        var o = other.raw ? other.raw() : other;
        for (var k = 0; k < capacity; k++) { syn[k] = gf_gadd(syn[k], o[k]); }
        return api;
    };

    /**
     * Recover the differing items.
     *
     * @return {Array<number>|null} null when the difference exceeded the
     *         capacity - which is not a partial failure but a total one, and
     *         is the whole reason a caller needs an estimate or a fallback
     */
    api.decode = function () {
        var allZero = true;
        for (var k = 0; k < capacity; k++) { if (!gf_gzero(syn[k])) { allZero = false; break; } }
        if (allZero) { return []; }

        /*
         * Berlekamp-Massey needs the whole syndrome run, and only the odd
         * ones were stored. The even ones follow for free: in characteristic
         * two, s_(2i) is s_i squared, because squaring is additive there.
         * That identity is the reason only half the syndromes ever cross the
         * wire.
         */
        var full = new Array(capacity * 2);
        for (var j = 1; j <= capacity * 2; j++) {
            if (j % 2 === 1) { full[j - 1] = syn[(j - 1) / 2]; }
            else { full[j - 1] = gf_gsqr(full[j / 2 - 1]); }
        }

        var bm = berlekampMassey(full);

        /*
         * A DIFFERENCE OF EXACTLY THE CAPACITY IS VALID, and rejecting it
         * would throw away the answer PinSketch guarantees.
         *
         * An earlier version required the degree to fall strictly below the
         * capacity, on the theory that the leftover syndromes were what made
         * the answer trustworthy. They do help - a sketch given less capacity
         * than the real difference returns a smaller, wrong set that
         * reproduces its syndromes perfectly, and nothing inside the sketch
         * can tell the two apart. But the rule also rejected every legitimate
         * difference that happened to equal the capacity exactly, which is
         * the case the algorithm is guaranteed to handle.
         *
         * The wrong-answer case is caught outside instead, by checking the
         * recovered difference against the fingerprints exchanged in the
         * handshake - evidence the decode never touched. That catches what
         * this rule caught and does not cost the boundary case.
         */
        if (bm.degree > capacity) { return null; }

        /*
         * The locator's roots are the INVERSES of the items, so the
         * polynomial is reversed before factoring and the roots come out as
         * the items directly. Reversing here rather than inverting each root
         * afterwards saves d inversions, each of which is 63 squarings.
         */
        var rev = bm.poly.slice().reverse();
        var roots = [];
        if (!findRoots(rev, roots, 0)) { return null; }
        if (roots.length !== bm.degree) { return null; }

        var out = [];
        for (var r = 0; r < roots.length; r++) {
            if (gf_gzero(roots[r])) { return null; }
            out.push(gf_toNumber(roots[r]));
        }

        out.sort(function (a, b) { return a - b; });
        return out;
    };

    /** The raw syndromes, for serialising. capacity * 8 bytes. */
    api.raw = function () { return syn; };
    api.capacity = capacity;

    return api;
}

sketch.fromRaw = function (capacity, raw) {
    var s = sketch(capacity);
    var syn = s.raw();
    for (var i = 0; i < capacity; i++) { syn[i] = raw[i]; }
    return s;
};


/* =========================================================================
 * Exports
 *
 * The internals are exported alongside the session because a simulation's
 * whole purpose is comparing PREDICTED cost against ACTUAL - and a decider
 * that predicts badly is the failure mode worth finding. Splitting them into
 * a separate file would make that harder for no benefit; the boundary that
 * matters is which functions are pure, and that is documented rather than
 * enforced by the file system.
 * ======================================================================= */

setrecon.core = {
    computeFingerprint: computeFingerprint,
    hashItem64: hashItem64,
    hashItem53: hashItem53,
    /* exported so a caller can keep a fingerprint current without
       recomputing it - add and remove are the same call */
    xor53: xor53,
    /*
     * THE SEED hashItem53 AND computeFingerprint ACTUALLY USE, as the
     * [lo, hi] pair hash32 takes.
     *
     * This used to export a BigInt `1n` left over from the splitmix64 era,
     * while hashItem53 expected the pair. Passing the exported seed into the
     * exported function therefore read seed[0] and seed[1] off a BigInt, got
     * undefined for both, and returned a DIFFERENT hash from the one the
     * fingerprint is built out of - silently. A caller following the
     * documented "keep your fingerprint current" recipe corrupted it.
     *
     * Calling hashItem53(item) with no seed was always correct and still is;
     * this only makes the explicit form agree with it.
     */
    SEED_ROOT: S32_ROOT,

    ibltNew: ibltNew,
    ibltInsert: ibltInsert,
    ibltSubtract: ibltSubtract,
    ibltDecode: ibltDecode,
    ibltParamsForDiff: ibltParamsForDiff,
    ibltToWire: ibltToWire,
    ibltFromWire: ibltFromWire,

    strataEncode: strataEncode,
    strataEstimate: strataEstimate,
    strataLevelsFor: strataLevelsFor,
    strataToWire: strataToWire,
    strataFromWire: strataFromWire,

    buildBuckets: buildBuckets,
    bucketCountFor: bucketCountFor,
    bucketOf: bucketOf,
    fullDumpDiff: fullDumpDiff,

    decidePreStrata: decidePreStrata,
    predictFullDumpBytes: predictFullDumpBytes,
    predictBucketsBytes: predictBucketsBytes,
    predictIbltBytes: predictIbltBytes,
    predictStrataBytes: predictStrataBytes
};

/* The BCH sketch, and the field it needs. Optional - see part 8. */
setrecon.sketch = sketch;
setrecon.gf64 = {
    mul: gf_gmul, add: gf_gadd, sqr: gf_gsqr, inv: gf_ginv,
    isZero: gf_gzero, equal: gf_geq, ZERO: gf_ZERO, ONE: gf_ONE,
    fromNumber: gf_fromNumber, toNumber: gf_toNumber,
    ref: { mul: gf_refMul, toBig: gf_toBig, fromBig: gf_fromBig }
};

if (typeof module !== 'undefined' && module.exports) { module.exports = setrecon; }
else if (typeof self !== 'undefined') { self.setrecon = setrecon; }
