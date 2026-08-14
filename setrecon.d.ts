/**
 * setrecon - set reconciliation between two peers, transport-agnostic.
 *
 * Both sides call setrecon({ items }), wire up 'send' to their transport, and
 * feed incoming messages back with feed(). The library picks one of five
 * algorithms on its own and reports what each side is missing.
 */
declare function setrecon(opts: setrecon.Options): setrecon.Session;

declare namespace setrecon {
    // ============================================================
    // Options
    // ============================================================

    interface Options {
        /**
         * The list of numeric IDs, if it fits in memory. Numbers up to 2^53,
         * unique, non-zero when the sketch is enabled.
         */
        items?: number[];

        /**
         * How many items you hold, when items are loaded lazily via the
         * 'need' event. Ignored if `items` is given.
         */
        count?: number;

        /**
         * Precomputed fingerprint (XOR of h(item, SEED_ROOT), truncated to
         * 53 bits). Optional, but keeping one is what makes an unchanged
         * sync cost 56 bytes and read no items at all.
         *
         * Self-inverse under XOR, so you can maintain it in O(1) per change:
         *   addItem(x)    -> fp ^= h(x, SEED_ROOT)
         *   removeItem(x) -> fp ^= h(x, SEED_ROOT)   // same operation
         */
        fingerprint?: number;

        /**
         * How many items you expect to differ. Give it ONLY from measurement
         * (time since last sync x observed rate). A guess within a factor of
         * two saves 2-6x; one that is ten times off is worse than none.
         */
        expectedDiff?: number;

        /** Measured round-trip time in milliseconds. */
        rtt?: number;

        /** Link bandwidth in Mbps, even roughly. */
        bandwidth?: number;

        /**
         * Stop once this many missing items have been found; the rest are
         * left for another sync. Ends the session with reason 'limit'.
         */
        limit?: number;

        /**
         * How many items to ask for in one message. 0 disables splitting.
         * Default 4000 (about 32 KB of identifiers).
         */
        chunkSize?: number;

        /**
         * Only this side needs the answer. Halves traffic on the symmetric
         * algorithms (FULL_DUMP and BUCKETS); no effect on the others.
         */
        pull?: boolean;

        /**
         * Allow the BCH sketch when expectedDiff is small. Trades bandwidth
         * for CPU - 70x fewer bytes below d=20, but many milliseconds of
         * decode work. Right for a shared link with many peers; wrong for a
         * fast private link.
         */
        sketch?: boolean;

        /**
         * Multiplier for byte predictions when your encoder is fatter than
         * the library's estimates. Default 1.
         */
        overhead?: number;

        /** Overall deadline in milliseconds. Default 30000. */
        timeout?: number;

        /** For simulations with virtual time. Defaults to the global. */
        setTimeout?: typeof setTimeout;
        /** For simulations with virtual time. Defaults to the global. */
        clearTimeout?: typeof clearTimeout;
    }

    // ============================================================
    // Events
    // ============================================================

    /**
     * Why the session ended.
     *
     * - 'complete'        both sides agreed and exchanged everything
     * - 'limit'           `limit` option was reached; partial answer
     * - 'timeout'         overall deadline expired
     * - 'app_timeout'     application never answered a 'need' event
     * - 'aborted'         local abort() was called (or a custom reason)
     * - 'rejected'        peer refused (e.g. protocol mismatch)
     * - 'bad_message'     peer sent something malformed
     * - 'no_item_source'  no items and no 'need' listener
     * - 'too_many_early'  too many messages arrived before the handshake
     */
    type FinishReason =
        | 'complete'
        | 'limit'
        | 'timeout'
        | 'app_timeout'
        | 'aborted'
        | 'rejected'
        | 'bad_message'
        | 'no_item_source'
        | 'too_many_early'
        | (string & {});

    /**
     * The algorithm that ran. Arrow variants indicate a fallback: e.g.
     * 'STRATA_IBLT→DUMP' means the IBLT failed to decode and the session
     * fell back to a full dump.
     */
    type Algorithm =
        | 'FULL_DUMP'
        | 'BUCKETS'
        | 'STRATA_IBLT'
        | 'SKETCH'
        | 'STRATA_IBLT→DUMP'
        | 'SKETCH→DUMP'
        | null;

    interface FinishStats {
        /** Which algorithm ran. null if the session never got past hello. */
        algorithm: Algorithm;
        /** Number of messages sent by this side. */
        messages: number;
        /**
         * Predicted total bytes sent by this side. An estimate - the library
         * does not serialise, so it cannot know your encoder's real cost. See
         * `overhead` for calibration.
         */
        predictedBytes: number;
        /** 'initiator' if this side called start(); 'responder' otherwise. */
        role: 'initiator' | 'responder' | null;
    }

    /**
     * Hint the library passes when it asks for items via 'need'.
     * `{ all: true }` for a full-list request; `{ buckets: [...] }` for a
     * bucket comparison, which only needs items whose hash falls in the
     * listed buckets.
     */
    type NeedHint = { all: true } | { buckets: number[] };

    /**
     * A wire message payload. The library keeps its own message types
     * internal - callers only pass this through their transport. Concrete
     * shape varies by `type` but is always a plain object of primitives and
     * number arrays (JSON-safe, structured-clone-safe).
     */
    type MessageData = Record<string, unknown>;

    // ============================================================
    // Session
    // ============================================================

    interface Session {
        /**
         * Subscribe to an event. Returns the session for chaining.
         *
         * - 'send'    outgoing message; encode `data` and hand `type` and
         *             the encoded payload to your transport.
         * - 'finish'  the session ended. Fires exactly once.
         * - 'need'    the library needs items you did not pass upfront.
         *             Call provide() with the requested items.
         */
        on(event: 'send', cb: (type: string, data: MessageData) => void): Session;
        on(
            event: 'finish',
            cb: (
                reason: FinishReason,
                myMissing: number[],
                peerMissing: number[],
                stats: FinishStats
            ) => void
        ): Session;
        on(event: 'need', cb: (hint: NeedHint) => void): Session;

        /**
         * Open the exchange. The other side does NOT call start() - the
         * first incoming message opens its session. Safe to call more than
         * once; the second and later calls are no-ops.
         */
        start(): Session;

        /**
         * Feed in an incoming message from the peer. The same `type` and
         * `data` that 'send' emitted on the other end, after a round trip
         * through your encoder.
         */
        feed(type: string, data: MessageData): Session;

        /**
         * Answer a 'need' event with the requested items. Must be called
         * within APP_RESPONSE_TIMEOUT (10 s by default); slow database
         * loads are covered by the library sending 'hold' messages
         * automatically.
         */
        provide(items: number[]): Session;

        /**
         * End the session early. Emits an 'abort' to the peer and fires
         * 'finish' locally with the given reason (default 'aborted').
         */
        abort(reason?: string): Session;
    }

    // ============================================================
    // Internals - for testing, simulation, and advanced use
    // ============================================================

    /**
     * Pure functions: the algorithms and the byte predictors. Exported
     * because a simulation's whole purpose is comparing PREDICTED cost
     * against ACTUAL, and a decider that predicts badly is the failure
     * mode worth finding.
     */
    interface Core {
        /** XOR of h(item, SEED_ROOT) over all items, truncated to 53 bits. */
        computeFingerprint(items: number[]): number;
        /** splitmix64(item ^ seed) as a BigInt. */
        hashItem64(item: number, seed: bigint): bigint;
        /** hashItem64 truncated to 53 bits. */
        hashItem53(item: number, seed: bigint): number;
        /** 53-bit XOR of two Numbers, avoiding int32 coercion. */
        xor53(a: number, b: number): number;
        /** The fingerprint seed. Same on both sides. */
        SEED_ROOT: bigint;

        ibltNew(cells: number, k: number): unknown;
        ibltInsert(iblt: unknown, item: number): void;
        ibltSubtract(a: unknown, b: unknown): unknown;
        ibltDecode(iblt: unknown): { pos: number[]; neg: number[] } | null;
        ibltParamsForDiff(diff: number): { cells: number; k: number };
        ibltToWire(iblt: unknown): MessageData;
        ibltFromWire(wire: MessageData): unknown;

        strataEncode(items: number[], levels: number): unknown;
        strataEstimate(a: unknown, b: unknown): number;
        strataLevelsFor(nA: number, nB: number): number;
        strataToWire(strata: unknown): MessageData;
        strataFromWire(wire: MessageData): unknown;

        buildBuckets(items: number[], B: number): { hashes: number[]; bins: number[][] };
        bucketCountFor(n: number): number;
        bucketOf(item: number, B: number): number;
        fullDumpDiff(a: number[], b: number[]): { onlyA: number[]; onlyB: number[] };

        /** What algorithm the decider would pick, before Strata runs. */
        decidePreStrata(
            nA: number,
            nB: number,
            expectedDiff: number | undefined,
            allowSketch: boolean,
            link: { rtt?: number; bandwidth?: number } | null
        ): Algorithm;

        predictFullDumpBytes(nA: number, nB: number): number;
        predictBucketsBytes(nA: number, nB: number, diff: number): number;
        predictIbltBytes(diff: number): number;
        predictStrataBytes(nA: number, nB: number): number;
    }

    /**
     * BCH sketch on its own. Optional path - only reached when
     * `sketch: true` is set on a session and the difference is small.
     *
     * Trades bandwidth for CPU: capacity 20 needs ~160 bytes on the wire
     * and hundreds of milliseconds to decode in JavaScript.
     */
    interface Sketch {
        /** Fold an item in (or out - same operation, since add is XOR). */
        add(item: number | bigint): Sketch;
        /** XOR another sketch in. Shared items cancel; the difference remains. */
        merge(other: Sketch | bigint[]): Sketch;
        /**
         * Recover the differing items. Returns null when the difference
         * exceeded the capacity - a total failure, not a partial one. This
         * is why a caller needs an estimate or a fallback path.
         */
        decode(): number[] | null;
        /** The raw syndromes, for serialising. capacity * 8 bytes. */
        raw(): bigint[];
        capacity: number;
    }

    interface SketchFactory {
        (capacity: number): Sketch;
        fromRaw(capacity: number, raw: bigint[]): Sketch;
    }

    /** GF(2^64) arithmetic, with a BigInt reference implementation. */
    interface GF64 {
        mul(a: bigint, b: bigint): bigint;
        add(a: bigint, b: bigint): bigint;
        sqr(a: bigint): bigint;
        inv(a: bigint): bigint;
        isZero(a: bigint): boolean;
        equal(a: bigint, b: bigint): boolean;
        ZERO: bigint;
        ONE: bigint;
        fromNumber(n: number): bigint;
        toNumber(a: bigint): number;
        ref: {
            mul(a: bigint, b: bigint): bigint;
            toBig(a: bigint): bigint;
            fromBig(a: bigint): bigint;
        };
    }

    const core: Core;
    const sketch: SketchFactory;
    const gf64: GF64;
}

export = setrecon;
