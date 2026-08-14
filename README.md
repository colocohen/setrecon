<h1 align="center">setrecon</h1>

<p align="center">
  <em>Find out what two peers are missing — without either sending its list</em>
</p>

<p align="center" style="max-width: 720px; margin: auto;">
  <strong>setrecon</strong> reconciles two sets of numeric IDs over any transport,
  moving as few bytes as it can. A million items differing by twenty cost 41 KB
  instead of 8 MB. Two sides that already agree cost 56 bytes and never read
  their lists at all. It picks between five algorithms on its own, so you never
  have to know in advance whether the difference is small or large — which,
  most of the time, you don't.
</p>

## 📑 Table of Contents

- [✨ Why this library?](#-why-this-library)
- [📦 Install](#-install)
- [🚀 Quick start](#-quick-start)
- [🚫 When not to use it](#-when-not-to-use-it)
- [📖 The basics](#-the-basics)
- [🔌 What the transport has to provide](#-what-the-transport-has-to-provide)
- [⚙️ Options](#-options)
  - [`fingerprint`](#fingerprint--the-one-worth-keeping)
  - [`expectedDiff`](#expecteddiff--how-many-you-expect-to-differ)
  - [`rtt` and `bandwidth`](#rtt-and-bandwidth--what-the-link-costs)
  - [`count` and `fingerprint`](#count-and-fingerprint--when-the-list-is-expensive-to-read)
  - [`chunkSize`](#chunksize--how-much-arrives-at-a-time)
  - [`limit`](#limit--i-only-need-some-of-them)
  - [`pull`](#pull--only-i-need-the-answer)
  - [`sketch`](#sketch--fewest-bytes-most-cpu)
- [📡 Events](#-events)
- [🛠 Methods](#-methods)
- [🧮 The algorithms](#-the-algorithms)
- [🔄 A sync is a snapshot](#-a-sync-is-a-snapshot)
  - [Don't abort on change](#dont-abort-on-change)
  - [The one case where a change is missed](#the-one-case-where-a-change-is-missed)
- [🧩 Recipes (what you can build)](#-recipes-what-you-can-build)
  - [Client that stays in sync with a server](#client-that-stays-in-sync-with-a-server)
  - [Detecting deletions](#detecting-deletions)
  - [Sync that costs nothing when nothing changed](#sync-that-costs-nothing-when-nothing-changed)
  - [P2P mesh with many peers](#p2p-mesh-with-many-peers)
  - [High-frequency reconciliation](#high-frequency-reconciliation)
  - [A list that lives in a database](#a-list-that-lives-in-a-database)
  - [Worker and tab, no network at all](#worker-and-tab-no-network-at-all)
  - [Testing your own protocol against it](#testing-your-own-protocol-against-it)
- [⚠️ Constraints](#-constraints)
- [🔬 Internals](#-internals)
- [📊 Why there's no sketch extension](#-why-theres-no-sketch-extension)
- [🧾 What it doesn't do](#-what-it-doesnt-do)
- [📚 References](#-references)
- [🙏 Support the Project](#-support-the-project)
- [📜 License](#-license)


## ✨ Why this library?

Two peers each hold a list of IDs and want to know what the other has. Almost
every application hits this, and almost every application solves it the same
way: **send the whole list.**

That works. It keeps working through development, through staging, through the
first year of production — and then the list reaches a few hundred thousand
rows on a mobile connection and the server starts falling over.

The fixes that follow are worse than the problem:

- 📄 **Pagination** — now the client manages cursors, and deletions stop being
  visible: a page you have already passed will not tell you something vanished
  from it.
- ⏱️ **`?since=` endpoints** — now you need synchronised clocks, and deletions
  need tombstones.
- 🗃️ **Cache invalidation** — now you have a second source of truth.

Each layer fixes a symptom and adds state, and eventually there is a sync
layer nobody designed. It accumulated.

**setrecon removes the decision.** You hand it the list; it works out what to
exchange:

```js
setrecon({ items: myIds })
```

Fifty items or five hundred thousand, it picks the right approach — a full
dump when that is genuinely cheapest, a bucket comparison in the middle, an
IBLT sized by a measured estimate when the lists are large and close, a BCH
sketch when you can tell it the difference is tiny. There is never a moment
where you have to stop and think about it.

**And it never touches a socket.** WebRTC, WebSockets, a relay, `postMessage`,
or two objects in the same process — all the same two lines.

## 📦 Install

```bash
npm install setrecon
```

```js
// CommonJS
const setrecon = require('setrecon');

// ES modules
import setrecon from 'setrecon';
// or, for the internals
import setrecon, { core, sketch, gf64 } from 'setrecon';

// browser
<script src="setrecon.js"></script>   // window.setrecon (also self.setrecon in workers)
```

One file, no dependencies, no build step.

## 🚀 Quick start

Two peers over a WebSocket. This is the whole thing — nothing is elided.

```js
const setrecon = require('setrecon');

function syncOverSocket(ws, myIds, onResult) {
    const s = setrecon({ items: myIds });

    // outgoing: encode however the rest of your system does
    s.on('send', (type, data) => {
        ws.send(JSON.stringify([type, data]));
    });

    // incoming: hand the decoded object straight back
    ws.on('message', raw => {
        const [type, data] = JSON.parse(raw);
        s.feed(type, data);
    });

    // the answer
    s.on('finish', (reason, myMissing, peerMissing, stats) => {
        if (reason !== 'complete') {
            console.error('sync ended early:', reason);
            return;
        }
        console.log(stats.algorithm, stats.predictedBytes, 'bytes');
        onResult(myMissing, peerMissing);
    });

    return s;
}
```

The side that opens the exchange calls `start()`:

```js
const s = syncOverSocket(ws, myIds, (mine, theirs) => {
    mine.forEach(id => request(id));      // they have it, I don't
    theirs.forEach(id => offer(id));      // I have it, they don't
});

s.start();
```

The side that receives does not — the first message opens the session:

```js
syncOverSocket(ws, myIds, (mine, theirs) => {
    mine.forEach(id => request(id));
    theirs.forEach(id => offer(id));
});
// no start()
```

That's it. Which algorithm ran, how big the difference was, and whether it was
worth measuring first are all decided inside.

## 🚫 When not to use it

**If one side already knows what the other is missing, send it.** A server
holding an ordered log and a version number per client needs none of this —
*"I'm at 4231"* / *"here are the next twelve"* is cheaper and simpler, and it's
what WhatsApp's app-state sync does.

This is for the case where **nobody holds the authoritative order**: two peers
meeting after both changed, a mempool, a device coming back online into a
network with no central copy.

---

## 📖 The basics

### One session, one peer

A session talks to exactly one counterparty. There's no peer identifier
anywhere in the API — which peer a session belongs to is your bookkeeping.

```js
const sessions = {};

net.on('message', (peerId, type, bytes) => {
    if (!sessions[peerId]) {
        const s = setrecon({ items: myIds });
        s.on('send', (t, d) => net.send(peerId, t, encode(d)));
        s.on('finish', mine => { mine.forEach(fetchItem); delete sessions[peerId]; });
        ```

The receiving side never calls `start()` — the first message opens the session.

### It does not serialise

`send` hands you a type string and a plain object of number arrays. You encode
it however the rest of your system does, and hand the decoded object back to
`feed()`. Putting a second wire format inside a system that already has one
would be the wrong trade.

```js
s.on('send', (type, data) => net.send(peer, type, litepack.encode(data)));
net.on('message', (peer, type, bytes) => s.feed(type, litepack.decode(bytes)));
```

### Both sides may start at once

Two peers deciding to sync at the same moment is normal, and so is deciding to
start just as the other side's first message lands. `start()` returns quietly
if the session is already under way.

---

## 🔌 What the transport has to provide

| Requirement | Needed? | Why |
|---|---|---|
| Reliable delivery | **yes** | there is no retransmission here; a lost message stalls the session until its timeout |
| No duplicates | no | a repeated message is harmless — 100% duplication was tested |
| Whole messages | **yes** | a truncated message is rejected, not reassembled |
| Ordered delivery | no | anything arriving before its prerequisite is held and replayed |

Ordering is worth spelling out: on a link that doesn't preserve it, a small
message really does overtake a large one from the same sender. This handles
that. It does not handle loss.

---

## ⚙️ Options

```js
setrecon({
    items:       [1001, 1002, 8871],   // the list, if it's in memory
    count:       1000000,              // ...or just how many, and load on demand
    fingerprint: 8123456789012345,     // if you keep one — see below
    expectedDiff:       40,                   // how many you expect to differ
    rtt:         100,                  // measured round-trip, ms
    bandwidth:   8,                    // Mbps, even roughly
    limit:       50,                   // stop once this many are found
    chunkSize:   500,                  // items per request; 0 disables splitting
    pull:        true,                 // only I need the answer
    sketch:      true,                 // allow the BCH sketch
    overhead:    1.15,                 // if your encoder is fatter than assumed
    timeout:     30000                 // overall deadline, ms
});
```

Everything but `items` or `count` is optional. Each one you supply is a fact
the decider can use — **you never choose an algorithm yourself.**

### `fingerprint` — the one worth keeping

It's an XOR of per-item hashes, so it updates in place: adding an item and
removing it are the same line.

```js
const { xor53, hashItem53, SEED_ROOT } = setrecon.core;

fp = xor53(fp, hashItem53(item, SEED_ROOT));   // added an item
fp = xor53(fp, hashItem53(item, SEED_ROOT));   // removed it — same line
```

Supplying it is what makes an unchanged sync cost 56 bytes **and no read at
all**. Without it, two identical sides still have to load their lists to
discover they're identical.

| | reads | time |
|---|---|---|
| identical, fingerprint supplied | 0 | 1 ms |
| identical, `count` only | 2 | full load |

### `expectedDiff` — how many you expect to differ

It's an estimate, and the name says so. `diff` is what comes *out* of a sync;
a parameter that reads like a fact invites you to supply one you don't have.

```js
expectedDiff = (Date.now() - lastSyncedWith(peer)) / 1000 * itemsPerSecond;
```

Every value produces a correct answer — a bad guess costs bytes, not
correctness. But it's only worth giving when it comes from **measurement**:

```js
✅  "synced 2s ago, 20 items/second"     → 40
✅  "counted 12 additions since"         → 12
❌  "a thousand, maybe?"                 → leave it out
```

**On its own it changes almost nothing.** Across seven combinations of list
size and difference the decider picks the same algorithm and spends the same
bytes with the estimate and without it — the rule it falls back on happens to
agree with the comparison the estimate enables.

**What it actually unlocks is the sketch**, which cannot be sized without a
number and is therefore never considered without one:

| | algorithm | bytes |
|---|---|---|
| `sketch: true` alone | `STRATA_IBLT` | 31.6 KB |
| `sketch: true` + `expectedDiff: 20` | `SKETCH` | 702 B |

A guess within a factor of two is fine. Ten times off is worse than nothing,
because it makes the decider confident and wrong — but every value still
produces a correct answer, so a bad estimate costs bytes, not correctness.

### `rtt` and `bandwidth` — what the link costs

Without them the decider ranks algorithms on bytes, which quietly assumes
bytes are the scarce thing. On a fast link with real latency they aren't:

```
200 items, 200 ms latency:
    without a link model   BUCKETS     800 ms
    with one               FULL_DUMP   400 ms   ← same answer, one crossing
```

Across 81 combinations of size, difference, latency and bandwidth, supplying
them was faster in 45 and slower in 1 — and that one traded 15 ms for ten
times the bytes.

**So it optimises time, and that isn't always what you want.** On a metered
link, or one shared with eight other peers, leave them out and let it minimise
bytes.

### `count` and `fingerprint` — when the list is expensive to read

Give a count instead of items and the list is fetched on demand, once, and
only after the algorithm is chosen. See
[a list that lives in a database](#a-list-that-lives-in-a-database).

### `chunkSize` — how much arrives at a time

The bucket exchange asks for items rather than being sent them, a chunk at a
time. That bounds the largest message and gives the asking side a natural place
to stop.

```js
setrecon({ items: myIds, chunkSize: 500 })
```

Measured on 3,000 shared items with 300 differing each way:

| `chunkSize` | requests | largest message | total |
|---|---|---|---|
| 4000 (default) | 2 | 26.3 KB | 54.5 KB |
| 500 | 14 | 4.2 KB | 54.8 KB |
| 100 | 58 | 1.1 KB | 56.1 KB |
| 0 (no splitting) | 2 | 26.3 KB | 54.5 KB |

And on a full dump of 20,000 items, where the difference is the whole list:

| `chunkSize` | messages | largest message | total |
|---|---|---|---|
| 0 | 6 | 156.3 KB | 156.4 KB |
| 4000 | 16 | 31.3 KB | 156.6 KB |
| 100 | 406 | 822 B | 166.1 KB |

💡 **Tips**

- The default is four thousand items, roughly 32 KB of identifiers — about the
  size of the Strata estimate, which is the largest message the library sends
  and cannot split. Going below that only buys round trips.
- It's counted in **items, not bytes**, because nothing here serialises and a
  number's encoded size is your encoder's business. A fatter encoding means a
  proportionally bigger message.
- Each side sends what the other asked for, so the two settings are
  independent — one peer can want small messages without imposing that on the
  other.
- Bucket comparison and full dumps both chunk. Strata, IBLT and the sketch are
  already bounded by their own parameters and go as single messages.
- A dump only splits when it is larger than a chunk — below that it goes as it
  always did and no round trip is spent asking.

### `limit` — I only need some of them

A feed needs fifty items it doesn't have, not the ten thousand it is behind by.

```js
setrecon({ items: myIds, limit: 50, pull: true })
```

Measured on 3,000 shared items with 2,000 missing:

| `limit` | got | bytes |
|---|---|---|
| none | 2,000 | 66.1 KB |
| 50 | 67 | 2.5 KB |
| 200 | 213 | 5.5 KB |
| 5,000 | 100 (all there was) | — |

💡 **Tips**

- **The count is approximate.** Asking for fifty returns sixty-seven. Items are
  grouped into buckets by hash and a bucket is taken whole, so the total lands
  past the target rather than on it.
- **Which ones you get is arbitrary** — a random handful, not the newest or the
  largest. If you need a specific fifty, you want an ordered query.
- **It doesn't converge.** Asking again gives another arbitrary handful. This
  is "enough for now", not paging.
- `reason` comes back as `'limit'` rather than `'complete'`, so you can tell a
  partial answer from a whole one.
- Pair it with `pull`. Without that the other side still asks you for
  everything it is missing, and the saving mostly disappears.
- Only bucket comparison can stop early. A sketch or an IBLT decodes
  completely or not at all.

### `pull` — only I need the answer

`FULL_DUMP` and `BUCKETS` are symmetric because both sides normally learn what
they're missing, and each needs the other's contents to work it out. When only
one side cares — a client catching up from a server, a read-only replica —
half that traffic is wasted.

```js
setrecon({ items: myIds, pull: true })   // send me yours; I'm keeping mine
```

| | both directions | `pull` |
|---|---|---|
| `FULL_DUMP` | 1,572 B | 964 B |
| `BUCKETS` | 18.5 KB | 9.8 KB |
| `BUCKETS`, large diff | 54.8 KB | 27.9 KB |
| `STRATA_IBLT` | 32.4 KB | 32.4 KB |
| `SKETCH` | 462 B | 462 B |

Strata and the sketch are already one-directional, so nothing changes there.

The side being pulled from finishes with two empty lists — it genuinely
learned nothing, because it never saw the puller's contents. That's the trade.

**If both sides set it, one gives.** Two pullers would deadlock — neither
sends anything and both sit out the timeout, which looks like a network fault
rather than a configuration mistake. The side with the higher fingerprint
drops its claim and answers normally. Both compute the same comparison, so
they agree without another message.

### `sketch` — fewest bytes, most CPU

Off by default. When on, and when `expectedDiff` says the difference is small, a BCH
sketch carries the whole difference in a few hundred bytes.

```
20,000 items, 20 differ:
    without    31.6 KB    188 ms
    with          638 B  1,404 ms
```

It's capped at a difference of 20, because decoding grows badly: about a
second at 20, and 168 seconds at 500. Above the cap the decider ignores it.

---

## 📡 Events

```js
s.on('send', (type, data) => {})
```
Encode `data` and send it, tagged with `type`. `data` is always a plain object
of numbers and number arrays.

```js
s.on('finish', (reason, myMissing, peerMissing, stats) => {})
```
Fires exactly once, however the session ends.

`reason` — why it ended, see the table below.
`myMissing` — IDs the peer has that you don't.
`peerMissing` — the reverse.
`stats` — `{ algorithm, messages, predictedBytes, role }`.

Results are always present. On a clean finish they're the answer; on a
timeout they're empty; and there is no third case where you have to guess
which.

```js
s.on('need', hint => {})
```
Only if you gave `count` without `items`. Answer with `s.provide(items)`.

```js
s.on('hello', (peer, accept) => {})
```
The counterparty's `{ count, fingerprint }`, before any real work. Call
`accept(false)` to refuse.

| reason | meaning |
|---|---|
| `complete` | the whole difference was found |
| `timeout` | the deadline passed |
| `aborted` | one side called `abort()` — or whatever string it passed |
| `rejected` | refused in the `hello` handler |
| `bad_message` | a malformed payload |
| `out_of_order` | a message arrived that could not be placed |
| `too_many_early` | more than 16 messages arrived before the handshake |
| `no_item_source` | `count` was given but nothing listens to `need` |
| `app_timeout` | `provide()` was never called |

---

## 🛠 Methods

```js
s.start()             // open the exchange. Safe to call when the other side did
s.feed(type, data) // feed in a decoded message
s.provide(items)      // answer a 'need'
s.abort(reason)       // stop, and tell the other side
s.fingerprint()       // the fingerprint, without starting anything
```

---

## 🧮 The algorithms

You don't pick one. The decider does, from the facts you supplied.

| | crossings | bytes | chosen when |
|---|---|---|---|
| identical | 2 | 56 B | fingerprints match |
| `FULL_DUMP` | 2 | all items | small lists, very lopsided sizes, or a fast link |
| `BUCKETS` | 4 | moderate | medium lists, medium differences |
| `STRATA_IBLT` | 4 | low | large lists, small differences |
| `SKETCH` | 2 | lowest | `sketch: true` and `expectedDiff` ≤ 20 |

Both sides run the same decision function over the same inputs, so no message
announces the choice — the type of the next message reveals it. The inputs
that only one side holds (`expectedDiff`, `rtt`, `bandwidth`) travel in the handshake
so both sides compute the same answer.

---

## 🔄 A sync is a snapshot

A session is single-use. It reads your list once, answers for what it read, and
ends — `done` or `fail` fires and that session is finished. The next sync starts
from wherever things are then.

**Items that change mid-sync are fine.** Every path works from the list it
actually read, not from an assumption about it, so the answer stays correct.
Whatever arrived too late is simply picked up next time.

```js
// this is safe — the two new items are found
s.on('send', (type, data) => {
    myIds.push(newId);              // arrived while syncing
    net.send(peer, type, encode(data));
});
```

### Don't abort on change

The tempting reaction is to cancel the sync in flight and start a clean one.
On a list that changes continuously — a mempool, a queue, a chat log — that
never finishes:

| new items per message | abort and restart | let it run |
|---|---|---|
| 0 | ✅ succeeds first try | 20 found |
| 1 | ❌ still failing after 20 attempts | 22 found |
| 5 | ❌ still failing after 20 attempts | 30 found |

Every attempt gets cancelled before it reaches the end. That is a livelock, not
caution.

**Chain the next sync off the end of the last one.** Not a timer, not a change
handler — both can open a second session while the first is still running, and
one session per peer at a time is the rule:

```js
function syncLoop(peer) {
    const s = setrecon({ items: myIds });

    s.on('send', (t, d) => net.send(peer, t, encode(d)));

    s.on('finish', (reason, mine, theirs) => {
        mine.forEach(fetch);
        theirs.forEach(offer);
        // back off after a failure, otherwise go again on the normal cadence
        setTimeout(() => syncLoop(peer), reason === 'complete' ? 2000 : 5000);
    });

    s.start();
}
```

The delay sits **between** syncs rather than between starts, so a slow sync
simply pushes the next one later instead of stacking on top of it. And because
`finish` fires however the session ended, there is no path that skips the
rescheduling.

If you want changes to trigger a sync sooner, let them shorten the gap rather
than start a session:

```js
let dirty = false, waiting = null;

function onChange() { dirty = true; }

function schedule(peer) {
    waiting = setTimeout(() => {
        waiting = null;
        dirty = false;
        syncLoop(peer);          // still only one at a time
    }, dirty ? 200 : 2000);
}
```

`abort()` is for when the sync itself has stopped being relevant — the peer
disconnected, the user navigated away, your own deadline passed. Not for data
moving underneath it.

### The one case where a change is missed

A `fingerprint` you supply is a **commitment**. If it is stale and the two sides
look identical, the handshake ends in 56 bytes before either list is read — and
the change waits for the next sync rather than being found in this one.

Keep it current and the problem disappears. It is one XOR per change, in either
direction:

```js
const { xor53, hashItem53 } = setrecon.core;

function onInsert(id) { fp = xor53(fp, hashItem53(id)); }
function onDelete(id) { fp = xor53(fp, hashItem53(id)); }
```

💡 **Tips**

- Passing `items` instead of a stored `fingerprint` sidesteps this entirely —
  the fingerprint is computed from what you hand over.
- The BCH sketch checks its answer against the fingerprints and falls back to a
  full dump when they disagree, so a stale one costs bytes rather than
  correctness.
- One session per peer at a time. Starting a second while the first is running
  gives you two answers for one question — which is why the loop above chains
  off `done` instead of running on a timer.

## 🧩 Recipes (what you can build)

Every byte count below was measured, not estimated.

### Client that stays in sync with a server

The client holds a copy of a server-side list and wants to catch up. Only the
client needs an answer, so it says so — the server answers and learns nothing,
which is both correct and cheaper.

```js
// ─────────────────────────── client ───────────────────────────
const setrecon = require('setrecon');

const ws = new WebSocket('wss://example.com/sync');

const s = setrecon({
    items: localIds,          // the IDs already stored locally
    pull:  true               // "send me yours; I'm keeping mine"
});

s.on('send', (type, data) => {
    ws.send(JSON.stringify([type, data]));
});

ws.onmessage = e => {
    const [type, data] = JSON.parse(e.data);
    s.feed(type, data);
};

s.on('finish', async (reason, myMissing, peerMissing, stats) => {
    console.log('synced via', stats.algorithm, 'in', stats.messages, 'messages');

    if (myMissing.length) {
        const fresh = await fetch('/items?ids=' + myMissing.join(','))
                            .then(r => r.json());
        fresh.forEach(store);
    }

    peerMissing.forEach(removeLocally);   // gone from the server — see below
});


ws.onopen = () => s.start();
```

```js
// ─────────────────────────── server ───────────────────────────
wss.on('connection', ws => {
    const s = setrecon({ items: allIds });   // no pull: nothing to learn

    s.on('send', (type, data) => ws.send(JSON.stringify([type, data])));
    s.on('finish', () => ws.close());
        // no start() — the client opens it
});
```

💡 **Tips**

- `pull` belongs on the side that **wants** the answer, not the side being
  read from. Putting it on the server would leave the client learning nothing.
- It saves about 47% on lists up to a few tens of thousands. Above that the
  chosen algorithm is already one-directional and `pull` changes nothing —
  which is fine, it just does not help.
- The puller still learns **both** directions. It receives the other side's
  contents and compares locally; what it skips is *sending* its own.
- One session per connection. A session is single-use: once `done` or `fail`
  fires, build a new one for the next sync.

### Detecting deletions

A symmetric difference does not say *why* an item is on one side only. `theirs`
means "I have it, they don't" — which is a deletion if they are the source of
truth, and a local creation if they are not.

```js
// server deleted 500, 501, 502 and added 9001, 9002
s.on('finish', (reason, mine, theirs) => {
    mine.forEach(fetchFromServer);   // [9001, 9002]
    theirs.forEach(deleteLocally);   // [500, 501, 502]
});
```

This is only sound when **one side is authoritative** — a client that never
creates IDs of its own. Where both sides create, you need something the library
cannot give you: tombstones, a version watermark, or an age rule.

💡 **Tips**

- Do not combine this with `pull` on the *server*. The server would learn the
  deletions and the client would learn nothing.
- Deletions cost nothing extra — they are already in the difference.

### Sync that costs nothing when nothing changed

The common case in any system that syncs regularly is that nothing changed.
Give it a stored fingerprint and that case costs **56 bytes and zero reads**.

```js
const s = setrecon({
    count:       myCount,          // a COUNT query, not the rows
    fingerprint: storedFingerprint
});

s.on('need', hint => db.loadItems(hint, items => s.provide(items)));
```

Keep the fingerprint current instead of recomputing it — adding an item and
removing it are the same line:

```js
const { xor53, hashItem53 } = setrecon.core;

function onInsert(id) { fp = xor53(fp, hashItem53(id)); saveFingerprint(fp); }
function onDelete(id) { fp = xor53(fp, hashItem53(id)); saveFingerprint(fp); }
```

💡 **Tips**

- Without a stored fingerprint, two identical sides still have to read their
  lists to discover they are identical. That is the whole cost this avoids.
- The fingerprint is 53 bits and order-independent, so it survives any
  insertion order.

### P2P mesh with many peers

One session per peer, and no peer identifier anywhere in the library — which
peer a session belongs to is your bookkeeping.

```js
const sessions = {};

function syncWith(peerId) {
    if (sessions[peerId]) { return; }

    const s = setrecon({ items: myIds });
    s.on('send', (t, d) => mesh.send(peerId, t, encode(d)));
    s.on('finish', (reason, mine, theirs, stats) => {
        mine.forEach(id => request(peerId, id));
        console.log(peerId, stats.algorithm, stats.predictedBytes);
        delete sessions[peerId];
    });

    sessions[peerId] = s;
    s.start();
}

mesh.on('message', (peerId, type, bytes) => {
    if (!sessions[peerId]) { syncWith(peerId); }
    sessions[peerId].feed(type, decode(bytes));
});
```

💡 **Tips**

- Both peers deciding to sync at the same moment is normal and handled — the
  one with the lower fingerprint drives, and both compute that identically.
- Calling `start()` after the other side's first message already arrived is
  also fine; it returns quietly.
- Eight peers reconciling 50,000 items every two seconds costs about 44% of one
  core. At 100,000 it is 79%. Above that, sync less often or shard the list.

### High-frequency reconciliation

A mempool, a shared queue, anything reconciled every couple of seconds. Here
you usually **can** estimate the difference — elapsed time times the observed
rate — and that unlocks the cheapest algorithm there is.

```js
const elapsed = (Date.now() - lastSyncedWith(peer)) / 1000;

const s = setrecon({
    items:        mempoolIds,
    expectedDiff: Math.round(elapsed * itemsPerSecond),
    sketch:       true
});
```

Measured on 5,000 items with 20 differing:

| | bytes | messages | CPU |
|---|---|---|---|
| default | 21.2 KB | 6 | 9 ms |
| `expectedDiff` | 21.2 KB | 6 | 6 ms |
| `+ sketch` | **678 B** | 4 | 337 ms |

💡 **Tips**

- The sketch trades bandwidth for CPU, and the trade only pays below a
  difference of about 20 — above that the decider ignores it. On a shared link
  with many peers it is the right trade; on a fast private link it is not.
- Give `expectedDiff` only when it comes from **measurement**. A guess within a
  factor of two saves 2–15×; one ten times off is worse than none, because it
  makes the decider confident and wrong.
- Every value still produces a correct answer. A bad estimate costs bytes, not
  correctness.

### A list that lives in a database

Do not page a million rows in before you know whether you need to.

```js
const s = setrecon({
    count:       await db.count(),
    fingerprint: await db.fingerprint()      // or omit; see below
});

s.on('need', (hint) => {
    // hint is { all: true } or { buckets: [3, 17, 40] }
    db.loadItems(hint, items => s.provide(items));
});
```

💡 **Tips**

- It asks **once** per session, and only after it knows which algorithm it is
  running — so `{ buckets: [...] }` really does mean it needs nothing else.
- Taking a few seconds to answer is fine. It sends `hold` messages so the far
  side does not time you out, and you do not manage that.
- `fingerprint` is optional here too. Without one the list is fetched once
  before the handshake, which still beats not being able to start.

### Worker and tab, no network at all

Because there is no transport inside, `postMessage` works as well as a socket.

```js
// tab
const s = setrecon({ items: tabIds });
s.on('send', (t, d) => worker.postMessage({ t, d }));
worker.onmessage = e => s.feed(e.data.t, e.data.d);
s.start();
```

```js
// worker
const s = setrecon({ items: workerIds });
s.on('send', (t, d) => postMessage({ t, d }));
onmessage = e => s.feed(e.data.t, e.data.d);
```

No encoding step either — structured clone carries the plain objects as-is.

### Testing your own protocol against it

Two sessions and a two-line network. This is why the transport is absent, and
it is not a nicety: reordering, duplication, simultaneous starts and slow
database loads are all reachable this way and reachable no other way.

```js
const a = setrecon({ items: listA });
const b = setrecon({ items: listB });

const link = (from, to) => from.on('send', (t, d) =>
    setTimeout(() => to.feed(t, d), 50 + Math.random() * 200));  // jitter

link(a, b); link(b, a);

a.on('finish', (reason, mine, theirs, st) =>
    console.log(reason, st.algorithm, st.predictedBytes, mine.length, theirs.length));

a.start();
```

Drop messages, duplicate them, deliver them backwards — the engine holds
anything that arrives before its prerequisite and replays it. Loss is the one
thing it does not handle.

`setrecon_lab.html` does this across seventeen scenarios in the browser, with a
timeline where each arrow's slope is its transmission time and messages sent
at the same moment fan out instead of hiding behind each other. Every option
has a control, so you can watch `chunkSize` turn ten messages into two hundred
without moving the byte count.

## ⚠️ Constraints

**Items are numbers up to 2^53, and must be unique.** The fingerprint is an
XOR, so a repeated item cancels itself: `[1,2,3,3]` and `[1,2,5,5]` would look
identical. This is checked, and a duplicate throws.

**Zero is not a valid item** when the sketch is enabled — its powers are all
zero, so it can't be carried. Offset your IDs by one.

**Strings and UUIDs need mapping.** Hash them into 53 bits before you get here,
and be aware that collisions become possible: at 100,000 items the chance is
small but not zero, and a collision looks like a missing item rather than an
error.

---

## 🔬 Internals

Exported for testing and simulation, not for everyday use:

```js
setrecon.core     // 26 pure functions: the algorithms and the byte predictors
setrecon.sketch   // the BCH sketch on its own
setrecon.gf64     // GF(2^64) arithmetic, with a BigInt reference implementation
```

The byte predictors are exported alongside the algorithms deliberately: a
simulation's whole purpose is comparing predicted cost against actual, and a
decider that predicts badly is the failure mode worth finding.

```js
const core = setrecon.core;
core.decidePreStrata(nA, nB, expectedDiff, allowSketch, link);  // what it would pick
core.predictStrataBytes(nA, nB);                                // what it thinks that costs
```

See [testing your own protocol against it](#testing-your-own-protocol-against-it)
for the two-line network these are meant to be measured in.

---

## 📊 Why there's no sketch extension

A sketch of capacity 2c contains the capacity-c sketch as its prefix, so a
failed exchange can be retried by sending only the extra syndromes. Bitcoin's
BIP 330 does exactly this, and it removes the need for `expectedDiff`
entirely: start small, double until it decodes.

It was measured before being built. Two 20,000-item lists, 100 ms link, 8 Mbps:

| difference | extension | Strata (what this does) |
|---|---|---|
| 1 | 80 B / 401 ms | 30.7 KB / 426 ms |
| 5 | 144 B / 2,982 ms | 30.8 KB / 407 ms |
| 20 | 456 B / 3,376 ms | 31.6 KB / 401 ms |
| 100 | 1.8 KB / 14,694 ms | 36.0 KB / 393 ms |

Seventy times fewer bytes, for eight to thirty-seven times the wall clock. Two
costs stack: every doubling decodes from scratch — incremental decoding is on
minisketch's own TODO list, so C pays that too — and each attempt is another
round trip.

C decodes capacity 128 in a fraction of a second, so six attempts cost nothing
there and fourteen seconds here. Same language wall the `d ≤ 20` cap comes
from. A WebAssembly build is what would change it.

## 🧾 What it doesn't do

- **No retransmission.** Bring a transport that delivers.
- **No hint engine.** `expectedDiff` is yours to compute; nothing here tracks sync
  history per peer.
- **Numbers only.** Up to 2^53, unique, non-zero when sketching.
- **No sketch extension.** Measured and rejected — see above. Revisit with
  WebAssembly.

## 📚 References

- Eppstein, Goodrich, Uyeda, Varghese — *What's the difference? Efficient set reconciliation without prior context* (SIGCOMM 2011) — the IBLT and the Strata Estimator
- Goodrich, Mitzenmacher — *Invertible Bloom Lookup Tables* (Allerton 2011)
- Dodis, Ostrovsky, Reyzin, Smith — *Fuzzy Extractors* (SIAM J. Computing 2008) — the PinSketch algorithm behind the BCH sketch
- Massey — *Shift-register synthesis and BCH decoding* (IEEE Trans. Inf. Theory 1969)
- Biswas, Herbert — *Efficient Root Finding of Polynomials over Fields of Characteristic 2* (2009) — the Berlekamp trace algorithm
- [bitcoin-core/minisketch](https://github.com/bitcoin-core/minisketch) — the reference BCH implementation, and the source of the benchmarks this was measured against
- [BIP 330](https://github.com/bitcoin/bips/blob/master/bip-0330.mediawiki) — Erlay reconciliation, and where sketch extensions are specified
- [hoytech/negentropy](https://github.com/hoytech/negentropy) — range-based set reconciliation, the other family

## 🙏 Support the Project

If this library saves you time, consider supporting:

- ⭐ **Star** the repo — helps visibility.
- 🐛 **Issues/PRs** — report bugs, propose features.
- 💖 **Sponsorships** — GitHub Sponsors or your preferred platform.
- 🧪 **Production stories** — share how you use `setrecon` (helps guide roadmap).

## 📜 License

**Apache License 2.0**

```
Copyright © 2026 colocohen

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
```
