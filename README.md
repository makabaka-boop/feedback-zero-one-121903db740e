# fedtrain

A federated training protocol with exact rational arithmetic, built only on
the Python standard library and SQLite.  A coordinator and several training
nodes talk JSON over HTTP; each process keeps its own state in its own
database file, and every decision is durable before it is announced.

```
fedtrain/
  rational.py      exact p/q arithmetic and half-away-from-zero rounding
  node.py          a node's private state machine (SQLite)
  node_server.py   the node's HTTP face: compute / decide / revoke
  coordinator.py   rounds, sealing, the global model and its HTTP face
  cluster.py       an in-process federation of HTTP nodes plus a coordinator
  service.py       the module-level façade: init / open / seal / status / model
  transport.py     JSON-over-HTTP plumbing
  store.py         SQLite connection and transaction helpers
  errors.py        the protocol's single rejection type
tools/
  run_node.py          run one node as its own process
  run_coordinator.py   run the coordinator as its own process
  demo.py              the acceptance example, printed step by step
tests/               the verification suite (unittest, standard library only)
```

## The model

The coordinator holds the global model `(w, m)`; both are exact rationals.
Every node holds a private target array `y`, a private cursor `c` and a
private residual `r`, all starting at `0`.  Values travel as `p/q` strings:
`p` is an integer, `q` is a positive integer, and every value is reduced to
lowest terms.  Nothing in the pipeline uses floating point.

A round proceeds in three states:

| state | meaning |
| --- | --- |
| `collecting` | the round exists, candidates are being gathered |
| `settling` | the member set and the new model are fixed, decisions are being applied |
| `done` | every registered node has confirmed its decision |

### Opening

`open(id)` admits a round under the identifier `id` (a non-empty ASCII
alphanumeric string).  Round numbers are allocated persistently from `1` and
increase by one.  Opening an identifier that already exists returns the round
exactly as it stands, without allocating anything.  A *new* identifier is
admitted only after the previous round is `done`.  Opening a round asks every
registered node for its candidate, so `status` can report which nodes are
`ready`.

### Computing a candidate

With the round's model `w` fixed, a node computes

```
y  = targets[c mod len(targets)]
g  = w - y
q  = round_half_away_from_zero(g + r)
r' = g + r - q
```

`round_half_away_from_zero` sends an exact half away from zero: `1/2 -> 1`,
`-1/2 -> -1`, `3/2 -> 2`.  The candidate is persisted together with the `w`
it was computed under, so a repeated request for the same round returns
exactly the same `q` and a request under a different `w` is refused.

### Sealing

`seal(id, members)` is only accepted while the round is `collecting`.  It
freezes `members` — a duplicate-free subset of the `ready` nodes — and
publishes, in one transaction:

```
m' = m / 2 + mean(q over the selected nodes)
w' = w - m'
```

The model and the member list become visible together, so a reader can never
see a new model next to the old member list.  Sealing the same member set
again (in any order) replays the result; sealing with a different set is
rejected without side effects.

An empty member set cancels the round: the model is left untouched, the round
becomes `done`, and every node is told to revoke, which discards its candidate
and blocks any late computation for that round.

### Applying a decision

Each node receives the decision for the round:

* **included** — advance `c` by one and replace `r` with `r'`, atomically.
* **excluded** — discard the candidate and change nothing.

A repeated decision changes nothing; a revocation that arrives before a
decision blocks the advance even if the computation shows up later.

### The model

`model()` returns the complete model, `{"w": "p/q", "m": "p/q"}`.

## The acceptance example

One node with `y = ["1/4"]`, selected in two consecutive rounds:

| round | w before | g + r | q | r' | m' | w after |
| --- | --- | --- | --- | --- | --- | --- |
| 1 | `0/1` | `-1/4` | `0` | `-1/4` | `0/1` | `0/1` |
| 2 | `0/1` | `-1/2` | `-1` | `1/2` | `-1/1` | `1/1` |

So the model goes `(0/1, 0/1)` then `(1/1, -1/1)`.  `python3 -m tools.demo`
prints exactly that.

## Running it

In one process:

```python
import fedtrain

fed = fedtrain.init({"n1": ["1/4"], "n2": ["1/6"]})   # once
fed.open("r1")                    # -> {round, state, ready, members}
fed.seal("r1", ["n1", "n2"])      # -> status; model published atomically
fed.model()                       # -> {"w": "0/1", "m": "0/1"}
```

As separate processes (each with its own database under `--db`):

```console
$ python3 tools/run_node.py --id n1 --y 1/4 --db /tmp/n1.db --port 7101
$ python3 tools/run_node.py --id n2 --y 1/6 --db /tmp/n2.db --port 7102
$ python3 tools/run_coordinator.py --db /tmp/c.db \
      --node n1=http://127.0.0.1:7101 --node n2=http://127.0.0.1:7102 --port 7100
```

Each runner prints one JSON line with the bound address, then serves
`POST /open`, `/seal`, `/status`, `/model` (coordinator) and `/compute`,
`/decide`, `/revoke` (nodes).

## Failures the protocol absorbs

* **Repeated communication** — every operation is idempotent: a repeated
  `open`, `seal`, `compute` or `decide` returns the outcome already stored, and
  a repeated `seal` with the same members never applies the model twice.
* **Lost replies** — the coordinator re-sends a candidate request or a decision
  on the next `status`, and each node recognises the replay.
* **Restarts** — the round number, the state, the frozen members, the model,
  the node's cursor and residual and the node's stored candidate all live on
  disk.  A restart resumes the round it was in.
* **Hard kills** — SQLite runs in WAL mode with `synchronous=FULL` and every
  transition is a single `BEGIN IMMEDIATE ... COMMIT`.  A decision that has
  been written survives `SIGKILL`, and a round interrupted between publishing
  the model and recording the acknowledgements converges on restart without
  moving the model again.
* **Cancellation races** — a revocation recorded before a computation blocks
  that computation; a revocation recorded after a decision cannot undo it.

## Rejections

A rejected request is answered with `{"error": <JSONPath>}` and HTTP 400.  The
path points at the offending member of the body, from the root `$`, with
zero-based indices — for example `$.id`, `$.members[2]`, `$.w`.  Rejections
have no side effects: a rejected `seal` leaves the round collecting, the
model unchanged and the pending candidates intact.

Identifiers that are not non-empty ASCII alphanumeric strings, round numbers
that are not positive integers, values that are not well-formed `p/q`, unknown
round or node references, a different `w` for the same round and a `seal` on
a round that is not collecting are all rejected this way.

## Tests

The suite uses only `unittest`:

```console
$ python3 -m unittest discover -s tests -t .
```

Highlights, all of which the acceptance criteria call for:

* `test_example.py` — the `y = ["1/4"]` example, two rounds, `(0/1, 0/1)` then
  `(1/1, -1/1)`.
* `test_protocol.py` — status transitions, sealing, exclusion, cancellation,
  ASCII ordering, exact fractional models.
* `test_cursor.py` — the cursor walks `y` modulo its length and only advances
  on inclusion.
* `test_errors.py` — every rejection path, including the JSONPath reported and
  the absence of side effects.
* `test_faults.py` — repeated deliveries, lost replies, early revocations,
  coordinator and node restarts, refused configuration changes.
* `test_settling.py` — a round held in `settling` by an unreachable node,
  then completed on recovery.
* `test_crash.py` — real processes killed with `SIGKILL` after a decision was
  committed; on restart the decision is still there and replays do not double-apply.
* `test_service.py` — initialisation happens once; each component persists in
  its own file.
* `test_rational.py` — exact arithmetic and the tie-breaking rule.
