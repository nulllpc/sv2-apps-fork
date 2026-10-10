# Issue #772: which share outcomes should feed vardiff (JDC and Pool)

Working document for [sv2-apps #772](https://github.com/stratum-mining/sv2-apps/issues/772).
Supersedes the earlier notes in `issue-772-vardiff-accounting.md`, which describe an older version of the issue.

Code references use the `sv2-apps-fork` checkout and the locked `stratum` revision
`28149a84d85fc3d0101202204c627389cb2d52ce`.

- Part 1 is my exploration journey: what I believed, what the code showed, what I corrected.
- Part 2 is the set of decisions I'll present to GitGab19, with a status on each. Nothing marked
  open or leaning is a decision yet.

---

## Part 1: Exploration journey

### 1.1 What the issue asks

The vardiff counter is incremented for shares that fail validation. Vardiff turns that count into
a nominal-hashrate estimate and then a target. In JDC, the per-channel estimates are summed into
one `UpdateChannel` to the shared upstream, so one channel's rejected submissions can reach the
aggregate. Pool updates each channel independently, with no aggregation step.

The issue does not say "stop counting rejects". #272 recorded a deliberate policy that rejected
submissions are counted as submission-rate control. The task is to reconcile that policy with
the untrusted-input boundary, define which outcomes are eligible, and enforce it consistently at
the standard and extended entry points in both apps.

Out of scope per the issue: replacing the vardiff algorithm, target ordering (#892), minimum
difficulty (#799), tProxy (it validates before incrementing).

### 1.2 Three things that sound alike

| Thing | What it is | Status in this issue |
| :--- | :--- | :--- |
| Share accounting | Accepted/rejected bookkeeping, credited work | Preserve as is |
| Vardiff observation | The counter feeding the hashrate estimate | **This issue** |
| Difficulty adjustment | `try_vardiff` plus the 60s loop, then `SetTarget` | Do not redesign |

Counting a share does not change difficulty. The count is an input that can change the next
periodic adjustment, and the thresholds mean one extra share rarely moves the target.

### 1.3 Where the counter is fed

JDC standard-share handler, `miner-apps/jd-client/src/lib/channel_manager/downstream_message_handler.rs:927-1151`.

My first model was that the handler's returned messages decide whether vardiff counts. That was
wrong. The increment at `:1130` is outside the validation closure, in the `Some(validation)` arm
of `match validation` (`:1125`). `Some` only means the channel was found. The arm never looks at
the inner `Result`, so every outcome reaches the increment, including the early `return Ok(messages)`
at `:1008` for rejected shares.

Corrections along the way:
- The `SeenSharesBudgetExhausted` arm (`:980-988`) returns from the closure, not the handler. The
  counter is still incremented and the channel is then disconnected.
- The `Option` is a real check (channel exists, `:1145` replies with an invalid-channel-id error)
  and can't be unwrapped.
- A missing `prev_hash` returns at `:921-924`, before the closure and before the increment. I
  haven't checked when it is `None`.

Increment call sites that exist in the fork (grep): JDC `downstream_message_handler.rs:1130`
(standard) and `:1433` (extended); Pool `mining_message_handler.rs:935` and `:1217`. I have read
only the JDC standard handler so far.

### 1.4 What the validator checks, and in what order

`validate_share`, `stratum/sv2/channels-sv2/src/server/standard.rs:763-995`.

| Order | Outcome | Line | Header hashed yet? |
| :--- | :--- | :--- | :--- |
| 1 | `SeenSharesBudgetExhausted` | `:770` | no |
| 2 | `Stale` (job id in stale set) | `:788-794` | no |
| 3 | `InvalidJobId` (unknown id, or no target for it) | `:797-803`, `:819-825` | no |
| 4 | `Invalid` (ntime too low / too far ahead / non-rollable version bits) | `:847-877` | no |
| 5 | header built and hashed | `:880-890` | yes |
| 6 | `Duplicate` (hash already seen, after meeting a target) | `:909-918`, `:967-976` | yes |
| 7 | `BlockFound` / `Valid` | `:958`, `:987` | yes |
| 8 | `DoesNotMeetTarget` | `:989-993` | yes, and it failed |

Consequences I had to correct in my own thinking:
- `Stale`, `InvalidJobId` and `Invalid` return before the share is hashed. The pool can't say
  whether the sender did any work for them. "A stale share is still a share that met the target"
  is an assumption, not something the code enforces. The stale set exists so late shares get an
  accurate error code (`job_store.rs:313`), not to credit work.
- Only shares that meet a target are recorded for dedup (`update_share_accounting` at `:919`,
  `:978`). A below-target share is never marked seen, so one message can be resent indefinitely and
  returns `DoesNotMeetTarget` each time.
- A pool recomputing a hash and finding it short proves nothing about the sender's work. Only a
  hash that meets the target is evidence of work.
- Duplicates are detected by header hash, not job id. The job's merkle root feeds the header, so
  pointing the same nonce at a different job gives a different hash, and counting it as `Valid`
  needs fresh work.

### 1.5 What vardiff does with the count

`channels-sv2/src/vardiff/classic.rs:169-250`.

- `realized_shares_per_minute = shares * 60 / delta_time`, converted to a hashrate through the
  current target.
- With zero counted shares, the new hashrate is the old one divided by 1.5, 2 or 3 depending on
  `delta_time` (`:226-231`). With the 60s loop this is usually 3 per tick, and it compounds toward
  `min_hashrate`.
- So any outcome excluded from the count is, to the estimator, indistinguishable from a silent
  miner. Excluding too much collapses the difficulty of channels that are still working.

### 1.6 The two views of the counter

- **Rate control (#272, GitGab19):** vardiff keeps the configured `shares_per_minute`. It does not
  matter whether shares are valid, so every received share increments.
- **Work-supported hashrate (the issue's concern, and the #272 reporter's proposal):** one counted
  share should stand for real work, so the estimate and the JDC aggregate reflect work.

My working criterion: **what does it cost a sender to fabricate this outcome, and can the pool
tell a repeat from a first occurrence?**

### 1.7 Not yet done

- JDC aggregation (`channel_manager/mod.rs:1370-1409`) not read. Loupe #110's cross-tenant claim
  is unreproduced and unbounded by me.
- Pool handlers and the extended handlers not read.
- Whether the extended channel returns `VersionRollingNotAllowed` (not returned in
  `standard.rs`).
- Magnitude of honest stale shares: sized structurally in 1.9, not measured.
- Fix shape not designed. One constraint noted: by the time the validation closure returns, the
  outcome has been turned into a `Vec` of messages.

### 1.8 Parked

- When `prev_hash` is `None` at `downstream_message_handler.rs:921`.
- Banning or disconnecting abusive channels (a different mechanism from vardiff).
- How a pool would measure "keeping up with the network" for stale shares.
- `should_acknowledge` internals.

### 1.9 Stale, in detail

- A stale share is one whose job id belongs to a job from a previous chain tip. Jobs replaced under
  the same tip are *past* jobs, which are still validated, hashed and countable.
- At a tip change, past jobs are moved wholesale into the stale set (`job_store.rs:313-317`) and
  `job_id_to_target` is cleared (`standard.rs:710`, `:723`). The stale set lives until the next tip
  change, so a stale job id stays usable for about a block interval. It holds at most
  `max_past_jobs + 1` ids (default `MAX_PAST_JOBS = 16`), but that bounds ids, not messages.
- The validator rejects stale shares by lookup alone (`standard.rs:788-794`), after
  `increment_rejected_shares`. No hash, no target, no dedup. Share accounting therefore already
  treats them as uncredited.
- Verifying a stale share would need the old `prev_hash`, `nbits` and job target, none of which
  survive a tip change.
- Honest stale shares come from in-flight submissions during the few seconds after a tip change.
  With an assumed window of about 5s, the lost fraction is about 8% of a 60s tick and under 2%
  of a 5-minute accumulation. The estimator needs 60% deviation at 60s and 15% at 300s to retarget
  (`classic.rs:210-217`), so the dip cannot trigger an update by itself. The window length is an
  assumption, not a measurement.
- Not verified: the extended-channel stale path, and whether a repeated `SetNewPrevHash` with an
  unchanged prev-hash also moves jobs into the stale set.

---

## Part 2: Decisions to present to GitGab19

GitGab19 asked that the specific changes be described and agreed before a PR is opened. This
part is that description, with honest status per item.

### 2.1 Framing question

#272 says counting every received share is intentional rate control. The issue asks us to
either make rejected input ineligible, or keep it eligible for traffic control and document and
enforce a boundary so it is not treated as validated work or leaked through the JDC aggregate.

**Q1.** Which is the contract for the counter: protecting the pool's submission rate, or
measuring work? The answer decides whether the table below is "exclude" or "count but fence".

### 2.2 Proposed eligibility per outcome

Status: **decided** (I'm settled), **leaning**, **open**.

| Outcome | Hashed? | Proposed | Status | Reason |
| :--- | :--- | :--- | :--- | :--- |
| `Valid` | yes | count | decided | Met the job target, first occurrence |
| `BlockFound` | yes | count | decided | Met the network target |
| `Duplicate` | yes | do not count | decided | Work was real but already credited by the first copy |
| `DoesNotMeetTarget` | yes (failed) | do not count | leaning | No proof of work, and not deduped, so resendable at no cost |
| `Invalid` | no | not settled | open | Never hashed; costs nothing to fabricate; no dedup |
| `InvalidJobId` | no | not settled | open | Never hashed; costs nothing to fabricate; no dedup |
| `Stale` | no | do not count | decided | Never hashed and not deduped, so repeatable at no cost for a whole block interval; the honest loss is a few seconds of in-flight shares per tip change, well under the retarget thresholds (see 1.9) |

Distinguish, as the issue asks, locally valid shares that a harder upstream target filters
(the JDC upstream-validation stage) from locally invalid submissions. The upstream stage does not
affect the counter today.

### 2.3 Open questions for GitGab19

- **Q1** (above): rate control or work measurement as the contract.
- **Q2, `Stale`:** I propose excluding it, which departs from "count every received share" for
  this one class. Making stale shares verifiable instead would not be a small validator change:
  the target mapping is cleared and the chain tip replaced at a tip change, so the old `prev_hash`,
  `nbits` and per-job targets would have to be retained. Is excluding acceptable, given that
  share accounting already rejects stale shares as uncredited work?
- **Q3, silent channels:** excluding outcomes means a flooding channel sees the zero-share path and
  its estimate divides by up to 3 per tick. Is that the intended isolation, and is it acceptable
  that an honest miner whose shares are all rejected looks identical?
- **Q4, scope of shared change:** should the eligibility decision live in `sv2-apps` (classify
  before incrementing), or in `channels-sv2` (the estimator receives outcomes)? The issue says a
  shared estimator change alone must not leave application classification unspecified.

### 2.4 Requirements carried over from the issue

- Apply the policy at standard and extended entry points in both JDC and Pool.
- Keep accepted/rejected accounting independent of vardiff observations.
- Behavioural tests with an abusive channel and an honest channel: per-channel observations,
  nominal hashrates and targets; in JDC also the aggregate `UpdateChannel`, upstream responses and
  honest-share forwarding.
- Cover active/past jobs and future/new jobs separately. Do not assert that `SetTarget`
  invalidates already-issued jobs.
- Record whether Loupe #110's cross-tenant impact is reproduced, bounded or unsupported.
- Preserve difficulty recovery for honest miners, including declining or silent hashrate.
- Reference Loupe #110 in the PR. Do not close after handling only one application.

### 2.5 What this document does not decide

The fix shape, the code location of the classification, `Stale`/`Invalid`/`InvalidJobId`, and the
JDC blast radius. Those follow from the answers above and from reading the unread code in 1.7.
