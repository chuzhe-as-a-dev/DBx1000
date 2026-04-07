# Polyjuice Port — Progress Log

## Status: Complete (all tests pass, code reviewed)

## Completed Work
1. [x] tpcc_txn_pj.cpp — Polyjuice-compatible op order (separate loops, customer at end)
2. [x] Build system — benchmarks_tpcc_per_op_pj target, PJ variant special-casing
3. [x] Access mapping — computed lookup_policy with ol_cnt, per-type wait step tables
4. [x] Write visibility — only PUBLIC writes exposed at piece boundaries
5. [x] Dependency dedup — add_dependency helper with upgrade semantics
6. [x] Adaptive backoff — thread-local per-txn-type exponential
7. [x] Validation fix — own_write check on tid-mismatch (not just lock-bit)
8. [x] Code review — fixed wait target interpretation bug, removed dead code

## Bugs Found and Fixed

### 1. Read-after-own-write validation failure
When a txn reads then writes the same row, piece validation advances tid_word.
Final commit sees tid mismatch and aborts — even though the change was ours.
Fix: check own_write on tid-mismatch path in both piece and final validation.

### 2. Wait target interpretation (found during code review)
Policy wait values (wait_new_order, wait_payment) are LOCAL per-txn-type step
values, not global acc_ids. Initial implementation used a single ACC_ID_TO_STEP
table treating them as global — wrong. Fixed with per-type remapping tables
(NO_WAIT_STEP, PAY_WAIT_STEP) that correctly convert Polyjuice local steps
to our consolidated steps.

## TODOs / Known Limitations
- **Piece retry not implemented**: Polyjuice retries from last validation point
  on early-validation failure; we abort the whole txn. Paper doesn't mention
  this, and it adds significant complexity. Skip for now.
- **Wait-for-commit ('c')**: Dead code in Polyjuice repo. Skip.
- **Contention adaptation**: Single policy table. Easy to swap values later.
- **Separate wait-before-validation**: Polyjuice has it, we consolidate into
  one wait. Paper says they consolidate too ("we consolidate the two kinds").
- **Delivery txn type**: Policy has acc_ids 18-25 for delivery. Not implemented
  in DBx1000. Wait targets for delivery deps map to 0 (no-wait).
- **Backoff tuning**: Current constants (MIN=1000, MAX=100000 cycles) are
  placeholder. Polyjuice uses learned per-txn-type multipliers.
- **Use-after-free on dep TxnState**: Mitigated by pj_txn_id generation check
  + ring buffer. The ring buffer has 8 slots; if a worker completes >8 txns
  between a waiter's checks, status is lost (conservative cascade-abort).
