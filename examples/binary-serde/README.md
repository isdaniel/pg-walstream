# binary-serde

Ships `ChangeEvent`s between processes in a binary format. **No database is needed**: the
example builds its events in memory, so it runs anywhere and doubles as a smoke test (it exits
non-zero if any check fails).

`ChangeEvent` implements serde's `Serialize` and `Deserialize`, so it works with any serde
format. This example uses five of them:

| Format | Crate | Why you would pick it |
|--------|-------|-----------------------|
| JSON | `serde_json` | readable, every language, and the one to store long-term |
| CBOR | `ciborium` | compact, self-describing, other languages can read it |
| MessagePack | `rmp-serde` | compact, self-describing, other languages can read it |
| postcard | `postcard` | smallest, `no_std` friendly, Rust to Rust |
| bincode 2 | `bincode` | like postcard; the crate is unmaintained, so prefer postcard in new code |

## Run

```bash
cd examples/binary-serde
cargo run
```

Expected output:

```text
1. One transaction of 6 events, in each format

   format          bytes   vs json
   json             1174      100%
   cbor              933       79%
   messagepack       498       42%
   bincode 2         361       31%
   postcard          349       30%

2. 6 events written back to back into 349 bytes, read back one by one
3. an event inside your own enum: postcard 64 bytes, internally tagged CBOR 158 bytes
4. truncated postcard: error; hostile postcard: "Hit the end of buffer, expected more data"; hostile bincode: "LimitExceeded"

All checks passed.
```

## What each step shows

1. **Sizes.** One transaction (BEGIN, an INSERT that carries an xid, an UPDATE, a DELETE, a
   logical message and COMMIT) is encoded in each format and decoded again; every event has to
   come back unchanged. postcard and bincode are about 30% of the JSON size.
2. **Many events in one buffer.** postcard values end where they end, so events written back to
   back can be read one at a time with `postcard::take_from_bytes`. That is what you want for a
   file, a pipe or a socket. A message queue that delivers one message per event needs no framing.
3. **Your own envelope.** `Frame` is an ordinary enum around a `ChangeEvent` and works with every
   format. `TaggedFrame` uses `#[serde(tag = "kind")]`, which serde can only read back from a
   self-describing format, so it goes through CBOR.
4. **Bytes you do not trust.** Truncated bytes and a row that claims about a billion columns make
   postcard return an error. bincode 2 allocates whatever length a string claims unless it is given
   a limit, so the example decodes with `with_limit::<MAX_EVENT_BYTES>()` and a string claiming
   `u64::MAX` bytes fails with `LimitExceeded`.

## Using it in your own consumer

Encode each event as it comes off the stream and acknowledge it after the hand-off:

```rust,ignore
Ok(event) => {
    queue.send(postcard::to_allocvec(&event)?).await?;
    stream.shared_lsn_feedback.update_applied_lsn(event.lsn.value());
}
```

and decode on the other side with `postcard::from_bytes::<ChangeEvent>(&bytes)`.

The caveats worth knowing before you pick a format (same version on both ends for postcard and
bincode, bytes written by 0.10.0, non-UTF-8 text, keeping the old MessagePack bytes) are in the
root README under [Shipping Events Between Processes](../../README.md#shipping-events-between-processes).
