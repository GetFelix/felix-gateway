- A publish resent after a reconnect is no longer appended twice (#74). With the
  `idempotent_publish` feature, a `join` names a `session` that the page keeps
  across reconnects and each `publish` carries a `seq`. The gateway publishes
  through Felix's idempotent producer and answers a resent `seq` it already
  acknowledged without sending it. The npm client's `publish` takes the `seq`.
