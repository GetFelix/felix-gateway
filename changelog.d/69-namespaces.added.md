- One gateway can serve several namespaces of its tenant (#69). List them with
  `namespaces = [...]` at the top of the scope file, and a `join` names one with
  `"namespace"`. The gateway refuses a namespace it does not list, and the
  session's Felix token is narrowed to the scope in that namespace, so Felix
  grants each person only the namespaces their roles reach. A scope file without
  `namespaces` serves `GATEWAY_NAMESPACE` only, as before.
