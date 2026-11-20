- A session that was already signed in can rejoin while the control plane is
  down (#73). The gateway keeps the Felix token the control plane issued for an
  ID token and scope, and reuses it only when the control plane cannot be
  reached and only until the earlier of the Felix token's and the ID token's
  expiry. Any other join is refused at once instead of waiting for the control
  plane: control-plane requests now time out after 3 seconds to connect and 10
  in all. See "When the control plane is down" in docs/configuration.md.
