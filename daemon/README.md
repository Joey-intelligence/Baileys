# Joey WhatsApp daemon

The production WhatsApp daemon Joey runs on this fork (Baileys 7.0.0-rc14).
It keeps a persistent connection to one WhatsApp number and exposes a small
local HTTP API (127.0.0.1:3987) for reading chats/messages and sending.

Layout: `src/index.js` is the whole daemon. It consumes the library from the
repository root (`"baileys": "file:.."`). Auth state and the message store
live outside the repo; their locations are set with the `WA_AUTH_DIR` and
`WA_DATA_DIR` environment variables (defaults: `auth/` and `data/` next to
the daemon). `baileys-wa.service` and `ensure.sh` are the systemd deployment
used in production (the unit file lives in ephemeral /etc, so ensure.sh
reinstalls it after a VM replacement).

Integration fixes carried here (learned in production, 2026-10-20):

- Message retry support. When a peer device cannot decrypt a message
  (garbled first delivery), WhatsApp asks the sender to re-encrypt and
  resend; Baileys serves that from `getMessage`. The daemon indexes its own
  sent text messages by id (loaded from the store at boot, updated live)
  and answers getMessage from the index, and it persists the
  `msgRetryCounterCache` to disk so retry accounting survives restarts.
  Without this, one garbled delivery leaves the peer stuck at a single
  tick with no recovery path.
- Egress proxy discipline. The WebSocket and the WA version fetch both go
  through the egress proxy agent, and the client takes the newest WA
  version across several fetches; a stale version gets the pairing socket
  dropped with 401s.
- Pairing once per registration. The pairing code is requested exactly
  once; re-requesting on reconnect rotates the code and makes pairing
  impossible. A 401 before registration completes is treated as a
  handshake restart, not a logout.
- LID-aware consumers. Incoming messages may arrive under a contact's
  anonymous LID rather than the phone JID; anything reading the store
  should match pushName and both identifiers.

Sending always requires the operator's explicit approval of the exact text
and recipient.
