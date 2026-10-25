# Current finite delivery roadmap

## Mainline: native Muse wake-up and queue execution

Keep model wake-up separate from transport. Muse's native schedule can start an agent in its supported platform context; the existing reverse SSH queue supplies scoped tasks, atomic claims and durable reports. A connected observer does not wake an agent. A queued run under a disabled schedule is not execution evidence.

The next validation is one bounded enabled native one-off, after any earlier queued validation is cancelled through the platform's supported mechanism and its cancellation is confirmed. If cancellation or an enabled one-off cannot be expressed through the official tools, stop that action and report the limitation. Await the actual result; do not create duplicate runs or enable an unbounded five-minute schedule. Schedule operations remain external platform actions requiring their existing approval scope; this repository does not perform them automatically.

Once that check succeeds, run one separately authorized small synthetic development task through claim, ACK, implementation artifacts, independent verification and review. Preserve exact project/run/attempt/endpoint identities and the fixed base commit. A model's completion message, a successful shell exit or a queue delivery receipt cannot substitute for acceptance evidence. Unknown outcomes retain capacity and require reconciliation before retry.

## Continue independently

- Dynamic resource allocation across projects, including shared account/endpoint limits and unknown capacity retention.
- Fixed project scopes, request receipts, read-only task-bridge queries and explicitly authorized local control commands.
- Session history/resume, event replay and independent verification/review.
- Offline watchdog contracts and fault tests. Any real supervisor/timer deployment needs a separate finite plan; user disable remains authoritative.

## Deferred: Gadget

Retain the original offline event mapper and fixtures, labeled `personal-experimental` and `disabled-unpaired`. Gadget is an isolated personal alternative, not a prerequisite, commercial provider or currently working real-time execution route. No further live integration is assumed. Hardware selection, token terms, token creation, service/account permissions and phone/BLE pairing all remain outside the current execution authorization. Do not work around headless pairing limitations or switch providers without an explicit choice.

See [MVP status](mvp-status.md) for what is already operable, what is validated only by fake processes/fixtures, and the finite gaps still to close.
