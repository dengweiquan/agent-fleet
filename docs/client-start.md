# Finite foreground connector trial

Use a pinned release commit. A plain HTTPS clone of this public repository requires no GitHub token. Node executes the TypeScript source directly; do not install packages merely to run the connector.

```sh
git clone https://github.com/dengweiquan/agent-fleet.git
cd agent-fleet
git checkout --detach RELEASE_COMMIT_SHA
node --experimental-strip-types src/connector-client.ts selfcheck
```

Choose a confirmed persistent workspace on the execution host. Keep the connector journal outside the source checkout and out of transient VM storage. Create a local configuration from the following shape, replacing every placeholder with the existing approved SSH alias and coordinator paths. Do not place credentials in this file.

```json
{
  "version": 1,
  "journalPath": "/PERSISTENT_WORKSPACE/agent-fleet-state/connector.sqlite",
  "sshAlias": "EXISTING_SSH_ALIAS",
  "connectionApproved": true,
  "server": {
    "node": "/ABSOLUTE/NODE_ON_COORDINATOR",
    "cli": "/ABSOLUTE/AGENT_FLEET/src/cli.ts",
    "pool": "/ABSOLUTE/COORDINATOR_STATE/resources.sqlite",
    "fleet": "/ABSOLUTE/COORDINATOR_STATE/fleet.json",
    "history": "/ABSOLUTE/COORDINATOR_STATE/history",
    "endpointId": "REGISTERED_ENDPOINT",
    "resourceId": "REGISTERED_RESOURCE"
  }
}
```

The coordinator pool, manifest and matching resource must already exist. This client constructs only an observer `fleet-remote serve` command. It does not bootstrap a coordinator, choose business projects or grant task execution. Verify its configuration before setting `connectionApproved`.

```sh
node --experimental-strip-types src/connector-client.ts run /ABSOLUTE/connector-config.json 60
node --experimental-strip-types src/connector-client.ts status /ABSOLUTE/connector-config.json
node --experimental-strip-types src/connector-client.ts disable /ABSOLUTE/connector-config.json
```

`run` is foreground and limited to 1–3600 seconds. Ctrl-C/SIGTERM persists the disabled flag; the watchdog must not restart a disabled connector. `disable` in another terminal also persists this control. Re-enable only through the explicit `enable CONFIG` command after inspection. Do not remove an uncertain owner record or reuse an old journal for a different endpoint.

Expected success is a validated AgentFleet hello and durable scoped event observations with `modelInvoked: false`. A successful local SSH process spawn alone is not health. Authentication failure, missing alias/config, changed host key, network-policy rejection or missing runtime produces a blocker; do not create keys, rewrite SSH settings, accept a changed fingerprint or modify a firewall automatically.

Watchdog supervision and verified-process restart are exposed as a library and tested with fake transports. There is no installed system service, native timer or automatic cross-process restart script in this release. Integrate the execution host's approved supervisor with positive owner termination evidence before enabling unattended restarts. Missing PID or heartbeat is not sufficient proof.

The connector does not wake or run a Muse model, claim a task, import artifacts or accept results. Those integrations require a verified platform API and a separate execution contract. Keep the first real connection trial read-only.
