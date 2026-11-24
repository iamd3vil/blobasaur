# Security policy

## Threat model

Blobasaur is an internal blob storage server built for trusted networks and trusted clients.

By design, Blobasaur does not implement authentication, TLS, or access control lists (ACLs). Any client that can reach the listening port has full access to read, overwrite, and delete data, and can run administrative commands such as `CLUSTER DELSLOTS` and `BLOBASAUR.VACUUM`.

Do not expose the Redis port (`6379`), cluster gossip port, or metrics HTTP port (`9090`) to the public internet or untrusted networks. Bind to `127.0.0.1` or a private interface and enforce access controls at the network layer (firewall rules, security groups, VPN, or service mesh).

## Out of scope for security advisories

Because untrusted network access is outside Blobasaur's security boundary, reports that rely on an unauthenticated client reaching a Blobasaur port (including unauthenticated data access, modification, or denial of service) are not treated as security advisories.

Malformed-input crashes or panics that can be triggered by a buggy client are still real reliability bugs and are welcome as regular GitHub issues.

## Reporting

- For normal bugs, including parser or crash bugs triggered by malformed client commands, open a [GitHub issue](https://github.com/iamd3vil/blobasaur/issues).
- If you believe you have found an issue that crosses a documented boundary, use GitHub's private vulnerability reporting on this repository.

Only the latest release is supported with fixes.
