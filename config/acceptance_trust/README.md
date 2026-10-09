# Acceptance trust policies

This directory is the only trust-root source accepted by
`scripts/acceptance_window.py verify`. A deployment policy is a reviewed,
release-bundled `<policy-id>.json` document containing only public keys, the
approved code/image/config identity, the deployment scope, and the exact next
external-anchor sequence/head. It also pins the independently reviewed replay cost
schedule and the exact verifier source/runtime dependency digest.

The verifier additionally compares this policy with the root-owned runtime release
measurement at `/run/funding-arbitrage/release-identity.json`; that path is fixed in
the verifier and cannot be supplied by evidence or CLI arguments. The complete path
must have root-owned, non-group/world-writable ancestry and is opened without
following symbolic links.

Evidence is never allowed to provide or override its own keyrings, key hashes,
environment, deployment, or anchor head. Adding or rotating a trust policy is a
release change and must not include private signing keys. No policy is shipped
by default, so provenance verification fails closed until operators commit an
approved public trust policy for a specific release.
