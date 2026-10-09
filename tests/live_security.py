from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta


def live_credential_policy_json(
    identifiers: Mapping[str, str],
    *,
    expected_egress_ip: str = "203.0.113.10",
) -> str:
    now = datetime.now(UTC)
    return json.dumps(
        {
            "version": "live-credential-policy-v1",
            "expected_egress_ip": expected_egress_ip,
            "credentials": [
                {
                    "venue": venue,
                    "credential_sha256": hashlib.sha256(identifier.encode()).hexdigest(),
                    "subaccount_id": f"funding-v1-{venue}",
                    "dedicated_account": True,
                    "permissions": ["read", "trade"],
                    "ip_allowlist": [f"{expected_egress_ip}/32"],
                    "issued_at": (now - timedelta(hours=1)).isoformat(),
                    "expires_at": (now + timedelta(days=30)).isoformat(),
                    "verified_at": now.isoformat(),
                    "verification_method": "operator_console",
                    "withdrawals_enabled": False,
                    "transfers_enabled": False,
                }
                for venue, identifier in identifiers.items()
            ],
        },
        sort_keys=True,
        separators=(",", ":"),
    )