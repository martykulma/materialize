# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

"""
Regression test for a TOCTOU race between ALTER SECRET and CREATE CONNECTION
that bypasses the GCP service-account-key `token_uri` allowlist.

The allowlist guard rejects a malicious `token_uri` when a GCP connection is
created/altered (against the secret's current contents) and when ALTER SECRET
rewrites a secret that a GCP connection already depends on. Neither check covers
the window in which a secret is rewritten to a malicious `token_uri` *while* a
new connection concurrently comes to depend on it: the ALTER SECRET sees no
dependent yet, and the CREATE CONNECTION reads the still-benign contents. An
attacker can therefore land a malicious `token_uri` in a GCP connection's secret
with neither statement being rejected.

The `alter_secret_pause_before_ensure` failpoint holds ALTER SECRET's durable
write open long enough for the concurrent CREATE CONNECTION to read the old
contents and commit the dependency. VALIDATE = false keeps the test hermetic (no
GCP network); the guard only inspects `token_uri`, so a throwaway key suffices.
"""

import threading
import time

from materialize.mzcompose.composition import Composition
from materialize.mzcompose.services.materialized import Materialized

# Hold ALTER SECRET's durable write open long enough that the concurrent CREATE
# CONNECTION below commits while the secret still has its benign value.
SERVICES = [
    Materialized(
        environment_extra=["FAILPOINTS=alter_secret_pause_before_ensure=sleep(15000)"],
    ),
]


def _key(token_uri: str) -> str:
    # Throwaway service-account key (never a real credential). Only `token_uri`
    # is inspected by the guard; the private key is never parsed under
    # VALIDATE = false.
    return (
        '{"type":"service_account","project_id":"x","client_email":"a@b.com",'
        f'"private_key":"dummy","token_uri":"{token_uri}"}}'
    )


BENIGN = _key("https://oauth2.googleapis.com/token")
MALICIOUS = _key("http://127.0.0.1:1/token")


def workflow_default(c: Composition) -> None:
    c.down(destroy_volumes=True)
    c.up("materialized")

    c.sql(
        "ALTER SYSTEM SET enable_connection_validation_syntax = true",
        port=6877,
        user="mz_system",
    )
    c.sql(f"CREATE SECRET s AS '{BENIGN}'")

    # Connection A: rewrite the secret to a malicious token_uri. Its
    # dependent-content guard runs first and sees no GCP connection depending on
    # `s` yet, so it proceeds; the durable write is then held at the failpoint.
    alter_result: dict[str, object] = {}
    sent = threading.Event()

    def do_alter() -> None:
        cursor = c.sql_connection().cursor()
        sent.set()
        try:
            cursor.execute(f"ALTER SECRET s AS '{MALICIOUS}'")
            alter_result["ok"] = True
        except Exception as e:
            alter_result["ok"] = False
            alter_result["err"] = str(e)

    alter_thread = threading.Thread(target=do_alter, name="alter-secret")
    alter_thread.start()

    # Let A's guard run and reach the paused write before B runs. Ordering
    # matters: if B committed first, A's guard would see the dependency and
    # reject the malicious value on its own.
    sent.wait(timeout=30)
    time.sleep(3)

    # Connection B: create a GCP connection on `s`. Its guard reads the
    # still-benign contents and commits, establishing the dependency.
    create_ok = False
    create_err = None
    try:
        c.sql(
            "CREATE CONNECTION c TO GCP (SERVICE ACCOUNT KEY = SECRET s) WITH (VALIDATE = false)",
            reuse_connection=False,
        )
        create_ok = True
    except Exception as e:
        create_err = str(e)

    alter_thread.join()

    # The malicious token_uri must be rejected on one of the two paths. On the
    # vulnerable code both succeed, leaving GCP connection `c` bound to a secret
    # whose token_uri points at an attacker-controlled address.
    assert not (alter_result.get("ok") and create_ok), (
        "TOCTOU: ALTER SECRET and CREATE CONNECTION both succeeded, installing a "
        "malicious token_uri into GCP connection `c` with no rejection "
        f"(alter={alter_result}, create_ok={create_ok}, create_err={create_err})"
    )
