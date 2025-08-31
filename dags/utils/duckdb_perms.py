from __future__ import annotations

import os
import stat


def ensure_world_writable(path: str) -> None:
    """Relax permissions on the DuckDB file so other containers can write.

    - Sets mode 0o666 on the file if it exists, else no-op.
    - Best-effort only; ignores any errors (e.g., file missing).
    """
    try:
        if not path:
            return
        # If the file does not exist yet, nothing to do
        if not os.path.exists(path):
            return
        # Compute new mode: rw for user/group/others
        new_mode = stat.S_IRUSR | stat.S_IWUSR | stat.S_IRGRP | stat.S_IWGRP | stat.S_IROTH | stat.S_IWOTH
        os.chmod(path, new_mode)
    except Exception:
        # Silent best-effort; Airflow owns the file so typically this succeeds
        pass

