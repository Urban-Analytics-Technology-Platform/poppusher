from __future__ import annotations

import os

env = os.getenv("ENV", "dev")
if env.lower() == "dev":
    PROD = False
elif env.lower() == "prod":
    PROD = True
else:
    err = f"Invalid ENV value: {env}, must be 'dev' or 'prod'."
    raise ValueError(err)
