from __future__ import annotations

import argparse
import json

from nse_momentum_lab.services.kite.auth import get_kite_auth


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh the local Kite instrument cache")
    parser.add_argument("--exchange", default="NSE", help="Exchange code to refresh")
    args = parser.parse_args()

    auth = get_kite_auth()
    count = auth.refresh_instruments(args.exchange)
    payload = {
        "exchange": args.exchange.strip().upper(),
        "instrument_cache_path": str(auth.get_instrument_master_path(args.exchange)),
        "refreshed": count,
    }
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
