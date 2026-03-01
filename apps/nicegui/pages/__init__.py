"""Pages package for NiceGUI dashboard."""

from __future__ import annotations

import sys
from pathlib import Path

_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))
if str(_root / "src") not in sys.path:
    sys.path.insert(0, str(_root / "src"))

# Make sure apps.nicegui.pages is on path for relative imports
apps_nicegui = Path(__file__).resolve().parent.parent
if str(apps_nicegui) not in sys.path:
    sys.path.insert(0, str(apps_nicegui))
