"""Path management for NiceGUI apps.

Ensures proper Python path resolution for all imports.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Set up paths
_apps_root = Path(__file__).resolve().parent.parent  # apps/nicegui/
_project_root = _apps_root.parent.parent  # project root

if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
if str(_project_root / "src") not in sys.path:
    sys.path.insert(0, str(_project_root / "src"))
if str(_apps_root) not in sys.path:
    sys.path.insert(0, str(_apps_root))

# Export for use in other modules
apps_root = _apps_root
project_root = _project_root
