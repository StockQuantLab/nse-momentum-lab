"""NiceGUI Dashboard CLI entry point.

This wrapper solves the import issues for the CLI entry point.
"""

from __future__ import annotations

import sys
from pathlib import Path

# Add paths before any imports
_project_root = Path(__file__).resolve().parent.parent.parent.parent
if str(_project_root) not in sys.path:
    sys.path.insert(0, str(_project_root))
if str(_project_root / "src") not in sys.path:
    sys.path.insert(0, str(_project_root / "src"))

# Now we can import from apps
from apps.nicegui.main import main

if __name__ == "__main__":
    main()
