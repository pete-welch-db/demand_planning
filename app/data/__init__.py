"""
Data access layer.

Design rules (demo pattern):
- Views call ONLY functions in this package.
- All DB calls must be wrapped to allow graceful fallback to mock data.
- No env var reads here (config-only).
"""

