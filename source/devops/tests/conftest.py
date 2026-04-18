"""
Shared test configuration and helpers.
"""
import sys
import os
import importlib.util

# Add project root to path so tests can import from source/
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, os.path.join(PROJECT_ROOT, "source"))


def load_module_from_path(module_name, file_path):
    """Load a Python module directly from file path (avoids needing __init__.py)."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
