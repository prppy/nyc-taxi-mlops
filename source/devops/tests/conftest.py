"""
Shared test configuration and helpers.
"""
import sys
import os
import importlib.util

# Tests live at source/devops/tests/ — walk up to get source/ and project root
TESTS_DIR = os.path.dirname(os.path.abspath(__file__))   # source/devops/tests/
SOURCE_ROOT = os.path.dirname(os.path.dirname(TESTS_DIR))  # source/
PROJECT_ROOT = os.path.dirname(SOURCE_ROOT)                 # nyc-taxi-mlops/

sys.path.insert(0, PROJECT_ROOT)
sys.path.insert(0, SOURCE_ROOT)  # enables: from mlops.xxx, from utils.xxx imports


def load_module_from_path(module_name, file_path):
    """Load a Python module directly from file path (avoids needing __init__.py)."""
    spec = importlib.util.spec_from_file_location(module_name, file_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module
