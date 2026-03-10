"""
conftest.py
-----------
Shared pytest fixtures.
Fixtures compartidos para pytest.
"""
import sys
import os

# Ensure the project root is in sys.path so imports resolve correctly.
# Asegura que la raíz del proyecto esté en sys.path para que los imports funcionen.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
