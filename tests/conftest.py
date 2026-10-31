import os
import sys
import tempfile

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
# storage_node.app opens its SQLite file at import time
os.environ.setdefault("DATA_DIR", tempfile.mkdtemp(prefix="erms-test-"))
