"""
thriftpy compatibility shim - installed as thriftpy/__init__.py
Aliases thriftpy2 submodules into the thriftpy namespace so that
streamparse's `import thriftpy` works without the broken C extension.
"""
import sys
import thriftpy2
import thriftpy2.transport
import thriftpy2.protocol

# Register thriftpy2 modules under the thriftpy namespace
sys.modules['thriftpy'] = thriftpy2
sys.modules['thriftpy.transport'] = thriftpy2.transport
sys.modules['thriftpy.protocol'] = thriftpy2.protocol

# Try to register optional submodules
for _sub in ('server', 'rpc', 'hook', 'utils', 'thrift'):
    try:
        import importlib
        _mod = importlib.import_module(f'thriftpy2.{_sub}')
        sys.modules[f'thriftpy.{_sub}'] = _mod
    except ImportError:
        pass

# Re-export everything from thriftpy2 at the top level
from thriftpy2 import *
