
from ._version import get_versions
__version__ = get_versions()['version']
del get_versions
from .model import UNSPECIFIED_PARENT, UNTYPED_COUPLE, UNTYPED_PARENT, BIOLOGICAL_PARENT
