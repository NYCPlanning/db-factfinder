import sys

if sys.version_info.minor <= 7:
    from .multi import Pool
else:
    from multiprocessing import Pool