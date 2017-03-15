'''
This class allows you to PDB inside a multiprocessing environment
To use:

    from FactoryTx.sdpdb import sdpdb
    sdpdb.set_trace()

'''




import pdb
import sys
class sdpdb(pdb.Pdb):
    """A Pdb subclass that may be used
    from a forked multiprocessing child

    """
    def interaction(self, *args, **kwargs):
        _stdin = sys.stdin
        try:
            sys.stdin = file('/dev/stdin')
            pdb.Pdb.interaction(self, *args, **kwargs)
        finally:
            sys.stdin = _stdin
