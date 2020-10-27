"""General Python-specific utility functions, in most cases probably skimmed off StackOverflow."""
import collections
import copy

def flatten(nested_list):
    """All I ever need to know about flattening irregular lists of lists I learned from
    http://stackoverflow.com/questions/2158395/flatten-an-irregular-list-of-lists-in-python/2158532#2158532"""
    for elt in nested_list:
        if isinstance(elt, collections.Iterable) and not isinstance(elt, basestring):
            for sub in flatten(elt):
                yield sub
        else:
            yield elt
