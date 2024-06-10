"""General Python-specific utility functions, in most cases probably skimmed off StackOverflow."""
import collections
import six

def flatten(nested_list):
    """All I ever need to know about flattening irregular lists of lists I learned from
    http://stackoverflow.com/questions/2158395/flatten-an-irregular-list-of-lists-in-python/2158532#2158532"""
    for elt in nested_list:
        if isinstance(elt, collections.abc.Iterable) and not isinstance(elt, six.string_types):
            for sub in flatten(elt):
                yield sub
        else:
            yield elt
