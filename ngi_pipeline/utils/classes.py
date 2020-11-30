import collections
import functools

from ngi_pipeline.utils.config import load_yaml_config, locate_ngi_config
from six.moves import zip


class with_ngi_config(object):
    """
    If no parsed config is passed, loads the config from the config_file_path argument.
    If config_file_path is not passed, tries to find it using a list of default locations.
    """
    def __init__(self, f):
        self.f = f
        # The idea is that ths will allow wrapped functions to keep their
        # original names, but it doesn't seem to be working as I expect
        functools.update_wrapper(self, f)

    def __call__(self, *args, **kwargs):
        # Assign positional args to relevant parameters
        # An alternative way to do this is with inspect.getargspec(self.f)
        # instead of self.f.func_code.co_varnames, but it's an additional import
        kwargs.update(dict(list(zip(self.f.__code__.co_varnames, args))))
        if not kwargs.get("config"):
            if not kwargs.get("config_file_path"):
                kwargs["config_file_path"] = locate_ngi_config()
            kwargs["config"] = load_yaml_config(kwargs["config_file_path"])
        if kwargs.get("quiet"):
            kwargs["config"]["quiet"] = True
        if kwargs.get("manual"):
            kwargs["config"]["manual"] = True
        return self.f(**kwargs)


class memoized(object):
    """
    Decorator, caches results of function calls.
    """
    def __init__(self, func):
        self.func   = func
        self.cached = {}
        functools.update_wrapper(self, func)
    def __call__(self, *args):
        if not isinstance(args, collections.Hashable):
            return self.func(*args)
        if args in self.cached:
            return self.cached[args]
        else:
            return_val = self.func(*args)
            self.cached[args] = return_val
            return return_val
    def __repr__(self):
        return self.func.__doc__
    # This ensures that attribute access (e.g. obj.attr)
    # goes through the __call__ function defined above
    def __get__(self, obj, objtype):
        return functools.partial(self.__call__, obj)
