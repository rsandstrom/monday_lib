"""
create a singlton decorator for any class,
useage: when defining a new class

@singleton
class foo(object)
    def...

then in your code:

c1 = foo.Instance() gets the database class.
"""


class Singleton:
    def __init__(self, klass):
        self.klass = klass
        self.instance = None

    def __call__(self, *args,**kwds):
        if self.instance is None:
            self.instance = self.klass(*args, **kwds)
        return self.instance


class SingletonV1:

    def __init__(self, cls):
        self._cls = cls

    def Instance(self):
        try:
            return self._instance
        except AttributeError:
            self._instance = self._cls()
            return self._instance

    def __call__(self):
        raise TypeError('Singletons must be accessed through `Instance()`.')

    def __instancecheck__(self, inst):
        return isinstance(inst, self._cls)
