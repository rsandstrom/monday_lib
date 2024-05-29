"""
CachedResources class that maintains cached objects, can be used by all webhooks
"""
from datetime import datetime
from datetime import timedelta
from typing import TypeVar

T = TypeVar("T")

EXPIRE_SECONDS_DEFAULT = 14400

class CacheItem:
    def __init__(self, obj, expire_seconds=EXPIRE_SECONDS_DEFAULT):
        self.expire_time: datetime = datetime.now() + timedelta(seconds=expire_seconds)
        self.obj = obj


class CachedResource:
    """
    CachedResource allows for the caching of any object using any set of fields for a cache key name.
    adding an item to the cache is as simple as calling update_cache(new_object, name1, name2, name3....)
    The names are used to build a unique key to locate cached objects and are needed to retrieve the objects
    You can enable or disable the cache as desired, useful when calling a parent class where the cache is enabled
    and pass the number of seconds a cached item will last.
    """

    def __init__(self, enable_cache=True, expire_seconds=EXPIRE_SECONDS_DEFAULT):
        self.cached_resource = {}
        self.enabled = enable_cache
        self.expire_seconds = expire_seconds

    @staticmethod
    def get_key(*args) -> str:
        key = ''
        for arg in args:
            key += ':' + str(arg)
        return key

    def disable_cache(self):
        self.cached_resource = {}
        self.enabled = False

    def enable_cache(self):
        self.enabled = True

    def clear_cache(self):
        self.cached_resource = {}

    def reset(self):
        self.cached_resource = {}

    def is_available(self, key):
        if not self.enabled:
            return False
        return key in self.cached_resource

    def get_cache_item(self, *args) -> object:
        """
        if the cache has expired, return none and reset the cache for this item
        """
        key = self.get_key(*args)

        if self.enabled:
            cached_item: CacheItem = self.cached_resource.get(key)
            if cached_item is None:
                return None
            if datetime.now() > cached_item.expire_time:
                self.cached_resource.pop(key)
                return None

            return cached_item.obj
        else:
            return None

    @staticmethod
    def cache_key(*args):
        return CachedResource.get_key(*args)

    def update_cache_item(self, obj=None, key=None, expire_seconds=None):
        assert obj is not None, "Object is required to cache"
        assert key is not None, "Key is required to cache"
        if expire_seconds is None:
            expire_seconds = EXPIRE_SECONDS_DEFAULT
        if self.enabled:
            self.cached_resource[key] = CacheItem(obj=obj, expire_seconds=expire_seconds)

    def update_cache(self, obj, *args):
        if self.enabled:
            key = self.get_key(*args)
            self.cached_resource[key] = CacheItem(obj=obj, expire_seconds=self.expire_seconds)

    def remove(self, *args):
        key = self.get_key(*args)
        if self.enabled and key in self.cached_resource:
            self.cached_resource.pop(key)

    @property
    def size(self):
        return len(self.cached_resource)


class CachedResources:
    def __init__(self, enable_cache=False, expire_seconds=14400):
        self.cached_resource = {}
        self.enabled = enable_cache
        self.expire_seconds = expire_seconds
        self.expire_time: datetime = datetime.now() + timedelta(seconds=self.expire_seconds)

    def get(self, idx, name):
        """
        if the cache has expired, return none and reset the cache for this item
        """
        if not self.enabled or datetime.now() > self.expire_time:
            self.expire_time: datetime = datetime.now() + timedelta(seconds=self.expire_seconds)
            if self.enabled and name in self.cached_resource:
                self.cached_resource.pop(name)
            return None

        item = self._get_item(idx, name)
        ci = self.cached_resource.get(item)
        return ci

    def update(self, idx, name, obj):
        if self.enabled:
            item = self._get_item(idx, name)
            ci = obj
            self.cached_resource[item] = ci

    def is_available(self, idx, name):
        if not self.enabled:
            return None
        item = self._get_item(idx, name)
        return item in self.cached_resource

    def remove(self, idx, name):
        item = self._get_item(idx, name)
        if self.enabled and item in self.cached_resource:
            self.cached_resource.pop(item)

    def reset(self):
        if self.enabled:
            self.cached_resource = {}

    @property
    def size(self):
        return len(self.cached_resource)

    def _get_item(self, idx, name):
        return f"{str(idx)}{name}"
