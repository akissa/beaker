# -*- coding: utf-8 -*-
# vim: ai ts=4 sts=4 et sw=4
"""
uwsgi beaker module
"""
from beaker.crypto.util import sha1
from beaker._compat import PY2, pickle
from beaker.util import verify_directory
from beaker.synchronization import file_synchronizer
from beaker.container import NamespaceManager, Container
from beaker.exceptions import InvalidCacheBackendError, MissingCacheParameter


uwsgi = None
MAX_KEY_LENGTH = 250


class UwsgiNamespaceManager(NamespaceManager):
    """Uwsgi namespace manager"""

    @classmethod
    def _init_dependencies(cls):
        """Initialize uwsgi module"""
        global uwsgi

        if uwsgi is not None:
            return
        try:
            import uwsgi
        except ImportError:
            raise InvalidCacheBackendError(
                "This backend can only be used when running under uWSGI")

    def __init__(self, namespace, **kwds):
        """Create a namespace manager for use with uwsgi cache
        """
        NamespaceManager.__init__(self, namespace)
        url = kwds.get('url')
        lock_dir = kwds.get('lock_dir')
        data_dir = kwds.get('data_dir')
        if not url:
            raise MissingCacheParameter("url is required")

        self.lock_dir = None

        if lock_dir:
            self.lock_dir = lock_dir
        elif data_dir:
            self.lock_dir = data_dir + "/container_ucd_lock"

        if self.lock_dir:
            verify_directory(self.lock_dir)

        self._cache = uwsgi
        self._cache_name = url

    def get_creation_lock(self, key):
        return file_synchronizer(
            identifier="uwsgicontainer/funclock/%s/%s" %
            (self.namespace, key), lock_dir=self.lock_dir)

    def _format_key(self, key):
        """Generate key name"""
        if not isinstance(key, str):
            key = key.decode('ascii')
        formated_key = (self.namespace + '_' + key).replace(' ', '\302\267')
        if len(formated_key) > MAX_KEY_LENGTH:
            if not PY2:
                formated_key = formated_key.encode('utf-8')
            formated_key = sha1(formated_key).hexdigest()
        return formated_key

    def __getitem__(self, key):
        item = self._cache.cache_get(self._format_key(key), self._cache_name)
        return pickle.loads(item)

    def __contains__(self, key):
        return self._cache.cache_exists(
            self._format_key(key),
            self._cache_name
        )

    def has_key(self, key):
        return self._cache.cache_exists(
            self._format_key(key),
            self._cache_name
        )

    def set_value(self, key, value, expiretime=0):
        if key in self:
            self._cache.cache_update(
                self._format_key(key),
                pickle.dumps(value),
                expiretime,
                self._cache_name
            )
        else:
            self._cache.cache_set(
                self._format_key(key),
                pickle.dumps(value),
                expiretime,
                self._cache_name
            )

    def __setitem__(self, key, value):
        self.set_value(key, value)

    def __delitem__(self, key):
        self._cache.cache_del(
            self._format_key(key),
            self._cache_name
        )

    def do_remove(self):
        self._cache.cache_clear(self._cache_name)

    def keys(self):
        raise NotImplementedError(
            "uWSGI cache does not "
            "support iteration of all cache keys")


class UwsgiContainer(Container):
    """Uwsgi container"""
    namespace_class = UwsgiNamespaceManager
