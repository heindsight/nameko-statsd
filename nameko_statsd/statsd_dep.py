from enum import Enum
from functools import wraps, partial
from mock import MagicMock
from warnings import warn

from nameko.extensions import DependencyProvider
from statsd import StatsClient, TCPStatsClient


class Protocols(Enum):
    tcp = 'tcp'
    udp = 'udp'


class LazyClient(object):

    """Provide an interface to `StatsClient` with a lazy client creation.
    """

    def __init__(self, **config):
        self.config = config
        self.enabled = config.pop('enabled')
        self._client = None

        protocol = self.config.pop('protocol', Protocols.udp.name)

        try:
            self.protocol = getattr(Protocols, protocol.lower())
        except AttributeError:
            raise ValueError(
                'Invalid protocol: {}'.format(protocol)
            )

    @property
    def client(self):
        if self._client is None:
            if self.protocol is Protocols.udp:
                self._client = StatsClient(**self.config)
            else:   # self.protocol is Protocols.tcp
                self._client = TCPStatsClient(**self.config)

        return self._client

    def __getattr__(self, name):
        if name in ('incr', 'decr', 'gauge', 'set', 'timing'):
            return partial(self._passthrough, name)
        else:
            message = "'{cls}' object has no attribute '{attr}'".format(
                cls=self.__class__.__name__, attr=name
            )
            raise AttributeError(message)

    def _passthrough(self, name, *args, **kwargs):
        if self.enabled:
            return getattr(self.client, name)(*args, **kwargs)

    def timer(self, *args, **kwargs):
        if self.enabled:
            return self.client.timer(*args, **kwargs)
        else:
            return MagicMock()


class StatsD(DependencyProvider):

    def __init__(self, key, name=None, *args, **kwargs):
        """
        Args:
            key (str): The key under the `STATSD` config dictionary.
            name (str): The name associated to the instance.
        """
        self._key = key
        self._auto_timers = {}

        if name is not None:
            warn(
                "The `name` argument to `StatsD` is no longer needed and has"
                " been deprecated.",
                DeprecationWarning
            )

        super(StatsD, self).__init__(*args, **kwargs)

    def worker_setup(self, worker_ctx):
        super(StatsD, self).worker_setup(worker_ctx)

        if not self.auto_timer:
            return

        entrypoint_name = worker_ctx.entrypoint.method_name

        dependency = self.get_dependency(worker_ctx)
        timer = dependency.timer(entrypoint_name)
        self._auto_timers[worker_ctx] = timer
        timer.start()

    def get_dependency(self, worker_ctx):
        return LazyClient(**self.config)

    def worker_teardown(self, worker_ctx):
        timer = self._auto_timers.pop(worker_ctx, None)

        if timer is not None:
            timer.stop()

    def setup(self):
        self.config = self.get_config()
        self.auto_timer = self.config.pop('auto_timer', False)
        return super(StatsD, self).setup()

    def get_config(self):
        return self.container.config['STATSD'][self._key]

    def timer(self, *targs, **tkwargs):

        def decorator(method):

            @wraps(method)
            def wrapper(svc, *args, **kwargs):
                dependency = getattr(svc, self.attr_name)

                if dependency.enabled:
                    with dependency.client.timer(*targs, **tkwargs):
                        res = method(svc, *args, **kwargs)
                else:
                    res = method(svc, *args, **kwargs)

                return res

            return wrapper

        return decorator
