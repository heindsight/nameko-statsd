import warnings

from mock import call, Mock, patch
import pytest
from nameko.testing.services import dummy, entrypoint_hook

from nameko_statsd.statsd_dep import StatsD
from nameko_statsd.bases import ServiceBase


class TestStatsD(object):

    """Test the dependency provider mechanics. """

    @pytest.fixture
    def lazy_client_cls(self):
        with patch('nameko_statsd.statsd_dep.LazyClient') as lc:
            yield lc

    def test_get_dependency(self, lazy_client_cls, stats_config):
        statsd = StatsD('test')
        statsd.container = Mock()
        statsd.container.config = stats_config
        statsd.setup()

        worker_ctx = Mock()

        dep = statsd.get_dependency(worker_ctx)

        assert lazy_client_cls.call_args_list == [
            call(
                host='statsd.host',
                port=1234,
                prefix='statsd.prefix',
                maxudpsize=1024,
                enabled=True,
            )
        ]
        assert dep == lazy_client_cls.return_value

    def test_get_dependency_tcp(self, lazy_client_cls, stats_config):
        statsd = StatsD('test-tcp')
        statsd.container = Mock()
        statsd.container.config = stats_config
        statsd.setup()

        worker_ctx = Mock()

        dep = statsd.get_dependency(worker_ctx)

        assert lazy_client_cls.call_args_list == [
            call(
                host='tcp.statsd.host',
                port=4321,
                prefix='tcp.statsd.prefix',
                timeout=5,
                enabled=True,
                protocol='tcp'
            )
        ]
        assert dep == lazy_client_cls.return_value

    def test_name_argument_deprecated(self, recwarn):
        warnings.simplefilter("always")
        StatsD('test', name='statsd')

        assert len(recwarn) == 1
        warn = recwarn.pop()
        assert str(warn.message) == (
            "The `name` argument to `StatsD` is no longer needed and has been"
            " deprecated."
        )


class DummyService(object):

    """Fake Service to test the `StatsD.timer` decorator. """

    name = 'dummy_service'

    statsd = StatsD('test')

    @dummy
    @statsd.timer('nice-stat', rate=3)
    def method(self, *args, **kwargs):
        sentinel = Mock()
        sentinel(*args, **kwargs)
        return sentinel


class DummyServiceDisabled(object):

    """Fake Service to test the `StatsD.timer` decorator when disabled. """

    name = 'dummy_service'

    statsd = StatsD('test-disabled')

    @dummy
    @statsd.timer('disabled-nice-stat')
    def method(self, *args, **kwargs):
        sentinel = Mock()
        sentinel(*args, **kwargs)
        return sentinel


class DummyServiceTCP(object):

    """Fake Service to test the `StatsD.timer` decorator. """

    name = 'dummy_service'

    statsd = StatsD('test-tcp')

    @dummy
    @statsd.timer('nice-tcp-stat', rate=3)
    def method(self, *args, **kwargs):
        sentinel = Mock()
        sentinel(*args, **kwargs)
        return sentinel


class DummyServiceDisabledTCP(object):

    """Fake Service to test the `StatsD.timer` decorator when disabled. """

    name = 'dummy_service'

    statsd = StatsD('test-tcp-disabled')

    @dummy
    @statsd.timer('disabled-nice-tcp-stat')
    def method(self, *args, **kwargs):
        sentinel = Mock()
        sentinel(*args, **kwargs)
        return sentinel


class DummyServiceManual(object):

    """
    Fake Service to test the `StatsD.timer` decorator, using the deprecated
    `name` argument
    """

    name = 'dummy_service'

    statsd = StatsD('test', name='statsd')

    @dummy
    @statsd.timer('nice-stat', rate=3)
    def method(self, *args, **kwargs):
        sentinel = Mock()
        sentinel(*args, **kwargs)
        return sentinel


class DummyServiceMeta(ServiceBase):

    """
    Fake Service to test the `StatsD.timer` decorator with the deprecated
    metaclass
    """

    name = 'dummy_service'

    statsd = StatsD('test')

    @dummy
    @statsd.timer('nice-stat', rate=3)
    def method(self, *args, **kwargs):
        sentinel = Mock()
        sentinel(*args, **kwargs)
        return sentinel


class TestTimer(object):

    """Test the `StatsD.timer` decorator. """

    @pytest.fixture
    def config(self, stats_config):
        return stats_config.copy()

    @pytest.fixture(autouse=True)
    def stats_client_cls(self):
        with patch('nameko_statsd.statsd_dep.StatsClient') as sc:
            yield sc

    @pytest.fixture(autouse=True)
    def stats_client_cls_tcp(self):
        with patch('nameko_statsd.statsd_dep.TCPStatsClient') as sc:
            yield sc

    def test_enabled(self, container_factory, config, stats_client_cls):
        container = container_factory(DummyService, config)
        container.start()

        with entrypoint_hook(container, 'method') as method:
            sentinel = method(3, 1, 4, name='pi')

        client = stats_client_cls.return_value

        assert client.timer.call_args_list == [call('nice-stat', rate=3)]
        assert sentinel.call_args_list == [call(3, 1, 4, name='pi')]

    def test_enabled_with_metaclass(
        self, container_factory, config, stats_client_cls
    ):
        container = container_factory(DummyServiceMeta, config)
        container.start()

        with entrypoint_hook(container, 'method') as method:
            sentinel = method(3, 1, 4, name='pi')

        client = stats_client_cls.return_value

        assert client.timer.call_args_list == [call('nice-stat', rate=3)]
        assert sentinel.call_args_list == [call(3, 1, 4, name='pi')]

    def test_enabled_name_argument(
        self, container_factory, config, stats_client_cls
    ):
        container = container_factory(DummyServiceManual, config)
        container.start()

        with entrypoint_hook(container, 'method') as method:
            sentinel = method(3, 1, 4, name='pi')

        client = stats_client_cls.return_value

        assert client.timer.call_args_list == [call('nice-stat', rate=3)]
        assert sentinel.call_args_list == [call(3, 1, 4, name='pi')]

    def test_disabled(self, container_factory, config, stats_client_cls):
        container = container_factory(DummyServiceDisabled, config)
        container.start()

        with entrypoint_hook(container, 'method') as method:
            sentinel = method(3, 1, 4, name='pi')

        client = stats_client_cls.return_value

        assert stats_client_cls.call_args_list == []
        assert client.timer.call_args_list == []
        assert sentinel.call_args_list == [call(3, 1, 4, name='pi')]

    def test_enabled_tcp(
        self, container_factory, config, stats_client_cls_tcp
    ):
        container = container_factory(DummyServiceTCP, config)
        container.start()

        with entrypoint_hook(container, 'method') as method:
            sentinel = method(3, 1, 4, name='pi')

        client = stats_client_cls_tcp.return_value

        assert client.timer.call_args_list == [call('nice-tcp-stat', rate=3)]
        assert sentinel.call_args_list == [call(3, 1, 4, name='pi')]

    def test_disabled_tcp(
        self, container_factory, config, stats_client_cls_tcp
    ):
        container = container_factory(DummyServiceDisabledTCP, config)
        container.start()

        with entrypoint_hook(container, 'method') as method:
            sentinel = method(3, 1, 4, name='pi')

        client = stats_client_cls_tcp.return_value

        assert stats_client_cls_tcp.call_args_list == []
        assert client.timer.call_args_list == []
        assert sentinel.call_args_list == [call(3, 1, 4, name='pi')]


class DummyServiceAutoTimer(ServiceBase):

    """Fake Service to test automatic entrypoint timing"""

    name = 'dummy_auto_timing'

    statsd = StatsD('test')

    @dummy
    def method(self):
        pass


class TestAutoTimer(object):

    @pytest.fixture
    def stats_config(self, stats_config):
        stats_config['STATSD']['test']['auto_timer'] = True
        return stats_config

    @pytest.fixture
    def lazy_client_cls(self):
        with patch('nameko_statsd.statsd_dep.LazyClient') as lc:
            yield lc

    def test_get_dependency(self, lazy_client_cls, stats_config):
        statsd = StatsD('test')
        statsd.container = Mock()
        statsd.container.config = stats_config
        statsd.setup()

        worker_ctx = Mock()

        dep = statsd.get_dependency(worker_ctx)

        assert lazy_client_cls.call_args_list == [
            call(
                host='statsd.host',
                port=1234,
                prefix='statsd.prefix',
                maxudpsize=1024,
                enabled=True,
            )
        ]
        assert dep == lazy_client_cls.return_value

    @pytest.fixture
    def stats_client_cls(self):
        with patch('nameko_statsd.statsd_dep.StatsClient') as sc:
            yield sc

    @pytest.fixture
    def dummy_service(self, container_factory, stats_config):
        container = container_factory(DummyServiceAutoTimer, stats_config)
        container.start()
        return container

    def test_enabled_with_metaclass(
        self, dummy_service, stats_client_cls
    ):
        with entrypoint_hook(dummy_service, 'method') as method:
            method()

        client = stats_client_cls.return_value

        assert client.timer.mock_calls == [
            call('method'),
            call('method').start(),
            call('method').stop(),
        ]
