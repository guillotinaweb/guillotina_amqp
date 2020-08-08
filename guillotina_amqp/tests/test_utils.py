from guillotina.tests.utils import make_mocked_request
from guillotina_amqp.utils import make_request
from guillotina_amqp.utils import metric_measure
from guillotina_amqp.utils import serialize_request

import pytest


class MockPrometheusMetric:
    def __init__(self, labels=None):
        self.labels_called = False
        self.observe_called = False
        self._values = []
        self._labels = {label: None for label in labels} if labels else {}

    def labels(self, **labels):
        self.labels_called = True
        for label, value in labels.items():
            if label not in self._labels:
                raise Exception()
            self._labels[label] = value
        return self

    def observe(self, value):
        self.observe_called = True
        self._values.append(value)


def test_metric_measure():
    # Measure with None metric just returns
    metric_measure(None, "foo", "bar")

    # Measure fills labels and observe
    histogram = MockPrometheusMetric(["label1", "label2"])
    metric_measure(histogram, 20, {"label1": "foo", "label2": "bar"})
    assert histogram.labels_called
    assert histogram.observe_called
    assert histogram._labels["label1"] == "foo"
    assert histogram._labels["label2"] == "bar"
    assert histogram._values == [20]


@pytest.mark.asyncio
async def test_serialize_request():
    request = make_mocked_request(
        "POST", "http://foobar.com", {"X-Header": "1"}, b"param1=yes,param2=no"
    )
    request.annotations = {"foo": "bar"}
    serialized = serialize_request(request)
    assert serialized["url"] == request.url
    assert serialized["method"] == request.method
    assert serialized["headers"] == dict(request.headers)
    assert serialized["annotations"] == request.annotations


@pytest.mark.asyncio
async def test_make_request():
    serialized = {
        "annotations": {"_corr_id": "foo"},
        "headers": {"Host": "localhost", "X-Header": "1"},
        "method": "POST",
        "url": "http://localhost/http://foobar.com?param1=yes,param2=no",
    }
    base_request = make_mocked_request("GET", "http://bar.ba", {"A": "2"}, b"caca=tua")
    request = make_request(base_request, serialized)
    assert serialized["url"] == request.url
    assert serialized["method"] == request.method
    assert serialized["headers"] == dict(request.headers)
    assert serialized["annotations"] == request.annotations
