from guillotina_amqp.utils import metric_measure


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
