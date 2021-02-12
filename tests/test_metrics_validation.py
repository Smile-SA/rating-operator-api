import unittest

from rating_operator.api.schema import Metrics, ValidationError


class TestMetricsValidation(unittest.TestCase):

    def test_metrics_validation_real_case(self):
        content = {
            'metrics': {
                'request_cpu': {
                    'presto_column': 'pod_request_cpu_core_seconds',
                    'presto_table': 'report_metering_pod_cpu_request_hourly',
                    'report_name': 'pod-cpu-request-hourly',
                    'unit': 'core-seconds'
                },
                'request_memory': {
                    'presto_column': 'pod_request_memory_byte_seconds',
                    'presto_table': 'report_metering_pod_memory_request_hourly',
                    'report_name': 'pod-memory-request-hourly',
                    'unit': 'byte-seconds'
                },
                'usage_cpu': {
                    'presto_column': 'pod_usage_cpu_core_seconds',
                    'presto_table': 'report_metering_pod_cpu_usage_hourly',
                    'report_name': 'pod-cpu-usage-hourly',
                    'unit': 'core-seconds'
                },
                'usage_memory': {
                    'presto_column': 'pod_usage_memory_byte_seconds',
                    'presto_table': 'report_metering_pod_memory_usage_hourly',
                    'report_name': 'pod-memory-usage-hourly',
                    'unit': 'byte-seconds'
                }
            }
        }
        Metrics(content)
        self.assertTrue(True)

    def test_metrics_validation_empty(self):
        content = {}
        with self.assertRaisesRegex(ValidationError,
                                    'Wrong type for metrics'):
            Metrics(content)

    def test_metrics_validation_incomplete_sub(self):
        content = {
            'metrics': {
                'thisistesting': {}
            }
        }
        with self.assertRaisesRegex(ValidationError,
                                    'Wrong parameter'):
            Metrics(content)

    def test_metrics_validation_missing_key(self):
        content = {
            'metrics': {
                'thisistesting': {
                    'presto_column': 'pod_request_cpu_core_seconds',
                    'presto_table': 'report_metering_pod_cpu_request_hourly',
                    'report_name': 'pod-cpu-request-hourly'
                }
            }
        }
        # Need to clarify this error
        with self.assertRaisesRegex(ValidationError,
                                    'Wrong parameter'):
            Metrics(content)

    def test_metrics_validation_additional(self):
        content = {
            'metrics': {
                'thy': {
                    'presto_column': 'pod_request_memory_seconds',
                    'presto_table': 'report_metering_pod_request_memory_hourly',
                    'report_name': 'pod-memory-request-hourly',
                    'unit': 'GiB-hours',
                    'pokemon': 'ivysaur'
                }
            }
        }
        Metrics(content)
        self.assertTrue(True)

    def test_metrics_validation_wrong_type(self):
        content = {
            'metrics': {
                'etaet': {
                    'presto_column': 42,
                    'presto_table': -1,
                    'report_name': 0.1,
                    'unit': b'fdp'
                }
            }
        }
        with self.assertRaisesRegex(ValidationError,
                                    'Wrong parameter'):
            Metrics(content)
