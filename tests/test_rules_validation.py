import unittest

from rating_operator.api.schema import Rules, ValidationError


class TestRulesValidation(unittest.TestCase):

    def test_rules_validation_real_case(self):
        content = {
            'rules': [
                {
                    'name': 'rules_small',
                    'labelSet': {
                        'instance_type': 'small'
                    },
                    'ruleset': [
                        {
                            'metric': 'request_cpu',
                            'value': 0.005,
                            'unit': 'core-hours'
                        },
                        {
                            'metric': 'usage_cpu',
                            'value': 0.015,
                            'unit': 'core-hours'
                        },
                        {
                            'metric': 'request_memory',
                            'value': 0.004,
                            'unit': 'GiB-hours'
                        },
                        {
                            'metric': 'usage_memory',
                            'value': 0.012,
                            'unit': 'GiB-hours'
                        }
                    ]
                },
                {
                    'name': 'rules_medium',
                    'labelSet': {
                        'instance_type': 'medium'
                    },
                    'ruleset': [
                        {
                            'metric': 'request_cpu',
                            'value': 0.00075
                        },
                        {
                            'metric': 'usage_cpu',
                            'value': 0.0015
                        },
                        {
                            'metric': 'request_memory',
                            'value': 0.0007
                        },
                        {
                            'metric': 'usage_memory',
                            'value': 0.0014
                        }
                    ]
                },
                {
                    'name': 'rules_large',
                    'labelSet': {
                        'instance_type': 'large'
                    },
                    'ruleset': [
                        {
                            'metric': 'request_cpu',
                            'value': 0.0008
                        },
                        {
                            'metric': 'usage_cpu',
                            'value': 0.0025
                        },
                        {
                            'metric': 'request_memory',
                            'value': 0.0007
                        },
                        {
                            'metric': 'usage_memory',
                            'value': 0.002
                        }
                    ]
                },
                {
                    'name': 'rules_default',
                    'ruleset': [
                        {
                            'metric': 'request_cpu',
                            'value': 0.0008
                        },
                        {
                            'metric': 'usage_cpu',
                            'value': 0.0025
                        },
                        {
                            'metric': 'request_memory',
                            'value': 0.0007
                        },
                        {
                            'metric': 'usage_memory',
                            'value': 0.002
                        }
                    ]
                }
            ]
        }
        Rules(content)
        self.assertTrue(True)

    def test_rules_validation_empty(self):
        content = {
            'rules': []
        }

        with self.assertRaisesRegex(ValidationError,
                                    'No configuration provided'):
            Rules(content)

    def test_rules_validation_incomplete_sub(self):
        content = {
            'rules': [{}]
        }
        with self.assertRaisesRegex(ValidationError,
                                    'Wrong parameter'):
            Rules(content)

    def test_rules_validation_additional_key_top(self):
        content = {
            'rules': [
                {
                    'name': 'rules_default',
                    'node_type': 'default',
                    'ruleset': [{
                        'metric': 'blabla',
                        'unit': 'core-hours',
                        'value': 1
                    }],
                    'holy': 'grail'
                }
            ]
        }
        Rules(content)
        self.assertTrue(True)

    def test_rules_validation_additional_key_sub(self):
        content = {
            'rules': [
                {
                    'name': 'rules_default',
                    'node_type': 'default',
                    'ruleset': [{
                        'metric': 'blabla',
                        'unit': 'core-hours',
                        'value': 1,
                        'holy': 'grail'
                    }],
                }
            ]
        }
        Rules(content)
        self.assertTrue(True)

    def test_rules_validation_missing_key(self):
        content = {
            'rules': [
                {
                    'name': 'rules_default',
                    'node_type': 'default',
                    'ruleset': [{
                        'metric': 'blabla',
                        'unit': 'core-hours',
                    }],
                }
            ]
        }
        with self.assertRaisesRegex(ValidationError,
                                    'Wrong parameter'):
            Rules(content)

    def test_rules_validation_no_unit(self):
        content = {
            'rules': [
                {
                    'name': 'rules_default',
                    'ruleset': [{
                        'metric': 'blabla',
                        'value': 1,
                    }],
                }
            ]
        }
        Rules(content)
        self.assertTrue(True)
        