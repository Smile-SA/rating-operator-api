from typing import AnyStr, Dict, List, Union


class ValidationError(Exception):
    """Simple class to handle Rules and Metrics validation error."""

    pass


class Rules:
    """
    Validate a Rules object.

    :content (Dict) A dictionary containing the 'rules' key

    This class is only meant to be instanciated. If nothing explode, your rules is valid !
    """

    def validate_rule_entry(self,
                            metric: AnyStr = None,
                            value: Union[int, float] = None,
                            unit: AnyStr = None) -> Dict:
        """
        Validate a rule entry.

        :metric (AnyStr) A name for the metric
        :value (int or float) A value for the transformation to be applied to the rule
        :unit (AnyStr) The transformation unit (only for metering-operator)

        Return the validated dictionary
        """
        if not isinstance(metric, str) or \
           not isinstance(value, (float, int)):
            raise ValidationError('Wrong parameter for Rule entry')
        return {
            'metric': metric,
            'value': value,
            'unit': unit
        }

    def validate_rule_config(self,
                             label_set: Dict = {},
                             ruleset: List = None) -> Dict:
        """
        Validate a ruleset entry.

        :label_set (Dict) A dictionary containing the labels
        :ruleset (List) A list of dictionary, each containing a rule

        Return the validated ruleset object, as a dictionary
        """
        if not isinstance(label_set, Dict) or \
           not isinstance(ruleset, list):
            raise ValidationError('Wrong parameter type for ruleset or labelSet')
        elif len(ruleset) < 1:
            raise ValidationError('No configuration provided for ruleset config')
        return {
            'labelSet': label_set,
            'ruleset': [
                self.validate_rule_entry(
                    metric=rule.get('metric'),
                    value=rule.get('value'),
                    unit=rule.get('unit')
                )
                for rule in ruleset]
        }

    def validate_rules(self,
                       rules: Dict = None) -> List:
        """
        Validate the rules.

        :rules (Dict) A dict filled with the rules file content

        Return the validated rule file, as a dictionary
        """
        if not isinstance(rules, list):
            raise ValidationError(
                f'Wrong type for rules, expected list got {type(rules)}')
        elif len(rules) < 1:
            raise ValidationError('No configuration provided for rules')
        validated_rules = []
        for rule in rules:
            validated_rules.append(
                self.validate_rule_config(
                    label_set=rule.get('labelSet', {}),
                    ruleset=rule.get('ruleset')
                )
            )
        return validated_rules

    def __init__(self, content: Dict):
        """
        Initialize the Rules validation class.

        :content (Dict) A dict filled with the rules file content
        """
        self.valid_ruleset = self.validate_rules(rules=content.get('rules'))


class Metrics:
    """
    Validate a Metrics object.

    :content (Dict) A dictionary containing the 'metrics' key

    This class is only meant to be instanciated.
    If nothing explode, your metrics object is valid !
    """

    def validate_metric_config(self,
                               presto_column: AnyStr = None,
                               presto_table: AnyStr = None,
                               report_name: AnyStr = None,
                               unit: AnyStr = None) -> Dict:
        """
        Validate a metric configuration object.

        :presto_column (AnyStr) A string representing the column in the presto database
        :presto_table (AnyStr) A string representing the table in the presto database
        :report_name (AnyStr) A string representing the name of the report
        :unit (AnyStr) A string representing the conversion unit

        Return a dictionary of validated metrics
        """
        if not isinstance(presto_column, str) or \
           not isinstance(presto_table, str) or \
           not isinstance(report_name, str) or \
           not isinstance(unit, str):
            raise ValidationError(
                f'Wrong parameter for metric config, got\
                [{presto_column},{presto_table},{report_name},{unit}]')
        return {
            'presto_column': presto_column,
            'presto_table': presto_table,
            'report_name': report_name,
            'unit': unit
        }

    def validate_metric(self, metrics: Dict = None) -> Dict:
        """
        Validate the content of the Metrics file.

        :metrics (Dict) The content of the metrics file

        Return the validated Metrics configuration, as a dictionary
        """
        if not isinstance(metrics, Dict):
            raise ValidationError(
                f'Wrong type for metrics, expected dict got {type(metrics)}')
        elif len(metrics) < 1:
            raise ValidationError('No configuration provided for metrics')
        metrics_config = {}
        for name, metric in metrics.items():
            metrics_config.update({
                name: self.validate_metric_config(
                    presto_column=metric.get('presto_column'),
                    presto_table=metric.get('presto_table'),
                    report_name=metric.get('report_name'),
                    unit=metric.get('unit')
                )
            })
        return metrics_config

    def __init__(self, content: Dict):
        """
        Initialize the Metrics validation class.

        :content (Dict) A dict filled with the metrics file content
        """
        self.valid_metrics = self.validate_metric(metrics=content.get('metrics'))


def validate_request_content(content: Dict):
    """
    Generate Metrics and Rules objects from the received configuration.

    If it doesn't crash, it works !

    :content (Dict) A dictionary containing both Rules and Metrics objects
    """
    try:
        Metrics(content)
        Rules(content)
    except ValidationError as exc:
        raise exc
