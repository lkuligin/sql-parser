from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Union

from utils import is_integer


_ALLOWED_JOIN_OPERATORS_WHERE = ['AND', 'OR']
_ALLOWED_OPERATORS_WHERE = ['>', '<', '=']
_ALLOWED_TEXT_TRANSFORMATIONS = ['UPPER', 'LOWER']


class SqlGenerationException(Exception):
    """Custom Exception raised during sql generation."""


class SqlExpressionGenerator(ABC):

    def __init__(self, expression: dict[str, Union[str, list]]):
        self._expression_repr = expression

    @property
    def fields(self):
        return self._expression_repr.get('fields')

    @property
    def table_name(self):
        return self._expression_repr.get('tableName')

    @abstractmethod
    def _validate(self):
        pass

    @abstractmethod
    def _generate(self):
        pass

    def generate(self):
        self._validate()
        return self._generate()

    def _generate_fields(self):
        return ', '.join([f'`{f}`' for f in self.fields])


class SqlInputExpressionGenerator(SqlExpressionGenerator):
    """A class to generate an Input expression."""

    def _generate(self):
        return f'SELECT {self._generate_fields()} FROM `{self.table_name}`'

    def _validate(self):
        if not self.fields or len(self.fields) < 1:
            raise SqlGenerationException('At least 1 field is required!')
        if not self.table_name:
            raise SqlGenerationException('Table name is required!')


class SqlFilterExpressionGenerator(SqlExpressionGenerator):
    """A class to generate an Input expression."""

    @property
    def operations(self):
        return self._expression_repr.get('operations')

    @property
    def field(self):
        return self._expression_repr.get('variable_field_name')

    @property
    def join_operator(self):
        return self._expression_repr.get('joinOperator')

    def _generate(self):
        return f'SELECT {self._generate_fields()} FROM `{self.table_name}` WHERE {self._generate_operations()}'

    def _generate_operation(self, operation):
        op = operation['operator']
        value = operation.get("value")
        # TODO: float or decimal values - round to smth?
        # TODO: support for unary operators
        if is_integer(value):
            value = int(value)
        else:
            value = f'`{value}`'
        return f'`{self.field}` {op} {value}'

    def _generate_operations(self):
        return f' {self.join_operator} '.join([self._generate_operation(o) for o in self.operations])

    def _validate(self):
        if not self.operations or len(self.operations) < 1:
            raise SqlGenerationException('At least 1 operation is required!')
        for op in self.operations:
            if op['operator'] not in _ALLOWED_OPERATORS_WHERE:
                raise SqlGenerationException(f'Operator {op["operator"]} is not supported!')
        if len(self.operations) > 1:
            if not self.join_operator and self.join_operator not in _ALLOWED_JOIN_OPERATORS_WHERE:
                raise SqlGenerationException('JoinOperator is required if there are > 2 operations!')
        if not self.table_name:
            raise SqlGenerationException('Table name is required!')


class SqlSortExpressionGenerator(SqlExpressionGenerator):
    """A class to generate a Sort expression."""

    @property
    def orderings(self):
        return self._expression_repr.get('orderings')

    def _generate(self):
        return f'SELECT {self._generate_fields()} FROM `{self.table_name}` ORDER BY {self._generate_orderings()}'

    @staticmethod
    def _generate_ordering(ordering):
        return f'`{ordering["target"]}` {ordering["order"]}'

    def _generate_orderings(self):
        return f', '.join([self._generate_ordering(o) for o in self.orderings])

    def _validate(self):
        if not self.orderings or len(self.orderings) < 1:
            raise SqlGenerationException('At least 1 ordering is required!')
        for o in self.orderings:
            if o['order'] not in ['ASC', 'DESC']:
                raise SqlGenerationException(f'Ordering {o["order"]} is not supported!')
        if not self.table_name:
            raise SqlGenerationException('Table name is required!')


class SqlTextTransformExpressionGenerator(SqlInputExpressionGenerator):
    """A class to generate a TextTransform expression."""

    @property
    def text_transformations(self):
        return self._expression_repr.get('text_transformations')

    def _generate_fields(self):
        # TODO: do we ultimately want to preserve the order?
        fields = []
        transformations = {t['column']: t['transformation'] for t in self.text_transformations}
        for f in self.fields:
            if f not in transformations:
                fields.append(f'`{f}`')
            else:
                fields.append(self._generate_transformation(f, transformations[f]))
        return ', '.join(fields)

    @staticmethod
    def _generate_transformation(column: str, transformation: str):
        return f'{transformation}(`{column}`)'

    def _validate(self):
        if not self.text_transformations or len(self.text_transformations) < 1:
            raise SqlGenerationException('At least 1 text transformation is required!')
        for t in self.text_transformations:
            if t['transformation'] not in _ALLOWED_TEXT_TRANSFORMATIONS:
                raise SqlGenerationException(f'Transformation {t["transformation"]} is not supported!')
        if not self.table_name:
            raise SqlGenerationException('Table name is required!')


class SqlOutputExpressionGenerator(SqlInputExpressionGenerator):
    """A class to generate a Output expression."""

    @property
    def limit(self):
        return self._expression_repr.get('limit')

    @property
    def offset(self):
        return self._expression_repr.get('offset')

    def _generate(self):
        sql = f'SELECT {self._generate_fields()} FROM `{self.table_name}`'
        if self.limit:
            sql = f'{sql} limit {self.limit}'
            if self.offset:
                sql = f'{sql} offset {self.offset}'
        return sql

    def _validate(self):
        if self.limit and not is_integer(self.limit):
            raise SqlGenerationException('Limit value should be integer!')
        if self.limit and int(self.limit) <= 0:
            raise SqlGenerationException('Limit value should be positive!')
        if self.offset and not is_integer(self.offset):
            raise SqlGenerationException('offset value should be integer!')
        if not self.table_name:
            raise SqlGenerationException('Table name is required!')
