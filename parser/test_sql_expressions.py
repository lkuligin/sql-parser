import unittest

from sql_expressions import (
    SqlInputExpressionGenerator, SqlFilterExpressionGenerator, SqlGenerationException,
    SqlSortExpressionGenerator, SqlTextTransformExpressionGenerator, SqlOutputExpressionGenerator
)


class TestSqlInputExpressionGenerator(unittest.TestCase):

    def test_generate_fields_single_field(self):
        g = SqlInputExpressionGenerator(expression={'fields': ['field1']})
        self.assertEqual(g._generate_fields(), '`field1`')

    def test_generate_fields_many_fields(self):
        g = SqlInputExpressionGenerator(expression={'fields': ['f1', 'f2', 'f3']})
        self.assertEqual(g._generate_fields(), '`f1`, `f2`, `f3`')

    def test_generate(self):
        g = SqlInputExpressionGenerator(expression={'fields': ['f1', 'f2', 'f3'], 'tableName': 'users'})
        self.assertEqual(g.generate(), 'SELECT `f1`, `f2`, `f3` FROM `users`')

    def test_generate_fails(self):
        g = SqlInputExpressionGenerator(expression={'tableName': 'users'})
        with self.assertRaises(SqlGenerationException):
            g.generate()


class TestSqlFilterExpressionGenerator(unittest.TestCase):

    def test_generate_operation(self):
        expression = {'operations': [{'operator': '>', 'value': '18'}], 'variable_field_name': 'age'}
        g = SqlFilterExpressionGenerator(expression=expression)
        self.assertEqual(g._generate_operation(expression['operations'][0]), '`age` > 18')

    def test_generate_operations_multiple(self):
        expression = {'operations': [{'operator': '>', 'value': '18'}, {'operator': '<', 'value': 'a'}],
                      'variable_field_name': 'age', 'joinOperator': 'AND'}
        g = SqlFilterExpressionGenerator(expression=expression)
        self.assertEqual(g._generate_operations(), '`age` > 18 AND `age` < `a`')

    def test_generate(self):
        expression = {'operations': [{'operator': '>', 'value': '18'}, {'operator': '<', 'value': 'a'}],
                      'variable_field_name': 'age', 'joinOperator': 'AND', 'tableName': 'A',
                      'fields': ['f1', 'f2', 'f3']}
        g = SqlFilterExpressionGenerator(expression=expression)
        self.assertEqual(g.generate(), 'SELECT `f1`, `f2`, `f3` FROM `A` WHERE `age` > 18 AND `age` < `a`')

    def test_generate_single(self):
        expression = {'operations': [{'operator': '>', 'value': '18'}],
                      'variable_field_name': 'age', 'joinOperator': 'AND', 'tableName': 'A',
                      'fields': ['f1', 'f2', 'f3']}
        g = SqlFilterExpressionGenerator(expression=expression)
        self.assertEqual(g.generate(), 'SELECT `f1`, `f2`, `f3` FROM `A` WHERE `age` > 18')


class TestSqlSortExpressionGenerator(unittest.TestCase):

    def test_generate(self):
        expression = {'orderings': [{'target': 'f1', 'order': 'ASC'}, {'target': 'f2', 'order': 'DESC'}],
                      'tableName': 'A', 'fields': ['f1', 'f2', 'f3']}
        g = SqlSortExpressionGenerator(expression=expression)
        self.assertEqual(g.generate(), 'SELECT `f1`, `f2`, `f3` FROM `A` ORDER BY `f1` ASC, `f2` DESC')


class TestSqlSortExpressionGenerator(unittest.TestCase):

    def test_generate(self):
        expression = {'orderings': [{'target': 'f1', 'order': 'ASC'}, {'target': 'f2', 'order': 'DESC'}],
                      'tableName': 'A', 'fields': ['f1', 'f2', 'f3']}
        g = SqlSortExpressionGenerator(expression=expression)
        self.assertEqual(g.generate(), 'SELECT `f1`, `f2`, `f3` FROM `A` ORDER BY `f1` ASC, `f2` DESC')


class TestSqlTextTransformExpressionGenerator(unittest.TestCase):

    def test_generate(self):
        expression = {'text_transformations': [
            {'column': 'f2', 'transformation': 'UPPER'}, {'column': 'f4', 'transformation': 'LOWER'}],
            'tableName': 'A', 'fields': ['f1', 'f2', 'f3', 'f4']}
        g = SqlTextTransformExpressionGenerator(expression=expression)
        self.assertEqual(g.generate(), 'SELECT `f1`, UPPER(`f2`), `f3`, LOWER(`f4`) FROM `A`')


class TestSqlOutputExpressionGenerator(unittest.TestCase):

    def test_generate(self):
        expression = {'limit': '100', 'offset': '20', 'tableName': 'A', 'fields': ['f1', 'f2', 'f3']}
        g = SqlOutputExpressionGenerator(expression=expression)
        self.assertEqual(g.generate(), 'SELECT `f1`, `f2`, `f3` FROM `A` limit 100 offset 20')


if __name__ == "__main__":
    unittest.main()
