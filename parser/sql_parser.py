from __future__ import annotations

import argparse
import copy
import json

from sql_expressions import (
    SqlInputExpressionGenerator, SqlFilterExpressionGenerator, SqlSortExpressionGenerator,
    SqlTextTransformExpressionGenerator, SqlOutputExpressionGenerator
)


class SqlParser:

    def __init__(self, edges, nodes):
        self._edges = edges
        self._nodes = nodes

    def generate(self) -> str:
        node_json = self._get_node_by_key(self._edges[0]['from'])
        if node_json['type'] != 'INPUT':
            raise ValueError('First node should always have INPUT type!')
        nodes_seen = {node_json['key']: node_json['transformObject']['fields']}
        sqls = [f'with {self._generate_for_node(node_json=node_json)}']
        for e in self._edges:
            table_from = e['from']
            table_to = e['to']
            if table_from not in nodes_seen:
                raise ValueError(f'Node {table_from} not seen!')
            if table_to in nodes_seen:
                raise ValueError(f'Node {table_to} referenced twice!')
            node_json = self._get_node_by_key(table_to)
            sqls.append(self._generate_for_node(node_json=node_json, input_node=table_from,
                                                fields=nodes_seen[table_from]))
            nodes_seen[table_to] = (
                node_json['fields'] if node_json['type'] == 'INPUT' else nodes_seen[table_from]
            )
        sql = ',\n'.join(sqls)
        sql += '\nSELECT * FROM table_to;'
        return sql

    def _generate_for_node(self, node_json, input_node: str = None, fields: list[str] = None):
        node_type = node_json['type']
        if node_type == 'INPUT':
            sql = SqlInputExpressionGenerator(expression=node_json['transformObject']).generate()
        if node_type == 'FILTER':
            representation = copy.deepcopy(node_json['transformObject'])
            representation['tableName'] = input_node
            representation['fields'] = fields
            sql = SqlFilterExpressionGenerator(expression=representation).generate()
        if node_type == 'SORT':
            representation = {'orderings': node_json['transformObject'], 'tableName': input_node, 'fields': fields}
            sql = SqlSortExpressionGenerator(expression=representation).generate()
        if node_type == 'TEXT_TRANSFORMATION':
            representation = {
                'text_transformations': node_json['transformObject'], 'tableName': input_node, 'fields': fields}
            sql = SqlTextTransformExpressionGenerator(expression=representation).generate()
        if node_type == 'OUTPUT':
            representation = copy.deepcopy(node_json['transformObject'])
            representation['tableName'] = input_node
            representation['fields'] = fields
            sql = SqlOutputExpressionGenerator(expression=representation).generate()
        return f'{node_json["key"]} as (\n    {sql}\n)'

    def _get_node_by_key(self, node_key):
        for node in self._nodes:
            if node['key'] == node_key:
                return node


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", type=str)
    args = parser.parse_args()
    with open(args.path, 'r') as f:
        json_repr = json.load(f)
    sql_parser = SqlParser(edges=json_repr['edges'], nodes=json_repr['nodes'])
    print(sql_parser.generate())


if __name__ == '__main__':
    main()
