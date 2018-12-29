
"""
Build a tensorflow pipeline. Accepts a set of queries, each of which detailing the path it describes
    variables graph with corresponding type hierarchy


The input graph could be supplied as native python types, or as a network graph using e.g. networkx

Also requires a list of the transformations to be used for each `concept type`, which is likely to have defaults for
entities, relationships, and each datatype of attribute. Should be overridable on a per-type basis

build_kgcn_model(query_features, components)

where query_features is an ordered dict:

    {
    'match $x isa traded-item id {}, has item-purpose $p; limit 10; get;': graph1,
    'match $x isa traded-item id {}; $tm($x) isa taxon-membership; limit 10; get;': graph2
    }

The graph should only contain one component per concept type. This is the basis upon which it is convolutional.
"""
import unittest

import collections
import networkx as nx
import tensorflow as tf

import model.builder


class TestBuildKGCNModel(unittest.TestCase):

    def setUp(self):

        # Create the input query and it's associated graph of variables
        query_graph = nx.Graph()
        query = 'match $x1 isa person id {}; $x2(employee: $x1) isa employment; limit 10; get;'
        query_graph.add_nodes_from(
            [
                ['x1', {'types': ['person']}],
                ['x2', {'types': ['employment']}],
             ])
        nx.add_path(query_graph, ['x1', 'x2'])

        print(f'nodes {query_graph.nodes}')
        print(f'node data {query_graph.nodes.data()}')
        print(f'edges {query_graph.edges}')

        self._query_features = collections.OrderedDict([(query, query_graph)])

        class MockChainableComponent:
            def __call__(self, input_tensor, unique_count_to_add):
                output_tensor = input_tensor + unique_count_to_add
                return output_tensor

        self._components = {'person': MockChainableComponent(),
                            'employment': MockChainableComponent(),
                            }
        self._tf_graph = model.builder.build_kgcn_model(self._query_features, self._components)

    def test_one_op_chains_to_next(self):
        # Find the person op
        op = self._tf_graph.get_operation_by_name('person/add')
        input_names = [inp.name for inp in op.inputs]

        # Check the input matches
        self.assertIn('employment/add:0', input_names)

    def test_each_type_has_unique_count_placeholder(self):

        with self.subTest('person/add has placeholder'):
            op = self._tf_graph.get_operation_by_name('person/add')
            input_names = [inp.name for inp in op.inputs]
            # self.assertIn('person/unique_count_placeholder:0', input_names)

            placeholder_name = 'person/unique_count_placeholder'
            self.assertIn(placeholder_name + ':0', input_names)
            placeholder_type = self._tf_graph.get_operation_by_name('person/unique_count_placeholder').type
            self.assertEqual('Placeholder', placeholder_type)

        with self.subTest('employment/add has placeholder'):
            op = self._tf_graph.get_operation_by_name('employment/add')
            input_names = [inp.name for inp in op.inputs]

            placeholder_name = 'employment/unique_count_placeholder'
            self.assertIn(placeholder_name + ':0', input_names)
            placeholder_type = self._tf_graph.get_operation_by_name('employment/unique_count_placeholder').type
            self.assertEqual('Placeholder', placeholder_type)
