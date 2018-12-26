
"""
Build a tensorflow pipeline. Accepts a set of queries, each of which detailing the path it describes
    variables graph with corresponding type hierarchy


The input graph could be supplied as native python types, or as a network graph using e.g. networkx

Also requires a list of the transformations to be used for each `concept type`, which is likely to have defaults for
entities, relationships, and each datatype of attribute. Should be overridable on a per-type basis

"""
import collections
import unittest
import networkx as nx

import model.builder

a = {'start': 'x',  # $x
     'end': 'y',  # $y
     'role_hierarchy': ['role-type',
                        'super-role-type',
                        'super-super-role-type'],
     'which_plays': 'end',
     'end_type_hierarchy': ['type',
                            'super-type',
                            'super-super-type'],
     }

"""
build_kgcn_model(query_features, components)

where query_features is an ordered dict:

    {
    'match $x isa traded-item id {}, has item-purpose $p; limit 10; get;': graph1,
    'match $x isa traded-item id {}; $tm($x) isa taxon-membership; limit 10; get;': graph2
    }
    
The graph should only contain one component per concept type. This is the basis upon which it is convolutional.
"""


class TestBuildKGCNModel(unittest.TestCase):

    def test_one_op_chains_to_next(self):

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

        query_features = collections.OrderedDict([(query, query_graph)])

        class MockChainableComponent:
            def __call__(self, input_tensor):
                output_tensor = input_tensor + 1
                return output_tensor

        components = {'person': MockChainableComponent(),
                      'employment': MockChainableComponent(),
                      }

        tf_graph = model.builder.build_kgcn_model(query_features, components)

        # Find the person op
        op = tf_graph.get_operation_by_name('person/add')
        input_names = [inp.name for inp in op.inputs]

        # Check the input matches
        self.assertIn('employment/add:0', input_names)


    # def test_chain_constructed(self):
    #
    #     # Create the input query and it's associated graph of variables
    #     query_graph = nx.Graph()
    #     query = ('match $x1 isa traded-item id {}; $x2($x1, $x3) isa taxon-membership; $x3 isa species, has name $x4; '
    #              'limit 10; get;')
    #     query_graph.add_nodes_from(
    #         [
    #             ['x1', {'types': ['traded-item']}],
    #             ['x2', {'types': ['taxon-membership']}],
    #             # ['x3', {'types': ['species', 'taxon']}],
    #             ['x3', {'types': ['species']}],
    #             ['x4', {'types': ['name']}],
    #          ])
    #     nx.add_path(query_graph, ['x1', 'x2', 'x3', 'x4'])
    #
    #     print(f'nodes {query_graph.nodes}')
    #     print(f'node data {query_graph.nodes.data()}')
    #     print(f'edges {query_graph.edges}')
    #
    #     query_features = collections.OrderedDict([(query, query_graph)])
    #
    #     class MockChainableComponent:
    #         def __call__(self, input_tensor):
    #             output_tensor = input_tensor + 1
    #             return output_tensor
    #
    #     components = {'traded-item': MockChainableComponent(),
    #                   'taxon-membership': MockChainableComponent(),
    #                   'species': MockChainableComponent(),
    #                   'name': MockChainableComponent(),
    #                   }
    #     # components = {'a': MockChainableComponent(),
    #     #               'b': MockChainableComponent(),
    #     #               'c': MockChainableComponent(),
    #     #               'd': MockChainableComponent(),
    #     #               }
    #
    #     tf_graph = model.builder.build_kgcn_model(query_features, components)
    #
    #     ops = tf_graph.get_operations()
    #
    #     # tf_graph_def = tf_graph.as_graph_def()
    #
    #     # x1-x4 should be placeholders
    #     # There should be a placeholder for the total query result count that feeds through
    #     # There should be an output from the 'traded-item' component
    #
    #     # expected_chain = collections.OrderedDict([
    #     #     ('name/add', {'inputs': {'x4/count', 'x4/values', 'query_count'}}),
    #     #     ('species/add', {'inputs': {'x3/count', 'name/add'}}),
    #     #     ('taxon-membership/add', {'inputs': {'x2/count', 'species/add'}}),
    #     #     ('traded-item/add', {'inputs': {'x1/count', 'taxon-membership/add'}})
    #     # ])
    #
    #     expected_chain = collections.OrderedDict([
    #         ('name/add', {'inputs': set()}),
    #         ('species/add', {'inputs': {'name/add'}}),
    #         ('taxon-membership/add', {'inputs': {'species/add'}}),
    #         ('traded-item/add', {'inputs': {'taxon-membership/add'}})
    #     ])
    #
    #     expected_chain_ops_found = set()
    #
    #     for expected_op_name, value in expected_chain.items():
    #         for op in ops:
    #             if op.name == expected_op_name:
    #                 expected_chain_ops_found.add(op.name)
    #                 with self.subTest(f'{op.name} inputs'):
    #                     # self.assertSetEqual(set(i.name for i in op.inputs), value['inputs'])
    #
    #                     inputs_found = set(i.name for i in op.inputs)
    #                     print(f'Expected inputs: { value["inputs"]}\n'
    #                           f'Inputs found: {inputs_found}\n')
    #                     self.assertTrue(value['inputs'].issubset(inputs_found))
    #                 break
    #     self.assertSetEqual(expected_chain_ops_found, set(expected_chain.keys()))
    #
    #
    #
    #
    #     # Verify that each of the query_graph elements are in the TensorFlow graph
    #     # for node in tf_graph_def.node:
    #     #     scopes = node.name.split('/')
    #     #     input_node_names = node.input
    #     #     input_nodes_scopes = [input_node_name.split('/') for input_node_name in input_node_names]
