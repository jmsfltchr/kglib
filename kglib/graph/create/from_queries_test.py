#
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing,
#  software distributed under the License is distributed on an
#  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#  KIND, either express or implied.  See the License for the
#  specific language governing permissions and limitations
#  under the License.
#

import unittest

import networkx as nx

from kglib.graph.create.from_queries import concept_dict_from_concept_map, combine_2_graphs, \
    concept_graph_to_indexed_graph
from kglib.graph.mock.answer import MockConceptMap
from kglib.graph.mock.concept import MockType, MockThing
from kglib.graph.test.case import GraphTestCase
from kglib.kgcn.core.ingest.traverse.data.context.neighbour import GraknEdge, Thing


class TestConceptDictsFromQuery(unittest.TestCase):
    def test_concept_dicts_are_built_as_expected(self):
        concept_map = MockConceptMap({'x': MockThing('V123', MockType('V456', 'person', 'ENTITY'))})
        concept_dicts = concept_dict_from_concept_map(concept_map)

        expected_concept_dicts = {'x': Thing('V123', 'person', 'entity')}

        self.assertEqual(expected_concept_dicts, concept_dicts)

    def test_concept_dicts_are_built_as_expected_with_2_concepts(self):
        concept_map = MockConceptMap({
            'x': MockThing('V123', MockType('V456', 'person', 'ENTITY')),
            'y': MockThing('V789', MockType('V765', 'employment', 'RELATION')),
        })

        concept_dicts = concept_dict_from_concept_map(concept_map)

        expected_concept_dict = {
            'x': Thing('V123', 'person', 'entity'),
            'y': Thing('V789', 'employment', 'relation'),
        }

        self.assertEqual(expected_concept_dict, concept_dicts)


class TestCombineGraphs(GraphTestCase):

    def test_graph_combined_as_expected(self):

        person = Thing('V123', 'person', 'entity')
        employment = Thing('V567', 'employment', 'relation')
        grakn_graph_a = nx.MultiDiGraph()
        grakn_graph_a.add_node(person)
        grakn_graph_a.add_node(employment)
        grakn_graph_a.add_edge(employment, person, type='employee')

        person_b = Thing('V123', 'person', 'entity')
        name = Thing('V1234', 'name', 'attribute', data_type='string', value='Bob')
        grakn_graph_b = nx.MultiDiGraph()
        grakn_graph_b.add_node(person_b)
        grakn_graph_b.add_node(name)
        grakn_graph_b.add_edge(person_b, name, type='has')

        combined_graph = combine_2_graphs(grakn_graph_a, grakn_graph_b)

        person_ex = Thing('V123', 'person', 'entity')
        employment_ex = Thing('V567', 'employment', 'relation')
        name_ex = Thing('V1234', 'name', 'attribute', data_type='string', value='Bob')
        expected_combined_graph = nx.MultiDiGraph()
        expected_combined_graph.add_node(person_ex)
        expected_combined_graph.add_node(name_ex)
        expected_combined_graph.add_node(employment_ex)
        expected_combined_graph.add_edge(employment_ex, person_ex, type='employee')
        expected_combined_graph.add_edge(person_ex, name_ex, type='has')

        self.assertGraphsEqual(expected_combined_graph, combined_graph)

    def test_when_graph_node_properties_are_mismatched_exception_is_raised(self):
        person_a = Thing('V123', 'person', 'entity')
        name_a = Thing('V1234', 'name', 'attribute', data_type='string', value='Bob')
        grakn_graph_a = nx.MultiDiGraph(name='a')
        grakn_graph_a.add_node(person_a, input=1, solution=1)
        grakn_graph_a.add_node(name_a, input=1, solution=1)
        grakn_graph_a.add_edge(person_a, name_a, type='has', input=0, solution=1)

        person_b = Thing('V123', 'person', 'entity')
        name_b = Thing('V1234', 'name', 'attribute', data_type='string', value='Bob')
        grakn_graph_b = nx.MultiDiGraph(name='b')
        grakn_graph_b.add_node(person_b, input=1, solution=1)
        grakn_graph_b.add_node(name_b, input=0, solution=1)
        grakn_graph_b.add_edge(person_b, name_b, type='has', input=0, solution=1)

        with self.assertRaises(ValueError) as context:
            combine_2_graphs(grakn_graph_a, grakn_graph_b)

        self.assertEqual(('Found non-matching node properties for node <name, V1234: Bob> '
                          'between graphs a and b:\n'
                          'In graph a: {\'input\': 1, \'solution\': 1}\n'
                          'In graph b: {\'input\': 0, \'solution\': 1}'), str(context.exception))

    def test_when_graph_edge_properties_are_mismatched_exception_is_raised(self):
        person_a = Thing('V123', 'person', 'entity')
        name_a = Thing('V1234', 'name', 'attribute', data_type='string', value='Bob')
        grakn_graph_a = nx.MultiDiGraph(name='a')
        grakn_graph_a.add_node(person_a, input=1, solution=1)
        grakn_graph_a.add_node(name_a, input=1, solution=1)
        grakn_graph_a.add_edge(person_a, name_a, type='has', input=0, solution=1)

        person_b = Thing('V123', 'person', 'entity')
        name_b = Thing('V1234', 'name', 'attribute', data_type='string', value='Bob')
        grakn_graph_b = nx.MultiDiGraph(name='b')
        grakn_graph_b.add_node(person_b, input=1, solution=1)
        grakn_graph_b.add_node(name_b, input=1, solution=1)
        grakn_graph_b.add_edge(person_b, name_b, type='has', input=1, solution=0)

        with self.assertRaises(ValueError) as context:
            combine_2_graphs(grakn_graph_a, grakn_graph_b)

        self.assertEqual(('Found non-matching edge properties for edge (<person, V123>, <name, V1234: Bob>, 0) '
                          'between graphs a and b:\n'
                          'In graph a: {\'type\': \'has\', \'input\': 0, \'solution\': 1}\n'
                          'In graph b: {\'type\': \'has\', \'input\': 1, \'solution\': 0}'), str(context.exception))


class TestConceptGraphToIndexedGraph(GraphTestCase):
    def test_standard_graph_converted_as_expected(self):
        person = Thing('V123', 'person', 'entity')
        employment = Thing('V567', 'employment', 'relation')
        grakn_graph = nx.MultiDiGraph()
        grakn_graph.add_node(person)
        grakn_graph.add_node(employment)
        grakn_graph.add_edge(employment, person, type='employee')

        person_exp = Thing('V123', 'person', 'entity')
        employment_exp = Thing('V567', 'employment', 'relation')
        expected_indexed_graph = nx.MultiDiGraph()
        expected_indexed_graph.add_node(0, concept=person_exp, type=person_exp.type_label)
        expected_indexed_graph.add_node(1, concept=employment_exp, type=employment_exp.type_label)
        expected_indexed_graph.add_edge(1, 0, type='employee')

        indexed_graph = concept_graph_to_indexed_graph(grakn_graph)

        self.assertIsIsomorphic(expected_indexed_graph, indexed_graph)

    def test_math_graph_converted_as_expected(self):
        person = Thing('V123', 'person', 'entity')
        employment = Thing('V567', 'employment', 'relation')
        employee = GraknEdge(employment, person, 'employee')
        grakn_graph = nx.MultiDiGraph()
        grakn_graph.add_node(person)
        grakn_graph.add_node(employment)
        grakn_graph.add_node(employee)

        grakn_graph.add_edge(employment, employee, type='relates')
        grakn_graph.add_edge(person, employee, type='plays')

        person_exp = Thing('V123', 'person', 'entity')
        employment_exp = Thing('V567', 'employment', 'relation')
        employee_exp = GraknEdge(employment, person, 'employee')
        expected_indexed_graph = nx.MultiDiGraph()
        expected_indexed_graph.add_node(0, concept=person_exp, type=person_exp.type_label)
        expected_indexed_graph.add_node(1, concept=employment_exp, type=employment_exp.type_label)
        expected_indexed_graph.add_node(2, concept=employee_exp, type=employee_exp.type_label)
        expected_indexed_graph.add_edge(1, 2, type='relates')
        expected_indexed_graph.add_edge(0, 2, type='plays')

        indexed_graph = concept_graph_to_indexed_graph(grakn_graph)

        self.assertIsIsomorphic(expected_indexed_graph, indexed_graph)


if __name__ == "__main__":
    unittest.main()
