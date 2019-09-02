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

import networkx as nx


def concept_dict_to_grakn_graph(concept_dict, variable_graph, add_role_func=None):
    """
    Create a new graph, based on a `variable_graph` that describes the interactions of variables in a query,
    and a `concept_dict` that holds objects that satisfy the query
    :param concept_dict: A dictionary with variable names as keys
    :param variable_graph: A graph with variable names as nodes
    :param add_role_func: The function to use to add roles to the graph
    :return: A graph with consecutive integers as node ids, with concept information stored in the data. Edges
    connecting the nodes have only Role type information in their data
    """
    grakn_graph = nx.MultiDiGraph()
    node_to_var = {}

    if set(variable_graph.nodes()) != set(concept_dict.keys()):

        in_var_graph_not_in_concept_dict = set(variable_graph).difference(set(concept_dict.keys()))
        in_concept_dict_not_in_var_graph = set(concept_dict.keys()).difference(set(variable_graph))

        raise ValueError(f'The variables in the variable_graph must match those in the concept_dict\n'
                         f'In the variable graph but not in the concept dict: {in_var_graph_not_in_concept_dict}\n'
                         f'In the concept dict but not in the variable graph: {in_concept_dict_not_in_var_graph}')

    # This assumes that all variables are nodes, which would not be the case for variable roles
    for variable, thing in concept_dict.items():
        data = variable_graph.nodes[variable]
        data.update(type=thing.type_label)
        if thing.base_type_label == 'attribute':
            data.update(datatype=thing.data_type, value=thing.value)

        grakn_graph.add_node(thing, **data)

        # Record the mapping of nodes from one graph to the other
        assert variable not in node_to_var
        node_to_var[variable] = thing

    for sending_var, receiving_var, data in variable_graph.edges(data=True):
        sender = node_to_var[sending_var]
        receiver = node_to_var[receiving_var]

        if sender.base_type_label != 'relation' and not (
                receiver.base_type_label == 'attribute' and data['type'] == 'has'):
            raise ValueError('An edge in the variable_graph originates from a non-relation, check the variable_graph!')

        if data['type'] == 'has':
            grakn_graph.add_edge(sender, receiver, **data)
        else:
            add_role_func(grakn_graph, sender, receiver, data)

    return grakn_graph