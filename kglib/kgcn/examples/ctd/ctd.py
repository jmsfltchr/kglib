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

import copy
import inspect
import time

from grakn.client import GraknClient

from kglib.kgcn.pipeline.pipeline import pipeline
from kglib.utils.graph.iterate import multidigraph_data_iterator
from kglib.utils.graph.query.query_graph import QueryGraph
from kglib.utils.graph.thing.queries_to_graph import build_graph_from_queries


def toxicogenomics_example(num_graphs=200,
                           num_processing_steps_tr=10,
                           num_processing_steps_ge=10,
                           num_training_iterations=1000,
                           keyspace="ctd_trial_2",
                           uri="localhost:48555"):

    tr_ge_split = int(num_graphs * 0.5)

    client = GraknClient(uri=uri)
    session = client.session(keyspace=keyspace)

    graphs = create_concept_graphs(list(range(num_graphs)), session)

    with session.transaction().read() as tx:
        # Change the terminology here onwards from thing -> node and role -> edge
        node_types = get_thing_types(tx)
        edge_types = get_role_types(tx)
        print(f'Found node types: {node_types}')
        print(f'Found edge types: {edge_types}')
        max_score = tx.query('compute max of score;').number()
        min_score = tx.query('compute min of score;').number()

    categorical_attributes = {
        'identifier': [res.get('x').value() for res in tx.query('match $x isa identifier; get;')],
        # 'gene-identifier': [],  # Determined by a query for the ids
        # 'disease-identifier': [],  # Determined by a query for the ids
        # 'chemical-identifier': [],  # Determined by a query for the ids
    }

    continuous_attributes = {
        'score': (min_score, max_score),  # Min and max to be determined by querying
    }

    ge_graphs, solveds_tr, solveds_ge = pipeline(graphs,
                                                 tr_ge_split,
                                                 node_types,
                                                 edge_types,
                                                 num_processing_steps_tr=num_processing_steps_tr,
                                                 num_processing_steps_ge=num_processing_steps_ge,
                                                 num_training_iterations=num_training_iterations,
                                                 continuous_attributes=continuous_attributes,
                                                 categorical_attributes=categorical_attributes,
                                                 output_dir=f"./events/{time.time()}/")

    # with session.transaction().write() as tx:
    #     write_predictions_to_grakn(ge_graphs, tx)

    session.close()
    client.close()

    return solveds_tr, solveds_ge


def create_concept_graphs(example_indices, grakn_session):
    graphs = []
    qh = QueryHandler()

    infer = True

    for example_id in example_indices:
        print(f'Creating graph for example {example_id}')
        graph_query_handles = qh.get_query_handles(example_id)
        with grakn_session.transaction().read() as tx:
            # Build a graph from the queries, samplers, and query graphs
            graph = build_graph_from_queries(graph_query_handles, tx, infer=infer)

        # Remove label leakage - change type labels that indicate candidates into non-candidates
        for data in multidigraph_data_iterator(graph):
            typ = data['type']
            if typ == 'candidate-ctd':
                data.update(type='diagnosis')
            elif typ == 'candidate-patient':
                data.update(type='patient')
            elif typ == 'candidate-diagnosed-disease':
                data.update(type='diagnosed-disease')

        graph.name = example_id
        graphs.append(graph)

    return graphs


# Existing elements in the graph are those that pre-exist in the graph, and should be predicted to continue to exist
PREEXISTS = dict(input=1, solution=0)

# Elements to infer are the graph elements whose existence we want to predict to be true, they are positive examples
TO_INFER = dict(input=0, solution=2)

# Candidates are neither present in the input nor in the solution, they are negative examples
CANDIDATE = dict(input=0, solution=1)


class QueryHandler:

    """
    Proposed workflow:

    We are interested in a particular chemical, which we have elsewhere found to be a candidate treatment for a disease.
    Ahead of clinical trials, we want to be alerted to whether this treatment candidate causes any other disease.

    The scenario we want to simulate is where our examples are chemicals about which we know little. We don't know any
    chemical-disease-associations (our ground truth). We may know the interactions it has with genes, and we will know
    the chemicals to which it is similar

    1. Find
    """

    def diagnosis_query(self, example_id):
        return inspect.cleandoc(f'''match
        $p isa person, has example-id {example_id};
        $s isa symptom, has name $sn;
        $d isa disease, has name $dn;
        $sp(presented-symptom: $s, symptomatic-patient: $p) isa symptom-presentation, has severity $sev;
        $c(cause: $d, effect: $s) isa causality;
        $diag(patient: $p, diagnosed-disease: $d) isa diagnosis;
        get;''')

    def chemical_disease_query(self, chemical_id, disease_id):
        # TODO Remember there is a direct sub here due to current low number of other chem disease associations
        inspect.cleandoc(f'''
        match
        $c isa chemical, has identifier "{chemical_id}";
        $d isa disease, has identifier "{disease_id}";
        $assoc(associated-chemical: $c, associated-disease: $d) isa! chemical-disease-association;
        $g isa gene, has identifier $gi;
        $cgi($g, $d);
        get; limit 1;
        ''')

        inspect.cleandoc(f'''
        match
        $c isa chemical, has identifier "{chemical_id}";
        $d isa disease, has identifier "{disease_id}";
        $cgi($c, $g) isa chemical-gene-interaction;
        $g isa gene, has identifier $gi;
        $gda($g, $d) isa gene-disease-association;
        get; limit 1;
        ''')

        inspect.cleandoc(f'''
        match
        $c isa chemical, has identifier $ci;
        $d1 isa disease, has identifier $di;
        $d2 isa disease;
        ($d1, $d2) isa disease-disease-association;
        $cgi($c, $g) isa chemical-gene-interaction;
        $g isa gene, has identifier $gi;
        $gda($g, $d2) isa gene-disease-association;
        get; count;
        ''')

        """match $c1 isa chemical, has identifier "MESH:D00003421"; 
        $d isa disease, has identifier $di;
        $cca($c1, $c2) isa chemical-chemical-association;
        $cgi($c2, $g2) isa chemical-gene-interaction;
        $cgi2($cg1, $g) isa chemical-gene-interaction;
        $g isa gene, has identifier $gi;
        $gda($g, $d) isa gene-disease-association;
        $cda($c2, $d) isa chemical-disease-association; get;
         offset 0; limit 1;"""

    def base_query_graph(self):
        vars = p, s, sn, d, dn, sp, sev, c = 'p', 's', 'sn', 'd', 'dn', 'sp', 'sev', 'c'
        g = QueryGraph()
        g.add_vars(*vars, **PREEXISTS)
        g.add_has_edge(s, sn, **PREEXISTS)
        g.add_has_edge(d, dn, **PREEXISTS)
        g.add_role_edge(sp, s, 'presented-symptom', **PREEXISTS)
        g.add_has_edge(sp, sev, **PREEXISTS)
        g.add_role_edge(sp, p, 'symptomatic-patient', **PREEXISTS)
        g.add_role_edge(c, s, 'effect', **PREEXISTS)
        g.add_role_edge(c, d, 'cause', **PREEXISTS)

        return g

    def diagnosis_query_graph(self):
        g = self.base_query_graph()
        g = copy.copy(g)

        diag, d, p = 'diag', 'd', 'p'
        g.add_vars(diag, **TO_INFER)
        g.add_role_edge(diag, d, 'diagnosed-disease', **TO_INFER)
        g.add_role_edge(diag, p, 'patient', **TO_INFER)

        return g

    def candidate_diagnosis_query(self, example_id):
        return inspect.cleandoc(f'''match
               $p isa person, has example-id {example_id};
               $s isa symptom, has name $sn;
               $d isa disease, has name $dn;
               $sp(presented-symptom: $s, symptomatic-patient: $p) isa symptom-presentation, has severity $sev;
               $c(cause: $d, effect: $s) isa causality;
               $diag(candidate-patient: $p, candidate-diagnosed-disease: $d) isa candidate-diagnosis; 
               get;''')

    def candidate_diagnosis_query_graph(self):
        g = self.base_query_graph()
        g = copy.copy(g)

        diag, d, p = 'diag', 'd', 'p'
        g.add_vars(diag, **CANDIDATE)
        g.add_role_edge(diag, d, 'candidate-diagnosed-disease', **CANDIDATE)
        g.add_role_edge(diag, p, 'candidate-patient', **CANDIDATE)

        return g

    def get_query_handles(self, example_id):

        return [
            (self.diagnosis_query(example_id), lambda x: x, self.diagnosis_query_graph()),
            (self.candidate_diagnosis_query(example_id), lambda x: x, self.candidate_diagnosis_query_graph()),
        ]


def get_thing_types(tx):
    schema_concepts = tx.query(
        "match $x sub thing; not{$x sub @has-attribute;}; not{ $x sub @key-attribute;}; get;").collect_concepts()
    thing_types = [schema_concept.label() for schema_concept in schema_concepts]
    [thing_types.remove(el) for el in ['thing', 'relation', 'entity', 'attribute']]
    return thing_types


def get_role_types(tx):
    schema_concepts = tx.query('''match $x sub role; 
    not{$x sub @has-attribute-owner;}; 
    not{$x sub @has-attribute-value;}; 
    not{$x sub @key-attribute-owner;}; 
    not{$x sub @key-attribute-value;}; get;''').collect_concepts()

    role_types = ['has'] + [role.label() for role in schema_concepts]
    [role_types.remove(el) for el in ['role']]
    return role_types


def write_predictions_to_grakn(graphs, tx):
    """
    Take predictions from the ML model, and insert representations of those predictions back into the graph.

    Args:
        graphs: graphs containing the concepts, with their class predictions and class probabilities
        tx: Grakn write transaction to use

    Returns: None

    """
    for graph in graphs:
        for node, data in graph.nodes(data=True):
            if data['prediction'] == 2:
                concept = data['concept']
                concept_type = concept.type_label
                if concept_type == 'diagnosis' or concept_type == 'candidate-diagnosis':
                    neighbours = graph.neighbors(node)

                    for neighbour in neighbours:
                        concept = graph.nodes[neighbour]['concept']
                        if concept.type_label == 'person':
                            person = concept
                        else:
                            disease = concept

                    p = data['probabilities']
                    query = (f'match'
                             f'$p id {person.id};'
                             f'$d id {disease.id};'
                             f'insert'
                             f'$pd(predicted-patient: $p, predicted-diagnosed-disease: $d) isa predicted-diagnosis,'
                             f'has probability-exists {p[2]:.3f},'
                             f'has probability-non-exists {p[1]:.3f},'  
                             f'has probability-preexists {p[0]:.3f};')
                    tx.query(query)
    tx.commit()


if __name__ == "__main__":
    toxicogenomics_example()
