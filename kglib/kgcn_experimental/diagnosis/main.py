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

from grakn.client import GraknClient

from kglib.kgcn_experimental.diagnosis.data import create_concept_graphs
from kglib.kgcn_experimental.model import model
from kglib.kgcn_experimental.plotting import plot_across_training, plot_input_vs_output
from kglib.kgcn_experimental.tmp.shortest_path import num_processing_steps_ge
from kglib.synthetic_graphs.diagnosis.main import generate_example_graphs


def main():

    num_graphs = 60

    # The value at which to split the data into training and evaluation sets
    tr_ge_split = int(num_graphs*0.5)
    keyspace = "diagnosis"
    uri = "localhost:48555"

    generate_example_graphs(num_graphs, keyspace=keyspace, uri=uri)

    # Get the node and edge types, these can simply be passed as a list, but here we query the schema and then remove
    # unwanted types and roles

    with GraknClient(uri=uri) as client:
        with client.session(keyspace=keyspace) as session:
            with session.transaction().read() as tx:
                schema_concepts = tx.query("match $x sub thing; get;").collect_concepts()
                all_node_types = [schema_concept.label() for schema_concept in schema_concepts]
                [all_node_types.remove(el) for el in
                 ['thing', 'relation', 'entity', 'attribute', '@has-attribute', 'candidate-diagnosis', 'example-id',
                  '@has-example-id']]
                print(all_node_types)

                roles = tx.query("match $x sub role; get;").collect_concepts()
                all_edge_types = [role.label() for role in roles]
                [all_edge_types.remove(el) for el in
                 ['role', '@has-attribute-value', '@has-attribute-owner', 'candidate-patient',
                  'candidate-diagnosed-disease', '@has-example-id-value', '@has-example-id-owner']]
                print(all_edge_types)

    concept_graphs = create_concept_graphs(list(range(num_graphs)), keyspace, uri)

    training_graphs = concept_graphs[:tr_ge_split]
    generalisation_graphs = concept_graphs[tr_ge_split:]

    train_values, test_values, log_info = model(training_graphs,
                                                generalisation_graphs,
                                                all_node_types,
                                                all_edge_types,
                                                num_processing_steps_tr=10,
                                                num_processing_steps_ge=10,
                                                num_training_iterations=100,
                                                log_every_seconds=2)
    plot_across_training(*log_info)
    plot_input_vs_output(generalisation_graphs, test_values, num_processing_steps_ge)


if __name__ == "__main__":
    main()
