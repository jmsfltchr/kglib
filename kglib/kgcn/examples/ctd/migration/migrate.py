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

from kglib.kgcn.examples.ctd.migration.CTD_chem_gene_ixn_types import migrate_chemical_gene_interaction_types
from kglib.kgcn.examples.ctd.migration.CTD_chem_gene_ixns_structured import migrate_chemical_gene_interactions
from kglib.kgcn.examples.ctd.migration.utils import parse_csv_to_dictionaries, parse_xml_to_tree_line_by_line

inputs = [
    {
        "data_path": "/Users/jamesfletcher/programming/research/kglib/kgcn/examples/ctd/data/CTD_chem_gene_ixn_types.csv",
        "template": migrate_chemical_gene_interaction_types,
        "reader_func": parse_csv_to_dictionaries
    },
    {
        "data_path": "/Users/jamesfletcher/programming/research/kglib/kgcn/examples/ctd/data/CTD_chem_gene_ixns_structured_snippet.xml",
        "template": migrate_chemical_gene_interactions,
        "reader_func": parse_xml_to_tree_line_by_line
    },
]


def migrate():
    keyspace = "ctd"
    uri = "localhost:48555"

    client = GraknClient(uri=uri)
    with client.session(keyspace=keyspace) as session:

        for ip in inputs:

            ip["template"](session, ip["data_path"])


if __name__ == "__main__":
    migrate()
