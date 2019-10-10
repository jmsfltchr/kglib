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

import time

from grakn.client import GraknClient

from kglib.kgcn.examples.ctd.migration.CTD_chem_gene_ixn_types import migrate_chemical_gene_interaction_types
from kglib.kgcn.examples.ctd.migration.CTD_chem_gene_ixns_structured import migrate_chemical_gene_interactions
from kglib.kgcn.examples.ctd.migration.CTD_chemicals import migrate_chemicals
from kglib.kgcn.examples.ctd.migration.CTD_chemicals_diseases import migrate_chemicals_diseases
from kglib.kgcn.examples.ctd.migration.CTD_diseases import migrate_diseases
from kglib.kgcn.examples.ctd.migration.CTD_genes import migrate_genes
from kglib.kgcn.examples.ctd.migration.CTD_genes_diseases import migrate_genes_diseases

inputs = [
    {
        "data_path": "/Users/jamesfletcher/programming/research/kglib/kgcn/examples/ctd/data/CTD_chem_gene_ixn_types.csv",
        "template": migrate_chemical_gene_interaction_types,
    },
    {
        "data_path": "/Users/jamesfletcher/programming/research/kglib/kgcn/examples/ctd/data/CTD_chem_gene_ixns_structured_snippet.xml",
        "template": migrate_chemical_gene_interactions,
    },
    {
        "data_path": "/Users/jamesfletcher/programming/research/kglib/kgcn/examples/ctd/data/CTD_diseases_snippet.csv",
        "template": migrate_diseases,
    },
    {
        "data_path": "/Users/jamesfletcher/programming/research/kglib/kgcn/examples/ctd/data/CTD_genes_snippet.csv",
        "template": migrate_genes,
    },
    {
        "data_path": "/Users/jamesfletcher/programming/research/kglib/kgcn/examples/ctd/data/CTD_chemicals_snippet.csv",
        "template": migrate_chemicals,
    },
    {
        "data_path": "/Users/jamesfletcher/programming/research/kglib/kgcn/examples/ctd/data/CTD_chemicals_diseases_snippet.csv",
        "template": migrate_chemicals_diseases,
    },
    {
        "data_path": "/Users/jamesfletcher/programming/research/kglib/kgcn/examples/ctd/data/CTD_genes_diseases_snippet.csv",
        "template": migrate_genes_diseases,
    },
]

KEYSPACE = "ctd"
URI = "localhost:48555"


def migrate():
    start_time = time.time()

    client = GraknClient(uri=URI)
    with client.session(keyspace=KEYSPACE) as session:

        for ip in inputs:
            print('==================================================')
            print(f'Loading from {ip["data_path"]}')
            print('==================================================')
            ip["template"](session, ip["data_path"])

    elapsed = time.time() - start_time
    print(f'Time elapsed {elapsed:.1f} seconds')


if __name__ == "__main__":
    migrate()
