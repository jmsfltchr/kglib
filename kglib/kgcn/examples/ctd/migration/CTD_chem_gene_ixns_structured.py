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

from inspect import cleandoc

from kglib.kgcn.examples.ctd.migration.utils import parse_xml_to_tree_line_by_line, put_by_keys, commit_and_refresh
from kglib.kgcn.examples.ctd.migration.type_codes import type_codes


class Chemical:
    def __init__(self, tx, identifier, name, index):
        self.name = name
        self.identifier = identifier
        self.index = index
        self.var = f'chem{self.index}'

        keys = {'identifier': identifier, 'name': f'"{name}"'}
        put_by_keys(tx, 'chemical', keys)
        self.match_statement = f'${self.var} isa chemical, has identifier {self.identifier};'


class Gene:
    def __init__(self, tx, identifier, name, index):
        self.name = name
        self.identifier = identifier
        self.index = index
        self.var = f'gene{self.index}'
        keys = {'identifier': identifier, 'name': f'"{name}"'}
        put_by_keys(tx, 'gene', keys)
        self.match_statement = f'${self.var} isa gene, has identifier {self.identifier};'


class Interaction:
    def __init__(self, tx, identifier, index, relation_type, degree, text, pmids):
        self.tx = tx
        self.text = text
        self.pmids = pmids
        self.identifier = identifier
        self.relation_type = relation_type
        self.degree = degree
        self.index = index
        self.var = f'inter{self.index}'
        self.actor1 = None
        self.actor2 = None
        self.match_statement = f'${self.var} isa chemical-gene-interaction, has identifier {self.identifier};'

    def add_actors(self, actor1, actor2):
        self.actor1 = actor1
        self.actor2 = actor2
        return self

    def match_insert(self, actor1, actor2):
        query = cleandoc(f'''
        match {actor1.match_statement}
              {actor2.match_statement}
        insert
            (from-actor: ${actor1.var}, 
            to-actor: ${actor2.var}
            ) isa {self.relation_type}, has degree {self.degree}, has identifier "{self.identifier}", has text "{self.text}";
        ''')
        print(query)
        self.tx.query(query)

        for pmid in self.pmids:
            if pmid != '':  # We get pmids = [''] if there are none
                pm_keys = {'pmid': pmid}
                put_by_keys(self.tx, 'pubmed-citation', pm_keys)

                pm_query = cleandoc(f'''
                match
                    $inter isa {self.relation_type}, has identifier "{self.identifier}";
                    $pm isa pubmed-citation, has pmid {pmid};
                insert
                    (sourced-data: $inter, data-source: $pm) isa data-sourcing;
                    ''')
                print(pm_query)
                self.tx.query(pm_query)


def recurse(tx, root, base_index):

    actor1 = None
    actor2 = None
    interaction = None
    pmids = []

    interaction_id = root.attrib['id']

    degree_values = {'+': 'increases', '-': 'decreases', '1': 'affects', '0': 'does not affect'}

    for i, node in enumerate(root):
        index = str(base_index) + str(i)
        print(node.tag, node.attrib)

        if node.tag == 'reference':
            pmid = node.attrib['pmid']
            pmids.append(pmid)
            keys = {'pmid': int(pmid)}
            put_by_keys(tx, 'pubmed-citation', keys)

        elif node.tag == 'axn':
            type_code = node.attrib['code']
            relation_type = type_codes[type_code]
            degreecode = node.attrib['degreecode']
            degree = f"'{degree_values[degreecode]}'"

            interaction = Interaction(tx, interaction_id, index, relation_type, degree, node.text, pmids)

            # TODO Add a switch statement to translate the code to words (or don't bother)

        elif node.tag == 'actor':
            type = node.attrib['type']  # 'gene' or 'chemical'
            identifier = f'"{node.attrib["id"]}"'
            position = node.attrib['position']  # 1 or 2

            if type == 'gene':
                # form = child.attrib['form'] # Sometimes present, either 'gene', 'protein', 'mRNA'
                # seqid = child.attrib['seqid']  # Only present sometimes
                actor = Gene(tx, identifier, node.text, index)

            elif type == 'chemical':
                actor = Chemical(tx, identifier, node.text, index)

            elif type == 'ixn':
                actor = recurse(tx, node, index)
            else:
                raise ValueError(f'Something not a gene nor a chemical nor an interaction was found, a {type}')

            if actor1 is None:
                actor1 = actor
            else:
                actor2 = actor

    interaction.add_actors(actor1, actor2)
    interaction.match_insert(actor1, actor2)
    return interaction


def migrate_chemical_gene_interactions(session, data_path):

    line_trees = parse_xml_to_tree_line_by_line(data_path)

    tx = session.transaction().write()
    for i, root in enumerate(line_trees):

        if root[0].tag == 'taxon' and root[0].attrib['id'] == '9606':

            recurse(tx, root, 0)

        tx = commit_and_refresh(session, tx, i, every=50)
    tx.commit()
