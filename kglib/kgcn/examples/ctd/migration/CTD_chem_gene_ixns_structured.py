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

from kglib.kgcn.examples.ctd.migration.utils import parse_xml_to_tree_line_by_line
from kglib.kgcn.examples.ctd.migration.type_codes import type_codes


def construct_query(gene_id, chemical_id, relation_type, degree, pmid):
    return f'''
            match
                $gene isa gene, has identifier {gene_id};
                $chem isa chemical, has identifier {chemical_id};
                $source isa pubmed-citation, has pmid {pmid};
            insert $r(interacting-gene: $gene,
                interacting-chemical: $chem,
                data-source: $source) isa {relation_type}, has degree {degree};
            '''


def exists_or_insert(tx, type, type_key, key_value):
    pm_query = f'match $x isa {type}, has {type_key} {key_value}; get;'
    results = list(tx.query(pm_query))
    if len(results) == 0:
        tx.query(f'insert $x isa {type}, has {type_key} {key_value};')


def migrate_chemical_gene_interactions(session, data_path):

    line_trees = parse_xml_to_tree_line_by_line(data_path)

    tx = session.transaction().write()
    for root in line_trees:

        if root[0].tag == 'taxon' and root[0].attrib['id'] == '9606':

            for child in root:
                print(child.tag, child.attrib)

                roles = []
                roles.append('interacting-gene: $g,')
                roles.append('interacting-chemical: $g,')

                if child.tag == 'reference':
                    pmid = child.attrib['pmid']
                    exists_or_insert(tx, 'pubmed-citation', 'pmid', int(pmid))

                if child.tag == 'axn':
                    type_code = child.attrib['code']
                    relation_type = type_codes[type_code]
                    # '+' (increases), '-' (decreases), '1' (affects) or '0' (does not affect).
                    degreecode = f"'{child.attrib['degreecode']}'"

                    # TODO Add a switch statement to translate the code to words (or don't bother)

                if child.tag == 'actor':
                    type = child.attrib['type']  # 'gene' or 'chemical'
                    identifier = f'"{child.attrib["id"]}"'
                    position = child.attrib['position']  # 1 or 2

                    if type == 'gene':
                        # form = child.attrib['form'] # Sometimes present, either 'gene', 'protein', 'mRNA'
                        # seqid = child.attrib['seqid']  # Only present sometimes
                        exists_or_insert(tx, 'gene', 'identifier', identifier)
                        gene_identifier = identifier

                    elif type == 'chemical':
                        exists_or_insert(tx, 'chemical', 'identifier', identifier)
                        chemical_identifier = identifier

                    elif type == 'ixn':
                        pass
                    else:
                        raise ValueError(f'Something not a gene nor a chemical nor an interaction was found, a {type}')

            query = construct_query(gene_identifier, chemical_identifier, relation_type, degreecode, pmid)
            tx.query(query)
        tx.commit()
        tx = session.transaction().write()
