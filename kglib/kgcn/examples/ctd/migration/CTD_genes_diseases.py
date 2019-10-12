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

from kglib.kgcn.examples.ctd.migration.utils import put_by_keys


def migrate_genes_diseases(batch, tx):
    for i, line_dict in batch:

        gene_symbol = line_dict['GeneSymbol']
        gene_id = line_dict['GeneID']
        dis_name = line_dict['DiseaseName']
        dis_id = line_dict['DiseaseID']

        # chem_name = line_dict['ChemicalName']
        # chem_identifier = line_dict['ChemicalID']

        # TODO Migrate OmimIDs
        pmids = line_dict['PubMedIDs'].split(sep='|')

        # We either have direct evidence, or inference via a gene with a score

        direct_evidence = line_dict['DirectEvidence']
        evidence = direct_evidence != ''

        if evidence:
            raise ValueError(f'Encountered a type of direct evidence: {direct_evidence}')
        else:
            chem_name = line_dict['InferenceChemicalName']
            score = line_dict['InferenceScore']

            relation_type = 'gene-disease-association'
            gene_role = 'associated-gene'
            dis_role = 'associated-disease'

        gene_keys = {'identifier': f'"{gene_id}"'}
        gene_extra_attributes_to_insert = {'symbol': f'"{gene_symbol}"'}
        put_by_keys(tx, 'gene', gene_keys, extra_attributes_to_insert=gene_extra_attributes_to_insert)

        dis_keys = {'identifier': f'"{dis_id}"'}
        dis_extra_attributes_to_insert = {'name': f'"{dis_name}"'}
        put_by_keys(tx, 'disease', dis_keys, extra_attributes_to_insert=dis_extra_attributes_to_insert)

        assoc_query = cleandoc(f'''
        match
            $g isa gene, has identifier "{gene_id}";
            $d isa disease, has identifier "{dis_id}";
        insert
            ({gene_role}: $g, {dis_role}: $d) isa {relation_type}, has identifier "{i}";
        ''')

        print(assoc_query)
        tx.query(assoc_query, exacty_one_result=True)

        for pmid in pmids:
            if pmid != '':  # We get pmids = [''] if there are none
                pm_keys = {'pmid': pmid}
                put_by_keys(tx, 'pubmed-citation', pm_keys)

                pm_query = cleandoc(f'''
                match
                    $assoc isa {relation_type}, has identifier "{i}";
                    $pm isa pubmed-citation, has pmid {pmid};
                insert
                    (sourced-data: $assoc, data-source: $pm) isa data-sourcing;
                    ''')
                print(pm_query)
                tx.query(pm_query, exacty_one_result=True)

        if not evidence:

            # put_by_keys(tx, 'chemical', {'identifier': f'"{chem_name}-missing-id"'}, extra_attributes_to_insert={'name': f'"{chem_name}"'})

            if len(list(tx.query(f'match $c isa chemical, has name "{chem_name}"; get;'))) == 0:
                tx.query(f'insert $c isa chemical, has identifier "{chem_name}", has name "{chem_name}";', exacty_one_result=True)

            # We can't put the gene if it doesn't exist, since we only have the symbol and not the identifier
            infer_query = cleandoc(f'''
            match
                $c isa chemical, has name "{chem_name}";
                $assoc isa {relation_type}, has identifier "{i}";
            insert
                (inferred-from: $c, inferred-conclusion: $assoc) isa inference, has score {score};                    
            ''')
            print(infer_query)
            tx.query(infer_query, exacty_one_result=True)
