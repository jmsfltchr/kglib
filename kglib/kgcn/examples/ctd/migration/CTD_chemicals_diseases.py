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

from kglib.kgcn.examples.ctd.migration.utils import parse_csv_to_dictionaries, put_by_keys, commit_and_refresh


def migrate_chemicals_diseases(session, data_path):
    # ChemicalName,ChemicalID,CasRN,DiseaseName,DiseaseID,DirectEvidence,InferenceGeneSymbol,InferenceScore,OmimIDs,PubMedIDs
    tx = session.transaction().write()

    line_dicts = parse_csv_to_dictionaries(data_path)

    for i, line_dict in enumerate(line_dicts):

        chem_name = line_dict['ChemicalName']
        chem_identifier = line_dict['ChemicalID']
        # TODO Migrate CasRN

        dis_name = line_dict['DiseaseName']
        dis_identifier = line_dict['DiseaseID']

        # TODO Migrate OmimIDs
        pmids = line_dict['PubMedIDs'].split(sep='|')

        # We either have direct evidence, or inference via a gene with a score

        direct_evidence = line_dict['DirectEvidence']
        evidence = direct_evidence != ''

        if evidence:
            if direct_evidence == 'therapeutic':
                relation_type = 'therapy'
                chem_role = 'therapeutic'
                dis_role = 'treated-disease'

            elif direct_evidence == 'marker/mechanism':
                relation_type = 'functional-association'
                chem_role = 'functional-chemical'
                dis_role = 'disease-function-of'

            else:
                raise ValueError(
                    f'Encountered a type of direct evidence that is not accounted for: {direct_evidence}')
        else:
            gene_symbol = line_dict['InferenceGeneSymbol']
            score = line_dict['InferenceScore']

            relation_type = 'chemical-disease-association'
            chem_role = 'associated-chemical'
            dis_role = 'associated-disease'

        chem_keys = {'identifier': f'"{chem_identifier}"'}
        chem_extra_attributes_to_insert = {'name': f'"{chem_name}"'}
        put_by_keys(tx, 'chemical', chem_keys, extra_attributes_to_insert=chem_extra_attributes_to_insert)

        dis_keys = {'identifier': f'"{dis_identifier}"'}
        dis_extra_attributes_to_insert = {'name': f'"{dis_name}"'}
        put_by_keys(tx, 'disease', dis_keys, extra_attributes_to_insert=dis_extra_attributes_to_insert)

        assoc_query = cleandoc(f'''
        match
            $c isa chemical, has identifier "{chem_identifier}";
            $d isa disease, has identifier "{dis_identifier}";
        insert
            ({chem_role}: $c, {dis_role}: $d) isa {relation_type}, has identifier "{i}";
        ''')

        print(assoc_query)
        tx.query(assoc_query)

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
                tx.query(pm_query)

        if not evidence:

            # We can't put the gene if it doesn't exist, since we only have the symbol and not the identifier
            infer_query = cleandoc(f'''
            match
                $g isa gene, has symbol "{gene_symbol}";
                $assoc isa {relation_type}, has identifier "{i}";
            insert
                (inferred-from: $g, inferred-conclusion: $assoc) isa inference, has score {score};                    
            ''')
            print(infer_query)
            tx.query(infer_query)

        commit_and_refresh(session, tx, i, every=50)
        # tx.commit()
        # tx = session.transaction().write()
    tx.commit()
