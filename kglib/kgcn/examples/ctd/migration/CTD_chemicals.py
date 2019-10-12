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


def migrate_chemicals(batch, tx):

    for i, line_dict in batch:
        name = line_dict['ChemicalName']
        identifier = line_dict['ChemicalID']
        definition = line_dict['Definition']
        keys = {'identifier': f'"{identifier}"'}

        extra_attributes_to_insert = {'name': f'"{name}"'}
        if definition != '':
            extra_attributes_to_insert.update({'definition': f'"{definition}"'})

        put_by_keys(tx, 'chemical', keys, extra_attributes_to_insert=extra_attributes_to_insert)

        parent_ids = line_dict['ParentIDs'].split(sep='|')

        for parent_id in parent_ids:
            parent_keys = {'identifier': f'"{parent_id}"'}
            put_by_keys(tx, 'chemical', parent_keys)

            query = cleandoc(f'''
                match
                    $d isa chemical, has identifier "{identifier}";
                    $par isa chemical, has identifier "{parent_id}";
                insert
                    (superior-chemical: $par, subordinate-chemical: $d) isa chemical-hierarchy;
                    ''')
            tx.query(query, exacty_one_result=True)
