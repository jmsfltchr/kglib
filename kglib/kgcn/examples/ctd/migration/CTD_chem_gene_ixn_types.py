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

"""
Migrate chemical-gene interaction types
"""
from inspect import cleandoc


def construct_query(type_name, parent_type):
    return cleandoc(f'''define {type_name} sub {parent_type},
               relates from-actor,
               relates to-actor;''')


def migrate_chemical_gene_interaction_types(batch, tx):

    tx.query(construct_query('chemical-gene-interaction', 'relation'))

    codes = {}

    for i, line_dict in batch:

        parent_code = line_dict["ParentCode"]

        if parent_code == "":
            parent_type = 'chemical-gene-interaction'
        else:
            parent_type = codes[parent_code]

        type_name = line_dict["TypeName"].replace(" ", "-")

        codes[line_dict["Code"]] = type_name

        q = construct_query(type_name, parent_type)
        print(q)
        tx.query(q)
