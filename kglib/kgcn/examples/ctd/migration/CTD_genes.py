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

from kglib.kgcn.examples.ctd.migration.utils import parse_csv_to_dictionaries, put_by_keys


def migrate_genes(session, data_path):
    with session.transaction().write() as tx:

        line_dicts = parse_csv_to_dictionaries(data_path)

        for i, line_dict in enumerate(line_dicts):

            name = line_dict['GeneName']
            identifier = line_dict['GeneID']
            symbol = line_dict['GeneSymbol']

            keys = {'identifier': f'"{identifier}"'}
            extra_attributes_to_insert = {'name': f'"{name}"', 'symbol': f'"{symbol}"'}

            put_by_keys(tx, 'gene', keys, extra_attributes_to_insert=extra_attributes_to_insert)

        tx.commit()
