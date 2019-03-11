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

import grakn

import examples.kgcn.adr.migration.xml_migrate as xml


class QueryTreeExecutor:
    def __init__(self, grakn_session: grakn.client.Session):
        self._grakn_session = grakn_session

    def insert(self, query_tree: xml.QueryTree):
        tx = self._grakn_session.transaction().write()

        def _recursive_insert(q_tree, parent_id):

            if parent_id is None:
                query = q_tree.query
            else:
                query = q_tree.query.format(parent_id)

            print(query)
            ans = tx.query(query)
            next_ans = next(ans)
            print(next_ans)
            thing_id = next_ans.get(q_tree.id_var).id
            print(thing_id)

            if q_tree.children:
                for child in q_tree.children:
                    _recursive_insert(child, thing_id)

        _recursive_insert(query_tree, None)
        tx.commit()
