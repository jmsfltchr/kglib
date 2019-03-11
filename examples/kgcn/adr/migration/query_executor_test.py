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
import unittest
import unittest.mock as mock

import grakn
import grakn.service.Session.Concept.Concept as Concept
import grakn.service.Session.util.ResponseReader as ResponseReader

import examples.kgcn.adr.migration.query_executor as ex
import examples.kgcn.adr.migration.xml_migrate as xml


class TestQueryTreeExecutor(unittest.TestCase):

    def test_single_query_insertion(self):
        query = 'insert $x1 isa tag-label;'
        query_tree = xml.QueryTree(query, "x1")

        grakn_session_mock = mock.MagicMock(grakn.Session)
        transaction_mock = mock.MagicMock(grakn.Transaction)
        grakn_session_mock.transaction.return_value = transaction_mock

        exe = ex.QueryTreeExecutor(grakn_session_mock)
        exe.insert(query_tree)

        grakn_session_mock.transaction.assert_called()

        transaction_mock.query.assert_called_with(query)

    def test_query_tree_insertion(self):
        query_1 = 'insert $x1 isa tag-label;'
        query_2 = 'match $x2 id {}; insert $x1 isa tag-text; (tag-container: $x2, tag-containee: $x1) isa ' \
                  'tag-containment;'
        query_tree = xml.QueryTree(query_1, "x1", children=[xml.QueryTree(query_2, "x1", sub_var='x2')])

        grakn_session_mock = mock.MagicMock(grakn.Session)
        transaction_mock = mock.MagicMock(grakn.Transaction)
        grakn_session_mock.transaction.return_value = transaction_mock

        response_mock = mock.MagicMock(ResponseReader.ResponseIterator)

        concept_map_mock = mock.MagicMock(ResponseReader.ConceptMap)
        concept_mock = mock.MagicMock(Concept.Concept)
        concept_mock.id = 'V123'
        concept_map_mock.get.return_value = concept_mock
        response_mock.__next__.return_value = concept_map_mock
        transaction_mock.query.return_value = response_mock

        exe = ex.QueryTreeExecutor(grakn_session_mock)
        exe.insert(query_tree)

        grakn_session_mock.transaction.assert_called()

        transaction_mock.query.assert_has_calls(
            [mock.call(query_1), mock.call().__next__(), mock.call().__next__().get('x1'),
             mock.call(query_2.format('V123'))])


if __name__ == "__main__":
    unittest.main()
