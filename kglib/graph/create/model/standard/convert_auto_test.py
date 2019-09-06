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

from kglib.graph.create.model.standard.convert_auto import get_roleplayers_dict, find_specific_roles, \
    get_attribute_owners
from kglib.kgcn.core.ingest.traverse.data.context.neighbour import Thing
from mock import Mock, call


class TestGetRolePlayersDict(unittest.TestCase):
    def test_querying_for_roles_correctly(self):
        tx = Mock()
        answers_mock = Mock()
        answers_mock.collect_concepts = Mock(return_value=[])
        tx.query = Mock(return_value=answers_mock)

        a_mock = Thing('V123', 'person', 'entity')
        r_mock = Thing('V456', 'parentship', 'relation')
        concept_dict = {'a': a_mock, 'r': r_mock}

        get_roleplayers_dict(concept_dict, 'r', tx)

        tx.query.assert_has_calls([
            call('match $r($role: $x); $x id V123; $r id V456; get $role;'),
            call('match $r($role: $x); $x id V456; $r id V456; get $role;')]
        )

    def test_output_with_some_roles_found(self):
        tx = Mock()
        role_mock1 = Mock()
        role_mock2 = Mock()

        answer1_mock = Mock()
        answer1_mock.collect_concepts = Mock(return_value=[role_mock1, role_mock2])
        answer2_mock = Mock()
        answer2_mock.collect_concepts = Mock(return_value=[])

        def return_answers(query):
            if query == 'match $r($role: $x); $x id V123; $r id V456; get $role;':
                return answer1_mock
            elif query == 'match $r($role: $x); $x id V456; $r id V456; get $role;':
                return answer2_mock
            else:
                raise ValueError('Query not recognised')

        tx.query = Mock(side_effect=return_answers)

        a_mock = Thing('V123', 'person', 'entity')
        r_mock = Thing('V456', 'parentship', 'relation')
        concept_dict = {'a': a_mock, 'r': r_mock}

        rp_dict = get_roleplayers_dict(concept_dict, 'r', tx)

        expected_dict = {'a': [role_mock1, role_mock2]}

        self.assertDictEqual(expected_dict, rp_dict)

    def test_output_with_no_roles_found(self):
        tx = Mock()
        answers_mock = Mock()
        answers_mock.collect_concepts = Mock(return_value=[])
        tx.query = Mock(return_value=answers_mock)

        a_mock = Thing('V123', 'person', 'entity')
        r_mock = Thing('V456', 'parentship', 'relation')
        concept_dict = {'a': a_mock, 'r': r_mock}

        rp_dict = get_roleplayers_dict(concept_dict, 'r', tx)

        self.assertDictEqual({}, rp_dict)


class TestGetAttributeOwnersDict(unittest.TestCase):
    def test_querying_for_owners_correctly(self):
        tx = Mock()
        answers_mock = Mock()
        answers_mock.collect_concepts.return_value = [Mock()]
        tx.query = Mock(return_value=answers_mock)

        e_mock = Thing('V123', 'person', 'entity')
        a_mock = Thing('V456', 'name', 'attribute', data_type='string', value='hi')
        concept_dict = {'e': e_mock, 'a': a_mock}

        get_attribute_owners(concept_dict, 'a', tx)

        tx.query.assert_has_calls([
            call('match $x has name "hi"; $x id V123; get;')
        ])

    def test_querying_for_owners_correctly_for_non_string(self):
        tx = Mock()
        answers_mock = Mock()
        answers_mock.collect_concepts.return_value = [Mock()]
        tx.query = Mock(return_value=answers_mock)

        e_mock = Thing('V123', 'person', 'entity')
        a_mock = Thing('V456', 'age', 'attribute', data_type='long', value=5)
        concept_dict = {'e': e_mock, 'a': a_mock}

        get_attribute_owners(concept_dict, 'a', tx)

        tx.query.assert_has_calls([
            call('match $x has age 5; $x id V123; get;')
        ])

    def test_output_is_as_expected(self):
        tx = Mock()
        answers_mock = Mock()
        answers_mock.collect_concepts.return_value = [Mock()]
        tx.query = Mock(return_value=answers_mock)

        e_mock = Thing('V123', 'person', 'entity')
        a_mock = Thing('V456', 'age', 'attribute', data_type='long', value=5)
        concept_dict = {'e': e_mock, 'a': a_mock}

        output = get_attribute_owners(concept_dict, 'a', tx)
        expected_output = [e_mock]

        self.assertListEqual(expected_output, output)


class TestFindSpecificRoles(unittest.TestCase):
    def test_simple_hierarchy(self):
        role1_mock = Mock()
        role2_mock = Mock()
        role3_mock = Mock()
        role1_mock.subs.return_value = [role2_mock, role3_mock]
        role2_mock.subs.return_value = [role3_mock]
        role3_mock.subs.return_value = []

        roles = [role1_mock, role2_mock, role3_mock]

        specific_roles = find_specific_roles(roles)
        self.assertSetEqual({role3_mock}, specific_roles)

    def test_dual_hierarchy(self):
        """
        This case is encountered when a thing plays more than one role in a relation
        """
        role1_mock = Mock()
        role2_mock = Mock()
        role3_mock = Mock()
        role4_mock = Mock()
        role5_mock = Mock()
        role1_mock.subs.return_value = [role2_mock, role3_mock, role4_mock, role5_mock]

        role2_mock.subs.return_value = [role3_mock]
        role3_mock.subs.return_value = []

        role4_mock.subs.return_value = [role5_mock]
        role5_mock.subs.return_value = []

        roles = [role3_mock, role5_mock, role1_mock, role2_mock, role4_mock]

        specific_roles = find_specific_roles(roles)
        self.assertSetEqual({role3_mock, role5_mock}, specific_roles)
