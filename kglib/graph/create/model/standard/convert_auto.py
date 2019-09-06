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


def get_roleplayers_dict(concept_dict, relation_var, tx):

    roleplayers_dict = {}

    relation = concept_dict[relation_var]
    if relation.base_type_label == 'relation':

        # Find roleplayers
        for variable, thing in concept_dict.items():
            roles = tx.query(
                f'match $r($role: $x); $x id {thing.id}; $r id {relation.id}; get $role;').collect_concepts()
            if len(roles) > 0:
                roleplayers_dict[variable] = roles
    else:
        # TODO Test for this
        raise ValueError(f'{relation.type_label} is not a relation')

    return roleplayers_dict


def find_specific_roles(roles):
    """
    Take a list containing a hierarchy of role concepts (order not necessarily known), and find the 'lowest' role.
    That is, the role without any of it's subtypes in the list.
    Args:
        roles: List of roles, containing the most specific roles and their supertypes

    Returns:
        The set of roles which had no subtypes in `roles` (the most granular roles)
    """

    all_roles = roles.copy()
    specific_roles = set()
    while len(roles) > 0:
        role = roles.pop(0)
        if len(set(role.subs()).intersection(set(all_roles))) == 0:
            specific_roles.add(role)
    return specific_roles


def get_attribute_owners(concept_dict, attribute_var, tx):
    """
    Find the owners of an attribute in a concept dict
    Args:
        concept_dict: Dictionary of concepts keyed by their corresponding variable
        attribute_var: Variable (key) for the attribute in the concept dict
        tx: Grakn transaction

    Returns:
        A dictionary
    """
    owners_dict = []

    attribute = concept_dict[attribute_var]
    if attribute.base_type_label == 'attribute':

        # Find owners
        for variable, thing in concept_dict.items():
            if variable != attribute_var:

                value = attribute.value
                if type(value) is str:
                    value = f'"{value}"'

                owners = tx.query(
                    f'match $x has {attribute.type_label} {value}; $x id {thing.id}; get;').collect_concepts()
                if len(owners) > 0:
                    owners_dict.extend(list(owners))
    else:
        # TODO Test for this
        raise ValueError(f'{attribute.type_label} is not an attribute')

    return owners_dict
