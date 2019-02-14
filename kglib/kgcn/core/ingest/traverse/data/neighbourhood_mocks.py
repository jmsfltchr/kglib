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

import kglib.kgcn.core.ingest.traverse.data.executor as ex
import kglib.kgcn.core.ingest.traverse.data.neighbourhood as trv


def gen(elements):
    for el in elements:
        yield el


def mock_traversal_output():
    c = trv.ThingContext(
        ex.Thing("0", "person", "entity"),
        gen([
            trv.Neighbour("employee", ex.TARGET_PLAYS, trv.ThingContext(
                ex.Thing("1", "employment", "relationship"),
                gen([
                    trv.Neighbour("employer", ex.NEIGHBOUR_PLAYS, trv.ThingContext(
                        ex.Thing("2", "company", "entity"), gen([])
                    )),
                ])
            )),
            trv.Neighbour("@has-name-owner", ex.TARGET_PLAYS, trv.ThingContext(
                ex.Thing("3", "@has-name", "relationship"),
                gen([
                    trv.Neighbour("@has-name-value", ex.NEIGHBOUR_PLAYS, trv.ThingContext(
                        ex.Thing("4", "name", "attribute", data_type='string', value="Employee Name"),
                        gen([])
                    )),
                ])
            ))

        ]))
    return c


def _build_data(role_label, role_direction, neighbour_id, neighbour_type, neighbour_metatype, data_type=None,
                value=None):
    return {'role_label': role_label, 'role_direction': role_direction,
            'neighbour_thing': ex.Thing(neighbour_id, neighbour_type, neighbour_metatype, data_type=data_type,
                                        value=value)}


def _role_wrapper(outcome, role_direction, query_direction):
    if role_direction == query_direction:
        return gen(outcome)
    else:
        return gen([])


def mock_executor(query_direction, *args):

    concept_id = args[0]
    if concept_id == "0":

        role_direction = ex.TARGET_PLAYS
        return _role_wrapper([
                _build_data("employee", role_direction, "1", "employment", "relationship"),
                _build_data("@has-name-owner", role_direction, "3", "@has-name", "relationship")
            ],
            role_direction,
            query_direction
        )

    elif concept_id == "1":

        role_direction = ex.NEIGHBOUR_PLAYS
        return _role_wrapper([_build_data("employer", role_direction, "2", "company", "entity")],
                             role_direction,
                             query_direction)

    elif concept_id == "3":

        role_direction = ex.NEIGHBOUR_PLAYS
        return _role_wrapper([_build_data("@has-name-value", role_direction, "4", "name", "attribute",
                                          data_type='string', value="Employee Name")],
                             role_direction,
                             query_direction)
    else:
        raise ValueError("This concept id hasn't been mocked")
