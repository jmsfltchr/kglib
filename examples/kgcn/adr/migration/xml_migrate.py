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

import xml.etree.ElementTree as ET

import kglib.kgcn.core.ingest.traverse.data.context.utils as utils


class XMLMigrator:
    def __init__(self, tag_mapping, tag_containment=None, attr_tag_mapping=None):
        self._tag_containment = tag_containment
        self._tag_mapping = tag_mapping
        self._attr_tag_mapping = attr_tag_mapping

    def get_insert_statements(self, file_path):
        root = ET.fromstring(file_path)

        def _parse_tags(element, root=False):
            tag_type = self._tag_mapping[element.tag]
            insert_query = f'insert $x1 isa {tag_type}'

            if self._attr_tag_mapping is not None:
                # Add attributes to the query
                for attr_name, value in element.attrib.items():
                    attr_type = self._attr_tag_mapping[attr_name]
                    insert_query += f', has {attr_type} \"{value}\"'

            insert_query += ';'

            sub_var = None
            if not root:
                insert_query += (f' ('
                                 f'{self._tag_containment["container_role"]}: $x2, '
                                 f'{self._tag_containment["containee_role"]}: $x1) isa '
                                 f'{self._tag_containment["relation"]}; $x2 id {{}};')
                sub_var = 'x2'

            children = []
            for child in element:
                children.append(_parse_tags(child))

            if len(children) == 0:
                return QueryTree(insert_query, sub_var=sub_var)
            else:
                return QueryTree(insert_query, children=children, sub_var=sub_var)

        return _parse_tags(root, root=True)


class QueryTree(utils.PropertyComparable):
    """
    Specifies the query to insert a parsed element, and gives the child queries to be executed, which should be
    formatted with the id of the parent Concept
    """
    def __init__(self, query, children=(), sub_var=None):
        self.query = query
        self.children = children
        self.sub_var = sub_var
