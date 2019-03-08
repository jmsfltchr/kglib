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


import xmltodict


class XMLMigrator:
    def __init__(self, tag_mapping):
        self._tag_mapping = tag_mapping

    def get_insert_statements(self, file_path):
        doc = xmltodict.parse(file_path)

        def _parse_tags(tags_dict):
            insert_queries = []
            for tag, contents in tags_dict.items():
                tag_type = self._tag_mapping[tag]
                insert_queries.append(f'insert $x isa {tag_type};')
            return insert_queries

        return _parse_tags(doc)
