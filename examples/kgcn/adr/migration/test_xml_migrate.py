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
A way to generically ingest xml files into Grakn, such that migration can be performed subsequently using Graql
queries to connect data.
"""
import unittest

import examples.kgcn.adr.migration.xml_migrate as xml_migrate


class TestMigratorInsertStatements(unittest.TestCase):
    # xml = (
    #     '<?xml version="1.0" encoding="UTF-8"?>'
    #     '<Label drug="adcetris" track="TAC2017_ADR">'
    #     '<Text>'
    #     '<Section name="adverse reactions" id="S1">'
    #     'ADVERSE REACTIONS INFORMATION'
    #     '</Section>'
    # )

    # 'define': ['xml-tag sub entity;',
    #            'tag-Label sub xml-tag;',
    #            'xml-tag-attribute sub attribute, datatype string;'
    #            'tag-attr-drug sub xml-tag-attribute;'],

    def test_single_tag(self):
        migrator = xml_migrate.XMLMigrator(tag_mapping={'Label': 'tag-label'})

        xml_content = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Label>'
            '</Label>'
        )
        inserts = migrator.get_insert_statements(xml_content)

        expected_inserts = ['insert $x isa tag-label;']
        self.assertListEqual(expected_inserts, inserts)

    def test_tag_attributes(self):
        migrator = xml_migrate.XMLMigrator(tag_mapping={'Label': 'tag-label'},
                                           attr_tag_mapping={'drug': 'tag-attr-drug', 'track': 'tag-attr-track'})

        xml_content = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Label drug="adcetris" track="TAC2017_ADR">'
            '</Label>'
        )

        inserts = migrator.get_insert_statements(xml_content)

        expected_inserts = ['insert $x isa tag-Label, has tag-attr-drug "adcetris", has tag-attr-track "TAC2017_ADR";']
