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

import examples.kgcn.adr.migration.xml_migrate as xml


class TestMigratorInsertStatements(unittest.TestCase):

    def test_single_tag(self):
        migrator = xml.XMLMigrator(tag_mapping={'Label': 'tag-label'})

        xml_content = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Label>'
            '</Label>'
        )
        inserts = migrator.get_insert_statements(xml_content)

        expected_inserts = xml.QueryTree('insert $x1 isa tag-label;', "x1", None)
        self.assertEqual(expected_inserts, inserts)

    def test_nested_tags(self):
        migrator = xml.XMLMigrator(tag_mapping={'Label': 'tag-label', 'Text': 'tag-text'},
                                   tag_containment={'relation': 'tag-containment',
                                                    'container_role': 'tag-container',
                                                    'containee_role': 'tag-containee'})
        xml_content = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Label>'
            '<Text>'
            '</Text>'
            '</Label>'
        )
        inserts = migrator.get_insert_statements(xml_content)

        expected_inserts = xml.QueryTree('insert $x1 isa tag-label;', "x1", children=[xml.QueryTree(
            'match $x2 id {}; insert $x1 isa tag-text; (tag-container: $x2, tag-containee: $x1) isa tag-containment;',
            "x1", sub_var='x2')])
        self.assertEqual(expected_inserts, inserts)

    def test_tag_attributes(self):
        migrator = xml.XMLMigrator(tag_mapping={'Label': 'tag-label'},
                                   attr_tag_mapping={'Label': {'drug': 'tag-attr-drug', 'track': 'tag-attr-track'}})

        xml_content = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Label drug="adcetris" track="TAC2017_ADR">'
            '</Label>'
        )

        inserts = migrator.get_insert_statements(xml_content)

        expected_inserts = xml.QueryTree(
            'insert $x1 isa tag-label, has tag-attr-drug "adcetris", has tag-attr-track "TAC2017_ADR";', "x1", None)
        self.assertEqual(expected_inserts, inserts)

    def test_tag_attribute_value_mapping(self):

        migrator = xml.XMLMigrator(tag_mapping={'Label': 'tag-label'},
                                   attr_tag_mapping={'Label': {'drug': 'tag-attr-drug', 'track': 'tag-attr-track'}},
                                   tag_value_mapper=lambda x: x.replace("\"", "\\\"")
                                   )

        xml_content = (
            '<?xml version="1.0" encoding="UTF-8"?>'
            '<Label drug="do stuff &quot; adcetris &quot; ok">'
            '</Label>'
        )

        inserts = migrator.get_insert_statements(xml_content)
        st = 'insert $x1 isa tag-label, has tag-attr-drug "do stuff \\\" adcetris \\\" ok";'
        print(st)
        expected_inserts = xml.QueryTree(st, "x1", None)
        self.assertEqual(expected_inserts, inserts)


class TestMigratorDefineStatements(unittest.TestCase):

    def test_single_labels(self):

        migrator = xml.XMLMigrator(tag_mapping={'Label': 'tag-label'})

        defines = migrator.get_define_statements()

        expected_defines = ['define tag-label sub entity;']
        self.assertListEqual(expected_defines, defines)

    def test_nested_labels(self):

        migrator = xml.XMLMigrator(tag_mapping={'Label': 'tag-label', 'Text': 'tag-text'},
                                   tag_containment={'relation': 'tag-containment',
                                                    'container_role': 'tag-container',
                                                    'containee_role': 'tag-containee'})

        defines = migrator.get_define_statements()

        expected_defines = ['define tag-containment sub relation, relates tag-container, relates tag-containee;',
                            'define tag-label sub entity, plays tag-container, plays tag-containee;',
                            'define tag-text sub entity, plays tag-container, plays tag-containee;']
        self.assertListEqual(expected_defines, defines)

    def test_tag_attributes(self):
        migrator = xml.XMLMigrator(tag_mapping={'Label': 'tag-label'},
                                   attr_tag_mapping={'Label': {'drug': 'tag-attr-drug', 'track': 'tag-attr-track'}})

        defines = migrator.get_define_statements()

        expected_defines = ['define tag-attr-drug sub attribute, datatype string;',
                            'define tag-attr-track sub attribute, datatype string;',
                            'define tag-label sub entity, has tag-attr-drug, has tag-attr-track;']
        self.assertListEqual(expected_defines, defines)

    def test_tag_with_and_without_attributes(self):
        migrator = xml.XMLMigrator(tag_mapping={'Label': 'tag-label', 'Text': 'tag-text'},
                                   attr_tag_mapping={'Label': {'drug': 'tag-attr-drug', 'track': 'tag-attr-track'}})

        defines = migrator.get_define_statements()

        expected_defines = ['define tag-attr-drug sub attribute, datatype string;',
                            'define tag-attr-track sub attribute, datatype string;',
                            'define tag-label sub entity, has tag-attr-drug, has tag-attr-track;',
                            'define tag-text sub entity;']
        self.assertListEqual(expected_defines, defines)


if __name__ == "__main__":
    unittest.main()
