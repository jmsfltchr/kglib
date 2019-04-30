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
import glob

import grakn

import examples.kgcn.adr.migration.query_executor as ex
import examples.kgcn.adr.migration.xml_migrate as xml_migrate


def main():
    migrator = xml_migrate.XMLMigrator(tag_mapping={'Label': 'tag-label',
                                                    'Text': 'tag-text',
                                                    'Section': 'tag-section',
                                                    'Mentions': 'tag-mentions',
                                                    'Mention': 'tag-mention',
                                                    'Relations': 'tag-relations',
                                                    'Relation': 'tag-relation',
                                                    'Reactions': 'tag-reactions',
                                                    'Reaction': 'tag-reaction',
                                                    'Normalization': 'tag-normalization',
                                                    },
                                       attr_tag_mapping={
                                           'Label': {'drug': 'label-drug', 'track': 'label-track'},
                                           'Section': {'name': 'section-name', 'id': 'section-id'},
                                           'Mention': {'id': 'mention-id', 'section': 'section-id',
                                                       'type': 'mention-type', 'start': 'mention-start',
                                                       'len': 'mention-len', 'str': 'name'},
                                           'Relation': {'id': 'relation-id', 'type': 'relation-type',
                                                        'arg1': 'mention-id-1', 'arg2': 'mention-id-2'},
                                           'Reaction': {'id': 'reaction-id', 'str': 'name'},
                                           'Normalization': {'id': 'normalization-id', 'meddra_pt': 'meddra-pt',
                                                             'meddra_pt_id': 'meddra-pt-id', 'meddra_llt': 'meddra-llt',
                                                             'meddra_llt_id': 'meddra-llt-id', 'flag': 'flag'}

                                       },
                                       tag_containment={'relation': 'tag-containment',
                                                        'container_role': 'tag-container',
                                                        'containee_role': 'tag-containee'},
                                       tag_value_mapper=lambda x: x.replace("\"", "\\\"")
                                       )

    define_statements = migrator.get_define_statements()

    client = grakn.client.GraknClient(uri="localhost:48555")
    session = client.session(keyspace="adr")
    tx = session.transaction().write()

    for define_statement in define_statements:
        tx.query(define_statement)

    tx.commit()
    executor = ex.QueryTreeExecutor(session)

    xml = 'data/adrs/train_xml/ADCETRIS.xml'
    insert_tree = migrator.get_insert_statements(xml)
    executor.insert(insert_tree)
    # xmls = glob.glob('data/adrs/train_xml/*.xml')
    # for xml in xmls:
    #     print('=============================================')
    #     print(f'Inserting xml data from {xml}')
    #     insert_tree = migrator.get_insert_statements(xml)
    #     executor.insert(insert_tree)

    session.close()
    client.close()


def add_relations():
    types = (
        'effect',
        'negated',
        'severity',
        'hypothetical'
    )
    define_plays = 'define tag-mention sub entity, plays link-source, plays link-dest;'
    define_relation = ('define relation-link sub relation, relates link-source, relates link-dest, has relation-type; '
                       'relation-type sub attribute, datatype string;')

    define_attribute_hierarchy = 'define mention-id-1 sub mention-id; mention-id-2 sub mention-id;'
    define_meddra_terms_are_names = 'define meddra-pt sub name; meddra-llt sub name;'

    # insert_relation = (
    #     'match '
    #     '$x1 isa tag-mention, has mention-id $x1-id; '
    #     '$x2 isa tag-mention, has mention-id $x2-id; '
    #     '$x1-id != $x2-id;'
    #     '$rel-tag isa tag-relation, has mention-id $x1-id, has mention-id $x2-id, has relation-type $type; '
    #     'insert '
    #     '$rel(links: $x1, links: $x2) isa relation-link, has relation-type $type;'
    # )  # Doesn't work due to a bug

    insert_relation = (
        'match '
        '$label isa tag-label, has label-drug $drug-name; '
        '$tc1($label, $x1) isa tag-containment; '
        '$tc2($label, $x2) isa tag-containment; '
        '$tc1($label, $rel-tag) isa tag-containment; '
        '$x1 isa tag-mention, has mention-id $x1-id; '
        '$x2 isa tag-mention, has mention-id $x2-id; '
        '$x1-id-1 == $x1-id; '
        '$x2-id-2 == $x2-id; '
        '$x1-id != $x2-id; '
        '$rel-tag isa tag-relation, has mention-id-1 $x1-id-1, has mention-id-2 $x2-id-2, has relation-type $type; '
        'insert '
        '$rel(link-source: $x1, link-dest: $x2) isa relation-link; '
        '(@has-relation-type-value: $type, @has-relation-type-owner: $rel) isa @has-relation-type;'
    )

    client = grakn.client.GraknClient(uri="localhost:48555")
    session = client.session(keyspace="adr")

    tx = session.transaction().write()
    tx.query(define_relation)
    tx.query(define_plays)
    tx.query(define_attribute_hierarchy)
    tx.query(define_meddra_terms_are_names)
    tx.commit()

    tx = session.transaction().write()
    insert_query = insert_relation
    print(insert_query)
    tx.query(insert_query)
    print('inserted')
    tx.commit()

    session.close()
    client.close()


def add_real():
    drug_insert_query = (
        'match '
        '$l isa tag-label, has label-drug $dn;'
        'insert '
        '$d isa drug, has name $dn;'
    )

    # adr_define_query = (
    #     'define '
    #     'condition sub entity, has normalization-id, has meddra-pt, has meddra-pt-id, has meddra-llt, '
    #     'has meddra-llt-id, has flag;'
    # )

    condition_insert_query = (
        'match $n isa tag-normalization, has meddra-pt-id $id; insert $c isa condition, has meddra-pt-id $id;'
    )  # hits a bug

    condition_insert_query = (
        'match $n isa tag-normalization, has meddra-pt-id $id, has attribute $a; $c isa condition, has meddra-pt-id $id;'
        'insert $c has attribute $a;'
    )  # hits a bug hard

    # for concept_map in results:
    #     normalization = concept_map.map().get('$n')
    #     attrs = normalization.attributes()
    #     condition = next(tx.query('insert $c isa condition;'))
    #     for attr in attrs:

    define_causality = (
        'define '
        'reaction-causality sub relation, relates reaction-cause, relates caused-reaction, has severity;'
        'severity sub attribute, datatype string;'
        'tag-label plays reaction-cause; '
        'tag-normalization plays caused-reaction;'
    )

    causality_insert_query = (
        'match $label isa tag-label, has label-drug $drug-name;'
        '$tc1($label, $reactions) isa tag-containment;'
        '$reactions isa tag-reactions;'
        '$tc2($reactions, $reaction) isa tag-containment;'
        '$reaction isa tag-reaction;'
        '$tc3($reaction, $norm) isa tag-containment;'
        '$norm isa tag-normalization;'
        'insert (reaction-cause: $label, caused-reaction: $norm) isa reaction-causality;'
    )

    mention_insert_query = (
        'match $label isa tag-label, has label-drug $drug-name; '
        '$tc1($label, $reactions) isa tag-containment; '
        '$reactions isa tag-reactions;'
        '$tc2($reactions, $reaction) isa tag-containment;' 
        '$reaction isa tag-reaction;'
        '$tc3($reaction, $norm) isa tag-containment;' 
        '$norm isa tag-normalization;\n'
        
        '{$reaction has name $token;} or {$norm has name $token;};\n'
        
        '$tc4($mentions, $label) isa tag-containment;'
        '$mentions isa tag-mentions;'
        '$tc5($mentions, $mention) isa tag-containment;'
        '$mention isa tag-mention, has name $token, has mention-type "AdverseReaction";\n'
        
        '$tc6($mentions, $rel-mention) isa tag-containment;'
        '$lr($mention, $rel-mention) isa relation-link;'
        '$rel-mention isa tag-mention, has name $value, has mention-type "Severity";\n'
        
        
        # Then add attributes to the existing relation
        '$rc(reaction-cause: $label, caused-reaction: $norm) isa reaction-causality;'
        # 'insert $rc has severity $value;'
        'insert $s isa severity; $s == $value; (@has-severity-owner: $rc, @has-severity-value: $s) isa @has-severity;'  # This instead of the line above to avoid a bug
    )



"""
Check the kinds of connections made by relation-links
match $rl(link-dest:$d, link-source:$s)isa relation-link, has relation-type $rt; $d has mention-type $dmt; $s has mention-type $smt; get $dmt, $smt, $rt;
"""

"""
Query for 
match 
$label isa tag-label, has label-drug $drug;
$section isa tag-section, has section-id $sec-id; $rsl($section, $label) isa tag-containment;
$mention isa tag-mention, has section-id $sec-id, has mention-type $mt; $rml($mention, $label) isa tag-containment;
get; offset 0; limit 1;
"""

"""
define
transitive-tag-containment sub rule,
when {
    (tag-container: $x1, tag-containee: $x2) isa tag-containment;
    (tag-container: $x2, tag-containee: $x3) isa tag-containment;
} then {
    (tag-container: $x1, tag-containee: $x3) isa tag-containment;
};
"""

"""
define transitive-tag-containment sub rule, when {(tag-container: $x1, tag-containee: $x2) isa tag-containment; (tag-container: $x2, tag-containee: $x3) isa tag-containment;}, then {(tag-container: $x1, tag-containee: $x3) isa tag-containment;};
"""

"""
Link a drug to the normalized names of ADRs it's related to 
match $label isa tag-label, has label-drug $drug-name; 
$tc1($label, $reactions) isa tag-containment; 
$reactions isa tag-reactions;
$tc2($reactions, $reaction) isa tag-containment; 
$reaction isa tag-reaction, has name $token;
$tc3($reaction, $norm) isa tag-containment; 
$norm isa tag-normalization, has meddra_pt $md; get $drug-name, $md; offset 0; limit 5;

---

match $label isa tag-label, has label-drug $drug-name; 
$tc1($label, $reactions) isa tag-containment; 
$reactions isa tag-reactions;
$tc2($reactions, $reaction) isa tag-containment; 
$reaction isa tag-reaction;
$tc3($reaction, $norm) isa tag-containment; 
$norm isa tag-normalization;

{$reaction has name $token;} or {$norm has name $token;};

$tc4($mentions, $label) isa tag-containment;
$mentions isa tag-mentions;
$tc5($mentions, $mention) isa tag-containment;
$mention isa tag-mention, has name $token;
get; offset 0; limit 5;

"""

if __name__ == "__main__":
    main()
    print("adding relations")
    add_relations()
