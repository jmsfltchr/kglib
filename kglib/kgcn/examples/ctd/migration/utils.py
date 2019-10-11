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

import csv
import xml.etree.ElementTree as ET


def parse_csv_to_dictionaries(data_path):
    with open(data_path) as data:
        for row in csv.DictReader(data, skipinitialspace=True):
            item = {key: value for key, value in row.items()}  # TODO Looks like this does nothing
            yield item


def parse_xml_to_tree_line_by_line(data_path):
    with open(data_path) as data:
        for line in data:
            yield ET.fromstring(line)


def put_by_keys(tx, type, keys_dict, extra_attributes_to_insert=None):
    pm_query = f'$x isa {type}'

    for type_key, key_value in keys_dict.items():
        pm_query += f', has {type_key} {key_value}'

    results = list(tx.query(f'match {pm_query}; get;'))
    if len(results) == 0:
        extra = ''
        if extra_attributes_to_insert is not None:
            for attr_name, value in extra_attributes_to_insert.items():
                attr_statement = f', has {attr_name} {value}'
                results = list(tx.query(f'match {pm_query}{attr_statement}; get;'))
                if len(results) == 0:
                    extra += attr_statement

        tx.query(f'insert {pm_query}{extra};')


def assert_one_inserted(answers):
    num_answers = len(list(answers))
    if num_answers != 1:
        raise ValueError(f"Found an incorrect number of answers. Expected 1, got {num_answers}")


def batcher(items_iterator, batch_size):
    batch = []

    for index, item in enumerate(items_iterator):

        batch.append((index, item))
        if index % batch_size == 0:
            yield batch
            batch = []


def split_file_into_batches(data_path, line_parser, batch_size):
    line_trees = line_parser(data_path)
    batch_generator = batcher(line_trees, batch_size)
    return batch_generator
