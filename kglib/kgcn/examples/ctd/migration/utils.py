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
            item = {key: value for key, value in row.items()}
            yield item


def parse_xml_to_tree_line_by_line(data_path):
    with open(data_path) as data:
        for line in data:
            yield ET.fromstring(line)
