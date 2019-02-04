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
Method to normalise input values on a class-by-class basis.

Expects a list of dicts of arrays
- Array dimensions are according to the location in the neighbourhood
- Array datatypes are according to the dictionary keys, as follows:

collections.OrderedDict(
            [('role_type', (np.dtype('U50'), '')),
             ('role_direction', (np.int, 0)),
             ('neighbour_type', (np.dtype('U50'), '')),
             ('neighbour_data_type', (np.dtype('U10'), '')),
             ('neighbour_value_long', (np.int, 0)),
             ('neighbour_value_double', (np.float, 0.0)),
             ('neighbour_value_boolean', (np.int, -1)),
             ('neighbour_value_date', ('datetime64[s]', '')),
             ('neighbour_value_string', (np.dtype('U50'), ''))]

TypeNormaliser should provide a method to normalise these values, which can be calibrated on on data set, and
subsequently reused with the parameters found for other data sets.
"""
import unittest


class TestTypeNormaliser(unittest.TestCase):
    def test_happy_path(self):
        normaliser = TypeNormaliser()


if __name__ == "__main__":
    unittest.main()
