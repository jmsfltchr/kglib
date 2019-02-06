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
Normalising CITES animaltrade data

In order to avoid building a large component to perform type-wise normalisation in the KGCN pipeline, try normalising
the knowledge graph instead.
"""
import grakn
import kglib.kgcn.management.grakn as grakn_mgmt

"""measured-quantity sub attribute, datatype double;"""

# Use compute to find the mean and standard deviation of all measured-quantity values on the training set

# Find instances of measured-quantity, and it's owner
# For each instance, calculate the normalised value
# Add the normed attribute
# Delete the old attribute


URI = "localhost:48555"

TRAIN = 'train'
EVAL = 'eval'
PREDICT = 'predict'

KEYSPACES = {
    TRAIN: "animaltrade_train",
    EVAL: "animaltrade_train",
    PREDICT: "animaltrade_test"
}


def main():
    client = grakn.Grakn(uri=URI)
    sessions = grakn_mgmt.get_sessions(client, KEYSPACES)

    type_label = 'measured-quantity'
    normaliser = Normaliser(type_label)
    print('Calibrating')
    normaliser.calibrate(sessions[TRAIN])
    print('Normalising training set')
    normaliser.normalise(sessions[TRAIN])
    print('Normalising test set')
    normaliser.normalise(sessions[PREDICT])


def norm(value, mean, std):
    return (value - mean) / std


class Normaliser:

    def __init__(self, type_label):
        self._type_label = type_label
        self._mean = None
        self._std = None

    def calibrate(self, session):

        transaction = session.transaction(grakn.TxType.WRITE)
        mean_query = f'compute mean of {self._type_label};'
        self._mean = next(transaction.query(mean_query)).number()

        std_query = f'compute std of {self._type_label};'
        self._std = next(transaction.query(std_query)).number()
        transaction.close()

    def normalise(self, session):
        to_add = {}
        unnormed_values = {}

        transaction = session.transaction(grakn.TxType.WRITE)
        find_query = f'match $x has {self._type_label} $t; get;'
        conceptmaps = transaction.query(find_query)

        for conceptmap in conceptmaps:
            attribute_value = conceptmap.get('t').value()
            owner_id = conceptmap.get('x').id
            normed_value = norm(attribute_value, self._mean, self._std)

            to_add[owner_id] = normed_value
            unnormed_values[owner_id] = attribute_value

        delete_all_query = f'match $x isa {self._type_label}; delete $x;'
        transaction.query(delete_all_query)
        transaction.commit()

        transaction = session.transaction(grakn.TxType.WRITE)

        print('Inserting ')
        # Insert the new values
        for owner_id, normed_value in to_add.items():
            insert_query = f'insert $x has {self._type_label} {normed_value:.12f}; $x id {owner_id};'
            transaction.query(insert_query)


if __name__ == "__main__":
    main()
