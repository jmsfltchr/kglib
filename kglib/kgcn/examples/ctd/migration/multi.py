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

from multiprocessing.dummy import Pool as ThreadPool

from grakn.client import GraknClient
from functools import partial


def migrate_batch(session, migration_func, batch):
    print(f"Processing batch {batch}")

    success = False
    while not success:
        tx = session.transaction().write()
        migration_func(batch, tx)
        try:
            tx.commit()
        except():
            success = False
            print('Unsuccessful batch')
        else:
            success = True
    print('Successful batch')


def multi_thread_batches(batches, keyspace, uri, migration_func, num_processes=None):
    client = GraknClient(uri=uri)
    session = client.session(keyspace)

    pool = ThreadPool(num_processes)
    pool.map(partial(migrate_batch, session, migration_func), batches)

    pool.close()
    pool.join()

    session.close()
    client.close()
