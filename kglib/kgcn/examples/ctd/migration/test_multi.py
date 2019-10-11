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

import multiprocessing as mp

from grakn.client import GraknClient


def process(keyspace, uri, migration_func, q, iolock):
    with iolock:
        print('Processing')
    # time.sleep(3)
    with GraknClient(uri=uri) as client:
        pass
        # with client.session(keyspace) as session:
        #     pass
    # client = GraknClient(uri=uri)
    # session = client.session(keyspace)
    # session.close()
    # client.close()
    while True:
    # while not q.empty():
        batch = q.get()
        if batch is None:
            break

        with iolock:
            print(f"processing batch {batch}")

        # success = False
        # while not success:
        #     tx = session.transaction().write()
        #     success = migration_func(batch, tx)
        #     with iolock:
        #         print('Unsuccessful batch')


def multi_process_batches(batches, keyspace, uri, migration_func, num_processes=None):
    if num_processes is None:
        num_processes = mp.cpu_count()

    q = mp.Queue(maxsize=num_processes)
    # q = mp.Queue()
    iolock = mp.Lock()
    client = GraknClient(uri=uri)
    session = client.session(keyspace)

    # pool = mp.Pool(num_processes, initializer=process, initargs=(keyspace, uri, migration_func, q, iolock))
    pool = mp.Pool(num_processes, initializer=process, initargs=(session, migration_func, q, iolock))
    for i, batch in enumerate(batches):
        with iolock:
            print(f'Queuing batch {i}')
        q.put(batch)  # blocks until queue is below its max size

    for _ in range(num_processes):
        q.put(None)

    pool.close()
    pool.join()

    session.close()
    client.close()
