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

from grakn.exception.GraknError import GraknError


def migrate_batch(session, migration_func, batch):
    print(f"++ Processing batch {batch}")

    success = False
    num_attempts = 0
    while not success:
        tx = TransactionManager(session.transaction().write())

        try:
            migration_func(batch, tx)
            tx.commit()
        except GraknError as e:
            success = False
            print('** Unsuccessful batch')
            print(str(e))
            num_attempts += 1
        else:
            success = True

        if num_attempts >= 5:
            print(f'**** Unsuccessful batch from {num_attempts} attempts')
            break

    print('++ Successful batch')


def multi_thread_batches(batches, keyspace, uri, migration_func, num_processes=None):
    client = GraknClient(uri=uri)
    session = client.session(keyspace)

    pool = ThreadPool(num_processes)
    pool.map(partial(migrate_batch, session, migration_func), batches)

    pool.close()
    pool.join()

    session.close()
    client.close()


def single_thread_batches(batches, keyspace, uri, migration_func, num_processes=None):
    client = GraknClient(uri=uri)
    session = client.session(keyspace)

    for batch in batches:
        tx = TransactionManager(session.transaction().write())
        migration_func(batch, tx)
        tx.commit()

    session.close()
    client.close()


class TransactionManager:
    def __init__(self, tx):
        self._tx = tx

    def query(self, query, infer=True, exacty_one_result=False):
        answers = self._tx.query(query, infer=infer)

        if exacty_one_result:
            ans = []
            for i, answer in enumerate(answers):
                if i > 0:
                    raise GraknError(f'Encountered 2+ answers, 1 expected for query:\n {query}')
                ans.append(answer)
                return ans

            raise GraknError(f'Encountered 0 answers, 1 expected for query:\n {query}')
        else:
            return answers

    def close(self):
        self._tx.close()

    def commit(self):
        self._tx.commit()
