import time

import grakn

import kgcn.examples.animal_trade.main

limit = 10

# concept_id = 'V495800'

queries = [
    'match $x isa traded-item id {}, has item-purpose $p; limit 10; get;',
    # Not currently worthwhile due to attribute having been removed
    # 'match $x isa traded-item id {}, has item-purpose $p; $y isa traded-item, has item-purpose $p; limit 10; get;',
    
    'match $x isa traded-item id {}; $tm($x, $t) isa taxon-membership; $t isa taxon, has name $n; limit 10; get;',  # $x, $tm, $t, $n
    'match $x isa traded-item id {}; $tm($x, $t) isa taxon-membership; $t isa taxon; limit 10; get;',
    'match $x isa traded-item id {}; $tm($x) isa taxon-membership; limit 10; get;',
    'match $x isa traded-item id {}; limit 10; get;',

    'match $x isa traded-item id {}; $e($x) isa exchange; limit 10; get;',
    'match $x isa traded-item id {}; $e($x) isa exchange, has exchange-date $d; limit 10; get;',
    'match $x isa traded-item id {}; $e($x, $c) isa exchange; $c isa country; limit 10; get;',
    'match $x isa traded-item id {}; $e($x, $c) isa exchange; $c isa country, has name $n; limit 10; get;',
    'match $x isa traded-item id {}; $e($x, $c) isa exchange; $c isa country, has ISO-id $n; limit 10; get;',
    'match $x isa traded-item id {}; $e($x, $c) isa exchange; $c isa country, has CITES-participation-type $n; limit 10; get;',
    'match $x isa traded-item id {}; $e($x, $c) isa exchange; $c isa country, has CITES-participation $n; limit 10; get;',
    'match $x isa traded-item id {}; $e($x, $c) isa exchange; $c isa country, has CITES-entry-into-force-date $n; limit 10; get;',
    
    'match $x isa traded-item id {}; $q($x) isa quantification; limit 10; get;',
    'match $x isa traded-item id {}; $q($x, $m) isa quantification; $m isa measurement; limit 10; get;',
    'match $x isa traded-item id {}; $q($x, $m) isa quantification; $m isa measurement, has measured-quantity $mq; limit 10; get;',
    'match $x isa traded-item id {}; $q($x, $m) isa quantification; $m isa measurement, has unit-of-measurement $uom; limit 10; get;',
    
    'match $x isa traded-item id {}; $e($x) isa exchange; $iec($e) isa import-export-correspondence; limit 10; get;',
]


# TODO Rephrase the queries to execute as a tree structure of types? (actually a graph since types can occur more than once in the tree)


# TODO What about non-chain queries
'match $x isa traded-item id {}; $e($x, $c) isa exchange; $c isa country, has name $n; limit 10; get;'

uri = "localhost:48555"
keyspace = 'animaltrade_train'
client = grakn.Grakn(uri=uri)
session = client.session(keyspace)
tx = session.transaction(grakn.TxType.WRITE)

BASE_PATH = 'dataset/30_concepts_ordered_7x2x2_without_attributes_with_rules/'
labels_file_root = BASE_PATH + 'labels/labels_{}.p'
concepts, labels = kgcn.examples.animal_trade.main.retrieve_persisted_labelled_examples(tx, labels_file_root.format(
    'train'))

total_start_time = time.time()
for concept in concepts:
    print(f'\n\n---- concept {concept.id}')
    for query in queries:
        start_time = time.time()
        response = list(tx.query(query.format(concept.id)))
        count = len(response)
        duration = time.time() - start_time
        print(f'time: {duration:.3f} -count: {count} -query: {query}')

total_duration = time.time() - total_start_time
print(f'\n\ntotal duration: {total_duration}')

tx.close()
