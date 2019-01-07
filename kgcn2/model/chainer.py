import tensorflow as tf


def chain_components(query_features, components):

    g = tf.Graph()
    with g.as_default():

        for query_str, query_graph in query_features.items():
            nodes_to = {edge[1] for edge in query_graph.edges}
            nodes_from = {edge[0] for edge in query_graph.edges}

            # nodes_only_from = set(query_graph.nodes)
            # nodes_only_from.difference_update(nodes_to)

            nodes_only_to = set(query_graph.nodes)
            nodes_only_to.difference_update(nodes_from)

            # Check we've found the starting node
            assert len(nodes_only_to) == 1

            starting_node = nodes_only_to.pop()
            assert starting_node in nodes_to

            query_count = tf.placeholder(tf.int32, (1,), name=None)
            tensor = query_count

            current_query_node = starting_node

            query_edges = set(query_graph.edges)

            for _ in query_graph.nodes:
                current_query_node_type = query_graph.nodes[current_query_node]['types'][0]
                with tf.name_scope(current_query_node_type):
                    tensor = components[current_query_node_type](tensor)

                matching_edges = set()
                for edge in query_edges:
                    if edge[1] == current_query_node:
                        matching_edges.add(edge)

                assert len(matching_edges) <= 1
                if len(matching_edges) == 1:
                    # Do up until the last iteration
                    current_query_edge = matching_edges.pop()
                    current_query_node = current_query_edge[0]

    return g
