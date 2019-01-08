"""
Chainable learning components
These components are designed to represent a top-level Grakn type. e.g. there will be a component that performs some
transformation to represent `person` or `employment`. Subtypes should chain with their parent types,
so that hierarchical transformations are constructed. In this way, whole queries can have a transformation built from
chaining these components, and the results of those queries used as the input data.

Components should interface to one another so that they can be chained. They should always require a unique count,
and have the capacity for a full count input for the case that they are the terminating end of the query.

Requirements:
-   Takes the same number of inputs as it gives outputs (should throw an error if untrue)
-   The same component (or it's characteristics e.g. weights, offsets) is reused everywhere that the type is used
-   Is accessible from the tf graph by name
-   (Possibly) Components should accept an input to indicate the proportion of results found that were unique for this
    type
"""

import unittest

import numpy as np
import tensorflow as tf

import model.chainables as chainables


class TestDenseChainable(unittest.TestCase):

    def test_input_shape_equals_output_shape(self):
        chainable = chainables.DenseChainable('person', 5)

        input_placeholder = tf.placeholder(tf.float32, shape=(20, 5))
        output_tensor = chainable(input_placeholder)

        input_vector = np.ones((20, 5))
        expected_output_vector_shape = (20, 5)

        # Create a session and do any necessary initialisation
        tf_session = tf.Session()
        init_global = tf.global_variables_initializer()

        tf_session.run(init_global)

        output_vector = tf_session.run(output_tensor, {input_placeholder: input_vector})
        self.assertEqual(expected_output_vector_shape, output_vector.shape)

    def test_if_input_shape_wrong_raise_exception(self):
        chainable = chainables.DenseChainable('person', 7)

        input_placeholder = tf.placeholder(tf.float32, shape=(20, 5))
        with self.assertRaises(ValueError) as context:
            chainable(input_placeholder)

        self.assertTrue('Input tensor has dimension 5, expected 7' in str(context.exception))

    def test_component_reuse(self):
        input_vector = np.ones((20, 5))

        def _create_dense(units):
            return tf.layers.Dense(units, activation=tf.nn.relu, kernel_initializer=tf.random_uniform_initializer)

        chainable = chainables.DenseChainable('person', 5, dense_layer=_create_dense)

        input_placeholder_1 = tf.placeholder(tf.float32, shape=(20, 5))
        output_tensor_1 = chainable(input_placeholder_1)

        input_placeholder_2 = tf.placeholder(tf.float32, shape=(20, 5))
        output_tensor_2 = chainable(input_placeholder_2)

        # Create a session and do any necessary initialisation
        tf_session = tf.Session()
        init_global = tf.global_variables_initializer()

        tf_session.run(init_global)

        output_vector_1, output_vector_2 = tf_session.run((output_tensor_1, output_tensor_2),
                                                          {input_placeholder_1: input_vector,
                                                           input_placeholder_2: input_vector})
        np.testing.assert_array_equal(output_vector_1, output_vector_2)

    def test_component_chains_with_itself(self):
        input_vector = np.ones((20, 5))

        def _create_dense(units):
            return tf.layers.Dense(units, activation=tf.nn.relu, kernel_initializer=tf.random_uniform_initializer)

        chainable = chainables.DenseChainable('person', 5, dense_layer=_create_dense)

        input_placeholder = tf.placeholder(tf.float32, shape=(20, 5))
        output_tensor_1 = chainable(input_placeholder)

        output_tensor_2 = chainable(output_tensor_1)

        # Create a session and do any necessary initialisation
        tf_session = tf.Session()
        init_global = tf.global_variables_initializer()

        tf_session.run(init_global)

        output_vector_2 = tf_session.run(output_tensor_2, {input_placeholder: input_vector})

    def test_accessible_by_name(self):
        chainable = chainables.DenseChainable('person', 5)
        input_placeholder = tf.placeholder(tf.float32, shape=(20, 5))
        output_tensor = chainable(input_placeholder)
        g = tf.get_default_graph()
        tensor = g.get_tensor_by_name('person/dense/MatMul:0')
        op = g.get_operation_by_name('person/dense/MatMul')


class TestChainableWithOutOfChainInputs(unittest.TestCase):
    def test_when_out_of_chain_input_then_output_length_correct(self):
        chainable = chainables.ChainableWithOutOfChainInputs()
        output_tensor = chainable()

