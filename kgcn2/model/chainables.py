import tensorflow as tf


class DenseChainable:

    def __init__(self, num_outputs, dense_layer=tf.layers.Dense):
        self._dense_layer = dense_layer(num_outputs)

    def __call__(self, input_tensor):
        return self._dense_layer(input_tensor)
