import kgcn.src.neighbourhood.data.utils as utils

TARGET_PLAYS = 0  # In this case, the neighbour is a relationship in which this concept plays a role
NEIGHBOUR_PLAYS = 1  # In this case the target

ROLES_PLAYED = 0
ROLEPLAYERS = 1


class TraversalExecutor:

    FIND_CONCEPT_QUERY = {
        'query': "match $x id {}; get $x;",
        'result_var': 'x'}

    def __init__(self, grakn_tx):
        self._grakn_tx = grakn_tx

    def __call__(self, query_direction, concept_id):
        """
        Takes a query to execute and the variables to return
        :param query_direction: whether we want to retrieve roles played or role players
        :param concept_id: id for the concept to find connections for
        :return:
        """

        if query_direction not in (ROLES_PLAYED, ROLEPLAYERS):
            raise ValueError('query_direction isn\'t properly defined')

        query = self.FIND_CONCEPT_QUERY['query'].format(concept_id)
        print(query)
        target_concept = next(self._grakn_tx.query(query)).get(self.FIND_CONCEPT_QUERY['result_var'])

        if query_direction == ROLEPLAYERS:
            roles_dict = target_concept.role_players_map()
            # [(key.label(), value) for key, value in target_concept.role_players_map().items()]

        else:
            roles_dict = {}
            # roles_played = [r.label() for r in list(target_concept.roles())]
            roles_played = target_concept.roles()
            for role in roles_played:
                roles_dict[role] = target_concept.relationships(role) # TODO Add role

        def _roles_iterator():
            for role, concepts in roles_dict.items():
                role_label = role.label()
                print(role_label)
                for concept in concepts:
                    concept_info = build_concept_info(concept)

                    yield {'role_label': role_label, 'role_direction': query_direction,
                           'neighbour_info': concept_info}

        return _roles_iterator()


class ConceptInfo(utils.PropertyComparable):
    def __init__(self, id, type_label, base_type_label, data_type=None, value=None):
        self.id = id
        self.type_label = type_label
        self.base_type_label = base_type_label  # TODO rename to base_type in line with Client Python

        # If the concept is an attribute
        self.data_type = data_type
        self.value = value


def build_concept_info(concept):

    id = concept.id
    type_label = concept.type().label()
    base_type_label = concept.base_type.lower()

    assert(base_type_label in ['entity', 'relationship', 'attribute'])

    if base_type_label == 'attribute':
        data_type = concept.type().data_type().name.lower()
        assert data_type in ('long', 'double', 'boolean', 'date', 'string')
        value = concept.value()

        return ConceptInfo(id, type_label, base_type_label, data_type, value)

    return ConceptInfo(id, type_label, base_type_label)
