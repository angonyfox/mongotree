from mongoengine import connect, disconnect
import pytest

from . import models
from mongotree.exceptions import InvalidPosition, MissingNodeOrderBy, NodeAlreadySaved, InvalidMoveToDescendant

BASE_DATA = [
    {'data': {'desc': '1'}},
    {'data': {'desc': '2'}, 'children': [
        {'data': {'desc': '21'}},
        {'data': {'desc': '22'}},
        {'data': {'desc': '23'}, 'children': [
            {'data': {'desc': '231'}},
        ]},
        {'data': {'desc': '24'}},
    ]},
    {'data': {'desc': '3'}},
    {'data': {'desc': '4'}, 'children': [
        {'data': {'desc': '41'}},
    ]}]
UNCHANGED = [
    ('1', 1, 0),
    ('2', 1, 4),
    ('21', 2, 0),
    ('22', 2, 0),
    ('23', 2, 1),
    ('231', 3, 0),
    ('24', 2, 0),
    ('3', 1, 0),
    ('4', 1, 1),
    ('41', 2, 0)]

def _prepare_db_test(request):
    return request.param

def idfn(fixture_value):
    return fixture_value.__name__


@pytest.fixture(scope='function', params=models.BASE_MODELS, ids=idfn)
def model(request):
    return _prepare_db_test(request)

@pytest.fixture(scope='function', params=models.SORTED_MODELS, ids=idfn)
def sorted_model(request):
    return _prepare_db_test(request)


@pytest.fixture(scope='function', params=models.RELATED_MODELS, ids=idfn)
def related_model(request):
    return _prepare_db_test(request)


@pytest.fixture(scope='function', params=models.INHERITED_MODELS, ids=idfn)
def inherited_model(request):
    return _prepare_db_test(request)

class TestTreeBase(object):

    def setup_method(self):
        connect('mongoenginetest', host='mongomock://localhost')

    def teardown_method(self):
        models.empty_models_tables(models.BASE_MODELS + models.SORTED_MODELS)
        disconnect()

    def got(self, model):
        if model in [models.NS_TestNode]:
            # this slows down nested sets tests quite a bit, but it has the
            # advantage that we'll check the node edges are correct
            d = {}
            for tree_id, lft, rgt in model.objects.values_list('tree_id',
                                                               'lft',
                                                               'rgt'):
                d.setdefault(tree_id, []).extend([lft, rgt])
            for tree_id, got_edges in d.items():
                assert len(got_edges) == max(got_edges)
                good_edges = list(range(1, len(got_edges) + 1))
                assert sorted(got_edges) == good_edges

        return [(o.desc, o.get_depth(), o.get_children_count())
                for o in model.get_tree()]

class TestEmptyTree(TestTreeBase):

    def test_load_bulk_empty(self, model):
        ids = model.load_bulk(BASE_DATA)
        got_descs = [obj.desc
                     for obj in model.objects.filter(id__in=ids)]
        expected_descs = [x[0] for x in UNCHANGED]
        assert sorted(got_descs) == sorted(expected_descs)
        assert self.got(model) == UNCHANGED

    def test_dump_bulk_empty(self, model):
        assert model.dump_bulk() == []

    def test_add_root_empty(self, model):
        model.add_root(desc='1')
        expected = [('1', 1, 0)]
        assert self.got(model) == expected

    def test_get_root_nodes_empty(self, model):
        got = model.get_root_nodes()
        expected = []
        assert [node.desc for node in got] == expected

    def test_get_first_root_node_empty(self, model):
        got = model.get_first_root_node()
        assert got is None

    def test_get_last_root_node_empty(self, model):
        got = model.get_last_root_node()
        assert got is None

    def test_get_tree(self, model):
        got = list(model.get_tree())
        assert got == []

class TestNonEmptyTree(object):

    # @classmethod
    # def setup_class(cls):
    def setup_method(self):
        connect('mongoenginetest', host='mongomock://localhost')
        # connect()
        for model in models.BASE_MODELS:
            model.load_bulk(BASE_DATA)

    # @classmethod
    # def teardown_class(cls):
    def teardown_method(self):
        models.empty_models_tables(models.BASE_MODELS + models.RELATED_MODELS)
        disconnect()

    def got(self, model):
        return TestTreeBase().got(model)

class TestClassMethods(TestNonEmptyTree):

    def test_load_bulk_existing(self, model):
        # inserting on an existing node
        node = model.objects.get(desc='231')
        ids = model.load_bulk(BASE_DATA, node)
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 4),
                    ('1', 4, 0),
                    ('2', 4, 4),
                    ('21', 5, 0),
                    ('22', 5, 0),
                    ('23', 5, 1),
                    ('231', 6, 0),
                    ('24', 5, 0),
                    ('3', 4, 0),
                    ('4', 4, 1),
                    ('41', 5, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        expected_descs = ['1', '2', '21', '22', '23', '231', '24',
                          '3', '4', '41']
        got_descs = [obj.desc for obj in model.objects.filter(id__in=ids)]
        assert sorted(got_descs) == sorted(expected_descs)
        assert self.got(model) == expected

    def test_get_tree_all(self, model):
        nodes = model.get_tree()
        got = [(o.desc, o.get_depth(), o.get_children_count())
               for o in nodes]
        assert got == UNCHANGED
        assert all([type(o) == model for o in nodes])

    def test_dump_bulk_all(self, model):
        assert model.dump_bulk(keep_ids=False) == BASE_DATA

    def test_get_tree_node(self, model):
        node = model.objects.get(desc='231')
        model.load_bulk(BASE_DATA, node)

        # the tree was modified by load_bulk, so we reload our node object
        node = model.objects.get(pk=node.pk)

        nodes = model.get_tree(node)
        got = [(o.desc, o.get_depth(), o.get_children_count())
               for o in nodes]
        expected = [('231', 3, 4),
                    ('1', 4, 0),
                    ('2', 4, 4),
                    ('21', 5, 0),
                    ('22', 5, 0),
                    ('23', 5, 1),
                    ('231', 6, 0),
                    ('24', 5, 0),
                    ('3', 4, 0),
                    ('4', 4, 1),
                    ('41', 5, 0)]
        assert got == expected
        assert all([type(o) == model for o in nodes])

    def test_get_tree_leaf(self, model):
        node = model.objects.get(desc='1')

        assert 0 == node.get_children_count()
        nodes = model.get_tree(node)
        got = [(o.desc, o.get_depth(), o.get_children_count())
               for o in nodes]
        expected = [('1', 1, 0)]
        assert got == expected
        assert all([type(o) == model for o in nodes])

    def test_dump_bulk_node(self, model):
        node = model.objects.get(desc='231')
        model.load_bulk(BASE_DATA, node)

        # the tree was modified by load_bulk, so we reload our node object
        node = model.objects.get(pk=node.pk)

        got = model.dump_bulk(node, False)
        expected = [{'data': {'desc': '231'}, 'children': BASE_DATA}]
        assert got == expected

    def test_load_and_dump_bulk_keeping_ids(self, model):
        exp = model.dump_bulk(keep_ids=True)
        model.objects.all().delete()
        model.load_bulk(exp, None, True)
        got = model.dump_bulk(keep_ids=True)
        assert got == exp
        # do we really have an unchaged tree after the dump/delete/load?
        got = [(o.desc, o.get_depth(), o.get_children_count())
               for o in model.get_tree()]
        assert got == UNCHANGED

    def test_load_and_dump_bulk_with_fk(self, related_model):
        related_model.objects.all().delete()
        related = models.RelatedModel.objects.modify(
            desc="Test %s" % related_model.__name__, upsert=True)
    
        related_data = [
            {'data': {'desc': '1', 'related': related.pk}},
            {'data': {'desc': '2', 'related': related.pk}, 'children': [
                {'data': {'desc': '21', 'related': related.pk}},
                {'data': {'desc': '22', 'related': related.pk}},
                {'data': {'desc': '23', 'related': related.pk}, 'children': [
                    {'data': {'desc': '231', 'related': related.pk}},
                ]},
                {'data': {'desc': '24', 'related': related.pk}},
            ]},
            {'data': {'desc': '3', 'related': related.pk}},
            {'data': {'desc': '4', 'related': related.pk}, 'children': [
                {'data': {'desc': '41', 'related': related.pk}},
            ]}]
        related_model.load_bulk(related_data)
        got = related_model.dump_bulk(keep_ids=False)
        assert got == related_data

    def test_get_root_nodes(self, model):
        got = model.get_root_nodes()
        expected = ['1', '2', '3', '4']
        assert [node.desc for node in got] == expected
        assert all([type(node) == model for node in got])

    def test_get_first_root_node(self, model):
        got = model.get_first_root_node()
        assert got.desc == '1'
        assert type(got) == model

    def test_get_last_root_node(self, model):
        got = model.get_last_root_node()
        assert got.desc == '4'
        assert type(got) == model

    def test_add_root(self, model):
        obj = model.add_root(desc='5')
        assert obj.get_depth() == 1
        got = model.get_last_root_node()
        assert got.desc == '5'
        assert type(got) == model

    def test_add_root_with_passed_instance(self, model):
        obj = model(desc='5')
        result = model.add_root(instance=obj)
        assert result == obj
        got = model.get_last_root_node()
        assert got.desc == '5'
        assert type(got) == model

    def test_add_root_with_already_saved_instance(self, model):
        obj = model.objects.get(desc='4')
        with pytest.raises(NodeAlreadySaved):
            model.add_root(instance=obj)

class TestSimpleNodeMethods(TestNonEmptyTree):
    def test_is_root(self, model):
        data = [
            ('2', True),
            ('1', True),
            ('4', True),
            ('21', False),
            ('24', False),
            ('22', False),
            ('231', False),
        ]
        for desc, expected in data:
            got = model.objects.get(desc=desc).is_root()
            assert got == expected

    def test_is_leaf(self, model):
        data = [
            ('2', False),
            ('23', False),
            ('231', True),
        ]
        for desc, expected in data:
            got = model.objects.get(desc=desc).is_leaf()
            assert got == expected

    def test_get_root(self, model):
        data = [
            ('2', '2'),
            ('1', '1'),
            ('4', '4'),
            ('21', '2'),
            ('24', '2'),
            ('22', '2'),
            ('231', '2'),
        ]
        for desc, expected in data:
            node = model.objects.get(desc=desc).get_root()
            assert node.desc == expected
            assert type(node) == model

    def test_get_parent(self, model):
        data = [
            ('2', None),
            ('1', None),
            ('4', None),
            ('21', '2'),
            ('24', '2'),
            ('22', '2'),
            ('231', '23'),
        ]
        data = dict(data)
        objs = {}
        for desc, expected in data.items():
            node = model.objects.get(desc=desc)
            parent = node.get_parent()
            if expected:
                assert parent.desc == expected
                assert type(parent) == model
            else:
                assert parent is None
            objs[desc] = node
            # corrupt the objects' parent cache
            node._parent_obj = 'CORRUPTED!!!'

        for desc, expected in data.items():
            node = objs[desc]
            # asking get_parent to not use the parent cache (since we
            # corrupted it in the previous loop)
            parent = node.get_parent(True)
            if expected:
                assert parent.desc == expected
                assert type(parent) == model
            else:
                assert parent is None

    def test_get_children(self, model):
        data = [
            ('2', ['21', '22', '23', '24']),
            ('23', ['231']),
            ('231', []),
        ]
        for desc, expected in data:
            children = model.objects.get(desc=desc).get_children()
            assert [node.desc for node in children] == expected
            assert all([type(node) == model for node in children])

    def test_get_children_count(self, model):
        data = [
            ('2', 4),
            ('23', 1),
            ('231', 0),
        ]
        for desc, expected in data:
            got = model.objects.get(desc=desc).get_children_count()
            assert got == expected

    def test_get_siblings(self, model):
        data = [
            ('2', ['1', '2', '3', '4']),
            ('21', ['21', '22', '23', '24']),
            ('231', ['231']),
        ]
        for desc, expected in data:
            siblings = model.objects.get(desc=desc).get_siblings()
            assert [node.desc for node in siblings] == expected
            assert all([type(node) == model for node in siblings])

    def test_get_first_sibling(self, model):
        data = [
            ('2', '1'),
            ('1', '1'),
            ('4', '1'),
            ('21', '21'),
            ('24', '21'),
            ('22', '21'),
            ('231', '231'),
        ]
        for desc, expected in data:
            node = model.objects.get(desc=desc).get_first_sibling()
            assert node.desc == expected
            assert type(node) == model

    def test_get_prev_sibling(self, model):
        data = [
            ('2', '1'),
            ('1', None),
            ('4', '3'),
            ('21', None),
            ('24', '23'),
            ('22', '21'),
            ('231', None),
        ]
        for desc, expected in data:
            node = model.objects.get(desc=desc).get_prev_sibling()
            if expected is None:
                assert node is None
            else:
                assert node.desc == expected
                assert type(node) == model

    def test_get_next_sibling(self, model):
        data = [
            ('2', '3'),
            ('1', '2'),
            ('4', None),
            ('21', '22'),
            ('24', None),
            ('22', '23'),
            ('231', None),
        ]
        for desc, expected in data:
            node = model.objects.get(desc=desc).get_next_sibling()
            if expected is None:
                assert node is None
            else:
                assert node.desc == expected
                assert type(node) == model

    def test_get_last_sibling(self, model):
        data = [
            ('2', '4'),
            ('1', '4'),
            ('4', '4'),
            ('21', '24'),
            ('24', '24'),
            ('22', '24'),
            ('231', '231'),
        ]
        for desc, expected in data:
            node = model.objects.get(desc=desc).get_last_sibling()
            assert node.desc == expected
            assert type(node) == model

    def test_get_first_child(self, model):
        data = [
            ('2', '21'),
            ('21', None),
            ('23', '231'),
            ('231', None),
        ]
        for desc, expected in data:
            node = model.objects.get(desc=desc).get_first_child()
            if expected is None:
                assert node is None
            else:
                assert node.desc == expected
                assert type(node) == model

    def test_get_last_child(self, model):
        data = [
            ('2', '24'),
            ('21', None),
            ('23', '231'),
            ('231', None),
        ]
        for desc, expected in data:
            node = model.objects.get(desc=desc).get_last_child()
            if expected is None:
                assert node is None
            else:
                assert node.desc == expected
                assert type(node) == model

    def test_get_ancestors(self, model):
        data = [
            ('2', []),
            ('21', ['2']),
            ('231', ['2', '23']),
        ]
        for desc, expected in data:
            nodes = model.objects.get(desc=desc).get_ancestors()
            assert [node.desc for node in nodes] == expected
            assert all([type(node) == model for node in nodes])

    def test_get_descendants(self, model):
        data = [
            ('2', ['21', '22', '23', '231', '24']),
            ('23', ['231']),
            ('231', []),
            ('1', []),
            ('4', ['41']),
        ]
        for desc, expected in data:
            nodes = model.objects.get(desc=desc).get_descendants()
            assert [node.desc for node in nodes] == expected
            assert all([type(node) == model for node in nodes])

    def test_get_descendant_count(self, model):
        data = [
            ('2', 5),
            ('23', 1),
            ('231', 0),
            ('1', 0),
            ('4', 1),
        ]
        for desc, expected in data:
            got = model.objects.get(desc=desc).get_descendant_count()
            assert got == expected

    def test_is_sibling_of(self, model):
        data = [
            ('2', '2', True),
            ('2', '1', True),
            ('21', '2', False),
            ('231', '2', False),
            ('22', '23', True),
            ('231', '23', False),
            ('231', '231', True),
        ]
        for desc1, desc2, expected in data:
            node1 = model.objects.get(desc=desc1)
            node2 = model.objects.get(desc=desc2)
            assert node1.is_sibling_of(node2) == expected

    def test_is_child_of(self, model):
        data = [
            ('2', '2', False),
            ('2', '1', False),
            ('21', '2', True),
            ('231', '2', False),
            ('231', '23', True),
            ('231', '231', False),
        ]
        for desc1, desc2, expected in data:
            node1 = model.objects.get(desc=desc1)
            node2 = model.objects.get(desc=desc2)
            assert node1.is_child_of(node2) == expected

    def test_is_descendant_of(self, model):
        data = [
            ('2', '2', False),
            ('2', '1', False),
            ('21', '2', True),
            ('231', '2', True),
            ('231', '23', True),
            ('231', '231', False),
        ]
        for desc1, desc2, expected in data:
            node1 = model.objects.get(desc=desc1)
            node2 = model.objects.get(desc=desc2)
            assert node1.is_descendant_of(node2) == expected


class TestAddChild(TestNonEmptyTree):
    def test_add_child_to_leaf(self, model):
        model.objects.get(desc='231').add_child(desc='2311')
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 1),
                    ('2311', 4, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_add_child_to_node(self, model):
        model.objects.get(desc='2').add_child(desc='25')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('25', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_add_child_with_passed_instance(self, model):
        child = model(desc='2311')
        result = model.objects.get(desc='231').add_child(instance=child)
        assert result == child
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 1),
                    ('2311', 4, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_add_child_with_already_saved_instance(self, model):
        child = model.objects.get(desc='21')
        with pytest.raises(NodeAlreadySaved):
            model.objects.get(desc='2').add_child(instance=child)


class TestAddSibling(TestNonEmptyTree):
    def test_add_sibling_invalid_pos(self, model):
        with pytest.raises(InvalidPosition):
            model.objects.get(desc='231').add_sibling('invalid_pos')

    def test_add_sibling_missing_nodeorderby(self, model):
        node_wchildren = model.objects.get(desc='2')
        with pytest.raises(MissingNodeOrderBy):
            node_wchildren.add_sibling('sorted-sibling', desc='aaa')

    def test_add_sibling_last_root(self, model):
        node_wchildren = model.objects.get(desc='2')
        obj = node_wchildren.add_sibling('last-sibling', desc='5')
        assert obj.get_depth() == 1
        assert node_wchildren.get_last_sibling().desc == '5'

    def test_add_sibling_last(self, model):
        node = model.objects.get(desc='231')
        obj = node.add_sibling('last-sibling', desc='232')
        assert obj.get_depth() == 3
        assert node.get_last_sibling().desc == '232'

    def test_add_sibling_first_root(self, model):
        node_wchildren = model.objects.get(desc='2')
        obj = node_wchildren.add_sibling('first-sibling', desc='new')
        assert obj.get_depth() == 1
        expected = [('new', 1, 0),
                    ('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_add_sibling_first(self, model):
        node_wchildren = model.objects.get(desc='23')
        obj = node_wchildren.add_sibling('first-sibling', desc='new')
        assert obj.get_depth() == 2
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('new', 2, 0),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_add_sibling_left_root(self, model):
        node_wchildren = model.objects.get(desc='2')
        obj = node_wchildren.add_sibling('left', desc='new')
        assert obj.get_depth() == 1
        expected = [('1', 1, 0),
                    ('new', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_add_sibling_left(self, model):
        node_wchildren = model.objects.get(desc='23')
        obj = node_wchildren.add_sibling('left', desc='new')
        assert obj.get_depth() == 2
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('new', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_add_sibling_left_noleft_root(self, model):
        node = model.objects.get(desc='1')
        obj = node.add_sibling('left', desc='new')
        assert obj.get_depth() == 1
        expected = [('new', 1, 0),
                    ('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_add_sibling_left_noleft(self, model):
        node = model.objects.get(desc='231')
        obj = node.add_sibling('left', desc='new')
        assert obj.get_depth() == 3
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 2),
                    ('new', 3, 0),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_add_sibling_right_root(self, model):
        node_wchildren = model.objects.get(desc='2')
        obj = node_wchildren.add_sibling('right', desc='new')
        assert obj.get_depth() == 1
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('new', 1, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_add_sibling_right(self, model):
        node_wchildren = model.objects.get(desc='23')
        obj = node_wchildren.add_sibling('right', desc='new')
        assert obj.get_depth() == 2
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('new', 2, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_add_sibling_right_noright_root(self, model):
        node = model.objects.get(desc='4')
        obj = node.add_sibling('right', desc='new')
        assert obj.get_depth() == 1
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0),
                    ('new', 1, 0)]
        assert self.got(model) == expected

    def test_add_sibling_right_noright(self, model):
        node = model.objects.get(desc='231')
        obj = node.add_sibling('right', desc='new')
        assert obj.get_depth() == 3
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 2),
                    ('231', 3, 0),
                    ('new', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_add_sibling_with_passed_instance(self, model):
        node_wchildren = model.objects.get(desc='2')
        obj = model(desc='5')
        result = node_wchildren.add_sibling('last-sibling', instance=obj)
        assert result == obj
        assert obj.get_depth() == 1
        assert node_wchildren.get_last_sibling().desc == '5'

    def test_add_sibling_already_saved_instance(self, model):
        node_wchildren = model.objects.get(desc='2')
        existing_node = model.objects.get(desc='4')
        with pytest.raises(NodeAlreadySaved):
            node_wchildren.add_sibling('last-sibling', instance=existing_node)


class TestDelete(TestNonEmptyTree):

    # @classmethod
    # def setup_class(cls):
    def setup_method(self):     
        # connect('mongoenginetest', host='mongomock://localhost')
        # connect()        
        super(TestDelete, self).setup_method()
        for model, dep_model in zip(models.BASE_MODELS, models.DEP_MODELS):
            for node in model.objects.all():
                dep_model(node=node).save()

    # @classmethod
    # def teardown_class(cls):
    def teardown_method(self):
        models.empty_models_tables(models.DEP_MODELS + models.BASE_MODELS)

    def test_delete_leaf(self, model):
        model.objects.get(desc='231').delete()
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_delete_node(self, model):
        model.objects.get(desc='23').delete()
        expected = [('1', 1, 0),
                    ('2', 1, 3),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_delete_root(self, model):
        model.objects.get(desc='2').delete()
        expected = [('1', 1, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_delete_filter_root_nodes(self, model):
        model.objects.filter(desc__in=('2', '3')).delete()
        expected = [('1', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_delete_filter_children(self, model):
        model.objects.filter(desc__in=('2', '23', '231')).delete()
        expected = [('1', 1, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_delete_nonexistant_nodes(self, model):
        model.objects.filter(desc__in=('ZZZ', 'XXX')).delete()
        assert self.got(model) == UNCHANGED

    def test_delete_same_node_twice(self, model):
        model.objects.filter(desc__in=('2', '2')).delete()
        expected = [('1', 1, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_delete_all_root_nodes(self, model):
        model.get_root_nodes().delete()
        count = model.objects.count()
        assert count == 0

    def test_delete_all_nodes(self, model):
        model.objects.all().delete()
        count = model.objects.count()
        assert count == 0


class TestMoveErrors(TestNonEmptyTree):
    def test_move_invalid_pos(self, model):
        node = model.objects.get(desc='231')
        with pytest.raises(InvalidPosition):
            node.move(node, 'invalid_pos')

    def test_move_to_descendant(self, model):
        node = model.objects.get(desc='2')
        target = model.objects.get(desc='231')
        with pytest.raises(InvalidMoveToDescendant):
            node.move(target, 'first-sibling')

    def test_move_missing_nodeorderby(self, model):
        node = model.objects.get(desc='231')
        with pytest.raises(MissingNodeOrderBy):
            node.move(node, 'sorted-child')
        with pytest.raises(MissingNodeOrderBy):
            node.move(node, 'sorted-sibling')


class TestMoveSortedErrors(TestTreeBase):

    def test_nonsorted_move_in_sorted(self, sorted_model):
        node = sorted_model.add_root(val1=3, val2=3, desc='zxy')
        with pytest.raises(InvalidPosition):
            node.move(node, 'left')


class TestMoveLeafRoot(TestNonEmptyTree):
    def test_move_leaf_last_sibling_root(self, model):
        target = model.objects.get(desc='2')
        model.objects.get(desc='231').move(target, 'last-sibling')
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0),
                    ('231', 1, 0)]
        assert self.got(model) == expected

    def test_move_leaf_first_sibling_root(self, model):
        target = model.objects.get(desc='2')
        model.objects.get(desc='231').move(target, 'first-sibling')
        expected = [('231', 1, 0),
                    ('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_move_leaf_left_sibling_root(self, model):
        target = model.objects.get(desc='2')
        model.objects.get(desc='231').move(target, 'left')
        expected = [('1', 1, 0),
                    ('231', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_move_leaf_right_sibling_root(self, model):
        target = model.objects.get(desc='2')
        model.objects.get(desc='231').move(target, 'right')
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 0),
                    ('24', 2, 0),
                    ('231', 1, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_move_leaf_last_child_root(self, model):
        target = model.objects.get(desc='2')
        model.objects.get(desc='231').move(target, 'last-child')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 0),
                    ('24', 2, 0),
                    ('231', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_move_leaf_first_child_root(self, model):
        target = model.objects.get(desc='2')
        model.objects.get(desc='231').move(target, 'first-child')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('231', 2, 0),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected


class TestMoveLeaf(TestNonEmptyTree):
    def test_move_leaf_last_sibling(self, model):
        target = model.objects.get(desc='22')
        model.objects.get(desc='231').move(target, 'last-sibling')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 0),
                    ('24', 2, 0),
                    ('231', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_move_leaf_first_sibling(self, model):
        target = model.objects.get(desc='22')
        model.objects.get(desc='231').move(target, 'first-sibling')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('231', 2, 0),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_move_leaf_left_sibling(self, model):
        target = model.objects.get(desc='22')
        model.objects.get(desc='231').move(target, 'left')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('21', 2, 0),
                    ('231', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_move_leaf_right_sibling(self, model):
        target = model.objects.get(desc='22')
        model.objects.get(desc='231').move(target, 'right')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('231', 2, 0),
                    ('23', 2, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_move_leaf_left_sibling_itself(self, model):
        target = model.objects.get(desc='231')
        model.objects.get(desc='231').move(target, 'left')
        assert self.got(model) == UNCHANGED

    def test_move_leaf_last_child(self, model):
        target = model.objects.get(desc='22')
        model.objects.get(desc='231').move(target, 'last-child')
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 1),
                    ('231', 3, 0),
                    ('23', 2, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_move_leaf_first_child(self, model):
        target = model.objects.get(desc='22')
        model.objects.get(desc='231').move(target, 'first-child')
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 1),
                    ('231', 3, 0),
                    ('23', 2, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected


class TestMoveBranchRoot(TestNonEmptyTree):
    def test_move_branch_first_sibling_root(self, model):
        target = model.objects.get(desc='2')
        model.objects.get(desc='4').move(target, 'first-sibling')
        expected = [('4', 1, 1),
                    ('41', 2, 0),
                    ('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0)]
        assert self.got(model) == expected

    def test_move_branch_last_sibling_root(self, model):
        target = model.objects.get(desc='2')
        model.objects.get(desc='4').move(target, 'last-sibling')
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_move_branch_left_sibling_root(self, model):
        target = model.objects.get(desc='2')
        model.objects.get(desc='4').move(target, 'left')
        expected = [('1', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0)]
        assert self.got(model) == expected

    def test_move_branch_right_sibling_root(self, model):
        target = model.objects.get(desc='2')
        model.objects.get(desc='4').move(target, 'right')
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('4', 1, 1),
                    ('41', 2, 0),
                    ('3', 1, 0)]
        assert self.got(model) == expected

    def test_move_branch_left_noleft_sibling_root(self, model):
        target = model.objects.get(desc='2').get_first_sibling()
        model.objects.get(desc='4').move(target, 'left')
        expected = [('4', 1, 1),
                    ('41', 2, 0),
                    ('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0)]
        assert self.got(model) == expected

    def test_move_branch_right_noright_sibling_root(self, model):
        target = model.objects.get(desc='2').get_last_sibling()
        model.objects.get(desc='4').move(target, 'right')
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0),
                    ('4', 1, 1),
                    ('41', 2, 0)]
        assert self.got(model) == expected

    def test_move_branch_first_child_root(self, model):
        target = model.objects.get(desc='2')
        model.objects.get(desc='4').move(target, 'first-child')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('4', 2, 1),
                    ('41', 3, 0),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0)]
        assert self.got(model) == expected

    def test_move_branch_last_child_root(self, model):
        target = model.objects.get(desc='2')
        model.objects.get(desc='4').move(target, 'last-child')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('4', 2, 1),
                    ('41', 3, 0),
                    ('3', 1, 0)]
        assert self.got(model) == expected


class TestMoveBranch(TestNonEmptyTree):
    def test_move_branch_first_sibling(self, model):
        target = model.objects.get(desc='23')
        model.objects.get(desc='4').move(target, 'first-sibling')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('4', 2, 1),
                    ('41', 3, 0),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0)]
        assert self.got(model) == expected

    def test_move_branch_last_sibling(self, model):
        target = model.objects.get(desc='23')
        model.objects.get(desc='4').move(target, 'last-sibling')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('4', 2, 1),
                    ('41', 3, 0),
                    ('3', 1, 0)]
        assert self.got(model) == expected

    def test_move_branch_left_sibling(self, model):
        target = model.objects.get(desc='23')
        model.objects.get(desc='4').move(target, 'left')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('4', 2, 1),
                    ('41', 3, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0)]
        assert self.got(model) == expected

    def test_move_branch_right_sibling(self, model):
        target = model.objects.get(desc='23')
        model.objects.get(desc='4').move(target, 'right')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('4', 2, 1),
                    ('41', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0)]
        assert self.got(model) == expected

    def test_move_branch_left_noleft_sibling(self, model):
        target = model.objects.get(desc='23').get_first_sibling()
        model.objects.get(desc='4').move(target, 'left')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('4', 2, 1),
                    ('41', 3, 0),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0)]
        assert self.got(model) == expected

    def test_move_branch_right_noright_sibling(self, model):
        target = model.objects.get(desc='23').get_last_sibling()
        model.objects.get(desc='4').move(target, 'right')
        expected = [('1', 1, 0),
                    ('2', 1, 5),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 1),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('4', 2, 1),
                    ('41', 3, 0),
                    ('3', 1, 0)]
        assert self.got(model) == expected

    def test_move_branch_left_itself_sibling(self, model):
        target = model.objects.get(desc='4')
        model.objects.get(desc='4').move(target, 'left')
        assert self.got(model) == UNCHANGED

    def test_move_branch_first_child(self, model):
        target = model.objects.get(desc='23')
        model.objects.get(desc='4').move(target, 'first-child')
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 2),
                    ('4', 3, 1),
                    ('41', 4, 0),
                    ('231', 3, 0),
                    ('24', 2, 0),
                    ('3', 1, 0)]
        assert self.got(model) == expected

    def test_move_branch_last_child(self, model):
        target = model.objects.get(desc='23')
        model.objects.get(desc='4').move(target, 'last-child')
        expected = [('1', 1, 0),
                    ('2', 1, 4),
                    ('21', 2, 0),
                    ('22', 2, 0),
                    ('23', 2, 2),
                    ('231', 3, 0),
                    ('4', 3, 1),
                    ('41', 4, 0),
                    ('24', 2, 0),
                    ('3', 1, 0)]
        assert self.got(model) == expected


class TestTreeSorted(TestTreeBase):

    def teardown_method(self):
        models.empty_models_tables(models.SORTED_MODELS)
        super(TestTreeSorted, self).teardown_method()

    def got(self, sorted_model):
        return [(o.val1, o.val2, o.desc, o.get_depth(), o.get_children_count())
                for o in sorted_model.get_tree()]

    def test_add_root_sorted(self, sorted_model):
        sorted_model.add_root(val1=3, val2=3, desc='zxy')
        sorted_model.add_root(val1=1, val2=4, desc='bcd')
        sorted_model.add_root(val1=2, val2=5, desc='zxy')
        sorted_model.add_root(val1=3, val2=3, desc='abc')
        sorted_model.add_root(val1=4, val2=1, desc='fgh')
        sorted_model.add_root(val1=3, val2=3, desc='abc')
        sorted_model.add_root(val1=2, val2=2, desc='qwe')
        sorted_model.add_root(val1=3, val2=2, desc='vcx')
        expected = [(1, 4, 'bcd', 1, 0),
                    (2, 2, 'qwe', 1, 0),
                    (2, 5, 'zxy', 1, 0),
                    (3, 2, 'vcx', 1, 0),
                    (3, 3, 'abc', 1, 0),
                    (3, 3, 'abc', 1, 0),
                    (3, 3, 'zxy', 1, 0),
                    (4, 1, 'fgh', 1, 0)]
        assert self.got(sorted_model) == expected

    def test_add_child_root_sorted(self, sorted_model):
        root = sorted_model.add_root(val1=0, val2=0, desc='aaa')
        root.add_child(val1=3, val2=3, desc='zxy')
        root.add_child(val1=1, val2=4, desc='bcd')
        root.add_child(val1=2, val2=5, desc='zxy')
        root.add_child(val1=3, val2=3, desc='abc')
        root.add_child(val1=4, val2=1, desc='fgh')
        root.add_child(val1=3, val2=3, desc='abc')
        root.add_child(val1=2, val2=2, desc='qwe')
        root.add_child(val1=3, val2=2, desc='vcx')
        expected = [(0, 0, 'aaa', 1, 8),
                    (1, 4, 'bcd', 2, 0),
                    (2, 2, 'qwe', 2, 0),
                    (2, 5, 'zxy', 2, 0),
                    (3, 2, 'vcx', 2, 0),
                    (3, 3, 'abc', 2, 0),
                    (3, 3, 'abc', 2, 0),
                    (3, 3, 'zxy', 2, 0),
                    (4, 1, 'fgh', 2, 0)]
        assert self.got(sorted_model) == expected

    def test_add_child_nonroot_sorted(self, sorted_model):
        get_node = lambda node_id: sorted_model.objects.get(pk=node_id)

        root_id = sorted_model.add_root(val1=0, val2=0, desc='a').pk
        node_id = get_node(root_id).add_child(val1=0, val2=0, desc='ac').pk
        get_node(root_id).add_child(val1=0, val2=0, desc='aa')
        get_node(root_id).add_child(val1=0, val2=0, desc='av')
        get_node(node_id).add_child(val1=0, val2=0, desc='aca')
        get_node(node_id).add_child(val1=0, val2=0, desc='acc')
        get_node(node_id).add_child(val1=0, val2=0, desc='acb')

        expected = [(0, 0, 'a', 1, 3),
                    (0, 0, 'aa', 2, 0),
                    (0, 0, 'ac', 2, 3),
                    (0, 0, 'aca', 3, 0),
                    (0, 0, 'acb', 3, 0),
                    (0, 0, 'acc', 3, 0),
                    (0, 0, 'av', 2, 0)]
        assert self.got(sorted_model) == expected

    def test_move_sorted(self, sorted_model):
        sorted_model.add_root(val1=3, val2=3, desc='zxy')
        sorted_model.add_root(val1=1, val2=4, desc='bcd')
        sorted_model.add_root(val1=2, val2=5, desc='zxy')
        sorted_model.add_root(val1=3, val2=3, desc='abc')
        sorted_model.add_root(val1=4, val2=1, desc='fgh')
        sorted_model.add_root(val1=3, val2=3, desc='abc')
        sorted_model.add_root(val1=2, val2=2, desc='qwe')
        sorted_model.add_root(val1=3, val2=2, desc='vcx')
        root_nodes = sorted_model.get_root_nodes()
        target = root_nodes[0]
        for node in root_nodes[1:]:
            # because raw queries don't update django objects
            node = sorted_model.objects.get(pk=node.pk)
            target = sorted_model.objects.get(pk=target.pk)
            node.move(target, 'sorted-child')
        expected = [(1, 4, 'bcd', 1, 7),
                    (2, 2, 'qwe', 2, 0),
                    (2, 5, 'zxy', 2, 0),
                    (3, 2, 'vcx', 2, 0),
                    (3, 3, 'abc', 2, 0),
                    (3, 3, 'abc', 2, 0),
                    (3, 3, 'zxy', 2, 0),
                    (4, 1, 'fgh', 2, 0)]
        assert self.got(sorted_model) == expected

    def test_move_sortedsibling(self, sorted_model):
        # https://bitbucket.org/tabo/django-treebeard/issue/27
        sorted_model.add_root(val1=3, val2=3, desc='zxy')
        sorted_model.add_root(val1=1, val2=4, desc='bcd')
        sorted_model.add_root(val1=2, val2=5, desc='zxy')
        sorted_model.add_root(val1=3, val2=3, desc='abc')
        sorted_model.add_root(val1=4, val2=1, desc='fgh')
        sorted_model.add_root(val1=3, val2=3, desc='abc')
        sorted_model.add_root(val1=2, val2=2, desc='qwe')
        sorted_model.add_root(val1=3, val2=2, desc='vcx')
        root_nodes = sorted_model.get_root_nodes()
        target = root_nodes[0]
        for node in root_nodes[1:]:
            # because raw queries don't update django objects
            node = sorted_model.objects.get(pk=node.pk)
            target = sorted_model.objects.get(pk=target.pk)
            node.val1 -= 2
            node.save()
            node.move(target, 'sorted-sibling')
        expected = [(0, 2, 'qwe', 1, 0),
                    (0, 5, 'zxy', 1, 0),
                    (1, 2, 'vcx', 1, 0),
                    (1, 3, 'abc', 1, 0),
                    (1, 3, 'abc', 1, 0),
                    (1, 3, 'zxy', 1, 0),
                    (1, 4, 'bcd', 1, 0),
                    (2, 1, 'fgh', 1, 0)]
        assert self.got(sorted_model) == expected

class TestInheritedModels(TestTreeBase):

    def setup_method(self):
        connect('mongoenginetest', host='mongomock://localhost')

        themodels = zip(models.BASE_MODELS, models.INHERITED_MODELS)
        for model, inherited_model in themodels:
            model.add_root(desc='1')
            model.add_root(desc='2')

            node21 = inherited_model(desc='21')
            model.objects.get(desc='2').add_child(instance=node21)

            model.objects.get(desc='21').add_child(desc='211')
            model.objects.get(desc='21').add_child(desc='212')
            model.objects.get(desc='2').add_child(desc='22')

            node3 = inherited_model(desc='3')
            model.add_root(instance=node3)

    # @classmethod
    # def teardown_class(cls):
    def teardown_method(self):
        # Will also empty INHERITED_MODELS by cascade
        models.empty_models_tables(models.BASE_MODELS)
        disconnect()

    def test_get_tree_all(self, inherited_model):
        got = [(o.desc, o.get_depth(), o.get_children_count())
               for o in inherited_model.get_tree()]
        expected = [
            ('1', 1, 0),
            ('2', 1, 2),
            ('21', 2, 2),
            ('211', 3, 0),
            ('212', 3, 0),
            ('22', 2, 0),
            ('3', 1, 0),
        ]
        assert got == expected

    def test_get_tree_node(self, inherited_model):
        node = inherited_model.objects.get(desc='21')

        got = [(o.desc, o.get_depth(), o.get_children_count())
               for o in inherited_model.get_tree(node)]
        expected = [
            ('21', 2, 2),
            ('211', 3, 0),
            ('212', 3, 0),
        ]
        assert got == expected

    def test_get_root_nodes(self, inherited_model):
        got = inherited_model.get_root_nodes()
        expected = ['1', '2', '3']
        assert [node.desc for node in got] == expected

    def test_get_first_root_node(self, inherited_model):
        got = inherited_model.get_first_root_node()
        assert got.desc == '1'

    def test_get_last_root_node(self, inherited_model):
        got = inherited_model.get_last_root_node()
        assert got.desc == '3'

    def test_is_root(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert node21.is_root() is False
        assert node3.is_root() is True

    def test_is_leaf(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert node21.is_leaf() is False
        assert node3.is_leaf() is True

    def test_get_root(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert node21.get_root().desc == '2'
        assert node3.get_root().desc == '3'

    def test_get_parent(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert node21.get_parent().desc == '2'
        assert node3.get_parent() is None

    def test_get_children(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert [node.desc for node in node21.get_children()] == ['211', '212']
        assert [node.desc for node in node3.get_children()] == []

    def test_get_children_count(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert node21.get_children_count() == 2
        assert node3.get_children_count() == 0

    def test_get_siblings(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert [node.desc for node in node21.get_siblings()] == ['21', '22']
        assert [node.desc for node in node3.get_siblings()] == ['1', '2', '3']

    def test_get_first_sibling(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert node21.get_first_sibling().desc == '21'
        assert node3.get_first_sibling().desc == '1'

    def test_get_prev_sibling(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert node21.get_prev_sibling() is None
        assert node3.get_prev_sibling().desc == '2'

    def test_get_next_sibling(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert node21.get_next_sibling().desc == '22'
        assert node3.get_next_sibling() is None

    def test_get_last_sibling(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert node21.get_last_sibling().desc == '22'
        assert node3.get_last_sibling().desc == '3'

    def test_get_first_child(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert node21.get_first_child().desc == '211'
        assert node3.get_first_child() is None

    def test_get_last_child(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert node21.get_last_child().desc == '212'
        assert node3.get_last_child() is None

    def test_get_ancestors(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert [node.desc for node in node21.get_ancestors()] == ['2']
        assert [node.desc for node in node3.get_ancestors()] == []

    def test_get_descendants(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert [node.desc for node in node21.get_descendants()] == [
            '211', '212']
        assert [node.desc for node in node3.get_descendants()] == []

    def test_get_descendant_count(self, inherited_model):
        node21 = inherited_model.objects.get(desc='21')
        node3 = inherited_model.objects.get(desc='3')
        assert node21.get_descendant_count() == 2
        assert node3.get_descendant_count() == 0

    def test_cascading_deletion(self, inherited_model):
        # Deleting a node by calling delete() on the inherited_model class
        # should delete descendants, even if those descendants are not
        # instances of inherited_model
        base_model = inherited_model.__bases__[0]

        node21 = inherited_model.objects.get(desc='21')
        node21.delete()
        node2 = base_model.objects.get(desc='2')
        for desc in ['21', '211', '212']:
            assert base_model.objects.filter(desc=desc).count() == 0
        assert [node.desc for node in node2.get_descendants()] == ['22']

