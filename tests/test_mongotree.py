from mongoengine import connect, disconnect
import pytest

from . import models

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


@pytest.fixture(scope='function',
                params=models.BASE_MODELS,
                ids=idfn)
def model(request):
    return _prepare_db_test(request)



class TestTreeBase(object):

    def setup_method(cls):
        connect('mongoenginetest', host='mongomock://localhost')

    def teardown_method(cls):
        for test_model in models.BASE_MODELS:
            test_model.objects.delete()
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
                for o in model.get_tree().order_by('tree_id')]

class TestEmptyTree(TestTreeBase):

    def test_load_bulk_empty(self, model):
        ids = model.load_bulk(BASE_DATA)
        got_descs = [obj.desc
                     for obj in model.objects.filter(id__in=ids)]
        expected_descs = [x[0] for x in UNCHANGED]
        assert sorted(got_descs) == sorted(expected_descs)
        assert self.got(model) == UNCHANGED

    # def test_dump_bulk_empty(self, model):
    #     assert model.dump_bulk() == []

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