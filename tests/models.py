import mongoengine as models
from mongotree.tree import nested_set_tree

class RelatedModel(models.DynamicDocument):
    desc = models.StringField()

    def __str__(self):
        return self.desc

class NS_TestNode(nested_set_tree):
    desc = models.StringField()

    def __str__(self):  # pragma: no cover
        return 'Node {}'.format(self.pk)

class NS_TestNodeSomeDep(models.DynamicDocument):
    node = models.ReferenceField('NS_TestNode', reverse_delete_rule=models.CASCADE)

    def __str__(self):  # pragma: no cover
        return 'Node %d' % self.pk

class NS_TestNodeRelated(nested_set_tree):
    desc = models.StringField()
    related = models.ReferenceField('RelatedModel', reverse_delete_rule=models.CASCADE)

    def __str__(self):  # pragma: no cover
        return 'Node %d' % self.pk

class NS_TestNodeInherited(NS_TestNode):
    extra_desc = models.StringField()

class NS_TestNodeSorted(nested_set_tree):
    node_order_by = ['val1', 'val2', 'desc']
    val1 = models.IntField()
    val2 = models.IntField()
    desc = models.StringField()

    def __str__(self):  # pragma: no cover
        return 'Node %d' % self.pk

BASE_MODELS = NS_TestNode,
SORTED_MODELS = NS_TestNodeSorted,
DEP_MODELS = NS_TestNodeSomeDep,
RELATED_MODELS = NS_TestNodeRelated,
INHERITED_MODELS = NS_TestNodeInherited,

def empty_models_tables(models):
    for model in models:
        model.objects.all().delete()