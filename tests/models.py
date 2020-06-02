import mongoengine as models

from mongotree.tree import nested_set_tree

class NS_TestNode(nested_set_tree):
    desc = models.StringField()

    def __str__(self):  # pragma: no cover
        return 'Node {}'.format(self.pk)


BASE_MODELS = NS_TestNode,