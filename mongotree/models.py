import sys

import mongoengine as models
from mongotree.exceptions import InvalidPosition, MissingNodeOrderBy

if sys.version_info >= (3, 0):
    from functools import reduce


class Node(models.DynamicDocument):
    meta = {
        'abstract': True
    }

    @classmethod
    def add_root(cls, **kwargs):
        """
        Add root node to tree
        """
        raise NotImplementedError

    @classmethod
    def get_foreign_keys(cls):
        """Get foreign keys and models they refer to, so we can pre-process
        the data for load_bulk
        """
        foreign_keys = {}
        for field, field_type in cls._fields.items():
            if isinstance(field_type, models.ReferenceField) and field != "parent":
                foreign_keys[field] = field.document_type
        return foreign_keys

    @classmethod
    def _process_foreign_keys(cls, foreign_keys, node_data):
        """For each foreign key try to load the actual object so load_bulk
        doesn't fail trying to load an int where django expects a
        model instance
        """
        for key in foreign_keys.keys():
            if key in node_data:
                node_data[key] = foreign_keys[key].objects.get(
                    pk=node_data[key])

    @classmethod
    def load_bulk(cls, bulk_data, parent=None, keep_ids=False):
        """
        Loads a list/dictionary structure to the tree.
        :param bulk_data:
            The data that will be loaded, the structure is a list of
            dictionaries with 2 keys:
            - ``data``: will store arguments that will be passed for object
              creation, and
            - ``children``: a list of dictionaries, each one has it's own
              ``data`` and ``children`` keys (a recursive structure)
        :param parent:
            The node that will receive the structure as children, if not
            specified the first level of the structure will be loaded as root
            nodes
        :param keep_ids:
            If enabled, loads the nodes with the same id that are given in the
            structure. Will error if there are nodes without id info or if the
            ids are already used.
        :returns: A list of the added node ids.
        """
        # tree, iterative preorder
        added = []
        # stack of nodes to analyze
        stack = [(parent, node) for node in bulk_data[::-1]]
        foreign_keys = cls.get_foreign_keys()

        while stack:
            parent, node_struct = stack.pop()
            # shallow copy of the data structure so it doesn't persist...
            node_data = node_struct['data'].copy()
            cls._process_foreign_keys(foreign_keys, node_data)
            if keep_ids:
                node_data['id'] = node_struct['id']
            if parent:
                node_obj = parent.add_child(**node_data)
            else:
                node_obj = cls.add_root(**node_data)
            added.append(node_obj.pk)
            if 'children' in node_struct:
                # extending the stack with the current node as the parent of
                # the new nodes
                stack.extend([
                    (node_obj, node)
                    for node in node_struct['children'][::-1]
                ])
        return added

    @classmethod
    def dump_bulk(cls, parent=None, keep_ids=True):  # pragma: no cover
        """
        Dumps a tree branch to a python data structure.
        :param parent:
            The node whose descendants will be dumped. The node itself will be
            included in the dump. If not given, the entire tree will be dumped.
        :param keep_ids:
            Stores the id value (primary key) of every node. Enabled by
            default.
        :returns: A python data structure, described with detail in
                  :meth:`load_bulk`
        """
        raise NotImplementedError

    @classmethod
    def get_root_nodes(cls):
        raise NotImplementedError

    @classmethod
    def get_first_root_node(cls):
        try:
            return cls.get_root_nodes()[0]
        except IndexError:
            return None

    @classmethod
    def get_last_root_node(cls):
        try:
            nodes = cls.get_root_nodes()
            return list(nodes)[-1]
        except IndexError:
            return None

    @classmethod
    def get_tree(cls, parent=None):
        raise NotImplementedError

    def get_depth(self):
        raise NotImplementedError

    def get_siblings(self):
        raise NotImplementedError

    def get_children(self):
        raise NotImplementedError

    def get_children_count(self):
        return self.get_children().count()

    def get_descendants(self):
        raise NotImplementedError

    def get_first_child(self):
        try:
            nodes = self.get_children()
            return list(nodes)[0]
        except IndexError:
            return None

    def get_last_child(self):
        try:
            nodes = self.get_children()
            return list(nodes)[-1]
        except IndexError:
            return None    

    def add_child(self, **kwargs):
        raise NotImplementedError

    def add_sibling(self, pos=None, **kwargs):
        raise NotImplementedError

    def get_root(self):
        raise NotImplementedError

    def is_root(self):
        return self.get_root().pk == self.pk

    def is_leaf(self):
        return not self.get_children().count() > 0

    def get_ancestors(self):
        raise NotImplementedError

    def get_parent(self, update=False):
        raise NotImplementedError

    def move(self, target, pos=None):
        raise NotImplementedError

    def delete(self):
        self.__class__.objects.filter(pk=self.pk).delete()

    def _prepare_pos_var(self, pos, method_name, valid_pos, valid_sorted_pos):
        if pos is None:
            if self.node_order_by:
                pos = 'sorted-sibling'
            else:
                pos = 'last-sibling'
        if pos not in valid_pos:
            raise InvalidPosition('Invalid relative position: %s' % (pos, ))
        if self.node_order_by and pos not in valid_sorted_pos:
            raise InvalidPosition(
                'Must use %s in %s when node_order_by is enabled' % (
                    ' or '.join(valid_sorted_pos), method_name))
        if pos in valid_sorted_pos and not self.node_order_by:
            raise MissingNodeOrderBy('Missing node_order_by attribute.')
        return pos

    _valid_pos_for_add_sibling = ('first-sibling', 'left', 'right',
                                  'last-sibling', 'sorted-sibling')
    _valid_pos_for_sorted_add_sibling = ('sorted-sibling',)

    def _prepare_pos_var_for_add_sibling(self, pos):
        return self._prepare_pos_var(
            pos,
            'add_sibling',
            self._valid_pos_for_add_sibling,
            self._valid_pos_for_sorted_add_sibling)

    @classmethod
    def _get_serializable_model(cls):
        current_class = cls
        return current_class