import sys
import operator
if sys.version_info >= (3, 0):
    from functools import reduce

import mongoengine as models
from mongoengine.queryset.visitor import Q
from mongoengine.queryset import (
    QuerySet,
    QuerySetManager,
)
from mongotree.models import Node
from mongotree.exceptions import InvalidMoveToDescendant, NodeAlreadySaved

def get_result_class(cls):
    return cls
class nested_set_query_set(QuerySet):
    def delete(self, removed_ranges=None):
        model = get_result_class(self._document)
        if removed_ranges is not None:
            super(nested_set_query_set, self).delete()

            for tree_id, drop_lft, drop_rgt in sorted(removed_ranges, reverse=True):
                model._get_close_gap(drop_lft, drop_rgt, tree_id)
        else:
            removed = {}
            for node in self.order_by('tree_id', 'lft'):
                found = False
                for rid, rnode in removed.items():
                    if node.is_descendant_of(rnode):
                        found = True
                        break
                if not found:
                    removed[node.pk] = node
            toremove = []
            ranges = []
            for id, node in removed.items():
                toremove.append(Q(lft__gte=node.lft) & Q(lft__lte=node.rgt) & Q(tree_id=node.tree_id))
                ranges.append((node.tree_id, node.lft, node.rgt))
            if toremove:
                model.objects.filter(reduce(operator.or_, toremove)).delete(removed_ranges=ranges)
class nested_set_manager(QuerySetManager):
    """Custom manager for nodes in a Nested Sets tree."""
    queryset_class = nested_set_query_set

    @staticmethod
    def get_queryset(doc_cls, queryset):
        """Sets the custom queryset as the default."""
        return queryset().order_by('tree_id', 'lft')
class nested_set_tree(Node):
    node_order_by = []

    lft = models.IntField()
    rgt = models.IntField()
    tree_id = models.IntField()
    depth = models.IntField()

    meta = {
        'indexes': [
            'lft',
            'rgt',
            'tree_id',
            'depth'
        ],
        'allow_inheritance': True
    }

    objects = nested_set_manager()

    @classmethod
    def add_root(cls, **kwargs):
        """Add root node to tree"""
        last_root = cls.get_last_root_node()
        
        if last_root and last_root.node_order_by:
          return last_root.add_sibling('sorted-sibling', **kwargs)

        if last_root:
            newtree_id = last_root.tree_id + 1
        else:
            # add first root node
            newtree_id = 1

        if len(kwargs) == 1 and 'instance' in kwargs:
            newobj = kwargs['instance']
            if newobj.pk:
                raise NodeAlreadySaved("Attemped to add a tree node that is already exists")
        else:
            # create new object
            newobj = get_result_class(cls)(**kwargs)

        newobj.depth = 1
        newobj.tree_id = newtree_id
        newobj.lft = 1
        newobj.rgt = 2
        newobj.save()
        return newobj

    @classmethod
    def _move_right(cls, tree_id, rgt, lftmove=False, incdec=2):
        if lftmove:
            lftop = 'gte'
        else:
            lftop = 'gt'
        get_result_class(cls).objects(tree_id=tree_id, rgt__gte=rgt).update(inc__rgt=incdec)
        get_result_class(cls).objects(tree_id=tree_id, rgt__gte=rgt, **{'lft__{}'.format(lftop):rgt}).update(inc__lft=incdec)

    @classmethod
    def _move_tree_right(cls, tree_id):
        get_result_class(cls).objects(tree_id__gte=tree_id).update(inc__tree_id=1)

    def add_child(self, **kwargs):
        if not self.is_leaf():
            if self.node_order_by:
                pos = 'sorted-sibling'
            else:
                pos = 'last-sibling'
            last_child = self.get_last_child()
            # last_child._cached_parend_obj self
            return last_child.add_sibling(pos, **kwargs)

        # cls.objects(tree_id=self.tree_id, rgt__gte=self.rgt).update(inc__rgt=2)
        self.__class__._move_right(self.tree_id, self.rgt, False, 2)
        self.rgt += 2

        if len(kwargs) == 1 and 'instance' in kwargs:
            # adding the passed (unsaved) instance to the tree
            newobj = kwargs['instance']
            if newobj.pk:
                raise NodeAlreadySaved("Attemped to add a tree node that is already exists")
        else:
            # creating a new object
            newobj = get_result_class(self.__class__)(**kwargs)

        newobj.tree_id = self.tree_id
        newobj.depth = self.depth + 1
        newobj.lft = self.lft + 1
        newobj.rgt = self.lft + 2

        newobj.save()

        return newobj

    def add_sibling(self, pos=None, **kwargs):
        pos = self._prepare_pos_var_for_add_sibling(pos)

        if len(kwargs) ==1 and 'instance' in kwargs:
            newobj = kwargs['instance']
            if newobj.pk:
                raise NodeAlreadySaved("Attemped to add a tree node that is already exists")
        else:
            newobj = get_result_class(self.__class__)(**kwargs)

        newobj.depth = self.depth

        target = self

        if target.is_root():
            newobj.lft = 1
            newobj.rgt = 2
            if pos == 'sorted-sibling':
                siblings = list(target.get_sorted_pos_queryset(target.get_siblings(), newobj))
                if siblings:
                    pos = 'left'
                    target = siblings[0]
                else:
                    pos = 'last-sibling'
            last_root = target.__class__.get_last_root_node()
            if ((pos == 'last-sibling') or 
                (pos == 'right' and target == last_root)
                ):
                newobj.tree_id = last_root.tree_id + 1
            else:
                newpos = {
                    'first-sibling': 1,
                    'left': target.tree_id,
                    'right': target.tree_id +1
                }[pos]
                # move tree right
                target.__class__._move_tree_right(newpos)
                newobj.tree_id = newpos
        else:
            newobj.tree_id = target.tree_id

            if pos == 'sorted-sibling':
                siblings = list(target.get_sorted_pos_queryset(target.get_siblings(), newobj))
                if siblings:
                    pos = 'left'
                    target = siblings[0]
                else:
                    pos = 'last-sibling'

            if pos in ('left', 'right', 'first-sibling'):
                siblings = list(target.get_siblings())

                if pos == 'right':
                    if target == siblings[-1]:
                        pos = 'last-sibling'
                    else:
                        pos = 'left'
                        found = False
                        for node in siblings:
                            if found:
                                target = node
                                break
                            elif node == target:
                                found = True
                elif pos == 'left':
                    if target == siblings[0]:
                        pos = 'first-sibling'
                elif pos == 'first-sibling':
                    target = siblings[0]

            move_right = self.__class__._move_right

            if pos == 'last-sibling':
                newpos = target.get_parent().rgt
                move_right(target.tree_id, newpos, False, 2)
            elif pos == 'first-sibling':
                newpos = target.lft
                move_right(target.tree_id, newpos - 1, False, 2)
            elif pos == 'left':
                newpos = target.lft
                move_right(target.tree_id, newpos, True, 2)

            newobj.lft = newpos
            newobj.rgt = newpos + 1

        newobj.save()
        return newobj

    def move(self, target, pos=None):
        pos = self._prepare_pos_var_for_move(pos)
        cls = get_result_class(self.__class__)
        parent = None

        if pos in ('first-child', 'last-child', 'sorted-child'):
            if target.is_leaf():
                parent = target
                pos = 'last-child'
            else:
                target = target.get_last_child()
                pos = {
                    'first-child': 'first-sibling',
                    'last-child': 'last-sibling',
                    'sorted-child': 'sorted-sibling'
                }[pos]

        if target.is_descendant_of(self):
            raise InvalidMoveToDescendant("Can't move node to a descendant.")

        if self == target and (
            (pos == 'left') or
            (pos in ('right', 'last-sibling') and
             target == target.get_last_sibling()) or
            (pos == 'first-sibling' and
             target == target.get_first_sibling())):
            # special cases, not actually moving the node so no need to UPDATE
            return

        if pos == 'sorted-sibling':
            siblings = list(target.get_sorted_pos_queryset(
                target.get_siblings(), self))
            if siblings:
                pos = 'left'
                target = siblings[0]
            else:
                pos = 'last-sibling'
        if pos in ('left', 'right', 'first-sibling'):
            siblings = list(target.get_siblings())

            if pos == 'right':
                if target == siblings[-1]:
                    pos = 'last-sibling'
                else:
                    pos = 'left'
                    found = False
                    for node in siblings:
                        if found:
                            target = node
                            break
                        elif node == target:
                            found = True
            if pos == 'left':
                if target == siblings[0]:
                    pos = 'first-sibling'
            if pos == 'first-sibling':
                target = siblings[0]

        move_right = cls._move_right
        gap = self.rgt - self.lft + 1
        target_tree = target.tree_id

        if pos == 'last-child':
            newpos = parent.rgt
            move_right(target.tree_id, newpos, False, gap)
        elif target.is_root():
            newpos = 1
            if pos == 'last-sibling':
                target_tree = list(target.get_siblings())[-1].tree_id + 1
            elif pos == 'first-sibling':
                target_tree = 1
                cls._move_tree_right(1)
            elif pos == 'left':
                cls._move_tree_right(target.tree_id)
        else:
            if pos == 'last-sibling':
                newpos = target.get_parent().rgt
                move_right(target.tree_id, newpos, False, gap)
            elif pos == 'first-sibling':
                newpos = target.lft
                move_right(target.tree_id, newpos - 1, False, gap)
            elif pos == 'left':
                newpos = target.lft
                move_right(target.tree_id, newpos, True, gap)

        # we reload 'self' because lft/rgt may have changed

        fromobj = cls.objects.get(pk=self.pk)
        depthdiff = target.depth - fromobj.depth
        if parent:
            depthdiff += 1

        cls.objects(tree_id=fromobj.tree_id, lft__gte=fromobj.lft, lft__lte=fromobj.rgt).update(tree_id=target_tree, inc__depth=depthdiff, inc__lft=newpos-fromobj.lft, inc__rgt=newpos-fromobj.lft)

        cls._get_close_gap(fromobj.lft, fromobj.rgt,  fromobj.tree_id)

    @classmethod
    def _get_close_gap(cls, drop_lft, drop_rgt, tree_id):
        gapsize = drop_rgt - drop_lft + 1
        get_result_class(cls).objects(tree_id=tree_id, rgt__gt=drop_lft).update(dec__rgt=gapsize)
        get_result_class(cls).objects(tree_id=tree_id, lft__gt=drop_lft).update(dec__lft=gapsize)

    @classmethod
    def load_bulk(cls, bulk_data, parent=None, keep_ids=False):
        """Loads a list/dictionary structure to the tree."""

        cls = get_result_class(cls)

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

    def get_children(self):
        return self.get_descendants().filter(depth=self.depth + 1)

    def get_depth(self):
        return self.depth

    def is_leaf(self):
        return self.rgt - self.lft == 1

    def get_root(self):
        if self.lft == 1:
            return self
        return get_result_class(self.__class__).objects.get(tree_id=self.tree_id, lft=1)

    def is_root(self):
        return self.lft == 1

    def get_siblings(self):
        if self.lft == 1:
            return self.get_root_nodes()
        return self.get_parent(True).get_children()

    @classmethod
    def dump_bulk(cls, parent=None, keep_ids=True):
        """Dumps a tree branch to a python data structure."""
        qset = cls._get_serializable_model().get_tree(parent)
        ret, lnk = [], {}
        for pyobj in qset:
            serobj = pyobj.to_mongo()
            serobj['pk'] = serobj['_id']
            fields = {k: serobj[k] for k in serobj if k not in ['pk', '_id', '_cls', 'lft', 'rgt', 'tree_id', 'depth']}
            depth = serobj['depth']

            newobj = {'data': fields}
            if keep_ids:
                newobj['id'] = serobj['pk']

            if (not parent and depth == 1) or\
               (parent and depth == parent.depth):
                ret.append(newobj)
            else:
                parentobj = pyobj.get_parent()
                parentser = lnk[parentobj.pk]
                if 'children' not in parentser:
                    parentser['children'] = []
                parentser['children'].append(newobj)
            lnk[pyobj.pk] = newobj
        return ret

    @classmethod
    def get_tree(cls, parent=None):
        cls = get_result_class(cls)
        if parent is None:
            return cls.objects()
        if parent.is_leaf():
            return cls.objects.filter(pk=parent.pk)
        return cls.objects(__raw__={"$and": [{"tree_id": parent.tree_id}, {"lft": {"$gte": parent.lft, "$lte": parent.rgt - 1}}]})

    def get_descendants(self):
        if self.is_leaf():
            return get_result_class(self.__class__).objects().none()
        return self.__class__.get_tree(self).filter(pk__ne=self.pk)

    def get_descendant_count(self):
        """:returns: the number of descendants of a node."""
        return (self.rgt - self.lft - 1) / 2

    def get_ancestors(self):
        if self.is_root():
            return get_result_class(self.__class__).objects().none()
        return get_result_class(self.__class__).objects.filter(tree_id=self.tree_id, lft__lt=self.lft, rgt__gt=self.rgt)

    def is_descendant_of(self, node):
        """
        :returns: ``True`` if the node if a descendant of another node given
            as an argument, else, returns ``False``
        """
        return (
            self.tree_id == node.tree_id and
            self.lft > node.lft and
            self.rgt < node.rgt
        )
        
    def get_parent(self, update=False):
        if self.is_root():
            return

        return list(self.get_ancestors())[-1]

    @classmethod
    def get_root_nodes(cls):
        return get_result_class(cls).objects.filter(lft=1)
