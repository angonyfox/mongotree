import mongoengine as models
from mongotree.models import Node
from mongotree.exceptions import InvalidMoveToDescendant, NodeAlreadySaved

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

    @classmethod
    def add_root(cls, **kwargs):
        """Add root node to tree"""
        last_root = cls.get_last_root_node()
        
        # if last_root and last_root.node_order_by:
        #   return last_root.add_sibling('sorted-sibling', **kwargs)

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
            newobj = cls(**kwargs)

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
        cls.objects(tree_id=tree_id, rgt__gte=rgt).update(inc__rgt=incdec)
        cls.objects(tree_id=tree_id, rgt__gte=rgt, **{'lft__{}'.format(lftop):rgt}).update(inc__rgt=incdec)

    @classmethod
    def _move_tree_right(cls, tree_id):
        cls.objects(tree_id__gte=tree_id).update(inc__tree_id=1)

    def add_child(self, **kwargs):
        cls = self.__class__
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
            newobj = cls(**kwargs)

        newobj.tree_id = self.tree_id
        newobj.depth = self.depth + 1
        newobj.lft = self.lft + 1
        newobj.rgt = self.lft + 2

        newobj.save()

        return newobj

    def add_sibling(self, pos=None, **kwargs):
        pos = self._prepare_pos_var_for_add_sibling(pos)
        cls = self.__class__

        if len(kwargs) ==1 and 'instance' in kwargs:
            newobj = kwargs['instance']
            if newobj.pk:
                raise NodeAlreadySaved("Attemped to add a tree node that is already exists")
        else:
            newobj = cls(**kwargs)

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

    @classmethod
    def get_root_nodes(cls):
        return cls.objects.filter(lft=1)

    def get_children(self):
        return self.get_descendants().filter(depth=self.depth + 1)

    def get_depth(self):
        return self.depth

    def is_leaf(self):
        return self.rgt - self.lft == 1

    def get_root(self):
        if self.lft == 1:
            return self
        return self.__class__.objects.get(tree_id=self.tree_id, lft=1)

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
            serobj = serializers.serialize('python', [pyobj])[0]
            # django's serializer stores the attributes in 'fields'
            fields = serobj['fields']
            depth = fields['depth']
            # this will be useless in load_bulk
            del fields['lft']
            del fields['rgt']
            del fields['depth']
            del fields['tree_id']
            if 'id' in fields:
                # this happens immediately after a load_bulk
                del fields['id']

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
        if parent is None:
            return cls.objects()
        if parent.is_leaf():
            return cls.objects.filter(pk=parent.pk)
        return cls.objects(__raw__={"$and": [{"tree_id": parent.tree_id}, {"lft": {"$gt": parent.lft, "$lt": parent.rgt - 1}}]})

    def get_descendants(self):
        if self.is_leaf():
            return self.__class__.objects().none()
        return self.__class__.get_tree(self).filter(pk__ne=self.pk)

    def get_ancestors(self):
        if self.is_root():
            return self.__class__.objects().none()
        return self.__class__.objects.filter(tree_id=self.tree_id, lft__lt=self.lft, rgt__gt=self.rgt)

    def get_parent(self, update=False):
        if self.is_root():
            return

        return list(self.get_ancestors())[-1]
