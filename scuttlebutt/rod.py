# -*- coding: utf-8 -*-

import random
import logging
from collections.abc import MutableMapping
from weakref import WeakSet
from typing import Dict

logger = logging.getLogger('scuttlebutt') # type: logging.Logger

class RandomlyOrderedDictItem(object):
    def __init__(self, key, value = None, previous_item: 'RandomlyOrderedDictItem' = None, next_item: 'RandomlyOrderedDictItem' = None):
        self.key = key
        self.previous_item = previous_item
        self.next_item = next_item
        self.iterators = WeakSet() # type: WeakSet[RandomlyOrderedDictIterator]
        self.value = value

class RandomlyOrderedDictIterator(object):
    nextItem = None # type: RandomlyOrderedDictItem
    
    def __init__(self, nextItem: RandomlyOrderedDictItem):
        self.set_next_item(nextItem)

    def set_next_item(self, item: RandomlyOrderedDictItem):
        if self.nextItem != None:
            self.nextItem.iterators.remove(self)

        self.nextItem = item

        if item != None:
            self.nextItem.iterators.add(self)
    
    def get_next_item(self) -> RandomlyOrderedDictItem:
        item = self.nextItem
        if item != None:
            self.set_next_item(item.next_item)
        return item

class RandomlyOrderedDict(MutableMapping):
    def __init__(self):
        self._root = None # type: RandomlyOrderedDictItem
        self._map = {} # type: Dict[str,RandomlyOrderedDictItem]

    def __setitem__(self, key, value):
        if value not in self._map:
            if self._root == None:
                self._root = RandomlyOrderedDictItem(key, value=value)
                self._map[key] = self._root.previous_item = self._root.next_item = self._root
            else:
                insert_before_key = random.choice(list(self._map.keys()))

                previous_item = self._map[insert_before_key].previous_item # type: RandomlyOrderedDictItem
                next_item = previous_item.next_item

                item = RandomlyOrderedDictItem(key, value=value, previous_item=previous_item, next_item=next_item)

                self._map[key] = previous_item.next_item = next_item.previous_item = item

                for iterator in next_item.iterators.copy(): # type: RandomlyOrderedDictIterator
                    if iterator != None:
                        iterator.set_next_item(item)

                if insert_before_key == self._root.key:
                    self._root = item

    def __delitem__(self, key):
        item = self._map[key] # type: RandomlyOrderedDictItem
        
        if item is item.next_item:
            next_item = None
        else:
            previous_item = item.previous_item
            next_item = item.next_item
            previous_item.next_item = next_item
            next_item.previous_item = previous_item

        if item is self._root:
            self._root = next_item

        for iterator in item.iterators.copy(): # type: RandomlyOrderedDictIterator
            if iterator != None:
                iterator.set_next_item(next_item)

        del self._map[key]

    def __getitem__(self, key):
        return self._map[key].value
    
    def __len__(self):
        return len(self._map)

    def __iter__(self):
        if len(self) == 0:
            return

        iterator = RandomlyOrderedDictIterator(self._root)

        item = iterator.get_next_item()
        while True:
            yield item.key
            item = iterator.get_next_item()
            if item is self._root:
                return
