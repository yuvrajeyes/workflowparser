from __future__ import annotations
import bisect


class TreeSet(object):
    def __init__(self, elements=None):
        self._treeset = []
        if elements is not None:
            self.addAll(elements)


    def addAll(self, elements):
        for element in elements:
            if element in self:
                continue
            self.add(element)


    def add(self, element):
        if element not in self:
            bisect.insort(self._treeset, element)


    def ceiling(self, e):
        index = bisect.bisect_right(self._treeset, e)
        if index > 0 and self._treeset[index - 1] == e:
            return e
        return self._treeset[index] if index < len(self._treeset) else None
    

    def floor(self, e):
        index = bisect.bisect_left(self._treeset, e)
        if index > 0 and self._treeset[index] == e:
            return e
        return self._treeset[index - 1] if index > 0 else None
    

    def __getitem__(self, num):
        return self._treeset[num]
    

    def __len__(self):
        return len(self._treeset)
    
    
    def __iter__(self):
        return iter(self._treeset)


    def clear(self):
        self._treeset = []


    def clone(self):
        return TreeSet(self._treeset.copy())
    

    def remove(self, element):
            self._treeset.remove(element)
            return True
    
        
    def removeAll(self, elements):
        removed = False
        for element in elements:
            # if element in self:
                self.remove(element)
                removed = True
        return removed
    

    def __iter__(self):
        for element in self._treeset:
            yield element


    def pop(self, index):
        return self._treeset.pop(index)


    def __str__(self):
        return str(self._treeset)
    

    def __eq__(self, target):
        if isinstance(target, TreeSet):
            return self._treeset == target._treeset
        elif isinstance(target, list):
            return self._treeset == target
        

    def __contains__(self, e):
        try:
            return e == self._treeset[bisect.bisect_left(self._treeset, e)]
        except:
            return False