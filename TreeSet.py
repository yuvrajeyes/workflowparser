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
        # try:
            self._treeset.remove(element)
            return True
        # except ValueError:
        #     return False
        
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



# @staticmethod
#     def run_clock_tick() -> bool:
#         print("CloudSim.future.size():", CloudSim.future.size())
#         for ent in CloudSim.entities:
#             if ent.get_state() == SimEntity.RUNNABLE:
#                 ent.run()
#         if CloudSim.future.size() > 0:
#             to_remove: List[SimEvent] = []
#             fit: Iterator[SimEvent] = CloudSim.future.iterator()
#             queueEmpty: bool = False
        #     events_processed = False
        #     first = None
        #     for event in fit:
        #         if first is None:
        #             first = event
        #         elif event.event_time() == first.event_time():
        #             to_remove.append(event)
        #         else:
        #             break

        #     if first is not None:
        #         CloudSim.process_event(first)
        #         CloudSim.future.remove(first)
        #         fit = CloudSim.future.iterator()
        #         CloudSim.future.removeAll(to_remove)
        #         print("CloudSim.future.size():", CloudSim.future.size())
        #         events_processed = True

        #     if not events_processed:
        #         queueEmpty = True
            
        # else:
        #     queueEmpty = True
        #     CloudSim._running = False
        #     CloudSim.print_message("Simulation: No more future events")
        # return queueEmpty
    