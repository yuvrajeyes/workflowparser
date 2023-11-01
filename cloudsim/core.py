from __future__ import annotations

from datetime import datetime
from typing import Final, List, Dict, Optional, Any, Union, Iterator, Collection, cast
from abc import ABC, abstractmethod
import time
import math
import copy
from TreeSet import TreeSet
from cloudsim.NetworkTopology import NetworkTopology
from cloudsim.Log import Log


class CloudSimTags:
    # Starting constant value for cloud-related tags
    BASE: Final[int] = 0

    # Starting constant value for network-related tags
    NETBASE: Final[int] = 100

    # Denotes boolean True in int value
    TRUE: Final[int] = 1

    # Denotes boolean False in int value
    FALSE: Final[int] = 0

    # Denotes the default baud rate for some CloudSim entities
    DEFAULT_BAUD_RATE: Final[int] = 9600

    # Schedules an entity without any delay
    SCHEDULE_NOW: Final[float] = 0.0

    # Denotes the end of simulation
    END_OF_SIMULATION: Final[int] = -1

    # Denotes an abrupt end of simulation
    ABRUPT_END_OF_SIMULATION: Final[int] = -2

    # Denotes insignificant simulation entity or time
    INSIGNIFICANT: Final[int] = BASE + 0

    # Sends an Experiment object between UserEntity and Broker entity
    EXPERIMENT: Final[int] = BASE + 1

    # Denotes a grid resource to be registered
    REGISTER_RESOURCE: Final[int] = BASE + 2

    # Denotes a grid resource that can support advance reservation to be registered
    REGISTER_RESOURCE_AR: Final[int] = BASE + 3

    # Denotes a list of all hostList, including the ones that can support advance reservation
    RESOURCE_LIST: Final[int] = BASE + 4

    # Denotes a list of hostList that only support advance reservation
    RESOURCE_AR_LIST: Final[int] = BASE + 5

    # Denotes grid resource characteristics information
    RESOURCE_CHARACTERISTICS: Final[int] = BASE + 6

    # Denotes grid resource allocation policy
    RESOURCE_DYNAMICS: Final[int] = BASE + 7

    # Denotes a request to get the total number of Processing Elements (PEs) of a resource
    RESOURCE_NUM_PE: Final[int] = BASE + 8

    # Denotes a request to get the total number of free Processing Elements (PEs) of a resource
    RESOURCE_NUM_FREE_PE: Final[int] = BASE + 9

    # Denotes a request to record events for statistical purposes
    RECORD_STATISTICS: Final[int] = BASE + 10

    # Denotes a request to get a statistical list
    RETURN_STAT_LIST: Final[int] = BASE + 11

    # Denotes a request to send an Accumulator object based on category into an event scheduler
    RETURN_ACC_STATISTICS_BY_CATEGORY: Final[int] = BASE + 12

    # Denotes a request to register a CloudResource entity to a regional CloudInformationService entity
    REGISTER_REGIONAL_GIS: Final[int] = BASE + 13

    # Denotes a request to get a list of other regional GIS entities from the system GIS entity
    REQUEST_REGIONAL_GIS: Final[int] = BASE + 14

    # Denotes request for grid resource characteristics information
    RESOURCE_CHARACTERISTICS_REQUEST: Final[int] = BASE + 15

    # This tag is used by an entity to send ping requests
    INFOPKT_SUBMIT: Final[int] = NETBASE + 5

    # This tag is used to return the ping request back to sender
    INFOPKT_RETURN: Final[int] = NETBASE + 6

    # Denotes the return of a Cloudlet back to sender
    CLOUDLET_RETURN: Final[int] = BASE + 20

    # Denotes the submission of a Cloudlet
    CLOUDLET_SUBMIT: Final[int] = BASE + 21

    # Denotes the submission of a Cloudlet with an acknowledgement
    CLOUDLET_SUBMIT_ACK: Final[int] = BASE + 22

    # Cancels a Cloudlet submitted in the CloudResource entity
    CLOUDLET_CANCEL: Final[int] = BASE + 23

    # Denotes the status of a Cloudlet
    CLOUDLET_STATUS: Final[int] = BASE + 24

    # Pauses a Cloudlet submitted in the CloudResource entity
    CLOUDLET_PAUSE: Final[int] = BASE + 25

    # Pauses a Cloudlet submitted in the CloudResource entity with an acknowledgement
    CLOUDLET_PAUSE_ACK: Final[int] = BASE + 26

    # Resumes a Cloudlet submitted in the CloudResource entity
    CLOUDLET_RESUME: Final[int] = BASE + 27

    # Resumes a Cloudlet submitted in the CloudResource entity with an acknowledgement
    CLOUDLET_RESUME_ACK: Final[int] = BASE + 28

    # Moves a Cloudlet to another CloudResource entity
    CLOUDLET_MOVE: Final[int] = BASE + 29

    # Moves a Cloudlet to another CloudResource entity with an acknowledgement
    CLOUDLET_MOVE_ACK: Final[int] = BASE + 30

    # Denotes a request to create a new VM in a Datacentre with acknowledgement information sent by the Datacentre
    VM_CREATE: Final[int] = BASE + 31

    # Denotes a request to create a new VM in a Datacentre with acknowledgement information sent by the Datacentre
    VM_CREATE_ACK: Final[int] = BASE + 32

    # Denotes a request to destroy a new VM in a Datacentre
    VM_DESTROY: Final[int] = BASE + 33

    # Denotes a request to destroy a new VM in a Datacentre
    VM_DESTROY_ACK: Final[int] = BASE + 34

    # Denotes a request to migrate a new VM in a Datacentre
    VM_MIGRATE: Final[int] = BASE + 35

    # Denotes a request to migrate a new VM in a Datacentre with acknowledgement information sent by the Datacentre
    VM_MIGRATE_ACK: Final[int] = BASE + 36

    # Denotes an event to send a file from a user to a datacenter
    VM_DATA_ADD: Final[int] = BASE + 37

    # Denotes an event to send a file from a user to a datacenter
    VM_DATA_ADD_ACK: Final[int] = BASE + 38

    # Denotes an event to remove a file from a datacenter
    VM_DATA_DEL: Final[int] = BASE + 39

    # Denotes an event to remove a file from a datacenter
    VM_DATA_DEL_ACK: Final[int] = BASE + 40

    # Denotes an internal event generated in a PowerDatacenter
    VM_DATACENTER_EVENT: Final[int] = BASE + 41

    # Denotes an internal event generated in a Broker
    VM_BROKER_EVENT: Final[int] = BASE + 42
    Network_Event_UP: Final[int] = BASE + 43
    Network_Event_send: Final[int] = BASE + 44
    RESOURCE_Register: Final[int] = BASE + 45
    Network_Event_DOWN: Final[int] = BASE + 46
    Network_Event_Host: Final[int] = BASE + 47
    NextCycle: Final[int] = BASE + 48

    def __init__(self):
        raise NotImplementedError("CloudSim Tags cannot be instantiated")



class Predicate(ABC):
    @abstractmethod
    def match(self, event: SimEvent) -> bool:
        pass



class PredicateAny(Predicate):
    def match(self, event: SimEvent) -> bool:
        return True



class PredicateNone(Predicate):
    def match(self, event: SimEvent) -> bool:
        return False



class PredicateFrom(Predicate):
    def __init__(self, source_ids):
        self.source_ids = source_ids

    def match(self, event: SimEvent) -> bool:
        src: int = event.get_source()
        return src in self.source_ids



class PredicateNotFrom(Predicate):
    def __init__(self, source_ids):
        self.source_ids = source_ids

    def match(self, event: SimEvent) -> bool:
        src: int = event.get_source()
        return src not in self.source_ids



class PredicateType(Predicate):
    def __init__(self, tags):
        self.tags: List[int] = list(tags)

    def match(self, event: SimEvent) -> bool:
        tag: int = event.get_tag()
        return tag in self.tags
    


class PredicateNotType(Predicate):
    def __init__(self, tags):
        self.tags: List[int] = list(tags)

    def match(self, event: SimEvent) -> bool:
        tag: int = event.get_tag()
        return tag not in self.tags



class FutureQueue:
    def __init__(self):
        self.sortedSet: TreeSet[SimEvent] = TreeSet()  # set
        self.serial: int = 0


    def __len__(self) -> int:
        return len(self.sortedSet)
    
    
    def __iter__(self) -> Iterator[SimEvent]:
        return iter(self.sortedSet)
    
    
    def ___str__(self) -> str:
        return f"{self.sortedSet}"
    

    def add_event(self, newEvent: SimEvent) -> None:  # HERE MAYBE FAULT DUE TO SORTED SET
        newEvent.set_serial(self.serial)
        self.sortedSet.add(newEvent)
        self.serial += 1


    def add_event_first(self, newEvent: SimEvent) -> None:
        newEvent.set_serial(0)
        self.sortedSet.add(newEvent)


    def iterator(self) -> Iterator[SimEvent]:
        return iter(self.sortedSet)
    

    def size(self) -> int:
        return len(self.sortedSet)
    

    def remove(self, event: SimEvent) -> bool:
        return self.sortedSet.remove(event)
    

    def removeAll(self, events: List[SimEvent]) -> bool:
        return self.sortedSet.removeAll(events)
    

    def clear(self) -> None:
        self.sortedSet.clear()


    def event_in_queue(self, event: SimEvent) -> bool:
        return event in self.sortedSet



class DeferredQueue:
    def __init__(self):
        self.lst: List[SimEvent] = []
        self.maxTime: float = -1.0

    def __len__(self) -> int:
        return len(self.lst)
    
    
    def __iter__(self):
        return iter(self.lst)
    

    def add_event(self, newEvent: SimEvent) -> None:
        event_time = newEvent.event_time()
        if event_time >= self.maxTime:
            self.lst.append(newEvent)
            self.maxTime = event_time
            return
        for index, event in enumerate(self.lst):
            if event.event_time() > event_time:
                self.lst.insert(index+1, newEvent)  # self.lst.insert(index, newEvent) previously
                return
        self.lst.append(newEvent)


    def iterator(self) -> Iterator[SimEvent]:
        return iter(self.lst)
    

    def size(self) -> int:
        return len(self.lst)
    
    
    def remove(self, event: SimEvent) -> None:
        self.lst.remove(event)


    def clear(self) -> None:
        self.lst.clear()



class SimEntity(ABC):
    RUNNABLE: Final[int] = 0
    WAITING: Final[int] = 1
    HOLDING: Final[int] = 2
    FINISHED: Final[int] = 3

    def __init__(self, name: str) -> None:
        if " " in name:
            raise ValueError("Entity names can't contain spaces.")
        self.name: str = name
        self.id: int = -1
        self.state: int = SimEntity.RUNNABLE
        self.evbuf: SimEvent = None
        CloudSim.add_entity(self)


    def get_name(self) -> str:
        return self.name
    

    def get_id(self) -> int:
        return self.id


    def schedule(self, dest: Union[int, str], delay: float, tag: int, data: Optional[object] = None) -> None:
        if (isinstance(dest, int)):
            if not CloudSim.running():
                return
            CloudSim.send(self.id, dest, delay, tag, data)
        elif (isinstance(dest, str)):
            self.schedule(CloudSim.get_entity_id(dest), delay, tag, data)


    def schedule_now(self, dest: int, tag: int, data: Optional[object] = None) -> None:
        if (isinstance(dest, int)):
            self.schedule(dest, 0, tag, data)
        elif (isinstance(dest, str)):
            self.schedule(CloudSim.get_entity_id(dest), 0, tag, data)


    def schedule_first(self, dest: int, delay: float, tag: int, data: Optional[object] = None) -> None:
        if (isinstance(dest, int)):
            if not CloudSim.running():
                return
            CloudSim.send_first(self.id, dest, delay, tag, data)
        elif (isinstance(dest, str)):
            self.schedule_first(CloudSim.get_entity_id(dest), delay, tag, data)


    def schedule_first_now(self, dest: int, tag: int, data: Optional[object] = None) -> None:
        if (isinstance(dest, int)):
            self.schedule_first(dest, 0, tag, data)
        elif (isinstance(dest, str)):
            self.schedule_first(CloudSim.get_entity_id(dest), 0, tag, data)


    def pause(self, delay: float) -> None:
        if delay < 0:
            raise ValueError("Negative delay supplied.")
        if not CloudSim.running():
            return
        CloudSim.pause(self.id, delay)


    def num_events_waiting(self, p: Predicate=None) -> int:
        if p is None:
            return CloudSim.waiting(self.id, CloudSim.SIM_ANY)
        return CloudSim.waiting(self.id, p)


    def select_event(self, p: Predicate) -> SimEvent:
        if not CloudSim.running():
            return None
        return CloudSim.select(self.id, p)


    def cancel_event(self, p: Predicate) -> SimEvent:
        if not CloudSim.running():
            return None
        return CloudSim.cancel(self.id, p)


    def get_next_event(self, p: Predicate=None) -> SimEvent:
        if p is None:
            p =  CloudSim.SIM_ANY
        if not CloudSim.running():
            return None
        if self.num_events_waiting(p) > 0:
            return self.select_event(p)
        return None
    
    
    def wait_for_event(self, p: Predicate) -> None:
        if not CloudSim.running():
            return
        CloudSim.wait(self.id, p)
        self.state = SimEntity.WAITING


    @abstractmethod
    def start_entity(self) -> None:
        pass


    @abstractmethod
    def process_event(self, ev: SimEvent) -> None:
        pass


    @abstractmethod
    def shutdown_entity(self) -> None:
        pass


    def run(self) -> None:
        ev: SimEvent = self.evbuf if self.evbuf is not None else self.get_next_event()
        while ev is not None:
            self.process_event(ev)
            if self.state != SimEntity.RUNNABLE:
                break
            ev = self.get_next_event()
        self.evbuf = None


    def clone(self) -> 'SimEntity':
        cp: SimEntity = copy.deepcopy(self)
        cp.set_name(self.name)
        cp.set_event_buffer(None)
        return cp


    def set_name(self, new_name: str) -> None:
        self.name = new_name


    def get_state(self) -> int:
        return self.state
    

    def get_event_buffer(self) -> Optional['SimEvent']:
        return self.evbuf
    

    def set_state(self, state: int) -> None:
        self.state = state


    def set_id(self, id: int) -> None:
        self.id = id


    def set_event_buffer(self, e: Optional['SimEvent']) -> None:
        self.evbuf = e


    def send(self, entity: Union[int, str], delay: float, cloudSimTag: int, data: Optional[object] = None) -> None:
        if isinstance(entity, int):
            entityId = entity
        elif isinstance(entity, str):
            entityId = CloudSim.get_entity_id(entity)
        else:
            raise ValueError("Invalid entity type")

        if entityId < 0:
            Log.print_line(f"{self.get_name()}.send(): Error - invalid entity id {entityId}")
            return
        # If delay is negative or infinite, reset it to 0.0
        if delay < 0 or math.isinf(delay):
            delay = 0
        srcId = self.get_id()
        if entityId != srcId:
            delay += self.get_network_delay(srcId, entityId)
        self.schedule(entityId, delay, cloudSimTag, data)


    def send_now(self, entityId: int, cloudSimTag: int, data: Optional[object] = None) -> None:
        self.send(entityId, 0, cloudSimTag, data)


    @staticmethod
    def get_network_delay(src: int, dst: int) -> float:
        if NetworkTopology.is_network_enabled():
            return NetworkTopology.get_delay(src, dst)
        return 0.0



class SimEvent():
    ENULL: Final[int] = 0
    SEND: Final[int] = 1
    HOLD_DONE: Final[int] = 2
    CREATE: Final[int] = 3

    def __init__(self, event_type: int = ENULL, time: float = -1.0, sourceEntityId: int = -1, destinationEntityId: int = -1, tag: int = -1, data: Optional[object] = None):
        self.event_type: int = event_type
        self.time: float = time
        self.endWaitingTime: float = -1.0  # Time when the event was removed from the queue for service
        self.sourceEntityId: int = sourceEntityId
        self.destinationEntityId: int = destinationEntityId
        self.tag: int = tag
        self.data: object = data
        self.serial: int = -1


    def __hash__(self):
        return hash((self.event_type, self.time, self.sourceEntityId, 
                self.destinationEntityId, self.tag, self.data, self.serial
                ))
    

    def __eq__(self, other):
        return (
            isinstance(other, SimEvent) and self.event_type == other.event_type and
            self.time == other.time and self.sourceEntityId == other.sourceEntityId and
            self.destinationEntityId == other.destinationEntityId and
            self.tag == other.tag and self.data == other.data and self.serial == other.serial
        )


    def set_serial(self, serial: int):
        self.serial = serial


    def set_end_waiting_time(self, endWaitingTime: float):
        self.endWaitingTime = endWaitingTime


    def __str__(self):
        return (
            f"Event tag = {self.tag} source = {self.sourceEntityId} "
            f"destination = {self.destinationEntityId}"
        )
    

    def get_event_type(self) -> int:
        return self.event_type
    

    def get_destination(self) -> int:
        return self.destinationEntityId


    def get_source(self) -> int:
        return self.sourceEntityId


    def event_time(self) -> float:
        return self.time


    def end_waiting_time(self) -> float:
        return self.end_waiting_time


    def type(self) -> int:
        return self.tag
    

    def scheduled_by(self) -> int:
        return self.sourceEntityId


    def get_tag(self) -> int:
        return self.tag


    def get_data(self) -> object:
        return self.data


    def clone(self) -> Optional[SimEvent]:
        return copy.deepcopy(self)  # SimEvent(self.event_type, self.time, self.sourceEntityId, self.destinationEntityId, self.tag, self.data)


    def set_source(self, sourceEntityId: int):
        self.sourceEntityId = sourceEntityId


    def set_destination(self, destinationEntityId: int):
        self.destinationEntityId = destinationEntityId


    def __lt__(self, other: SimEvent=None):
        if other is None:
            return 0
        elif self.time < other.time:
            return 1
        elif self.time > other.time:
            return -1
        elif self.serial < other.serial:
            return 1
        elif self == other:
            return 1
        else:
            return 0
        


class CloudSimShutdown(SimEntity):
    def __init__(self, name: str, numUser: int) -> None:
        super().__init__(name)
        self.numUser = numUser


    def process_event(self, ev: SimEvent) -> None:
        self.numUser -= 1
        if self.numUser == 0 or ev.get_tag() == CloudSimTags.ABRUPT_END_OF_SIMULATION:
            CloudSim.abruptally_terminate()


    def start_entity(self) -> None:
        pass


    def shutdown_entity(self) -> None:
        pass



class CloudInformationService(SimEntity):
    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.resList: List[int] = []
        self.arList: List[int] = []
        self.gisList: List[int] = []


    def start_entity(self) -> None:
        pass


    def process_event(self, ev: SimEvent) -> None:
        id_: int = -1  # requester id
        tag = ev.get_tag()
        data = ev.get_data()
        if tag == CloudSimTags.REGISTER_REGIONAL_GIS:
            self.gisList.append(data)
        elif tag == CloudSimTags.REQUEST_REGIONAL_GIS:
            id_ = int(data)
            super().send(id_, 0, tag, self.gisList)
        elif tag == CloudSimTags.REGISTER_RESOURCE:
            self.resList.append(data)
        elif tag == CloudSimTags.REGISTER_RESOURCE_AR:
            self.resList.append(data)
            self.arList.append(data)
        elif tag == CloudSimTags.RESOURCE_LIST:
            id_ = int(data)
            super().send(id_, 0, tag, self.resList)
        elif tag == CloudSimTags.RESOURCE_AR_LIST:
            id_ = int(data)
            super().send(id_, 0, tag, self.arList)
        else:
            self.process_other_event(ev)


    def shutdown_entity(self) -> None:
        self.notify_all_entity()


    def get_list(self) -> List[int]:
        return self.resList


    def get_adv_reserv_list(self) -> List[int]:
        return self.arList


    def resource_support_ar(self, id_: int) -> bool:
        if id_ is None or id_ < 0:
            return False
        return self.check_resource(self.arList, id_)


    def resource_exist(self, id_: int) -> bool:
        if id_ is None or id_ < 0:
            return False
        return self.check_resource(self.resList, id_)


    def process_other_event(self, ev: SimEvent) -> None:
        if ev is None:
            Log.print_line("CloudInformationService.process_other_event(): "
                            "Unable to handle a request since the event is None.")
            return
        Log.print_line("CloudInformationService.process_other_event(): " "Unable to handle a request from " +
                        CloudSim.get_entity_name(ev.get_source()) + " with event tag = " + ev.get_tag())


    def process_end_simulation(self) -> None:
        pass


    def check_resource(self, resource_list: Collection[int], id_: int) -> bool:
        if resource_list is None or id_ < 0:
            return False
        for obj in resource_list:
            if obj == id_:
                return True
        return False
    

    def notify_all_entity(self) -> None:
        Log.print_line(self.get_name() + ": Notify all CloudSim entities for shutting down.")
        self.signal_shutdown(self.resList)
        self.signal_shutdown(self.gisList)
        self.resList.clear()
        self.gisList.clear()


    def signal_shutdown(self, entity_id_list: Collection[int]) -> None:
        if entity_id_list is None:
            return
        for obj in entity_id_list:
            id_ = obj
            super().send(id_, 0, CloudSimTags.END_OF_SIMULATION)


class CloudSim:
    CLOUDSIM_VERSION_STRING: Final[str] = "3.0"
    cisId: int = -1
    shutdownId: int = -1
    cis: CloudInformationService = None
    NOT_FOUND: Final[int] = -1
    traceFlag: bool = False
    calendar: datetime = None
    terminateAt: float = -1
    minTimeBetweenEvents: float = 0.1

    entities: List[SimEntity] = []
    entitiesByName: Dict[str, SimEntity] = {}
    future: FutureQueue = FutureQueue()
    deferred: DeferredQueue = DeferredQueue()
    _clock: float = 0
    _running: bool = False
    waitPredicates: Dict[int, Predicate] = {}
    paused: bool = False
    pauseAt: int = -1
    abruptTerminate: bool = False

    def __init__(self) -> None:
        pass

    @staticmethod
    def init_common_variable(clndr: Optional[datetime], traceFlag: bool, num_user: int) -> None:
        CloudSim.initialize()
        CloudSim.traceFlag = traceFlag
        if clndr is None:
            CloudSim.calendar = datetime.now()
        else:
            CloudSim.calendar = clndr

        # create a CloudSimShutdown object
        shutdown: CloudSimShutdown = CloudSimShutdown("CloudSimShutdown", num_user)
        CloudSim.shutdownId = shutdown.get_id()


    @staticmethod
    def init(num_user: int, cal: Optional[datetime], traceFlag: bool, period_between_events: float=None) -> None:
        if period_between_events is not None:
            if period_between_events <= 0:
                raise ValueError("The minimal time between events should be positive, but is:", period_between_events)
            CloudSim.minTimeBetweenEvents = period_between_events
        try:
            CloudSim.init_common_variable(cal, traceFlag, num_user)
            CloudSim.cis = CloudInformationService("CloudInformationService")
            CloudSim.cisId = CloudSim.cis.get_id()
        except ValueError as e:
            Log.print_line("CloudSim.init(): The simulation has been terminated due to an unexpected error")
            Log.print_line(str(e))
        except Exception as e:
            Log.print_line("CloudSim.init(): The simulation has been terminated due to an unexpected error")
            Log.print_line(str(e))


    @staticmethod
    def start_simulation() -> float:
        Log.print_line("Starting CloudSim version " + CloudSim.CLOUDSIM_VERSION_STRING)
        try:
            _clock: float = CloudSim.run()
            # reset all static variables
            CloudSim.cisId = -1
            CloudSim.shutdownId = -1
            CloudSim.cis = None
            CloudSim.calendar = None
            CloudSim.traceFlag = False
            return _clock
        except ValueError as e:
            Log.print_line("CloudSim.startCloudSimulation(): Error - you haven't initialized CloudSim.")
            raise e


    @staticmethod
    def stop_simulation() -> None:
        try:
            CloudSim.run_stop()
        except ValueError:
            raise ValueError("CloudSim.stopCloudSimulation(): Error - can't stop Cloud Simulation.")


    @staticmethod
    def terminate_simulation(time: float=None) -> bool:
        if time is not None:
            if time <= CloudSim._clock:
                return False
            else:
                CloudSim.terminateAt = time
                return True
        CloudSim._running = False
        CloudSim.print_message("Simulation: Reached termination time.")
        return True


    @staticmethod
    def get_min_time_between_events() -> float:
        return CloudSim.minTimeBetweenEvents


    @staticmethod
    def get_simulation_calendar() -> datetime:
        clone = copy.deepcopy(CloudSim.calendar) if CloudSim.calendar is not None else None
        return clone
    

    @staticmethod
    def get_cloud_info_service_entity_id() -> int:
        return CloudSim.cisId


    @staticmethod
    def get_cloud_resource_list() -> List[int]:
        if CloudSim.cis is None:
            return []
        return CloudSim.cis.get_list()
    
    # SIMULATION METHODS
    @staticmethod
    def initialize() -> None:
        Log.print_line("Initialising...")
        CloudSim.entities: List[SimEntity] = []
        CloudSim.entitiesByName: Dict[str, SimEntity] = {}
        CloudSim.future: FutureQueue = FutureQueue()
        CloudSim.deferred: DeferredQueue = DeferredQueue()
        CloudSim._clock: float = 0
        CloudSim._running: bool = False
        CloudSim.waitPredicates: Dict[int, Predicate] = {}
        CloudSim.paused: bool = False
        CloudSim.pauseAt: int = -1
        CloudSim.abruptTerminate: bool = False


    # Two Standard Predicates
    SIM_ANY: PredicateAny = PredicateAny()
    SIM_NONE: PredicateNone = PredicateNone()


    # Public Access Methods
    @staticmethod
    def clock() -> float:
        return CloudSim._clock
    

    @staticmethod
    def get_num_entities() -> int:
        return len(CloudSim.entities)
    

    @staticmethod
    def get_entity(id: Union[int, str]) -> SimEntity:
        if (isinstance(id, int)):
            return CloudSim.entities[id]
        elif (isinstance(id, str)):
            return CloudSim.entitiesByName[id]
    

    @staticmethod
    def get_entity_id(name: str) -> int:
        obj: SimEntity = CloudSim.entitiesByName.get(name)
        if obj is None:
            return CloudSim.NOT_FOUND
        else:
            return obj.get_id()


    @staticmethod
    def get_entity_name(id: int) -> str:
        return CloudSim.get_entity(id).get_name()
        

    @staticmethod
    def get_entity_list() -> List[SimEntity]:
        lst: List[SimEntity] = CloudSim.entities
        return lst
    

    @staticmethod
    def add_entity(e: SimEntity) -> None:
        if CloudSim._running:
            # post an event to make this entity
            evt: SimEvent = SimEvent(SimEvent.CREATE, CloudSim._clock, 1, 0, 0, e)
            CloudSim.future.add_event(evt)
        if (e.get_id() == -1): # only add once
            id: int = len(CloudSim.entities)
            e.set_id(id)
            CloudSim.entities.append(e)
            CloudSim.entitiesByName[e.get_name()] = e


    @staticmethod
    def add_entity_dynamically(e: SimEntity) -> None:
        if e is None:
             raise ValueError("Adding null entity.")
        else:
            CloudSim.print_message(f"Adding: {e.get_name()}")
        e.start_entity()


    @staticmethod
    def run_clock_tick() -> bool:
        queueEmpty: bool = False
        for ent in CloudSim.entities:
            if ent.get_state() == SimEntity.RUNNABLE:
                ent.run()
        if CloudSim.future.size() > 0:
            to_remove: List[SimEvent] = []
            fit: TreeSet[SimEvent] = CloudSim.future.sortedSet
            first: SimEvent = fit[0]
            CloudSim.process_event(first)
            CloudSim.future.remove(first)
            fit = CloudSim.future.sortedSet
            for next_event in fit:
                if next_event.event_time()==first.event_time():
                    CloudSim.process_event(next_event)
                    to_remove.append(next_event)
            CloudSim.future.removeAll(to_remove)
        else:
            queueEmpty = True
            CloudSim._running = False
            CloudSim.print_message("Simulation: No more future events")
        return queueEmpty
    

    @staticmethod
    def run_stop() -> None:
        CloudSim.print_message("Simulation completed.")


    @staticmethod
    def hold(src: int, delay: int) -> None:
        e: SimEvent = SimEvent(SimEvent.HOLD_DONE, CloudSim._clock+delay, src)
        CloudSim.future.add_event(e)
        CloudSim.entities[src].set_state(SimEntity.HOLDING)


    @staticmethod
    def pause(src:int, delay: float) -> None:
        e: SimEvent = SimEvent(SimEvent.HOLD_DONE, CloudSim._clock+delay, src)
        CloudSim.future.add_event(e)
        CloudSim.entities[src].set_state(SimEntity.HOLDING)


    @staticmethod
    def send(src: int, dest: int, delay: float, tag: int, data: Optional[object] = None) -> None:
        if (delay < 0):
            raise ValueError("Send delay can't be negative.")
        e: SimEvent = SimEvent(SimEvent.SEND, CloudSim._clock+delay, src, dest, tag, data)
        CloudSim.future.add_event(e)


    @staticmethod
    def send_first(src: int, dest: int, delay: float, tag: int, data: Optional[object] = None) -> None:
        if (delay < 0):
            raise ValueError("Send delay can't be negative.")
        e: SimEvent = SimEvent(SimEvent.SEND, CloudSim._clock+delay, src, dest, tag, data)
        CloudSim.future.add_event_first(e)


    @staticmethod
    def wait(src: int, p: Predicate) -> None:
        CloudSim.entities[src].set_state(SimEntity.WAITING)
        if (p != CloudSim.SIM_ANY):
            # If a predicate has been used store it in order to check it
            CloudSim.waitPredicates[src] = p


    @staticmethod
    def waiting(d: int, p: Predicate) -> int:
        count: int = 0
        for event in CloudSim.deferred.iterator():
            if (event.get_destination() == d and p.match(event)):
                count += 1
        return count
    
    
    @staticmethod
    def select(src: int, p: Predicate) -> SimEvent:
        for event in CloudSim.deferred.iterator():
            if event.get_destination() == src and p.match(event):
                ev: SimEvent = event
                CloudSim.deferred.remove(ev)
                return ev
        return None
    

    @staticmethod
    def find_first_deferred(src: int, p: Predicate) -> SimEvent:
        ev: SimEvent = None
        for event in CloudSim.deferred.iterator():
            if event.get_destination() == src and p.match(event):
                ev = event
                break
        return ev
    
    
    @staticmethod
    def cancel(src: int, p: Predicate) -> SimEvent:
        ev: SimEvent = None
        for event in CloudSim.future.iterator():
            if event.get_source() == src and p.match(event):
                ev = event
                break
        CloudSim.future.remove(ev)
        return ev
    
    
    @staticmethod
    def cancelAll(src: int, p: Predicate) -> bool:
        previousSize: int = CloudSim.future.size()
        events_to_remove: List[SimEvent] = []
        for event in CloudSim.future.iterator():
            if event.get_source() == src and p.match(event):
                events_to_remove.append(event)
        CloudSim.future.removeAll(events_to_remove)
        return CloudSim.future.size() > previousSize
    

    @staticmethod
    def process_event(e: SimEvent) -> None:
        # Update the system's clock
        # if e.event_time() < CloudSim.clock():
        #     raise ValueError("Past event detected.")
        CloudSim._clock = e.event_time()
        # Ok, now process it
        event_type: int = e.get_event_type()
        if event_type == SimEvent.ENULL:
            raise ValueError("Event has a null type.")

        if event_type == SimEvent.CREATE:
            newe: SimEntity = cast(SimEntity, e.get_data())
            CloudSim.add_entity_dynamically(newe)

        elif event_type == SimEvent.SEND:
            # Check for matching wait
            dest: int = e.get_destination()
            if dest < 0:
                raise ValueError("Attempt to send to a null entity detected.")
            else:
                tag: int = e.get_tag()
                dest_ent: SimEntity = CloudSim.entities[dest]
                if dest_ent.get_state() == SimEntity.WAITING:
                    dest_obj = int(dest)
                    p: Predicate = CloudSim.waitPredicates.get(dest_obj)
                    if p is None or tag == 9999 or p.match(e):
                        dest_ent.set_event_buffer(cast(SimEntity, e.clone()))
                        dest_ent.set_state(SimEntity.RUNNABLE)
                        CloudSim.waitPredicates.pop(dest_obj)
                    else:
                        CloudSim.deferred.add_event(e)
                else:
                    CloudSim.deferred.add_event(e)

        elif event_type == SimEvent.HOLD_DONE:
            src: int = e.get_source()
            if src < 0:
                raise ValueError("Null entity holding.")
            else:
                CloudSim.entities[src].set_state(SimEntity.RUNNABLE)


    @staticmethod
    def run_start() -> None:
        CloudSim._running = True
        # Start all the entities
        for ent in CloudSim.entities:
            ent.start_entity()
        CloudSim.print_message("Entities started.")


    @staticmethod
    def running() -> bool:
        return CloudSim._running


    @staticmethod
    def pause_simulation(time: float=None) -> bool:
        if time is not None:
            if time <= CloudSim._clock:
                return False
            CloudSim.pauseAt = time
            return True
        CloudSim.paused = True
        return CloudSim.paused
        

    @staticmethod
    def resume_simulation() -> bool:
        CloudSim.paused = False
        if CloudSim.pauseAt <= CloudSim._clock:
            CloudSim.pauseAt = -1
        return not CloudSim.paused
     
     
    @staticmethod
    def run() -> float:
        if not CloudSim._running:
            CloudSim.run_start()
        while True:
            if CloudSim.run_clock_tick() or CloudSim.abruptTerminate:
                break

            if CloudSim.terminateAt > 0.0 and CloudSim._clock >= CloudSim.terminateAt:
                CloudSim.terminate_simulation()
                CloudSim._clock = CloudSim.terminateAt
                break
            if CloudSim.pauseAt != -1 and ((CloudSim.future.size() > 0 and CloudSim._clock <= CloudSim.pauseAt and CloudSim.pauseAt <=
                                         next(CloudSim.future.iterator()).event_time()) or (CloudSim.future.size() == 0 and CloudSim.pauseAt <= CloudSim._clock)):
                CloudSim.pause_simulation()
                CloudSim._clock = CloudSim.pauseAt

            while CloudSim.paused:
                try:
                    time.sleep(0.1)
                except Exception as e:
                    Log.print_line(str(e))
                pass

        _clock = CloudSim.clock()
        CloudSim.finish_simulation()
        CloudSim.run_stop()
        return _clock
    

    @staticmethod
    def finish_simulation() -> None:
        if not CloudSim.abruptTerminate:
            for ent in CloudSim.entities:
                if ent.get_state() != SimEntity.FINISHED:
                    ent.run()
        for ent in CloudSim.entities:
            ent.shutdown_entity()
        CloudSim.entities = None
        CloudSim.entitiesByName = None
        CloudSim.future = FutureQueue()
        CloudSim.deferred = None
        CloudSim._clock = 0
        CloudSim._running = False
        CloudSim.waitPredicates = None
        CloudSim.paused = False
        CloudSim.pauseAt = -1
        CloudSim.abruptTerminate = False


    @staticmethod
    def abruptally_terminate() -> None:
        CloudSim.abruptTerminate = True


    @staticmethod
    def print_message(message: str) -> None:
        Log.print_line(message)


    @staticmethod
    def is_paused() -> bool:
        return CloudSim.paused
