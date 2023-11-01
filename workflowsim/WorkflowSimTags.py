class WorkflowSimTags:
    # Starting constant value for cloud-related tags
    BASE = 1000
    
    # VM Status constants
    VM_STATUS_READY = BASE + 2
    VM_STATUS_BUSY = BASE + 3
    VM_STATUS_IDLE = BASE + 4
    
    START_SIMULATION = BASE + 0
    JOB_SUBMIT = BASE + 1
    CLOUDLET_UPDATE = BASE + 5
    CLOUDLET_CHECK = BASE + 6

    def __init__(self):
        raise NotImplementedError("WorkflowSim Tags cannot be instantiated")
