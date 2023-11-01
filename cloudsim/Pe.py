from __future__ import annotations

import cloudsim.provisioners as provisioners


class Pe:
    FREE = 1
    BUSY = 2
    FAILED = 3

    def __init__(self, id: int, peProvisioner: provisioners.PeProvisioner):
        self.id: int = id
        self.status: int = self.FREE
        self.peProvisioner: provisioners.PeProvisioner = peProvisioner


    def get_id(self) -> int:
        return self.id
    

    def set_mips(self, mips: float) -> None:
        self.peProvisioner.set_mips(mips)


    def get_mips(self) -> int:
        return int(self.peProvisioner.get_mips())
    

    def set_status_free(self) -> None:
        self.set_status(self.FREE)


    def set_status_busy(self) -> None:
        self.set_status(self.BUSY)


    def set_status_failed(self) -> None:
        self.set_status(self.FAILED)


    def set_status(self, status: int) -> None:
        self.status = status


    def get_status(self) -> int:
        return self.status
    

    def set_pe_provisioner(self, peProvisioner: provisioners.PeProvisioner) -> None:
        self.peProvisioner = peProvisioner


    def get_pe_provisioner(self) -> provisioners.PeProvisioner:
        return self.peProvisioner