from .base import Backend


class MPIBackend(Backend):
    def __init__(self):
        from mpi4py import MPI
        super().__init__(latency_model=None)
        self.comm = MPI.COMM_WORLD

    def size(self):
        return self.comm.Get_size()

    def pid(self):
        return self.comm.Get_rank()

    def send(self, dst, data):
        self.comm.send(data, dest=dst)
        self.send_count += 1

    def recv(self, src):
        self.recv_count += 1
        return self.comm.recv(source=src)

    def barrier(self):
        self.comm.Barrier()

