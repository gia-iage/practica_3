import multiprocessing as mp
from .base import Backend


class MPBackend(Backend):
    def __init__(self, pid, size, queues, barrier, latency_model=None):
        super().__init__(latency_model)
        self._pid = pid
        self._size = size
        self.queues = queues
        self._barrier = barrier
        self._buffer = {i: [] for i in range(size)}

    def size(self):
        return self._size

    def pid(self):
        return self._pid

    def send(self, dst, data):
        if isinstance(data, list):
            nbytes = sum(getattr(item, "nbytes", 0) for item in data)
        else:
            nbytes = getattr(data, "nbytes", 0)
        
        # Simulamos coste de TRANSMISIÓN (Egress)
        # El emisor está ocupado poniendo datos en la red    
        if self.latency:
            self.latency.apply(nbytes)
        
        self.queues[dst].put((self._pid, data))
        self.send_count += 1

    def recv(self, src):
        # 1. Si ya tenemos el mensaje en el buffer local, lo extraemos
        if self._buffer[src]:
            data = self._buffer[src].pop(0)
        else:
            # 2. Si no, leemos de la cola hasta encontrar el del emisor 'src'
            while True:
                sender, data = self.queues[self._pid].get()
                if sender == src:
                    break
                else:
                    # Guardamos el mensaje en el buffer correcto para ese emisor
                    self._buffer[sender].append(data)

        # 3. Simulamos coste de RECEPCIÓN (Ingress / Serialización)
        # El receptor está ocupado leyendo datos de la red.
        # Esto evita que el Root reciba múltiples mensajes instantáneamente.
        if self.latency:
            nbytes = self._get_nbytes(data)
            self.latency.apply(nbytes)
        
        self.recv_count += 1
        return data

    def barrier(self):
        self._barrier.wait()

    def _get_nbytes(self, data):
        """Helper para calcular el tamaño real (soporta Payloads y Listas)"""
        if isinstance(data, list):
            return sum(getattr(item, "nbytes", 0) for item in data)
        return getattr(data, "nbytes", 0)

