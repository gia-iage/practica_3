"""
Payload Class: Vector Abstraction for Collective Benchmarks.

This class represents a contiguous memory buffer (vector) containing numerical data.
It provides high-level primitives required for collective communication algorithms:

1.  **Data Partitioning:** .split() method for Scatter operations.
2.  **Data Assembly:** .concat() static method for Gather operations.
3.  **Vector Arithmetic:** Overloaded (+) operator for element-wise summation (Reduce).
4.  **Metric Tracking:** .nbytes property to calculate transmission bandwidth.

It serves as the data contract between the benchmark harness and the 
communication algorithms (Naive, Tree, Ring).

"""
import math

class Payload:
    def __init__(self, data, element_size=4):
        """
        Simula un buffer de memoria contiguo (Vector).
        
        Args:
            data: An integer (converted to vector [val]) or list integer.
            element_size: Bytes per element (eg. 4 for float32/int32).
        """
        if isinstance(data, list):
            self.data = list(data)
        else:
            self.data = [data]
        self.element_size = element_size

    @property
    def nbytes(self):
        return len(self.data) * self.element_size

    @property
    def value(self):
        """Suma de control para validación"""
        return sum(self.data)

    def __len__(self):
        return len(self.data)

    def __getitem__(self, key):
        """Permite hacer slicing: payload[0:10]"""
        if isinstance(key, slice):
            return Payload(self.data[key], self.element_size)
        return self.data[key]

    def __add__(self, other):
        """
        Suma Vectorial (Element-wise).
        """
        if not isinstance(other, Payload):
            return self # Soporte para sum(..., 0)
        
        if len(self) != len(other):
            min_len = min(len(self), len(other))
            new_data = [a + b for a, b in zip(self.data[:min_len], other.data[:min_len])]
            return Payload(new_data, self.element_size)
            
        # Sumamos elemento a elemento
        new_data = [a + b for a, b in zip(self.data, other.data)]
        return Payload(new_data, self.element_size)

    def __radd__(self, other):
        return self.__add__(other)

    @staticmethod
    def concat(payloads):
        """
        Une trozos en un vector grande (Gather/Allgather).
        """
        if not payloads:
            return Payload([], 4)
        
        full_data = []
        elem_size = payloads[0].element_size
        for p in payloads:
            full_data.extend(p.data)
        return Payload(full_data, elem_size)

    def split(self, num_chunks):
        """
        Divide el vector en N trozos.
        """
        k, m = divmod(len(self.data), num_chunks)
        chunks = []
        start = 0
        for i in range(num_chunks):
            # Distribuye el resto 'm' entre los primeros trozos
            end = start + k + (1 if i < m else 0)
            chunks.append(self[start:end])
            start = end
        return chunks

    def __repr__(self):
        if len(self.data) > 5:
            return f"Payload({self.nbytes}B, val=[{self.data[0]}..{self.data[-1]}])"
        return f"Payload({self.nbytes}B, val={self.data})"
