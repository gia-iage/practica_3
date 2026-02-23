import random
import time

class LatencyModel:
    def __init__(
        self,
        base_us=0.0,
        jitter_us=0.0,
        p99_us=0.0,
        p99_rate=0.0,
        bw_Gbps=0.0,
        enabled=True,
    ):
        self.base_us = base_us
        self.jitter_us = jitter_us
        self.p99_us = p99_us
        self.p99_rate = p99_rate
        self.bw_Gbps = bw_Gbps
        self.bw_GBs = bw_Gbps / 8.0
        self.enabled = enabled

    def apply(self, nbytes=0):
        if not self.enabled:
            return

        if self.base_us <= 0:
            return
            
        delay_us = self.base_us

        if self.jitter_us > 0:
            delay_us += random.uniform(-self.jitter_us, self.jitter_us)

        if self.p99_us > 0 and random.random() < self.p99_rate:
            delay_us += self.p99_us

        # El cálculo del retardo por ancho de banda:
        # (nbytes / (BW_en_GB/s * 10^9)) nos da los segundos, 
        # luego multiplicamos por 10^6 para pasar a microsegundos.
        if self.bw_GBs > 0 and nbytes > 0:
            delay_us += (nbytes / (self.bw_GBs * 1e9)) * 1e6

        if delay_us > 0:
            time.sleep(delay_us * 1e-6)

