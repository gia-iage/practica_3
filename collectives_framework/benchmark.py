import time
import statistics


class BenchmarkResult:
    def __init__(self, name, impl, time_max, time_mean, sends, recvs):
        self.name = name
        self.impl = impl
        self.time_max = time_max
        self.time_mean = time_mean
        self.sends = sends
        self.recvs = recvs


def benchmark_collective(
    backend,
    collective_fn,
    name,
    impl,
    warmup=5,
    iters=25,
):
    pid = backend.pid()
    size = backend.size()

    backend.reset_counters()

    for _ in range(warmup):
        collective_fn()

    backend.barrier()
    backend.reset_counters()

    t0 = time.perf_counter()
    for _ in range(iters):
        collective_fn()
    t1 = time.perf_counter()

    elapsed = (t1 - t0) / iters

    backend.barrier()

    times = None
    sends = backend.send_count
    recvs = backend.recv_count

    if pid == 0:
        times = [elapsed]
        sends_total = sends
        recvs_total = recvs
        for i in range(1, size):
            t_worker = backend.recv(i)
            s_worker = backend.recv(i)
            r_worker = backend.recv(i)
            times.append(t_worker)
            sends_total += s_worker
            recvs_total += r_worker

        return BenchmarkResult(
            name,
            impl,
            max(times),
            statistics.mean(times),
            sends_total // iters,
            recvs_total // iters,
        )
    else:
        backend.send(0, elapsed)
        backend.send(0, sends)
        backend.send(0, recvs)
        return None

