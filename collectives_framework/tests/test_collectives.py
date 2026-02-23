import multiprocessing as mp
from payload import Payload

class CollectiveTester:
    def __init__(self, backend_class, size=4):
        self.backend_class = backend_class
        self.size = size

    def _run_test_proc(self, pid, queues, barrier, test_name, impl_funcs, result_dict):
        """
        Ejecuta una colectiva específica usando el diccionario de implementación impl_funcs.
        """
        backend = self.backend_class(pid, self.size, queues, barrier)
        
        try:
            # 1. TEST BROADCAST
            if test_name == "broadcast":
                val = Payload(100) if pid == 0 else None
                res = impl_funcs['broadcast'](backend, val, root=0)
                assert res is not None and res.nbytes == 100, f"P{pid} falló Broadcast"

            # 2. TEST REDUCE_SUM
            elif test_name == "reduce_sum":
                val = Payload(10)
                res = impl_funcs['reduce_sum'](backend, val, root=0)
                if pid == 0:
                    assert res is not None and res.nbytes == 10 * self.size, f"Root falló Reduce"
                else:
                    assert res is None, f"P{pid} (no root) debería recibir None en Reduce"

            # 3. TEST GATHER
            elif test_name == "gather":
                val = Payload(pid)
                res = impl_funcs['gather'](backend, val, root=0)
                if pid == 0:
                    assert isinstance(res, list) and len(res) == self.size
                    for i in range(self.size):
                        assert res[i].nbytes == i, f"Gather error: índice {i} tiene {res[i].nbytes}"
                else:
                    assert res is None

            # 4. TEST SCATTER
            elif test_name == "scatter":
                data = [Payload(i) for i in range(self.size)] if pid == 0 else None
                res = impl_funcs['scatter'](backend, data, root=0)
                assert res is not None and res.nbytes == pid, f"P{pid} recibió nbytes={res.nbytes}, esperaba {pid}"

            # 5. TEST ALLGATHER
            elif test_name == "allgather":
                val = Payload(pid)
                res = impl_funcs['allgather'](backend, val)
                assert isinstance(res, list) and len(res) == self.size, f"P{pid} no recibió lista completa"
                for i in range(self.size):
                    assert res[i].nbytes == i, f"P{pid} ve error en índice {i} de Allgather"

            # 6. TEST ALLREDUCE_SUM
            elif test_name == "allreduce_sum":
                val = Payload(10)
                res = impl_funcs['allreduce_sum'](backend, val)
                expected = 10 * self.size
                assert res is not None and res.nbytes == expected, f"P{pid} obtuvo {res.nbytes}, esperaba {expected}"

            result_dict[pid] = "OK"
        except Exception as e:
            import traceback
            result_dict[pid] = f"Error: {str(e)}\n{traceback.format_exc()}"

    def run_suite(self, name_label, impl_funcs):
        """
        Ejecuta todas las pruebas para una implementación específica.
        impl_funcs debe ser un dict con: 'broadcast', 'reduce_sum', etc.
        """
        tests = ["broadcast", "reduce_sum", "gather", "scatter", "allgather", "allreduce_sum"]
        print(f"\n=== Verificando Implementación: {name_label} ({self.size} procesos) ===")
        
        all_passed = True
        for t in tests:
            if t not in impl_funcs:
                print(f"[SKIP] {t} (no implementada)")
                continue

            manager = mp.Manager()
            result_dict = manager.dict()
            queues = [mp.Queue() for _ in range(self.size)]
            barrier = mp.Barrier(self.size)
            processes = []

            for i in range(self.size):
                p = mp.Process(target=self._run_test_proc, 
                               args=(i, queues, barrier, t, impl_funcs, result_dict))
                p.start()
                processes.append(p)

            for p in processes:
                p.join()

            failures = [v for v in result_dict.values() if v != "OK"]
            if not failures:
                print(f"[PASSED] {t}")
            else:
                all_passed = False
                print(f"[FAILED] {t}")
                for f in failures: print(f"  --> {f}")
        
        return all_passed
