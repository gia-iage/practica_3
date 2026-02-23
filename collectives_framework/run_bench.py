import argparse
import sys
import csv
import socket
import datetime
import hashlib
import multiprocessing as mp
import getpass

from latency import LatencyModel
from payload import Payload
from collectives import naive, optimized
from benchmark import benchmark_collective
from tests.test_collectives import CollectiveTester

try:
    from backend.mp_backend import MPBackend
except ImportError:
    MPBackend = None

try:
    from backend.mpi_backend import MPIBackend
except ImportError:
    MPIBackend = None


# ==================================================
# Tamaños de mensaje
# ==================================================
MESSAGE_SIZES = [
    4,                 # 4 bytes
    64,	               # 64 bytes
    1024,              # 1 KB
    1024 * 16,         # 16 KB
    1024 * 64,         # 64 KB
    1024 * 256,        # 256 KB
    1024 * 1024,       # 1 MB
    1024 * 2048,       # 2 MB
]

# ==================================================
# Iteraciones
# ==================================================
WARMUP_ITERS = 5
MEASURE_ITERS = 40

# ==================================================
# Perfiles de Red (Simulación)
# ==================================================
NETWORK_PROFILES = {
    "1gbit": LatencyModel(
        base_us=50,        # Latencia típica LAN (stack TCP + switch)
        jitter_us=15,      # Variabilidad moderada
        p99_us=100,        # Picos por interrupciones
        p99_rate=0.03,     # 3% rate
        bw_Gbps=1.0,       # 1 Gigabit Ethernet
    ),
    "10gbit": LatencyModel(
        base_us=10,        # Latencia baja
        jitter_us=3,       # Muy estable
        p99_us=50,         # Menos ruido
        p99_rate=0.01,     # 1% rate
        bw_Gbps=10.0,      # 10 Gigabit Ethernet
    ),
    "ideal": LatencyModel(
        base_us=0, jitter_us=0, p99_us=0, p99_rate=0, bw_Gbps=200.0 # Red infinita (para debug)
    )
}

COLLECTIVE_SUITE = [
    {
        "name": "Broadcast",
        "naive": naive.broadcast,
        "variants": [
            ("Tree", optimized.broadcast_tree)
        ],
        "args": lambda backend, p: (backend, p, 0)
    },
    {
        "name": "Reduce",
        "naive": naive.reduce_sum,
        "variants": [
            ("Tree", optimized.reduce_sum_tree),
            ("Ring", optimized.reduce_sum_ring)
        ],
        "args": lambda backend, p: (backend, p, 0)
    },
    {
        "name": "ReduceScatter",
        "naive": naive.reduce_scatter,
        "variants": [
            ("Ring", optimized.reduce_scatter_ring)
        ],
        "args": lambda backend, p: (backend, p)
    },
    {
        "name": "Allreduce",
        "naive": naive.allreduce_sum,
        "variants": [
            ("Tree", optimized.allreduce_sum_tree),
            ("Ring", optimized.allreduce_sum_ring)
        ],
        "args": lambda backend, p: (backend, p)
    },
    {
        "name": "Gather",
        "naive": naive.gather,
        "variants": [("Tree", optimized.gather_tree)],
        "args": lambda backend, p: (backend, p, 0)
    },
    {
        "name": "Allgather",
        "naive": naive.allgather,
        "variants": [
            ("Tree", optimized.allgather_tree),
            ("Ring", optimized.allgather_ring)
        ],
        "args": lambda backend, p: (backend, p)
    },
    {
        "name": "Scatter",
        "naive": naive.scatter,
        "variants": [("Tree", optimized.scatter_tree)],
        "args": lambda backend, p: (backend, p, 0)
    }
]

def benchmark_logic(backend, output_file, test_only=False):
    try:
        run_validation(backend)
    except Exception as e:
        print(f"\n[!] ERROR DE VALIDACIÓN: {e}")
        return
    
    pid = backend.pid()
    csv_writer = None
    f = None
    SECRET_SALT = "GIA_IAGE_2526"
    
    if pid == 0:
        try:
            hostname = socket.gethostname()
            username = getpass.getuser()
            timestamp = datetime.datetime.now().isoformat()
            f = open(output_file, 'w', newline='')
            csv_writer = csv.writer(f)
            # Cabecera del CSV
            csv_writer.writerow(['Collective', 'Implementation', 'MsgSize', 'Processes', 'TimeMax', 'TimeMean', 'Sends', 'Recvs', 'Hostname', 'Username', 'Timestamp', 'ID'])
        except IOError as e:
            print(f"Error abriendo fichero CSV: {e}")
    
    sizes_to_run = [MESSAGE_SIZES[0], MESSAGE_SIZES[-1]] if test_only else MESSAGE_SIZES
    
    if pid == 0:
            print(f"--- Benchmarking con tamaños de mensaje ---\n{sizes_to_run}")
            print(f"--- Guardando resultados en: {output_file} ---")
                        
    for msg_size in sizes_to_run:
        # Generamos un vector contiguo
        num_elements = max(1, msg_size // 4)
        vector_data = Payload([1] * num_elements, element_size=4)
        # Para Gather/Allgather, cada proceso necesita su trozo local, no el vector entero
        # Simulamos que cada uno tiene un trozo del tamaño correcto (total/size)
        chunk_data = vector_data.split(backend.size())[pid]
        
        if pid == 0:
            print(f"=== Message size: {msg_size} bytes ===")

        for spec in COLLECTIVE_SUITE:
            name = spec["name"]
            naive_fn = spec["naive"]
            args_gen = spec["args"]
            
            # Selección de datos según la operación (Vector Global vs Trozo Local)
            if name in ["Gather", "Allgather"]:
                data_in = chunk_data
            else:
                data_in = vector_data
            
            # --- Ejecutar Naive ---
            runner = lambda: naive_fn(*args_gen(backend, data_in))
            r = benchmark_collective(backend, runner, name, "Naive", WARMUP_ITERS, MEASURE_ITERS)
            if pid == 0:
                 print(f"{name:13s} | {'Naive':6s} | time={r.time_max:.6f}s")
                 if csv_writer:
                    raw_str = f"{name}{'Naive'}{msg_size}{r.time_max:.6f}{hostname}{username}{timestamp}{SECRET_SALT}"
                    signature = hashlib.md5(raw_str.encode()).hexdigest()[:8]
                    csv_writer.writerow([name, 'Naive', msg_size, backend.size(), f"{r.time_max:.6f}", f"{r.time_mean:.6f}", r.sends, r.recvs, hostname, username, timestamp, signature])
                    f.flush() # Forzar escritura en disco
                    
            # --- Ejecutar variantes optimizadas ---
            for var_name, var_fn in spec["variants"]:
                runner = lambda: var_fn(*args_gen(backend, data_in))
                r = benchmark_collective(backend, runner, name, var_name, WARMUP_ITERS, MEASURE_ITERS)
                if pid == 0:
                    print(f"{name:13s} | {var_name:6s} | time={r.time_max:.6f}s")
                    if csv_writer:
                        raw_str = f"{name}{var_name}{msg_size}{r.time_max:.6f}{hostname}{username}{timestamp}{SECRET_SALT}"
                        signature = hashlib.md5(raw_str.encode()).hexdigest()[:8]
                        csv_writer.writerow([name, var_name, msg_size, backend.size(), f"{r.time_max:.6f}", f"{r.time_mean:.6f}", r.sends, r.recvs, hostname, username, timestamp, signature])
                        f.flush()


def run_mpi(output_file, test_only):
    if MPIBackend is None:
        print("Error: mpi4py no está instalado.")
        return

    backend = MPIBackend()
    pid = backend.pid()
    if pid == 0:
        print(f"=== MPI backend (Network simulation ignored) ===")

    benchmark_logic(backend, output_file, test_only)


def mp_worker(pid, size, queues, barrier, network_model, output_file, test_only):
    backend = MPBackend(pid, size, queues, barrier, network_model)
    benchmark_logic(backend, output_file, test_only)


def run_multiprocessing(size, output_file, test_only, network_model):
    ctx = mp.get_context("spawn")
    queues = [ctx.Queue() for _ in range(size)]
    barrier = ctx.Barrier(size)

    procs = []
    for pid in range(size):
        p = ctx.Process(
            target=mp_worker,
            args=(pid, size, queues, barrier, network_model, output_file, test_only),
        )
        p.start()
        procs.append(p)

    for p in procs:
        p.join()


def run_validation(backend):
    pid = backend.pid()
    size = backend.size()
    root = 0

    if pid == root:
        print(f"--- Iniciando Validación Funcional ---")
    
    def sync_assert(name, condition, error_msg=""):
        if not condition:
           # Imprimimos el error detallado antes de lanzar la excepción
            print(f"[!] ERROR en {name} (PID {pid}): {error_msg}")
            # Usamos flush para asegurar que el print salga antes de morir
            sys.stdout.flush()
            raise RuntimeError(f"Fallo en {name} (Proceso {pid})")
        backend.barrier()

    # Generador de datos
    def get_test_data(name):
        # Creamos un vector [0, 1, 2...size-1] para identificar datos
        vector = Payload([i for i in range(size)], element_size=4)
        
        if name == "Scatter":
            # Root tiene el vector completo, los demás nada
            return vector if pid == root else None
        elif "reduce" in name.lower() or "allreduce" in name.lower():
            # Para sumas, usamos vector de 1s para verificar fácil
            return Payload([1] * size, element_size=4)
        elif name == "Broadcast":
            return Payload([100] * size, element_size=4) if pid == root else None
        else:
            # Para Gather/Allgather, cada uno contribuye con su trozo
            # Usamos un vector de 1 elemento [pid]
            return Payload([pid], element_size=4)
            
    for spec in COLLECTIVE_SUITE:
        name = spec["name"]
        naive_fn = spec["naive"]
        variants = spec["variants"]

        # --- Definición de Checks con mensajes de error ---
        def check_fn(res, op_name):
            if "allreduce" in op_name.lower():
                val = res.value if hasattr(res, 'value') else "NoAttr"
                expected = size * size
                return val == expected, f"Esperado {expected}, Recibido {val}"

            elif "reducescatter" in op_name.lower():
                if not isinstance(res, Payload):
                    return False, f"Esperaba Payload, recibió {type(res)}"
                
                if res.value != size:
                     return False, f"Esperaba valor {size}, recibió {res.value}"
                return True, ""
                        
            elif "reduce" in op_name.lower():
                if pid == root:
                    val = res.value if hasattr(res, 'value') else "NoAttr"
                    expected = size * size
                    return val == expected, f"Root esperaba {expected}, recibió {val}"
                else:
                    return res is None, f"Worker esperaba None, recibió {res}"
             
            elif "broadcast" in op_name.lower():
                val = res.value if hasattr(res, 'value') else "NoAttr"
                expected = 100 * size
                return val == expected, f"Esperado {expected}, recibido {val}"
            
            elif "allgather" in op_name.lower() or "gather" in op_name.lower():
                target_sum = sum(range(size))
                if "allgather" in op_name.lower() or pid == root:
                    val = res.value if hasattr(res, 'value') else "NoAttr"
                    return val == target_sum, f"Esperado suma {target_sum}, recibido {val}"
                else:
                    return res is None, f"Worker esperaba None, recibió {res}"
            
            elif op_name == "Scatter":
                # Diagnóstico específico para Scatter
                if not isinstance(res, Payload):
                    return False, f"Esperaba Payload, recibió tipo {type(res)}: {res}"
                # Esperamos que el valor sea igual a nuestro PID (Payload([pid]))
                if res.value != pid:
                    return False, f"Esperaba valor {pid}, recibió valor {res.value} (Data: {res.data})"
                return True, ""
            
            return True, ""

        # Validar Naive
        try:
            data = get_test_data(name)
            args = spec["args"](backend, data)
            res = naive_fn(*args)            
            ok, msg = check_fn(res, name)
            sync_assert(f"Naive {name}", ok, msg)
        except Exception as e:
             raise RuntimeError(f"Naive {name} Exception: {e}")

        # Validar Variantes (Tree, Ring, etc.)
        for var_name, var_fn in variants:
            try:
                data = get_test_data(name)
                args = spec["args"](backend, data)
                res = var_fn(*args)                
                ok, msg = check_fn(res, name)
                sync_assert(f"{var_name} {name}", ok, msg)
            except Exception as e:
                import traceback
                traceback.print_exc()
                raise RuntimeError(f"{var_name} {name} Exception: {e}")

    if pid == root:
        print("[OK] Validación funcional superada\n")
               

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--mpi", action="store_true",
                        help="Use MPI backend (must be launched with mpirun)")
    parser.add_argument("--np", type=int, default=2,
                        help="Number of processes (multiprocessing only)")
    parser.add_argument("--output", type=str, default="results.csv",
                        help="Output CSV file for results")
    parser.add_argument("--test-only", action="store_true",
                        help="Only run validation tests and the smallest benchmark")                    
    parser.add_argument("--network", type=str, default="1gbit", 
                        choices=["1gbit", "10gbit", "ideal"],
                        help="Select network profile for simulation")
                       
    args = parser.parse_args()
    
    if args.mpi:
        run_mpi(args.output, args.test_only)
    else:
        # Seleccionamos el perfil de red
        selected_network = NETWORK_PROFILES[args.network]
        print(f"=== MultiProcessing (MP) backend ({args.np} procs) | Network: {args.network} ===")
        run_multiprocessing(args.np, args.output, args.test_only, selected_network)

