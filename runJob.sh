#!/bin/bash
#
#SBATCH --job-name=job
#SBATCH --output=%x_%j.out
#SBATCH -t 00:30:00
#SBATCH --exclusive

if [ -z "$1" ]; then
    echo "❌ ERROR: Debes especificar una ETIQUETA para identificar la ejecución."
    echo "   Uso: sbatch -N <numero_nodos> runJob.sh <etiqueta> [red]"
    echo "   Ejemplo 1: sbatch -N 2 runJob.sh prueba_final     (Usa InfiniBand por defecto)"
    echo "   Ejemplo 2: sbatch -N 4 runJob.sh prueba_final ethernet (Fuerza Gigabit Ethernet)"
    exit 1
fi

if [ "$SLURM_JOB_NUM_NODES" -lt 2 ]; then
    echo "❌ ERROR: Has solicitado menos de 2 nodos (o te has olvidado de usar '-N')."
    echo "   Para medir la red, necesitas al menos 2 máquinas físicas."
    echo "   Uso correcto: sbatch -N <numero_nodos> runJob.sh <etiqueta> [red]"
    exit 1
fi

if [ -n "$SLURM_NTASKS" ]; then
    echo "❌ ERROR: Has usado '-n' (o --ntasks) para pedir cores específicos."
    echo "   En esta práctica reservamos nodos completos. Por favor, elimina el flag '-n'."
    echo "   Usa '-N' para indicar la cantidad de nodos (ej: sbatch -N 2 ...)"
    exit 1
fi

module load gnu12/12.2.1 python/3.12.9 openmpi4/4.1.4 openssl/1.1.1w
source $HOME/mpi_venv/bin/activate

TAG="$1"
USER_ID=$(whoami)
NODES=$SLURM_JOB_NUM_NODES
echo "----------------------------------------------------------------"
echo "👤 Usuario: $USER_ID"
echo "🏷️  Etiqueta del trabajo: $TAG"
echo "🖥️  Nodos Asignados: $NODES"

# --- CONFIGURACIÓN DE RED ---
NETWORK_CHOICE="${2:-infiniband}" # 'infiniband' por defecto si $2 está vacío

if [ "$NETWORK_CHOICE" == "ethernet" ]; then
    echo "🌐 Red: ETHERNET (TCP/IP over eno1)"
    MCA_FLAGS="--mca pml ob1 --mca btl vader,tcp,self --mca btl_tcp_if_include eno1"
else
    echo "🌐 Red: INFINIBAND (RDMA / Default)"
    NETWORK_CHOICE="infiniband"
    MCA_FLAGS=""
fi

echo "----------------------------------------------------------------"

date
echo SLURM_JOBID=$SLURM_JOBID
# --------------------------------
for PPN in 8 16 32; do
    NP=$((NODES * PPN))
    OUTPUT_FILE="${TAG}_${USER_ID}_${NETWORK_CHOICE}_N${NODES}_${PPN}ppn.csv"
    
    echo "----------------------------------------------------------------"
    echo "🚀 LANZANDO: $NP procesos totales ($PPN procesos por nodo)"
    echo "   - Fichero de salida: $OUTPUT_FILE"
    echo "----------------------------------------------------------------"
    mpirun -np $NP --map-by ppr:${PPN}:node -display-allocation $MCA_FLAGS \
        python3 collectives_framework/run_bench.py --mpi --output "$OUTPUT_FILE"
    
    sleep 5
done

echo "----------------------------------------------------------------"
echo "✅ Todos los benchmarks finalizados."
# --------------------------------
date
