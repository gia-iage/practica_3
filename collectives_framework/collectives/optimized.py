"""
Optimized collective communication primitives.

*Pedagogical* optimized implementations of collective operations.

The goal is NOT to provide state-of-the-art implementations, but rather:
- algorithms that are easy to understand
- that may improve over naive implementations
- that expose different communication patterns (tree, ring, etc.)

TYPE CONTRACT:
These functions strictly expect 'data' to be an instance of the 'Payload' class.
They rely on specific methods like split() and static methods like Payload.concat().
Generic Python types are NOT supported to maintain benchmarking simplicity.

"""

from payload import Payload

def check_power_of_two(p):
    # Comprueba si P es potencia de 2 usando trucos de bits
    # (P > 0) y (P & (P - 1) == 0) es True solo para potencias de 2
    if not ((p > 0) and (p & (p - 1) == 0)):
        raise ValueError(f"For simplicity, tree algorithms require P to be a power of 2. (Current P={p})")

# ---------------------------------------------------------------------------
# Broadcast
# ---------------------------------------------------------------------------
def broadcast_tree(backend, data, root=0):
    """
    Tree-based Broadcast (Binomial Tree).

    Assumes:
    - Root has 'data' of size M.
    - All nodes participate.

    Algorithm:
    - Recursive doubling.
    - Step 1: P0 -> P1.
    - Step 2: P0 -> P2, P1 -> P3.
    - ...
    - Process 'i' acts as sender after receiving data.

    Complexity:
    - Latency: O(log P) steps. (Excellent for small messages).
    - Bandwidth: O(log P * M). 
      Unlike naive (P*M), here the load is distributed, but total data 
      sent in the system grows with log P.
    """
    pid = backend.pid()
    size = backend.size()
    check_power_of_two(size)
    
    step = 1
    while step < size:
        if pid < step:
            dst = pid + step
            if dst < size:
                backend.send(dst, data)
        elif pid < 2 * step:
            src = pid - step
            data = backend.recv(src)
        step *= 2

    return data


# ---------------------------------------------------------------------------
# Reduce
# ---------------------------------------------------------------------------
def reduce_sum_tree(backend, data, root=0):
    """
    Tree-based Reduce (Binomial Tree).

    Assumes:
    - All nodes have vector of size M.
    - Root ends with the sum.

    Algorithm:
    - Reverse of Broadcast Tree.
    - In each step, half the active nodes send their data to the other half.
    - Receiver computes sum immediately.
    - Finishes at Root.

    Complexity:
    - Latency: O(log P) steps.
    - Bandwidth: O(log P * M). 
      Good for small/medium M. For huge M, bandwidth efficiency is lower 
      than Ring because links are not fully saturated simultaneously.
    """
    pid = backend.pid()
    size = backend.size()
    check_power_of_two(size)
    
    step = 1
    result = data

    while step < size:
        if pid % (2 * step) == 0:
            src = pid + step
            if src < size:
                incoming = backend.recv(src)
                result = result + incoming
        else:
            dst = pid - step
            backend.send(dst, result)
            break
        step *= 2

    if pid == root:
        return result
    return None


def reduce_sum_ring(backend, data, root=0):
    """
    Ring-based Reduce.

    Assumes:
    - All nodes have vector of size M.
    - Root ends with the sum.

    Algorithm:
    1. Reduce-Scatter Ring (Optimized). Each node ends with M/P sum.
    2. Gather Tree (to Root). Root collects pieces.

    Complexity:
    - Latency: O(P) steps (due to ring) + O(log P) (gather).
    - Bandwidth: O(M). 
      Reduce-Scatter is O(M). Gather is O(M).
      More bandwidth efficient than Reduce Tree for very large vectors.
    """
    pid = backend.pid()
    size = backend.size()
    
    # 1. Reduce-Scatter
    my_result_chunk = reduce_scatter_ring(backend, data)    
    # 2. Gather
    final_parts = gather_tree(backend, my_result_chunk, root)
    
    return final_parts


# ---------------------------------------------------------------------------
# Scatter
# ---------------------------------------------------------------------------
def scatter_tree(backend, data, root=0):
    """
    Tree-based Scatter (Binomial Tree Descending).

    Assumes:
    - Root has vector of size M.
    - Process 'i' receives specific chunk 'i' of size M/P.

    Algorithm (Divide and Conquer):
    - Root starts with full vector M.
    - Sends top half (M/2) to partner process.
    - Both recurse with M/2 size.
    - Repeats until chunks are size M/P.

    Complexity:
    - Latency: O(log P) steps.
    - Bandwidth: O(M). 
      Root sends M/2 + M/4 + ... + M/P approx M bytes.
      Root egress is still the limit, so bandwidth is similar to Naive.
      Advantage is purely latency.
    """
    pid = backend.pid()
    size = backend.size()
    check_power_of_two(size)
    
    if pid == root:
        chunk = data
    else:
        chunk = None

    step = 1
    while step * 2 < size:
        step *= 2
 
    while step >= 1:
        if pid % (2 * step) == 0:
            dst = pid + step
            if dst < size:
                # Dividimos la lista en dos mitades
                half = len(chunk) // 2
                to_send = chunk[half:]
                chunk = chunk[:half]             
                backend.send(dst, to_send)
        elif (pid - step) % (2 * step) == 0:
            src = pid - step
            chunk = backend.recv(src)
        step //= 2

    return chunk


# ---------------------------------------------------------------------------
# Gather
# ---------------------------------------------------------------------------
def gather_tree(backend, data, root=0):
    """
    Tree-based Gather.

    Assumes:
    - Each node has chunk of size M/P.
    - Root ends with full vector M.

    Algorithm:
    - Reverse of Scatter Tree.
    - Pairs exchange and concatenate chunks.
    - Data size doubles at each step up the tree.

    Complexity:
    - Latency: O(log P) steps.
    - Bandwidth: O(M).
      Root receives M/2 bytes in the last step. 
      Total ingress at root is ~M bytes.
    """
    pid = backend.pid()
    size = backend.size()
    check_power_of_two(size)

    result = [data]
    step = 1

    while step < size:
        if pid % (2 * step) == 0:
            src = pid + step
            if src < size:
                incoming = backend.recv(src)
                result.append(incoming)
        else:
            dst = pid - step
            chunk_to_send = Payload.concat(result)
            backend.send(dst, chunk_to_send)
            break
        step *= 2

    if pid == root:
        return Payload.concat(result)
    return None


# ---------------------------------------------------------------------------
# Reduce-Scatter
# ---------------------------------------------------------------------------
def reduce_scatter_ring(backend, data):
    """
    Ring-based Reduce-Scatter.

    Assumes:
    - Input: Vector of size M at all nodes.
    - Output: Process 'i' has the i-th chunk (size M/P) of the global sum.

    Algorithm:
    - Vector split into P chunks.
    - P-1 steps. In each step 'k':
      - Send chunk 'i' to neighbor.
      - Receive chunk 'i-1'.
      - Accumulate received chunk.
    - Pipeline effect: All links are active simultaneously.

    Complexity:
    - Latency: O(P) steps. (High latency for small messages).
    - Bandwidth: O(M). Specifically ((P-1)/P * M).
      Constant bandwidth cost independent of P. Optimal for large M.
    """
    pid = backend.pid()
    size = backend.size()
    
    chunks = data.split(size) # Dividimos el vector
    my_chunks = list(chunks) 

    for i in range(size - 1):
        send_idx = (pid - i) % size
        recv_idx = (pid - i - 1) % size     
        dst = (pid + 1) % size
        src = (pid - 1) % size
        
        # Deadlock Free
        if pid % 2 == 0:
            # Los PARES toman la iniciativa: Envían primero
            backend.send(dst, my_chunks[send_idx])
            incoming = backend.recv(src)
        else:
            # Los IMPARES esperan pasivamente: Reciben primero
            incoming = backend.recv(src)
            backend.send(dst, my_chunks[send_idx])

        # Suma Vectorial al chunk correspondiente
        my_chunks[recv_idx] = my_chunks[recv_idx] + incoming

    result_idx = (pid + 1) % size

    return my_chunks[result_idx]


# ---------------------------------------------------------------------------
# Allgather
# ---------------------------------------------------------------------------
def allgather_ring(backend, data):
    """
    Ring-based Allgather (Bucket Brigade).

    Assumes:
    - Each node starts with chunk of size M/P.
    - Each node ends with full vector M.

    Algorithm:
    - P-1 steps.
    - In each step, send the chunk just received to the next neighbor.
    - Simultaneously receive new chunk from previous neighbor.

    Complexity:
    - Latency: O(P) steps.
    - Bandwidth: O(M). Specifically ((P-1)/P * M).
      Traffic is perfectly distributed. Optimal for large M.
    """
    pid = backend.pid()
    size = backend.size()
    
    chunks_buffer = [None] * size
    my_idx = (pid + 1) % size
    chunks_buffer[my_idx] = data
    
    for i in range(size - 1):
        send_idx = (pid - i + 1) % size
        recv_idx = (pid - i) % size      
        dst = (pid + 1) % size
        src = (pid - 1) % size
        
        # Deadlock Free
        if pid % 2 == 0:
            backend.send(dst, chunks_buffer[send_idx])
            incoming = backend.recv(src)
        else:
            incoming = backend.recv(src)
            backend.send(dst, chunks_buffer[send_idx])
            
        chunks_buffer[recv_idx] = incoming
        
    return Payload.concat(chunks_buffer)

    
def allgather_tree(backend, data):
    """
    Tree-based Allgather.
    
    Composition: Gather (Tree) + Broadcast (Tree).

    Complexity:
    - Latency: 2* O(log P). Excellent latency.
    - Bandwidth: O(M) for gather + O(M log P) for broadcast.
      Better than Ring for small data
      Worse than Ring for large data.
    """
    # 1. Recolectar en root (O(log P))
    gathered = gather_tree(backend, data, root=0)
    # 2. Difundir desde el root (O(log P))
    return broadcast_tree(backend, gathered, root=0)


# ---------------------------------------------------------------------------
# Allreduce
# ---------------------------------------------------------------------------
def allreduce_sum_tree(backend, data):
    """
    Tree-based Allreduce.
    
    Composition: Reduce (Tree) + Broadcast (Tree).

    Complexity:
    - Latency: 2 * O(log P). Excellent latency.
    - Bandwidth: Suboptimal. Root (or tree tops) handle O(M) traffic multiple times.
    """
    # 1. Reducir al root (O(log P))
    reduced = reduce_sum_tree(backend, data, root=0)
    # 2. Difundir desde el root (O(log P))
    return broadcast_tree(backend, reduced, root=0)


def allreduce_sum_ring(backend, data):
    """
    Ring-based Allreduce.
    
    Composition: Reduce-Scatter (Ring) + Allgather (Ring).
    
    Algorithm:
    1. Sum vectors distributedly (Result is scattered).
    2. Share results (Result is gathered).

    Complexity:
    - Latency: 2 * O(P). High start-up cost (bad for small M).
    - Bandwidth: 2 * O(M). Specifically 2 * ((P-1)/P * M).
      Cost is CONSTANT regarding P. 
      This is the standard for training large AI models (NCCL/MPI).
    """
    # Fase 1: Reduce Scatter
    my_piece = reduce_scatter_ring(backend, data)
    # Fase 2: Allgather
    result = allgather_ring(backend, my_piece)
    
    return result
