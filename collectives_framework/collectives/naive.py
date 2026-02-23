"""
Naive collective communication primitives.

*Pedagogical* naive implementations of collective operations.

TYPE CONTRACT:
These functions strictly expect 'data' to be an instance of the 'Payload' class.
They rely on specific methods like split() and static methods like Payload.concat().
Generic Python types are NOT supported to maintain benchmarking simplicity.

"""

from payload import Payload

# ---------------------------------------------------------------------------
# Broadcast
# ---------------------------------------------------------------------------
def broadcast(backend, data, root=0):
    """
    Naive Broadcast (Linear / Flat Tree).

    Assumes:
    - Root has 'data' of size M.
    - Others receive 'data'.

    Algorithm:
    - Root loops P-1 times.
    - In each iteration, Root sends 'data' to process 'i'.

    Complexity:
    - Latency: O(P) steps.
      The root sends P-1 messages of size M (root egress bottleneck).
    """


# ---------------------------------------------------------------------------
# Reduce
# ---------------------------------------------------------------------------
def reduce_sum(backend, data, root=0):
    """
    Naive Reduce (Linear).

    Assumes:
    - All processes have 'data' of size M.
    - Root computes the sum of all vectors.

    Algorithm:
    - Root initializes accumulator.
    - Root loops P-1 times receiving data from each worker.
    - Root adds incoming data to accumulator.

    Complexity:
    - Latency: O(P) steps.
      The root receives P-1 messages of size M (root ingress bottleneck).
    """


# ---------------------------------------------------------------------------
# Scatter
# ---------------------------------------------------------------------------
def scatter(backend, data, root=0):
    """
    Naive Scatter (Linear).

    Assumes:
    - Root starts with vector of size M.
    - Each process 'i' receives a segment of size M/P.

    Algorithm:
    - Root splits data into P chunks.
    - Root loops P-1 times sending chunk 'i' to process 'i'.

    Complexity:
    - Latency: O(P) steps.
      Root sends P * (M/P) = M bytes (root egress bottleneck).
      Bandwidth is constant w.r.t P, but Latency is linear.
    """


# ---------------------------------------------------------------------------
# Gather
# ---------------------------------------------------------------------------
def gather(backend, data, root=0):
    """
    Naive Gather (Linear).

    Assumes:
    - Each process has a segment of size M/P.
    - Root collects them into a vector of size M.

    Algorithm:
    - Root loops P-1 times.
    - Receives M/P bytes from each process 'i'.
    - Places data into the correct position in result vector.

    Complexity:
    - Latency: O(P) steps.
    - Bandwidth: O(M). Root receives P * (M/P) = M bytes.
      Bandwidth is constant w.r.t P, but Latency is linear.
    """


# ---------------------------------------------------------------------------
# Reduce-Scatter
# ---------------------------------------------------------------------------
def reduce_scatter(backend, data):
    """
    Naive Reduce-Scatter.

    Assumes:
    - All processes start with a vector of size M.
    - Each process 'i' ends with a segment of size M/P.

    Algorithm:
    1. Reduce_sum to root (Root gets vector of size M).
    2. Scatter from root (Root splits and sends segments of size M/P).

    Complexity:
    - Latency: O(P) steps (2 * P approx).
      Very heavy on Root.
    """


# ---------------------------------------------------------------------------
# Allgather
# ---------------------------------------------------------------------------
def allgather(backend, data):
    """
    Naive Allgather.

    Assumes:
    - Each process starts with segment of size M/P.
    - All processes end with full vector of size M.

    Algorithm:
    1. Gather to Root (Root gets M bytes).
    2. Broadcast from Root (Root sends M bytes P-1 times).

    Complexity:
    - Latency: O(P) steps.
      The Broadcast phase kills performance.
      Root sends the full aggregated message to everyone.
    """


# ---------------------------------------------------------------------------
# Allreduce
# ---------------------------------------------------------------------------
def allreduce_sum(backend, data):
    """
    Naive Allreduce.

    Assumes:
    - All processes start with vector of size M.
    - All processes end with sum vector of size M.

    Algorithm:
    1. Reduce_sum to root (Root gets vector of size M).
    2. Broadcast from Root (Root sends M bytes P-1 times).

    Complexity:
    - Latency: O(P) steps.
      Extremely expensive. Total traffic through root is 2*(P-1)*M.
    """

