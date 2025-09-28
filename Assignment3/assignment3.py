#!/usr/bin/env python3
"""
Parallel Segmented Sieve of Eratosthenes (MPI version)

Goal:
- Find all prime numbers <= n using multiple MPI ranks (processes).
- Rank 0 acts as a *supervisor* (it does not sieve a segment).
- Ranks > 0 are *workers*; each worker sieves a different sub-interval [lo, hi).

High-level idea:
1) Rank 0 reads n from CLI, then broadcasts n to everyone.
2) We split the global interval [2..n] fairly among the workers
   into non-overlapping half-open segments [lo, hi) (hi is excluded).
3) Rank 0 builds "base primes" (all primes <= sqrt(n)) once,
   then broadcasts that small list to all ranks.
4) Each worker uses only that base_primes list to cross out composites
   in its own [lo, hi) segment (segmented sieve).
5) We gather lightweight results (counts and small samples) at rank 0
   and print a summary.

Why segmented sieve?
- Building base primes up to sqrt(n) is cheap.
- Each worker avoids re-sieving from scratch; it only marks multiples
  of the base primes inside its chunk, which is efficient.
"""

from math import isqrt
import argparse
from mpi4py import MPI


# --------------------------- Helper functions ---------------------------

def sieve_root(m: int) -> list[int]:
    """
    Return all prime numbers <= m (classic sieve, small range).

    Used ONLY on rank 0 to compute "base_primes" = primes <= sqrt(n).
    This array is small (size ~ sqrt(n)), and will be broadcast to all ranks.

    Implementation details:
    - is_prime[i] == True means "i is still prime".
    - We mark multiples from p*p (no need to start at 2p).
    """
    if m < 2:
        return []

    # Initially assume all numbers 0..m are prime
    is_prime = [True] * (m + 1)
    is_prime[0] = is_prime[1] = False  # 0 and 1 are not prime

    limit = isqrt(m)
    for p in range(2, limit + 1):
        if is_prime[p]:
            # Mark p*p, p*p+p, p*p+2p, ...
            for j in range(p * p, m + 1, p):
                is_prime[j] = False

    # Collect numbers that stayed True
    return [i for i, ok in enumerate(is_prime) if ok]


def sieve_segment(lo: int, hi: int, base_primes: list[int]) -> list[int]:
    """
    Segmented sieve on the half-open interval [lo, hi).

    Only uses 'base_primes' (primes <= sqrt(n)) to cross out multiples.
    This function does NOT compute primes <= sqrt(n) itself; it expects
    them to be provided. That is why rank 0 precomputes base_primes and
    broadcasts it.

    Parameters:
    - lo (inclusive): start of the worker's chunk
    - hi (exclusive): end of the worker's chunk
    - base_primes: small list of all primes <= sqrt(n)

    Returns:
    - A list of all primes in [lo, hi).

    Key points:
    - We maintain a boolean list 'seg' of length (hi - lo) where
      seg[i] corresponds to the integer (lo + i).
    - We start by assuming every number in the segment is prime (True),
      then mark composites (False) by crossing out multiples of base primes.
    - To find the first multiple of p inside [lo, hi), we use:
      start = ceil(lo / p) * p  = ((lo + p - 1) // p) * p
      but also do not start before p*p (no need to mark smaller multiples).
    - The interval is half-open, so hi is not included.
    """
    seg_len = max(0, hi - lo)
    if seg_len <= 0:
        return []

    # seg[i] = True means "lo + i is still considered prime"
    seg = [True] * seg_len

    for p in base_primes:
        # First multiple of p that is >= lo:
        # ceil(lo/p) * p using integer math
        start = ((lo + p - 1) // p) * p

        # Do not start before p*p (smaller multiples were sieved earlier)
        if start < p * p:
            start = p * p

        # If even that is outside the segment, nothing to do for this p
        if start >= hi:
            continue

        # Mark p, 2p, 3p, ... inside the segment
        for x in range(start, hi, p):
            seg[x - lo] = False

    # Convert True entries back to actual numbers and skip anything < 2
    return [lo + i for i in range(seg_len) if seg[i] and (lo + i) >= 2]


# ------------------------------- Main (MPI) ------------------------------

def main() -> None:
    """
    Parallel Segmented Sieve with MPI.

    Rank 0 (supervisor):
    - Parses CLI to get n
    - Broadcasts n to all ranks
    - Computes base_primes = primes <= sqrt(n) and broadcasts them
    - Gathers results and prints a summary

    Ranks > 0 (workers):
    - Receive n and base_primes
    - Compute their own [lo, hi) chunk
    - Run segmented sieve in that chunk
    - Send back count and a small sample
    """
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # ------------------------- Step 1: input n -------------------------
    # Only rank 0 reads CLI; afterward we broadcast n to all ranks.
    n_val = None
    if rank == 0:
        parser = argparse.ArgumentParser(
            description="Parallel segmented sieve (rank 0 is supervisor)"
        )
        parser.add_argument(
            "-n", type=int, default=100000,
            help="Find all primes <= n (choose n big enough for timing)"
        )
        args = parser.parse_args()
        n_val = args.n

    # Share n with everyone (now every rank has the same n_val)
    n_val = comm.bcast(n_val, root=0)

    # We need at least 2 ranks: one supervisor + at least one worker
    if size <= 1:
        if rank == 0:
            print("Please run with MPI, e.g.: mpirun -np 4 python3 assignment3.py -n 1000000")
        return

    # -------------------- Step 2: split [2..n] among workers --------------------
    # We distribute the interval [2..n] into non-overlapping half-open chunks [lo, hi).
    # There are (size - 1) workers: ranks 1..size-1.
    workers = size - 1

    # Total number of integers to check is (n - 1): the numbers 2..n inclusive.
    total = max(0, n_val - 1)

    # Fair block split with small remainders: the first 'remainder' workers get one extra element.
    base = total // workers
    remainder = total % workers

    if rank == 0:
        local_steps = 0
        start_steps_before = 0
    else:
        # Worker index in [0..workers-1]
        w_idx = rank - 1
        # How many numbers this worker gets
        local_steps = base + (1 if w_idx < remainder else 0)
        # Where this worker's block starts (offset) in [0..total)
        start_steps_before = w_idx * base + min(w_idx, remainder)

    # Convert from block offsets to the actual integer range [lo, hi)
    # Recall: global interval is [2..n], so offset 0 corresponds to integer 2.
    i_start = start_steps_before        # inclusive index in [0..total)
    i_end = i_start + local_steps       # exclusive index
    lo = 2 + i_start                    # inclusive integer bound
    hi = lo + local_steps               # exclusive integer bound

    # ---------------- Step 3: build and broadcast base primes (<= sqrt(n)) ----------------
    root = isqrt(n_val)
    if rank == 0:
        base_primes = sieve_root(root)  # e.g., for n=100, this is [2,3,5,7]
    else:
        base_primes = None
    base_primes = comm.bcast(base_primes, root=0)

    # ---------------- Step 4: workers sieve their segment ----------------
    if rank == 0:
        # Supervisor does no segment work
        local_count = 0
        sample = []
    else:
        # Each worker sieves only its [lo, hi) range using base_primes
        local_primes = sieve_segment(lo, hi, base_primes)
        local_count = len(local_primes)
        # A small preview (optional): avoid sending huge lists back
        sample = local_primes[:10]

    # ---------------- Step 5: gather results at rank 0 ----------------
    counts = comm.gather(local_count, root=0)  # counts from all ranks
    samples = comm.gather(sample, root=0)      # small samples for debugging/preview

    if rank == 0:
        total_count = sum(counts)  # total number of primes <= n
        # Build a short preview by concatenating small samples
        preview = []
        for s in samples:
            preview.extend(s)
            if len(preview) >= 10:
                break

        print(f"[MPI] n={n_val} ranks={size} primes={total_count} sample={preview[:10]}")

        # If you need full prime list, you could gather all local_primes,
        # but that is often too heavy for large n. Counting is enough for timing experiments.


# Standard Python entry point
if __name__ == "__main__":
    main()
