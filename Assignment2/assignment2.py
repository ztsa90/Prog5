#!/usr/bin/env python3
# Parallel trapezoidal integration with mpi4py (super simple version)

from mpi4py import MPI
import argparse
import math


# The function to integrate; you can hardcode it for the assignment
def f(x):
    return math.sin(x)


def main():
    # ---------- Parse CLI arguments inside main (simple & clear) ----------
    # All MPI ranks see the same argv, so parsing on every rank is fine.
    parser = argparse.ArgumentParser(
        description="Trapezoidal rule with MPI (simple version)"
    )
    parser.add_argument("-a", type=float, required=True, help="Lower bound of the integral")
    parser.add_argument("-b", type=float, required=True, help="Upper bound of the integral")
    parser.add_argument("-n", type=int,   required=True, help="Number of steps (subintervals)")
    args = parser.parse_args()

    a = args.a
    b = args.b
    n = args.n
    # ---------------------------------------------------------------------

    # ---------- Initialize MPI ----------
    comm = MPI.COMM_WORLD           # communicator containing all processes
    rank = comm.Get_rank()          # this process's ID: 0,1,2,...
    size = comm.Get_size()          # total number of processes
    # -----------------------------------

    # Step size for trapezoidal rule
    h = (b - a) / n

    # Each rank sums a subset of indices using stride: rank, rank+size, rank+2*size, ...
    local_sum = 0.0
    for i in range(rank, n + 1, size):
        x_i = a + i * h
        # Endpoints have coefficient 1; interior points have coefficient 2
        coef = 1 if (i == 0 or i == n) else 2
        local_sum += coef * f(x_i)

    # Local trapezoid contribution from this rank
    local_trap = (h / 2.0) * local_sum

    # Sum all local contributions on rank 0
    total = comm.reduce(local_trap, op=MPI.SUM, root=0)

    # Only rank 0 prints the final result
    if rank == 0:
        print(f"result={total}")


if __name__ == "__main__":
    main()
