#!/usr/bin/env python3
"""
Parallel trapezoidal integration with mpi4py (fair block split).
Assignment 2 - Prog5
"""

from mpi4py import MPI
import argparse
import math


def f(x_value: float) -> float:
    """
    Function to integrate.
    Here we hardcode f(x) = sin(x).
    """
    return math.sin(x_value)


def main() -> None:
    """
    Main function for parallel trapezoidal integration.
    Uses MPI to divide the interval [a, b] across ranks (processes).
    Each rank computes a local trapezoidal sum, then results
    are reduced (summed) at rank 0.
    """
    # ---------- Parse command line arguments ----------
    parser = argparse.ArgumentParser(
        description="Trapezoidal rule with MPI (fair block split)"
    )
    parser.add_argument("-a", type=float, required=True,
                        help="Lower bound of the integral")
    parser.add_argument("-b", type=float, required=True,
                        help="Upper bound of the integral")
    parser.add_argument("-n", type=int, required=True,
                        help="Number of steps (subintervals)")
    args = parser.parse_args()

    a_val = args.a   # lower bound of the interval
    b_val = args.b   # upper bound of the interval
    n_val = args.n   # number of trapezoids (subintervals)
    # ---------------------------------------------------

    # ---------- Initialize MPI ----------
    comm = MPI.COMM_WORLD           # communicator with all processes
    rank = comm.Get_rank()          # ID of this process (0, 1, 2, ...)
    size = comm.Get_size()          # total number of processes
    # -----------------------------------

    # Step size for the trapezoidal rule
    h_val = (b_val - a_val) / n_val

    # ---------- Divide work fairly among ranks ----------
    base = n_val // size
    remainder = n_val % size
    local_steps = base + (1 if rank < remainder else 0)
    start_steps_before = rank * base + min(rank, remainder)
    i_start = start_steps_before
    i_end = i_start + local_steps
    # -----------------------------------------------------

    # local_sum stores the partial result of this rank only
    local_sum = 0.0

    # Loop through the internal points of this rank's interval
    for i in range(i_start + 1, i_end):
        x_val = a_val + i * h_val
        local_sum += 2.0 * f(x_val)

    # Add contribution of endpoints if this rank owns them
    if i_start == 0:
        local_sum += f(a_val)  # first endpoint
    if i_end == n_val:
        local_sum += f(b_val)  # last endpoint

    # Local trapezoid contribution for this rank
    local_trap = (h_val / 2.0) * local_sum

    # ---- Reduce all partial sums at rank 0 ----
    total = comm.reduce(local_trap, op=MPI.SUM, root=0)

    # ---------- Only rank 0 prints final result ----------
    if rank == 0:
        approx = total
        exact = -math.cos(b_val) + math.cos(a_val)   # exact ∫ sin(x) dx
        error = abs(exact - approx)
        print(f"result={approx}")
        print(f"exact={exact} error={error:.3e} size={size} steps={n_val}")


if __name__ == "__main__":
    main()
