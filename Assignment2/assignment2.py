#!/usr/bin/env python3
"""
Parallel trapezoidal integration with mpi4py (fair block split).
Assignment 2 - Prog5
"""

import argparse                     # parse command-line arguments (stdlib)
import math                         # math functions like sin, cos (stdlib)
from mpi4py import MPI              # MPI bindings for Python (third-party)


def f(x_value: float) -> float:
    """
    Function to integrate.
    Here we hardcode f(x) = sin(x).
    """
    return math.sin(x_value)        # return sin(x)


def main() -> None:
    """
    Main function for parallel trapezoidal integration.
    Uses MPI to divide the interval [a, b] across ranks (processes).
    Each rank computes a local trapezoidal sum, then results
    are reduced (summed) at rank 0.
    """
    comm = MPI.COMM_WORLD           # communicator with all processes
    rank = comm.Get_rank()          # rank ID of this process (0, 1, 2, ...)
    size = comm.Get_size()          # total number of processes (ranks)

    # --- make sure these names exist on ALL ranks before bcast ---
    a_val = b_val = None            # placeholders on non-root ranks
    n_val = None

    if rank == 0:                   # only rank 0 reads the CLI args
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

        a_val = args.a              # lower bound of the interval (a)
        b_val = args.b              # upper bound of the interval (b)
        n_val = args.n              # number of trapezoids (n)
    # ---------------------------------------------------

    # Broadcast problem definition to all ranks (rank 0 is controller)
    a_val, b_val, n_val = comm.bcast((a_val, b_val, n_val), root=0)
    # after this line every rank has the same a_val, b_val, n_val

    # ---------- Initialize MPI ----------
    # (nothing else to init; communicator already set)

    # Step size for the trapezoidal rule
    h_val = (b_val - a_val) / n_val  # step length h = (b - a) / n

    # ---------- Divide work fairly among ranks ----------
    worker = size - 1                # number of workers if rank 0 is supervisor
                                     # example: size=33 → workers=32

    base = n_val // worker           # base steps per worker (minimum each gets)
    remainder = n_val % worker       # leftover steps; give 1 extra to first 'remainder' workers

    local_steps = base + (1 if rank < remainder else 0)
    # steps for THIS rank: add +1 only for the first 'remainder' ranks
    # NOTE: this uses 'rank' directly. If rank 0 is supervisor,
    # then worker ranks are 1..size-1, not 0..workers-1.

    start_steps_before = rank * base + min(rank, remainder)
    # number of steps before this rank’s chunk starts

    i_start = start_steps_before     # start index (inclusive) for this rank
    i_end = i_start + local_steps    # end index (exclusive) for this rank
    # -----------------------------------------------------

    # Main job is Integral calculation of Trapezoid:
    # Formula: (h / 2) * [ f(a) + 2 * sum f(a + i*h) + f(b) ]
    # Trapezoid concept: weight 1 at ends, weight 2 for internal points

    # local_sum stores the partial result of this rank only
    local_sum = 0.0                  # initialize local integral sum

    # Loop through the internal points of this rank's interval
    for i in range(i_start + 1, i_end):
        x_val = a_val + i * h_val    # sample point x = a + i*h
        local_sum += 2.0 * f(x_val)  # internal points have weight 2

    # ADDED: include left chunk boundary as an internal point (weight = 2),
    # only when this chunk is not empty and not the global left end.
    if local_steps > 0 and i_start > 0:                      # ADDED
        local_sum += 2.0 * f(a_val + i_start * h_val)        # ADDED

    # Add contribution of endpoints if this rank owns them
    if i_start == 0:
        local_sum += f(a_val)        # add first endpoint f(a)
    if i_end == n_val:
        local_sum += f(b_val)        # add last endpoint f(b)

    # Local trapezoid contribution for this rank
    local_trap = (h_val / 2.0) * local_sum  # multiply by h/2 at the end

    # ---- Reduce all partial sums at rank 0 ----
    total = comm.reduce(local_trap, op=MPI.SUM, root=0)
    # all ranks send their local_trap; rank 0 receives the sum

    # ---------- Only rank 0 prints final result ----------
    if rank == 0:
        approx = total
        exact = -math.cos(b_val) + math.cos(a_val)   # exact integral of sin(x)
        error = abs(exact - approx)                  # absolute error
        print(f"result={approx}")
        print(f"exact={exact} error={error:.3e} Rank(size)={size} steps={n_val}")


if __name__ == "__main__":
    main()                          # run main when script is executed directly
