#!/usr/bin/env python3
"""Numerical integration with the composite trapezoidal rule."""

import argparse   # to read values from the command line
import math       # for cos() and sin()


def trapezoid(f, lower, upper, n=256):
    """Approximate the integral of f(x) from lower to upper using n steps."""
    if n <= 0:
        # n must be positive, otherwise step size is invalid
        raise ValueError("n must be a positive integer.")

    h = (upper - lower) / n        # step size
    I = f(lower) + f(upper)        # first and last terms

    # add the middle terms, each multiplied by 2
    for index in range(1, n):
        I += 2 * f(lower + index * h)

    # multiply by h/2 to finish the trapezoid rule
    return I * (h / 2)


def main():
    """Parse inputs, run the method, and print the error."""
    parser = argparse.ArgumentParser(description="Trapezoid model")
    parser.add_argument("-a", type=float, required=True,
                        help="lower bound of the integral")
    parser.add_argument("-b", type=float, required=True,
                        help="upper bound of the integral")
    parser.add_argument("-n", type=int, required=True,
                        help="number of steps (subintervals)")
    args = parser.parse_args()

    f = math.cos  # we integrate cos(x)
    # exact value of integral(cos(x)) from a to b is sin(b) - sin(a)
    exact_answer = math.sin(args.b) - math.sin(args.a)

    # numerical answer using our trapezoid function
    approximate_answer = trapezoid(f, args.a, args.b, args.n)

    # absolute error between exact and approximate answers
    error = abs(exact_answer - approximate_answer)

    # print steps and error (6 digits after the decimal point)
    print(f"{args.n}, {error:.6f}")


if __name__ == "__main__":
    main()
