#!/usr/bin/env python3
"""Numerical integration with the composite trapezoidal rule."""

from typing import Callable
import argparse
import math


def trapezoid(
    func: Callable[[float], float],
    lower: float,
    upper: float,
    num_steps: int = 256,
) -> float:
    """Approximate ∫_lower^upper func(x) dx using the composite trapezoid rule."""
    if num_steps <= 0:
        raise ValueError("num_steps must be a positive integer.")

    step = (upper - lower) / num_steps          # step size
    total = func(lower) + func(upper)           # first and last terms

    for idx in range(1, num_steps):
        total += 2.0 * func(lower + idx * step)

    return total * (step / 2.0)


def main() -> None:
    """Parse inputs, run the method, and print the error."""
    parser = argparse.ArgumentParser(description="Trapezoid model")
    parser.add_argument(
        "-a", "--lower", type=float, required=True, help="lower bound of the integral"
    )
    parser.add_argument(
        "-b", "--upper", type=float, required=True, help="upper bound of the integral"
    )
    parser.add_argument(
        "-n", "--steps", type=int, required=True, help="number of steps (subintervals)"
    )
    args = parser.parse_args()

    lower, upper, num_steps = args.lower, args.upper, args.steps

    func = math.cos  # we integrate cos(x)
    exact_answer = math.sin(upper) - math.sin(lower)
    approximate_answer = trapezoid(func, lower, upper, num_steps)
    error = abs(exact_answer - approximate_answer)

    print(f"{num_steps}, {error:.6f}")


if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
