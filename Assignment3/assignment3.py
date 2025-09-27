#!/usr/bin/env python3
from math import isqrt
import argparse

def sieve_like(n: int):
    """
    Sieve of Eratosthenes
    ---------------------
    Input:  n → find all prime numbers up to n
    Output: list of prime numbers ≤ n
    """

    # If n < 2, there are no prime numbers
    if n < 2:
        return []

    # At the start, assume all numbers 0..n are prime (True)
    flag_prime = [True] * (n + 1)

    # 0 and 1 are not prime
    flag_prime[0] = flag_prime[1] = False

    # We only need to check numbers up to sqrt(n)
    limit = isqrt(n)

    # Loop through each number from 2 to sqrt(n)
    for i in range(2, limit + 1):
        if flag_prime[i]:  # If i is still marked prime
            # Mark all multiples of i as not prime (False)
            # Start at i*i because smaller multiples were already marked
            for j in range(i * i, n + 1, i):
                flag_prime[j] = False

    # Collect all numbers that are still marked True → these are primes
    primes = [idx for idx, is_p in enumerate(flag_prime) if is_p]
    return primes


if __name__ == "__main__":
    # Command line argument parsing
    p = argparse.ArgumentParser()
    p.add_argument("-n", type=int, required=False, default=10000,
                   help="Find all prime numbers up to n")
    args = p.parse_args()

    primes = sieve_like(args.n)

    # Print summary
    print(f"n={args.n} primes={len(primes)}")
