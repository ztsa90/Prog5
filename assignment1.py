#!/usr/bin/env python3
"""Numerical integration with the composite trapezoidal rule.
"""
import argparse
import math
import sys

def trapezoid(f, lower, upper, n=256):
    ''' function to implement integration based on lower bound (a),
    upper bound(b) and number of steps(n)'''
    
    h = (upper - lower) / n  
    I = (f(lower) + f( upper))  

    for index in range(1, n):
        I += 2 * f(lower + index * h)  
    
    return I * (h / 2) 

def main():
    parser = argparse.ArgumentParser(description='Trapezoid model')
    parser.add_argument('-a', type=float, required=True, help='lower bound')
    parser.add_argument('-b', type=float, required=True, help='upper bound')
    parser.add_argument('-n', type=int, required=True, help='number of steps')
    args = parser.parse_args()

    f = math.cos
    exact_answer = math.sin(args.b) - math.sin(args.a)
    approximate_answer = trapezoid(f, args.a, args.b, args.n)
    error = abs(exact_answer - approximate_answer)
    print(f"{args.n}, {error:.6f}")

if __name__ == "__main__":
    main()
