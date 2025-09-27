# Assignment 3 Report


# 1.1 Where is most calculation time spent?

The profiler shows that most of the time is used in the inner loop:

```python
for j in range(i * i, n + 1, i):
    flag_prime[j] = False
```

This loop runs many times and takes the longest time.



# 1.2 Distribution of execution times

The execution time is not even.
Almost all the time is spent in the marking loop, and the other parts take very little.
This means a few lines take most of the time (heavy-tailed).



