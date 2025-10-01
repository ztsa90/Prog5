
# Assignment 3 Report

## Part 1 — Profiling the single-threaded sieve

### 1.1 Where is most calculation time spent?

The profiler output (`sieve_profiler_output.txt`) shows that most of the calculation time is spent inside the inner loop:

```python
for j in range(i * i, n + 1, i):
    flag_prime[j] = False
````

* This loop runs for every prime `i` up to `√n`.
* For each prime, it marks all multiples as composite.
* Because this loop executes so many times, it dominates the total runtime.

### 1.2 Distribution of execution times

The distribution of runtime is **not homogeneous** and **uniform**.

* Almost all of the time is concentrated in the marking loop above.
* Other parts of the code (initialization, collecting results, etc.) take very little time.

Most of the time is spent in only a few places (the inner loop and list comprehension).

The rest of the program is very light.

So the distribution is heavy-tailed: a small part of the code takes almost all the time.

----

## Part 2 — Parallel Sieve (MPI with segmentation)

### Design

2. Scaling and Performance

Q1:At what point (cores) does it not matter anymore if you add any more (where does the plateau begin)?
The results show that speed improves up to about 5 tasks.
After this point (10–32 tasks), adding more does not help and can even make it slower


Q2: Can you explain this behavior?
When we use more ranks, each one has less work to do.
But MPI communication (broadcast, gather, sync) stays the same and becomes important.
This overhead makes the speedup stop, so we get no benefit from more cores.

Q3: Does this behavior change when you run it on multiple hosts rather than just multiple cpu's on one host? Why or why not?
On many hosts, the problem gets worse.
MPI must use the network between nodes, which is slower than shared memory on one machine.
So network delays make the program slower even if more nodes are added.

