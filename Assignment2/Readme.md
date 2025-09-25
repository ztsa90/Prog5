# Assignment 2 — Trapezoid Integration with MPI

This project has two files:  
- **`assignment2.py`** → Python code with MPI (using `mpi4py`)  
- **`assignment2.sh`** → SLURM script to run tests on the cluster  

---

## Run without SLURM (local, with mpirun)

On your own machine or terminal you can run with **mpirun** (or `mpiexec`):

```bash
mpirun -n 4 python3 assignment2.py -a 0 -b 3.14 -n 100000
```

- `-n 4` → total ranks = 4 (1 supervisor + 3 workers)  
- `-a 0` → lower bound of integration  
- `-b 3.14` → upper bound (≈ pi)  
- `-n 100000` → number of steps (trapezoids)  

---

## Run with SLURM (on cluster)

Submit the SLURM job script:

```bash
sbatch assignment2.sh
```

Inside the script, the program is run with **srun**:

```bash
srun -n 33 python3 assignment2.py -a 0 -b 3.14 -n 1000000
```

- SLURM gives 33 tasks (1 supervisor + 32 workers).  
- Results and timings are written to `results.csv`.  

---

## Debug: which node is running?

In the `.sh` script:

```bash
hostname   # prints the node name
date       # prints the start time
```

This tells you on which machine (node) and when the job started.  

---

## Debug: which rank is working?

In the Python code you can add:

```python
print("Hello from rank", rank, "of", size)
```

- `rank` = process ID (0, 1, 2, …).  
- `size` = total number of ranks.  

This shows which MPI rank is doing work.  

## Metadata

- Course: **Prog5**  
- Assignment: **Assignment 2**  
- Student: **Zahra Taheri Hanjani**  
- Email: **z.taheri.hanjani@st.hanze.nl**  
- Lecturer: **Martijn Herber**  
