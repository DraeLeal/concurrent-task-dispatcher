# Task Dispatcher
Course: CSCI 3334 Systems Programming (Spring 2026)
Project: Final Task Dispatcher Simulation

## 1. Project Overview
This project simulates a network task scheduler managing concurrent execution across 8 worker threads. It evaluates two policies: FIFO and an Optimized IO-first priority dispatch designed to reduce total runtime by clearing lower-cost tasks faster. The system strictly enforces a 100% CPU capacity cap and manages a workload of 200 tasks across two different workload configurations.

## 2. How to Build and Run
Ensure you have the Rust toolchain installed.

Build the project:
cargo build --release

Run the simulation:
cargo run --release

## 3. Design Summary
Concurrency Model: Uses a SharedQueues structure consisting of an Arc<(Mutex, Condvar)> to coordinate between a producer (Generator) and consumers (Workers).

Queue Architecture: Implements a two-queue system using VecDeque to separate CPU-intensive tasks from IO-bound tasks.

Scheduling Policies:
- FIFO: Tasks are processed in the order they arrive across both queues.
- Optimized: The dispatcher prioritizes the IO queue because IO tasks carry a lower CPU cost (10% vs 35%), which frees up capacity faster and reduces total runtime.

## 4. Summary of Experiments
Experiment A — Balanced Workload (70% IO / 30% CPU): FIFO completed in 9,607 ms. The optimized policy reduced total runtime to 9,107 ms by clearing IO tasks faster and keeping CPU headroom available.

Experiment B — Stressed Workload (20% IO / 80% CPU): FIFO completed in 17,055 ms. The optimized policy reduced total runtime to 16,074 ms. Worker utilization remained low in both runs because CPU tasks consume 35% capacity each, limiting the system to roughly 2 concurrent CPU tasks at any time.

## 5. Tool Use Disclosure
Tools used: Claude (Anthropic)

Type of help: Debugging concurrency panics, refining metrics logic, and improving report clarity.

Example of advice accepted: Using saturating_sub to prevent subtract-with-overflow panics when releasing CPU capacity during high-concurrency moments.

Example of advice rejected: An initial suggestion to use a single global queue was rejected in favor of a two-queue system (CPU vs IO) to better fulfill the professor's suggested architecture and allow priority dispatching without re-sorting.