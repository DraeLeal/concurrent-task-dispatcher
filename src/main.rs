use std::collections::VecDeque;
use std::sync::{Arc, Mutex, Condvar};
use std::thread;
use std::time::{Duration, Instant};

// Task definition

#[derive(Debug, Clone, PartialEq)]
pub enum TaskKind {
    Cpu,
    Io,
}

#[derive(Debug, Clone)]
pub struct Task {
    pub id: u64,
    pub kind: TaskKind,
    pub duration_ms: u64,
    pub enqueue_time: Instant,
}

impl Task {
    pub fn new(id: u64, kind: TaskKind, duration_ms: u64) -> Self {
        Task { id, kind, duration_ms, enqueue_time: Instant::now() }
    }
}

// Shared queue
// Two separate queues: one for CPU tasks, one for IO tasks
// Protected by a single Mutex; Condvar wakes sleeping workers

pub struct TaskQueues {
    pub cpu: VecDeque<Task>,
    pub io:  VecDeque<Task>,
    pub shutdown: bool,
}

impl TaskQueues {
    pub fn new() -> Self {
        TaskQueues { cpu: VecDeque::new(), io: VecDeque::new(), shutdown: false }
    }
}

type SharedQueues = Arc<(Mutex<TaskQueues>, Condvar)>;

// Completed task record

#[derive(Debug)]
pub struct CompletedTask {
    pub id: u64,
    pub kind: TaskKind,
    pub wait_ms: u128,
    pub turnaround_ms: u128,
    pub worker_id: usize,
    pub busy_ms: u128, 
}

type SharedResults = Arc<Mutex<Vec<CompletedTask>>>;

// Generator thread
// Creates total tasks and pushes them into the queues
// Mix of CPU and IO tasks with varying durations

fn run_generator(queues: SharedQueues, total: u64) {
    let (lock, cvar) = &*queues;
    for i in 0..total {
        // Alternate between CPU-heavy and IO-heavy tasks
        let duration_ms = 150 + (i % 5) * 50;
        let task = Task::new(i + 1, TaskKind::Cpu, duration_ms);
        println!("[generator] created task {} ({:?}, {}ms)", task.id, task.kind, task.duration_ms);

        {
            let mut q = lock.lock().unwrap();
            q.cpu.push_back(task);
            cvar.notify_one();
        }

            thread::sleep(Duration::from_millis(20));
        }

    // Signal shutdown: no more tasks will arrive
    {
        let mut q = lock.lock().unwrap();
        q.shutdown = true;
        cvar.notify_all(); // wake all workers so they can exit
    }
    println!("[generator] done, shutdown signalled");
}

// Worker thread
// Scheduling policy: prefer CPU queue first (keeps CPU workers busy)
// fall back to IO queue. Simple priority-based dispatch

fn run_worker(id: usize, queues: SharedQueues, results: SharedResults) {
    let (lock, cvar) = &*queues;
    loop {
        let task = {
            let mut q = lock.lock().unwrap();
            // Wait until there's work or a shutdown signal
            loop {
                if !q.cpu.is_empty() || !q.io.is_empty() {
                    break; // work available
                }
                if q.shutdown {
                    return; // no more work ever
                }
                q = cvar.wait(q).unwrap();
            }
            // prefer CPU queue, then IO queue
            if let Some(t) = q.cpu.pop_front() {
                t
            } else {
                q.io.pop_front().unwrap()
            }
        };

        let wait_ms = task.enqueue_time.elapsed().as_millis();
        let _start = Instant::now();

        println!("[worker {}] running task {} ({:?}, {}ms, waited {}ms)",
            id, task.id, task.kind, task.duration_ms, wait_ms);

        // Simulate work
        thread::sleep(Duration::from_millis(task.duration_ms));

        let turnaround_ms = task.enqueue_time.elapsed().as_millis();

        {
            let mut r = results.lock().unwrap();
            r.push(CompletedTask {
                id: task.id,
                kind: task.kind,
                wait_ms,
                turnaround_ms,
                worker_id: id,
            });
        }
    }
}

// Metrics printer

fn print_metrics(results: &[CompletedTask], makespan_ms: u128) {
    let total = results.len();
    let cpu_count = results.iter().filter(|r| r.kind == TaskKind::Cpu).count();
    let io_count  = results.iter().filter(|r| r.kind == TaskKind::Io).count();

    let avg_wait = results.iter().map(|r| r.wait_ms).sum::<u128>() / total as u128;
    let avg_turnaround = results.iter().map(|r| r.turnaround_ms).sum::<u128>() / total as u128;
    let max_wait = results.iter().map(|r| r.wait_ms).max().unwrap_or(0);

    println!("\n══════════════════════════════════════════");
    println!("  SCHEDULER METRICS");
    println!("══════════════════════════════════════════");
    println!("  Total tasks completed : {}", total);
    println!("  CPU tasks             : {}", cpu_count);
    println!("  IO tasks              : {}", io_count);
    println!("  Makespan              : {} ms", makespan_ms);
    println!("  Avg wait time         : {} ms", avg_wait);
    println!("  Avg turnaround time   : {} ms", avg_turnaround);
    println!("  Max wait time         : {} ms", max_wait);
    println!("══════════════════════════════════════════\n");
}

// Main

fn main() {
    let num_workers: usize = 2;  // change for stress test
    let num_tasks:   u64   = 40; // change for experiments

    println!("Starting scheduler: {} workers, {} tasks", num_workers, num_tasks);

    let queues:  SharedQueues  = Arc::new((Mutex::new(TaskQueues::new()), Condvar::new()));
    let results: SharedResults = Arc::new(Mutex::new(Vec::new()));

    let start_time = Instant::now();

    // Spawn workers
    let mut worker_handles = vec![];
    for id in 0..num_workers {
        let q = Arc::clone(&queues);
        let r = Arc::clone(&results);
        worker_handles.push(thread::spawn(move || run_worker(id, q, r)));
    }

    // Run generator on its own thread
    let gen_queues = Arc::clone(&queues);
    let gen_handle = thread::spawn(move || run_generator(gen_queues, num_tasks));

    // Wait for generator and all workers to finish
    gen_handle.join().unwrap();
    for h in worker_handles {
        h.join().unwrap();
    }

    let makespan_ms = start_time.elapsed().as_millis();

    let r = results.lock().unwrap();
    print_metrics(&r, makespan_ms);
}
