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
    pub cpu_cost: u32,
}

impl Task {
    pub fn new(id: u64, kind: TaskKind, duration_ms: u64, cpu_cost: u32) -> Self {
        Task { 
            id, 
            kind, 
            duration_ms, 
            enqueue_time: Instant::now(),
            cpu_cost
         }
    }
}

// Two separate queues for CPU and IO tasks
// Both are protected by a single Mutex so only one thread touches them at a time
// The Condvar is used to wake up sleeping workers when new work is added
pub struct TaskQueues {
    pub cpu: VecDeque<Task>,
    pub io:  VecDeque<Task>,
    pub current_cpu_usage: u32,
    pub active_workers: usize,
    pub shutdown: bool,
}

impl TaskQueues {
    pub fn new() -> Self {
        TaskQueues { 
            cpu: VecDeque::new(), 
            io: VecDeque::new(), 
            shutdown: false,
            active_workers: 0,
            current_cpu_usage: 0,
        }
    }
}

type SharedQueues = Arc<(Mutex<TaskQueues>, Condvar)>;

// Stores info about a task after it has been completed
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

// Creates all the tasks and puts them into the right queue
// 70% of tasks are IO and 30% are CPU, each one takes 200ms to run
fn run_generator(queues: SharedQueues, total: u64, io_ratio: u64) {
    let (lock, cvar) = &*queues;
    for i in 0..total {
        // io_ratio controls how many out of every 10 tasks are IO (e.g. 7 = 70%)
        let is_io = i % 10 < io_ratio;
        
        let (kind, cost) = if is_io {
            (TaskKind::Io, 10) // IO task: 10% cpu consumption
        } else {
            (TaskKind::Cpu, 35) // CPU task: 35% cpu consumption
        };

        let task = Task { 
            id: i + 1, 
            kind, 
            duration_ms: 200, 
            enqueue_time: Instant::now(),
            cpu_cost: cost,
        };

        // println!("[generator] created task {} ({:?}, {}ms, {}% CPU)", 
        //          task.id, task.kind, task.duration_ms, task.cpu_cost);

        {
            let mut q = lock.lock().unwrap();
            // put the task in the correct queue based on its type
            if is_io {
                q.io.push_back(task);
            } else {
                q.cpu.push_back(task);
            }
            cvar.notify_one();
        }

        // space out task arrivals by 20ms
        thread::sleep(Duration::from_millis(20));
    }

    // all tasks have been created, tell the workers they can stop waiting for more
    {
        let mut q = lock.lock().unwrap();
        q.shutdown = true;
        cvar.notify_all();
    }
    //println!("[generator] done, shutdown signaled");
}

// Each worker loop grabs a task, runs it, then records the result
// In optimized mode we check the IO queue first because those tasks use less CPU
// In FIFO mode we just check CPU first then IO, no special priority
fn run_worker(id: usize, queues: SharedQueues, results: SharedResults, is_optimized: bool) {
    let (lock, cvar) = &*queues;
    loop {
        let task = {
            let mut q = lock.lock().unwrap();
            loop {
                // pick which queue to pull from based on the scheduling mode
                let task_option = if is_optimized {
                    q.io.pop_front().or_else(|| q.cpu.pop_front())
                } else {
                    q.cpu.pop_front().or_else(|| q.io.pop_front())
                };

                if let Some(t) = task_option {
                    // make sure we have enough CPU left to run this task
                    if q.current_cpu_usage + t.cpu_cost <= 100 {
                        q.current_cpu_usage += t.cpu_cost;
                        q.active_workers += 1;
                        break t;
                    } else {
                        // not enough CPU available right now, put the task back and wait
                        if t.kind == TaskKind::Io {
                            q.io.push_front(t);
                        } else {
                            q.cpu.push_front(t);
                        }
                    }
                }

                // if the generator is done and both queues are empty we can exit
                if q.shutdown && q.cpu.is_empty() && q.io.is_empty() {
                    return;
                }

                // nothing to do right now, sleep until something changes
                q = cvar.wait(q).unwrap();
            }
        };

        // how long the task sat in the queue before we picked it up
        let wait_ms = task.enqueue_time.elapsed().as_millis();

        // println!("[worker {}] running task {} ({:?}, {}ms, waited {}ms)",
        //         id, task.id, task.kind, task.duration_ms, wait_ms);

        // simulate the task actually doing work
        thread::sleep(Duration::from_millis(task.duration_ms));

        // give back the CPU that this task was using and wake up any waiting workers
        {
            let mut q = lock.lock().unwrap();
            q.current_cpu_usage = q.current_cpu_usage.saturating_sub(task.cpu_cost);
            if q.active_workers > 0 {
                q.active_workers -= 1;
            }
            cvar.notify_all();
        }

        // save the completed task stats so we can print them at the end
        let turnaround_ms = task.enqueue_time.elapsed().as_millis();
        {
            let mut r = results.lock().unwrap();
            r.push(CompletedTask {
                id: task.id,
                kind: task.kind,
                wait_ms,
                turnaround_ms,
                worker_id: id,
                busy_ms: task.duration_ms as u128,
            });
        }
    }
}

// Metrics printer
fn print_metrics(title: &str, results: &[CompletedTask], makespan_ms: u128, total_runtime: u128, io_ratio: u64, num_tasks: u64) {
    let total = results.len();
    if total == 0 { return; }

    let cpu_tasks: Vec<_> = results.iter().filter(|r| r.kind == TaskKind::Cpu).collect();
    let io_tasks: Vec<_> = results.iter().filter(|r| r.kind == TaskKind::Io).collect();

    let avg_wait = results.iter().map(|r| r.wait_ms).sum::<u128>() as f64 / total as f64;
    let avg_turnaround = results.iter().map(|r| r.turnaround_ms).sum::<u128>() as f64 / total as f64;
    let max_task = results.iter().max_by_key(|r| r.wait_ms).unwrap();
    let total_busy_time: u128 = results.iter().map(|r| r.busy_ms).sum();
    let avg_workers_active = total_busy_time as f64 / makespan_ms as f64;

    println!("\n== {} simulation ==", title);
    println!("{} tasks, {}% IO / {}% CPU, 8 workers, cap 100%", num_tasks, io_ratio * 10, 100 - io_ratio * 10);
    println!("\n- results -");
    println!("{:<20} : {} ms", "total runtime", total_runtime);
    println!("{:<20} : {} ms", "makespan", makespan_ms);
    println!("{:<20} : {}  (IO={}, CPU={})", "tasks completed", total, io_tasks.len(), cpu_tasks.len());
    println!("{:<20} : {:.2} ms", "avg wait time", avg_wait);

    // only show the per-type wait breakdown for the optimized run
    if title == "Optimized" {
        let avg_wait_io = io_tasks.iter().map(|r| r.wait_ms).sum::<u128>() as f64 / io_tasks.len() as f64;
        let avg_wait_cpu = cpu_tasks.iter().map(|r| r.wait_ms).sum::<u128>() as f64 / cpu_tasks.len() as f64;
        println!("{:<20} : {:.2} ms", "avg wait (IO only)", avg_wait_io);
        println!("{:<20} : {:.2} ms", "avg wait (CPU only)", avg_wait_cpu);
    }

    println!("{:<20} : {:.2} ms", "avg turnaround time", avg_turnaround);
    println!("{:<20} : {} ms (task #{})", "max wait time", max_task.wait_ms, max_task.id);
    println!("{:<20} : {:.2} / 8", "avg workers active", avg_workers_active);
    println!("{:<20} : monitor_log.csv", "monitor csv");
    println!("---------------------------------------------\n");
}

// Main
fn main() {
    let num_workers: usize = 8;
    let num_tasks: u64 = 200;

    println!("Starting Scheduler Simulation: {} workers, {} tasks", num_workers, num_tasks);

    // Experiment A: balanced workload, 70% IO / 30% CPU
    println!("\n===== EXPERIMENT A: Balanced Workload (70% IO / 30% CPU) =====");
    let (fifo_makespan, fifo_results) = run_simulation_instance(num_workers, num_tasks, false, 7);
    print_metrics("FIFO", &fifo_results, fifo_makespan, fifo_makespan, 7, num_tasks);

    println!("\n{}\n", "-".repeat(45));

    let (opt_makespan, opt_results) = run_simulation_instance(num_workers, num_tasks, true, 7);
    print_metrics("Optimized", &opt_results, opt_makespan, opt_makespan, 7, num_tasks);

    println!("\n{}\n", "=".repeat(45));

    // Experiment B: stressed workload, 80% CPU / 20% IO
    // CPU tasks cost 35% each so only 2 can run at once, this creates a heavy backlog
    println!("===== EXPERIMENT B: Stressed Workload (20% IO / 80% CPU) =====");
    let (fifo_makespan_b, fifo_results_b) = run_simulation_instance(num_workers, num_tasks, false, 2);
    print_metrics("FIFO", &fifo_results_b, fifo_makespan_b, fifo_makespan_b, 2, num_tasks);

    println!("\n{}\n", "-".repeat(45));

    let (opt_makespan_b, opt_results_b) = run_simulation_instance(num_workers, num_tasks, true, 2);
    print_metrics("Optimized", &opt_results_b, opt_makespan_b, opt_makespan_b, 2, num_tasks);
}

// runs one full simulation and returns the makespan and all completed task records
// we call this twice so each run starts with a completely clean state
fn run_simulation_instance(workers: usize, tasks: u64, optimized: bool, io_ratio: u64) -> (u128, Vec<CompletedTask>) {
    let queues: SharedQueues = Arc::new((Mutex::new(TaskQueues::new()), Condvar::new()));
    let results: SharedResults = Arc::new(Mutex::new(Vec::new()));
    let start_time = Instant::now();

    // Spawn Workers
    let mut worker_handles = vec![];
    for id in 0..workers {
        let q = Arc::clone(&queues);
        let r = Arc::clone(&results);
        worker_handles.push(thread::spawn(move || {
            run_worker(id, q, r, optimized)
        }));
    }

    // Spawn Generator
    let gen_queues = Arc::clone(&queues);
    let gen_handle = thread::spawn(move || {
        run_generator(gen_queues, tasks, io_ratio)
    });

    // Wait for completion (The Join Fix)
    gen_handle.join().unwrap();
    for h in worker_handles {
        h.join().unwrap();
    }

    let makespan = start_time.elapsed().as_millis();
    
    let final_results = results.lock().unwrap().drain(..).collect();
    
    (makespan, final_results)
}