#include "thread_pool.hpp"
#include <spdlog/spdlog.h>

ThreadPool::ThreadPool(size_t num_threads) {
    // Use at least 1 thread in case hardware_concurrency() returns 0
    if (num_threads == 0) {
        num_threads = 1;
    }

    spdlog::info("ThreadPool: starting {} worker threads", num_threads);

    // Spawn all worker threads up front — they immediately block on the
    // condition variable waiting for tasks to arrive.
    workers_.reserve(num_threads);
    for (size_t i = 0; i < num_threads; ++i) {
        workers_.emplace_back(&ThreadPool::worker_loop, this);
    }
}

ThreadPool::~ThreadPool() {
    // Signal all workers to stop accepting new tasks and finish up
    stopped_.store(true);

    // Wake every worker so they can check stopped_ and exit their loop
    cv_.notify_all();

    // Wait for every worker to finish its current task and exit
    for (std::thread& worker : workers_) {
        if (worker.joinable()) {
            worker.join();
        }
    }

    spdlog::info("ThreadPool: all {} workers shut down cleanly", workers_.size());
}

void ThreadPool::worker_loop() {
    // Each worker runs this loop independently — it sleeps until a task
    // arrives, executes it, then goes back to sleep. This continues until
    // stopped_ is true and the queue is empty.
    while (true) {
        std::function<void()> task;

        {
            std::unique_lock<std::mutex> lock(queue_mutex_);

            // Sleep until either a task is available or the pool is stopping.
            cv_.wait(lock, [this]() {
                return !tasks_.empty() || stopped_.load();
            });

            // If woken to stop and there are no remaining tasks, exit
            if (stopped_.load() && tasks_.empty()) {
                return;
            }

            // Take ownership of the next task from the front of the queue
            task = std::move(tasks_.front());
            tasks_.pop();
        }

        // Execute the task outside the lock so other workers can dequeue their
        // own tasks concurrently while this one runs
        try {
            task();
        } catch (const std::exception& e) {
            // Log but do not rethrow — a crashing worker would terminate the
            // whole pool.
            spdlog::error("ThreadPool: worker caught exception: {}", e.what());
        } catch (...) {
            spdlog::error("ThreadPool: worker caught unknown exception");
        }
    }
}

size_t ThreadPool::queue_size() const {
    std::unique_lock<std::mutex> lock(queue_mutex_);
    return tasks_.size();
}