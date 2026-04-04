#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

// ThreadPool manages a fixed set of worker threads that pull tasks from a
// shared queue. All parallel processing in the analytics pipeline routes
// through here — indicator calculations, Kafka message handling, and
// InfluxDB writes all submit tasks to this pool rather than spawning
// their own threads.
class ThreadPool {
public:
    // Constructor - starts num_threads worker threads.
    // Defaults number to CPU cores available on the machine.
    explicit ThreadPool(size_t num_threads = std::thread::hardware_concurrency());

    // Destructor - all workers to stop and waits for them to finish
    // their current tasks before returning. No tasks are dropped.
    ~ThreadPool();

    // Submits a callable and its arguments to the task queue. Returns a
    // std::future so the caller can wait for the result or check for
    // exceptions.
    template<typename F, typename... Args>
    auto submit(F&& func, Args&&... args) -> std::future<std::invoke_result_t<F, Args...>>;

    // Returns the num of worker threads in the pool.
    size_t size() const {
        return workers_.size();
    }

    // Returns the num of tasks currently waiting in the queue.
    size_t queue_size() const;

    bool is_stopped() const {
        return stopped_.load();
    }

private:
    // Worker thread function — loops pulling tasks from the queue until
    // stopped_ is set and the queue is empty.
    void worker_loop();

    std::vector<std::thread> workers_;        // fixed set of worker threads
    std::queue<std::function<void()>> tasks_; // pending task queue
    std::mutex queue_mutex_;                  // protects access to tasks_
    std::condition_variable cv_;              // wakes workers when tasks arrive
    std::atomic<bool> stopped_{ false };      // signals workers to shut down
};

// Must be in the header so the compiler can instantiate it for any callable
// type it passes in.
template<typename F, typename... Args>
auto ThreadPool::submit(F&& func, Args&&... args) -> std::future<std::invoke_result_t<F, Args...>> {
    using ReturnType = std::invoke_result_t<F, Args...>;

    // Wrap the callable in a packaged_task so we can extract a future from it.
    // shared_ptr is needed because std::function requires copyable callables
    // but packaged_task is move-only.
    auto task = std::make_shared<std::packaged_task<ReturnType()>>(
        std::bind(std::forward<F>(func), std::forward<Args>(args)...)
    );

    std::future<ReturnType> future = task->get_future();

    {
        std::unique_lock<std::mutex> lock(queue_mutex_);
        if (stopped_.load()) {
            throw std::runtime_error("ThreadPool: cannot submit task — pool has been stopped");
        }

        // Enqueue a lambda that invokes the packaged_task when a worker picks it up
        tasks_.emplace([task]() { (*task)(); });
    }

    // Wake one sleeping worker to pick up the new task can try notify_all() later on..
    cv_.notify_one();
    return future;
}