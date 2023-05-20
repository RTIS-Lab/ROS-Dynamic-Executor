#include "priority_executor/multithread_priority_executor.hpp"
namespace timed_executor
{
    std::unordered_map<MultithreadTimedExecutor *, std::shared_ptr<rclcpp::detail::MutexTwoPriorities>>
        MultithreadTimedExecutor::wait_mutex_set_;
    std::mutex MultithreadTimedExecutor::shared_wait_mutex_;
    MultithreadTimedExecutor::MultithreadTimedExecutor(
        const rclcpp::ExecutorOptions &options,
        std::string name,
        int number_of_threads, std::chrono::nanoseconds next_exec_timeout)
        : TimedExecutor(options, name)
    {
        std::lock_guard<std::mutex> wait_lock(MultithreadTimedExecutor::shared_wait_mutex_);
        wait_mutex_set_[this] = std::make_shared<rclcpp::detail::MutexTwoPriorities>();
        number_of_threads_ = number_of_threads;
        next_exec_timeout_ = next_exec_timeout;
    }

    size_t MultithreadTimedExecutor::get_number_of_threads()
    {
        return number_of_threads_;
    }

    void MultithreadTimedExecutor::spin()
    {
        if (spinning.exchange(true))
        {
            throw std::runtime_error("spin() called while already spinning");
        }

        std::vector<std::thread> threads;
        size_t thread_id = 0;
        {
            auto wait_mutex = MultithreadTimedExecutor::wait_mutex_set_[this];
            auto low_priority_wait_mutex = wait_mutex->get_low_priority_lockable();
            std::lock_guard<rclcpp::detail::MutexTwoPriorities::LowPriorityLockable> wait_lock(low_priority_wait_mutex);
            for (; thread_id < number_of_threads_ - 1; ++thread_id)
            {
                auto func = std::bind(&MultithreadTimedExecutor::run, this, thread_id);
                threads.emplace_back(func);
            }
        }
        run(thread_id);
        for (auto &thread : threads)
        {
            thread.join();
        }
    }

    void MultithreadTimedExecutor::run(size_t thread_number)
    {
        // set affinity
        cpu_set_t cpuset;
        CPU_ZERO(&cpuset);
        CPU_SET(thread_number, &cpuset);
        int rc = pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &cpuset);
        if (rc != 0)
        {
            std::cout << "Error calling pthread_setaffinity_np: " << rc << "\n";
        }

        while (rclcpp::ok(this->context_) && spinning.load())
        {
            rclcpp::AnyExecutable any_executable;
            {
                auto wait_mutex = MultithreadTimedExecutor::wait_mutex_set_[this];
                auto low_priority_wait_mutex = wait_mutex->get_low_priority_lockable();
                std::lock_guard<rclcpp::detail::MutexTwoPriorities::LowPriorityLockable> wait_lock(low_priority_wait_mutex);
                if (!rclcpp::ok(this->context_) || !spinning.load())
                {
                    return;
                }
                if (!get_next_executable(any_executable, next_exec_timeout_))
                {
                    continue;
                }
                if (any_executable.timer)
                {
                    if (scheduled_timers_.count(any_executable.timer) != 0)
                    {
                        if (any_executable.callback_group)
                        {
                            any_executable.callback_group->can_be_taken_from().store(true);
                        }
                        continue;
                    }
                    scheduled_timers_.insert(any_executable.timer);
                }
            }
            execute_any_executable(any_executable);
            if (any_executable.timer)
            {
                auto wait_mutex = MultithreadTimedExecutor::wait_mutex_set_[this];
                auto high_priority_wait_mutex = wait_mutex->get_high_priority_lockable();
                std::lock_guard<rclcpp::detail::MutexTwoPriorities::HighPriorityLockable> wait_lock(high_priority_wait_mutex);
                auto it = scheduled_timers_.find(any_executable.timer);
                if (it != scheduled_timers_.end())
                {
                    scheduled_timers_.erase(it);
                }
            }
            any_executable.callback_group.reset();
            if (prio_memory_strategy_ != nullptr)
            {
                auto wait_mutex = MultithreadTimedExecutor::wait_mutex_set_[this];
                auto low_priority_wait_mutex = wait_mutex->get_low_priority_lockable();
                std::lock_guard<rclcpp::detail::MutexTwoPriorities::LowPriorityLockable> wait_lock(low_priority_wait_mutex);
                // std::shared_ptr<PriorityMemoryStrategy<>> prio_memory_strategy_ = std::dynamic_pointer_cast<PriorityMemoryStrategy>(memory_strategy_);
                prio_memory_strategy_->post_execute(any_executable, thread_number);
            }
        }
    }
}
