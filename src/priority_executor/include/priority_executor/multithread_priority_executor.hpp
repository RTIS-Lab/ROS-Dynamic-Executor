#ifndef RTIS_MULTITHREAD_EXECUTOR
#define RTIS_MULTITHREAD_EXECUTOR

#include <priority_executor/priority_executor.hpp>
#include "rclcpp/detail/mutex_two_priorities.hpp"
#include <set>

namespace timed_executor
{
  class MultithreadTimedExecutor : public TimedExecutor
  {
  public:
    RCLCPP_PUBLIC
    explicit MultithreadTimedExecutor(
        const rclcpp::ExecutorOptions &options = rclcpp::ExecutorOptions(), std::string name = "unnamed executor", int number_of_threads = 2, std::chrono::nanoseconds next_exec_timeout = std::chrono::nanoseconds(-1));

    RCLCPP_PUBLIC size_t get_number_of_threads();
    RCLCPP_PUBLIC void spin() override;

  protected:
    RCLCPP_PUBLIC void run(size_t thread_number);

  private:
    size_t number_of_threads_;
    std::set<rclcpp::TimerBase::SharedPtr> scheduled_timers_;
    static std::unordered_map<MultithreadTimedExecutor *,
                              std::shared_ptr<rclcpp::detail::MutexTwoPriorities>>
        wait_mutex_set_;
    static std::mutex shared_wait_mutex_;
    std::chrono::nanoseconds next_exec_timeout_ = std::chrono::nanoseconds(-1);
  };
}

#endif