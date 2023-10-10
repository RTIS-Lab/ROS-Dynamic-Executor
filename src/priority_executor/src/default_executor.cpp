// Copyright 2015 Open Source Robotics Foundation, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "priority_executor/default_executor.hpp"
#include "rcpputils/scope_exit.hpp"
#include "rclcpp/any_executable.hpp"
#include "simple_timer/rt-sched.hpp"

ROSDefaultExecutor::ROSDefaultExecutor(const rclcpp::ExecutorOptions &options)
    : rclcpp::Executor(options)
{
  logger_ = create_logger();
}

ROSDefaultExecutor::~ROSDefaultExecutor() {}

void ROSDefaultExecutor::wait_for_work(std::chrono::nanoseconds timeout)
{
  {
    std::unique_lock<std::mutex> lock(memory_strategy_mutex_);

    // Collect the subscriptions and timers to be waited on
    memory_strategy_->clear_handles();
    bool has_invalid_weak_nodes = memory_strategy_->collect_entities(weak_nodes_);

    // Clean up any invalid nodes, if they were detected
    if (has_invalid_weak_nodes)
    {
      auto node_it = weak_nodes_.begin();
      auto gc_it = guard_conditions_.begin();
      while (node_it != weak_nodes_.end())
      {
        if (node_it->expired())
        {
          node_it = weak_nodes_.erase(node_it);
          memory_strategy_->remove_guard_condition(*gc_it);
          gc_it = guard_conditions_.erase(gc_it);
        }
        else
        {
          ++node_it;
          ++gc_it;
        }
      }
    }
    // clear wait set
    rcl_ret_t ret = rcl_wait_set_clear(&wait_set_);
    if (ret != RCL_RET_OK)
    {
      rclcpp::exceptions::throw_from_rcl_error(ret, "Couldn't clear wait set");
    }

    // The size of waitables are accounted for in size of the other entities
    ret = rcl_wait_set_resize(
        &wait_set_, memory_strategy_->number_of_ready_subscriptions(),
        memory_strategy_->number_of_guard_conditions(), memory_strategy_->number_of_ready_timers(),
        memory_strategy_->number_of_ready_clients(), memory_strategy_->number_of_ready_services(),
        memory_strategy_->number_of_ready_events());
    if (RCL_RET_OK != ret)
    {
      rclcpp::exceptions::throw_from_rcl_error(ret, "Couldn't resize the wait set");
    }

    if (!memory_strategy_->add_handles_to_wait_set(&wait_set_))
    {
      throw std::runtime_error("Couldn't fill wait set");
    }
  }
  rcl_ret_t status =
      rcl_wait(&wait_set_, std::chrono::duration_cast<std::chrono::nanoseconds>(timeout).count());
  if (status == RCL_RET_WAIT_SET_EMPTY)
  {
    RCUTILS_LOG_WARN_NAMED(
        "rclcpp",
        "empty wait set received in rcl_wait(). This should never happen.");
  }
  else if (status != RCL_RET_OK && status != RCL_RET_TIMEOUT)
  {
    using rclcpp::exceptions::throw_from_rcl_error;
    throw_from_rcl_error(status, "rcl_wait() failed");
  }

  // check the null handles in the wait set and remove them from the handles in memory strategy
  // for callback-based entities
  memory_strategy_->remove_null_handles(&wait_set_);
}

bool ROSDefaultExecutor::get_next_executable(rclcpp::AnyExecutable &any_executable, std::chrono::nanoseconds timeout)
{
  bool success = false;
  // Check to see if there are any subscriptions or timers needing service
  // TODO(wjwwood): improve run to run efficiency of this function
  // sched_yield();
  wait_for_work(timeout);
  success = get_next_ready_executable(any_executable);
  return success;
}

void ROSDefaultExecutor::spin()
{
  if (spinning.exchange(true))
  {
    throw std::runtime_error("spin() called while already spinning");
  }
  RCPPUTILS_SCOPE_EXIT(this->spinning.store(false););
  while (rclcpp::ok(this->context_) && spinning.load())
  {
    rclcpp::AnyExecutable any_executable;
    if (get_next_executable(any_executable))
    {
      execute_any_executable(any_executable);
    }
  }
}

bool ROSDefaultMultithreadedExecutor::get_next_executable(rclcpp::AnyExecutable &any_executable, std::chrono::nanoseconds timeout)
{
  bool success = false;
  // Check to see if there are any subscriptions or timers needing service
  // TODO(wjwwood): improve run to run efficiency of this function

  // try to get an executable
  // record the start time
  auto start = std::chrono::steady_clock::now();
  success = get_next_ready_executable(any_executable);
  // and the end time
  auto end = std::chrono::steady_clock::now();
  std::stringstream oss;
  oss << "{\"operation\":\"get_next_executable\", \"result\":\"" << success << "\", \"duration\":\"" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "\"}";
  log_entry(logger_, oss.str());

  // If there are none
  if (!success)
  {
    // Wait for subscriptions or timers to work on
    // queue refresh
    start = std::chrono::steady_clock::now();
    wait_for_work(timeout);
    // and the end time
    end = std::chrono::steady_clock::now();
    auto wait_duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
    oss.str("");
    oss << "{\"operation\":\"wait_for_work\", \"result\":\"" << success << "\", \"wait_duration\":\"" << wait_duration << "\"}";
    log_entry(logger_, oss.str());
    if (!spinning.load())
    {
      return false;
    }
    // Try again
    start = std::chrono::steady_clock::now();
    success = get_next_ready_executable(any_executable);
    // and the end time
    end = std::chrono::steady_clock::now();
    oss.str("");
    oss << "{\"operation\":\"get_next_executable\", \"result\":\"" << success << "\", \"duration\":\"" << std::chrono::duration_cast<std::chrono::microseconds>(end - start).count() << "\"}";
    log_entry(logger_, oss.str());
  }
  return success;
}

std::unordered_map<ROSDefaultMultithreadedExecutor *, std::shared_ptr<rclcpp::detail::MutexTwoPriorities>> ROSDefaultMultithreadedExecutor::wait_mutex_set_;
std::mutex ROSDefaultMultithreadedExecutor::shared_wait_mutex_;

ROSDefaultMultithreadedExecutor::ROSDefaultMultithreadedExecutor(
    const rclcpp::ExecutorOptions &options,
    int number_of_threads, std::chrono::nanoseconds next_exec_timeout)
    : Executor(options)
{
  std::lock_guard<std::mutex> wait_lock(ROSDefaultMultithreadedExecutor::shared_wait_mutex_);
  wait_mutex_set_[this] = std::make_shared<rclcpp::detail::MutexTwoPriorities>();
  number_of_threads_ = number_of_threads;
  next_exec_timeout_ = next_exec_timeout;
  logger_ = create_logger();
}

void ROSDefaultMultithreadedExecutor::spin()
{
  if (spinning.exchange(true))
  {
    throw std::runtime_error("spin() called while already spinning");
  }

  std::vector<std::thread> threads;
  size_t thread_id = 0;
  {
    auto wait_mutex = ROSDefaultMultithreadedExecutor::wait_mutex_set_[this];
    auto low_priority_wait_mutex = wait_mutex->get_low_priority_lockable();
    std::lock_guard<rclcpp::detail::MutexTwoPriorities::LowPriorityLockable> wait_lock(low_priority_wait_mutex);
    for (; thread_id < number_of_threads_ - 1; ++thread_id)
    {
      auto func = std::bind(&ROSDefaultMultithreadedExecutor::run, this, thread_id);
      threads.emplace_back(func);
    }
  }
  run(thread_id);
  for (auto &thread : threads)
  {
    thread.join();
  }
}

void ROSDefaultMultithreadedExecutor::run(size_t thread_number)
{
  while (rclcpp::ok(this->context_) && spinning.load())
  {
    rclcpp::AnyExecutable any_exec;
    {
      auto wait_mutex = ROSDefaultMultithreadedExecutor::wait_mutex_set_[this];
      auto low_priority_wait_mutex = wait_mutex->get_low_priority_lockable();
      std::lock_guard<rclcpp::detail::MutexTwoPriorities::LowPriorityLockable> wait_lock(low_priority_wait_mutex);
      if (!rclcpp::ok(this->context_) || !spinning.load())
      {
        return;
      }
      if (!get_next_executable(any_exec, next_exec_timeout_))
      {
        continue;
      }
      if (any_exec.timer)
      {
        // Guard against multiple threads getting the same timer.
        if (scheduled_timers_.count(any_exec.timer) != 0)
        {
          // Make sure that any_exec's callback group is reset before
          // the lock is released.
          if (any_exec.callback_group)
          {
            any_exec.callback_group->can_be_taken_from().store(true);
          }
          continue;
        }
        scheduled_timers_.insert(any_exec.timer);
      }
    }
    // if (yield_before_execute_)
    // {
    //   std::this_thread::yield();
    // }

    execute_any_executable(any_exec);

    if (any_exec.timer)
    {
      auto wait_mutex = ROSDefaultMultithreadedExecutor::wait_mutex_set_[this];
      auto high_priority_wait_mutex = wait_mutex->get_high_priority_lockable();
      std::lock_guard<rclcpp::detail::MutexTwoPriorities::HighPriorityLockable> wait_lock(high_priority_wait_mutex);
      auto it = scheduled_timers_.find(any_exec.timer);
      if (it != scheduled_timers_.end())
      {
        scheduled_timers_.erase(it);
      }
    }
    // Clear the callback_group to prevent the AnyExecutable destructor from
    // resetting the callback group `can_be_taken_from`
    any_exec.callback_group.reset();
  }
}
