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

ROSDefaultMultithreadedExecutor::~ROSDefaultMultithreadedExecutor() {}


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
  success = get_next_ready_executable(any_executable);
  std::stringstream oss;
  oss << "{\"operation\":\"get_next_executable\", \"result\":\"" << success << "\"}";
  log_entry(logger_, oss.str());

  // If there are none
  if (!success)
  {
    // Wait for subscriptions or timers to work on
    // queue refresh
    wait_for_work(timeout);
    if (!spinning.load())
    {
      return false;
    }
    // Try again
    success = get_next_ready_executable(any_executable);
    oss.str("");
    oss << "{\"operation\":\"wait_for_work\", \"result\":\"" << success << "\"}";
  }
  return success;
}
