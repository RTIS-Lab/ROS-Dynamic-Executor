// Copyright 2014 Open Source Robotics Foundation, Inc.
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

#ifndef RTIS_DEFAULT_EXECUTOR
#define RTIS_DEFAULT_EXECUTOR

#include <rmw/rmw.h>

#include <cassert>
#include <cstdlib>
#include <memory>
#include <vector>
#include <set>

#include "rclcpp/executor.hpp"
#include "rclcpp/rclcpp.hpp"
#include "rclcpp/macros.hpp"
#include "rclcpp/memory_strategies.hpp"
#include "rclcpp/node.hpp"
#include "rclcpp/utilities.hpp"
#include "rclcpp/rate.hpp"
#include "rclcpp/visibility_control.hpp"
#include "rclcpp/detail/mutex_two_priorities.hpp"
#include "priority_executor/priority_memory_strategy.hpp"
#include "simple_timer/rt-sched.hpp"

class RTISTimed
{
public:
  node_time_logger logger_;
};

class ROSDefaultMultithreadedExecutor : public rclcpp::Executor, public RTISTimed
{
public:
  RCLCPP_PUBLIC
  explicit ROSDefaultMultithreadedExecutor(
      const rclcpp::ExecutorOptions &options = rclcpp::ExecutorOptions(), int number_of_threads = 2, std::chrono::nanoseconds next_exec_timeout = std::chrono::nanoseconds(-1));

  RCLCPP_PUBLIC size_t get_number_of_threads();
  RCLCPP_PUBLIC void spin() override;
  bool get_next_executable(rclcpp::AnyExecutable &any_executable, std::chrono::nanoseconds timeout = std::chrono::nanoseconds(-1));

protected:
  RCLCPP_PUBLIC void run(size_t thread_number);

private:
  size_t number_of_threads_;
  std::set<rclcpp::TimerBase::SharedPtr> scheduled_timers_;
  static std::unordered_map<ROSDefaultMultithreadedExecutor *,
                            std::shared_ptr<rclcpp::detail::MutexTwoPriorities>>
      wait_mutex_set_;
  static std::mutex shared_wait_mutex_;
  std::chrono::nanoseconds next_exec_timeout_ = std::chrono::nanoseconds(-1);
};

/// Single-threaded executor implementation.
/**
 * This is the default executor created by rclcpp::spin.
 */
class ROSDefaultExecutor : public rclcpp::Executor, public RTISTimed
{
public:
  RCLCPP_SMART_PTR_DEFINITIONS(ROSDefaultExecutor)

  /// Default constructor. See the default constructor for Executor.
  RCLCPP_PUBLIC
  explicit ROSDefaultExecutor(
      const rclcpp::ExecutorOptions &options = rclcpp::ExecutorOptions());

  /// Default destructor.
  RCLCPP_PUBLIC
  virtual ~ROSDefaultExecutor();

  /// Single-threaded implementation of spin.
  /**
   * This function will block until work comes in, execute it, and then repeat
   * the process until canceled.
   * It may be interrupt by a call to rclcpp::Executor::cancel() or by ctrl-c
   * if the associated context is configured to shutdown on SIGINT.
   * \throws std::runtime_error when spin() called while already spinning
   */
  RCLCPP_PUBLIC
  void
  spin() override;
  bool get_next_executable(rclcpp::AnyExecutable &any_executable, std::chrono::nanoseconds timeout = std::chrono::nanoseconds(-1));

  RCLCPP_PUBLIC
  void
  wait_for_work(std::chrono::nanoseconds timeout = std::chrono::nanoseconds(-1));

private:
  RCLCPP_DISABLE_COPY(ROSDefaultExecutor)
};

#endif // RCLCPP__EXECUTORS__SINGLE_THREADED_EXECUTOR_HPP_