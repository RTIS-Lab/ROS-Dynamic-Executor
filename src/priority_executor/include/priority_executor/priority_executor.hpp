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

#ifndef RTIS_TIMED_EXECUTOR
#define RTIS_TIMED_EXECUTOR

#include <rmw/rmw.h>

#include <cassert>
#include <cstdlib>
#include <memory>
#include <vector>
#include <time.h>

#include "rclcpp/executor.hpp"
#include "rclcpp/macros.hpp"
#include "rclcpp/memory_strategies.hpp"
#include "rclcpp/node.hpp"
#include "rclcpp/utilities.hpp"
#include "rclcpp/rate.hpp"
#include "rclcpp/visibility_control.hpp"
#include "priority_executor/priority_memory_strategy.hpp"
namespace timed_executor
{

  /// Single-threaded executor implementation.
  /**
 * This is the default executor created by rclcpp::spin.
 */
  class TimedExecutor : public rclcpp::Executor
  {
  public:
    RCLCPP_SMART_PTR_DEFINITIONS(TimedExecutor)

    /// Default constructor. See the default constructor for Executor.
    RCLCPP_PUBLIC
    explicit TimedExecutor(
        const rclcpp::ExecutorOptions &options = rclcpp::ExecutorOptions(), std::string name = "unnamed executor");

    /// Default destructor.
    RCLCPP_PUBLIC
    virtual ~TimedExecutor();

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
    std::string name;

    void set_use_priorities(bool use_prio);
    std::shared_ptr<PriorityMemoryStrategy<>> prio_memory_strategy_ = nullptr;

  protected:
    bool
    get_next_executable(rclcpp::AnyExecutable &any_executable, std::chrono::nanoseconds timeout = std::chrono::nanoseconds(-1));

  private:
    RCLCPP_DISABLE_COPY(TimedExecutor)
    void
    wait_for_work(std::chrono::nanoseconds timeout);

    bool
    get_next_ready_executable(rclcpp::AnyExecutable &any_executable);

    bool use_priorities = true;
  };

} // namespace timed_executor

#endif // RCLCPP__EXECUTORS__SINGLE_THREADED_EXECUTOR_HPP_
