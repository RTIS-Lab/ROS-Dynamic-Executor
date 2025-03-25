#include "priority_executor/priority_executor.hpp"
#include "priority_executor/priority_memory_strategy.hpp"
#include "rclcpp/rclcpp.hpp"
#include "std_msgs/msg/string.hpp"
#include <fstream>
#include <string>
#include <unistd.h>

// re-create the classic talker-listener example with two listeners
class Talker : public rclcpp::Node
{
public:
  Talker() : Node("talker")
  {
    // Create a publisher on the "chatter" topic with 10 msg queue size.
    pub_ = this->create_publisher<std_msgs::msg::String>("chatter", 10);
    // Create a timer of period 1s that calls our callback member function.
    timer_ = this->create_wall_timer(std::chrono::seconds(1),
                                     std::bind(&Talker::timer_callback, this));
  }
  // the timer must be public
  rclcpp::TimerBase::SharedPtr timer_;

private:
  void timer_callback()
  {
    std_msgs::msg::String msg;
    msg.data = "Hello World!";
    RCLCPP_INFO(this->get_logger(), "Publishing: '%s'", msg.data.c_str());
    pub_->publish(msg);
  }
  rclcpp::Publisher<std_msgs::msg::String>::SharedPtr pub_;
};

class Listener : public rclcpp::Node
{
public:
  Listener(std::string name) : Node(name)
  {
    // Create a subscription on the "chatter" topic with the default callback
    // method.
    sub_ = this->create_subscription<std_msgs::msg::String>(
        "chatter", 10,
        std::bind(&Listener::callback, this, std::placeholders::_1));
  }
  // the publisher must be public
  rclcpp::Subscription<std_msgs::msg::String>::SharedPtr sub_;

private:
  void callback(const std_msgs::msg::String::SharedPtr msg)
  {
    RCLCPP_INFO(this->get_logger(), "I heard: '%s'", msg->data.c_str());
  }
};

int main(int argc, char **argv)
{
  rclcpp::init(argc, argv);
  auto talker = std::make_shared<Talker>();
  auto listener1 = std::make_shared<Listener>("listener1");
  auto listener2 = std::make_shared<Listener>("listener2");
  rclcpp::ExecutorOptions options;

  auto strategy = std::make_shared<PriorityMemoryStrategy<>>();
  options.memory_strategy = strategy;
  auto executor = new timed_executor::TimedExecutor(options);

  // must be set to post_execute can set new deadlines
  executor->prio_memory_strategy_ = strategy;


  // the new funcitons in PriorityMemoryStrategy accept the handle of the
  // timer/subscription as the first argument
  strategy->set_executable_deadline(talker->timer_->get_timer_handle(), 1000,
                                    TIMER, 0);
  // you _must_ set the timer_handle for each chain
  strategy->get_priority_settings(talker->timer_->get_timer_handle())
      ->timer_handle = talker->timer_;
  // you _must_ mark the first executable in the chain
  strategy->set_first_in_chain(talker->timer_->get_timer_handle());
  // set the same period and chain_id for each callback in the chain
  strategy->set_executable_deadline(listener1->sub_->get_subscription_handle(),
                                    1000, SUBSCRIPTION, 0);
  strategy->set_executable_deadline(listener2->sub_->get_subscription_handle(),
                                    1000, SUBSCRIPTION, 0);
  // you _must_ mark the last executable in the chain (used to keep track of different instances of the same chain)
  strategy->set_last_in_chain(listener2->sub_->get_subscription_handle());
  // add all the nodes to the executor
  executor->add_node(talker);
  executor->add_node(listener1);
  executor->add_node(listener2);

  // if the executor behaves unexpectedly, you can print the priority settings to make sure they are correct
  std::cout << *strategy->get_priority_settings(
                   talker->timer_->get_timer_handle())
            << std::endl;
  std::cout << *strategy->get_priority_settings(
                   listener1->sub_->get_subscription_handle())
            << std::endl;
  std::cout << *strategy->get_priority_settings(
                   listener2->sub_->get_subscription_handle())
            << std::endl;

  executor->spin();
}
