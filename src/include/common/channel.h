//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// channel.h
//
// Identification: src/include/common/channel.h
//
// Copyright (c) 2015-2023, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <condition_variable>  // NOLINT
#include <mutex>               // NOLINT
#include <queue>
#include <utility>

namespace bustub {

/**
 * Channels allow for safe sharing of data between threads. This is a multi-producer multi-consumer channel.
 * Channels 允许线程间消息之间的安全共享，这是一个多对多的channel
 */
template <class T>
class Channel {
 public:
  Channel() = default;
  ~Channel() = default;

  /**
   * @brief Inserts an element into a shared queue.
   *插入元素到一个共享队列
   * @param element The element to be inserted.
   * 这个element参数将被插入
   */
  void Put(T element) {
    std::unique_lock<std::mutex> lk(m_);
    q_.push(std::move(element));
    lk.unlock();
    cv_.notify_all();
  }

  /**
   * @brief Gets an element from the shared queue. If the queue is empty, blocks until an element is available.
   */
  auto Get() -> T {
    std::unique_lock<std::mutex> lk(m_);
    cv_.wait(lk, [&]() { return !q_.empty(); });
    T element = std::move(q_.front());
    q_.pop();
    return element;
  }

 private:
  std::mutex m_;
  std::condition_variable cv_;
  std::queue<T> q_;
};
}  // namespace bustub
