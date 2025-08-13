//
// Created by user on 25-7-31.
//
#pragma once

#include <chrono>
#include<random>
#include <thread>

#include "type.h"

#include <bthread/bthread.h>

namespace backup_tool {

class RetryPolicy {
public:
  struct Options {
    int  max_attempts      = 3;                          // 总尝试次数（含首次）
    std::chrono::milliseconds base_delay{200};          // 首次 back-off
    double multiplier       = 2.0;                       // 指数增长
    std::chrono::milliseconds max_delay{3000};          // 封顶
    bool enable_jitter      = true;                      // 是否加随机抖动
  };

  explicit RetryPolicy(Options opts) : opts_(opts) {}

  /**
   * 包装可重试操作，返回最终 Status。
   *   Fn 是可调用对象，签名：Status f()
   *   若返回 OK 则立刻结束；否则由 is_retryable() 决定是否再试
   */
  template <typename Fn>
  Status WithRetry(Fn&& fn) const {
    std::default_random_engine rng{std::random_device{}()};
    std::uniform_real_distribution<double> jitter(0.5, 1.5);

    Status last;
    for (int attempt = 1; attempt <= opts_.max_attempts; ++attempt) {
      last = fn();
      if (last.ok()) return last;
      DB_WARNING("retry for %d times...",attempt)
      if (!last.is_retryable() || attempt == opts_.max_attempts) break;

      // 计算延迟
      auto delay = opts_.base_delay *
                   std::pow(opts_.multiplier, attempt - 1);
      if (delay > opts_.max_delay) delay = opts_.max_delay;
      if (opts_.enable_jitter) {                      // 抖动防止雪崩
        delay = std::chrono::milliseconds(
            static_cast<int64_t>(delay.count() * jitter(rng)));
      }
      bthread_usleep( delay.count() * 1000);
    }
    return last;   // 最终失败状态
  }

private:
  Options opts_;
};


}

