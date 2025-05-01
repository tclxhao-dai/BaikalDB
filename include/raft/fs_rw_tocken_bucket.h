#include <iostream>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>
#include <cmath>
#include <butil/time.h>

namespace baikaldb {

class FsRWTokenBucket {
public:
    FsRWTokenBucket() {
        _rate = 0;
    }

    ~FsRWTokenBucket() {
    }
    
    void consume(uint64_t bits_requested) {
        BAIDU_SCOPED_LOCK(_mutex);
        if (_rate == 0) {
            return;
        }
        refill();  // 先补充令牌
        if (_rate < bits_requested) {
            bthread_usleep(1000 * 1000);
            _tokens = 0;
            return;
        }
        if (_tokens < bits_requested) {
            auto diff = bits_requested - _tokens;
            auto wait_time = diff * 1000 * 1000 / _rate;
            if (wait_time > 1000 * 1000) {
                wait_time = 1000 * 1000;
            }
            bthread_usleep(wait_time);
            _tokens = 0;
        } else {
            _tokens -= bits_requested;
        }
    }

    static FsRWTokenBucket* get_read_instance() {
        static FsRWTokenBucket _instance;
        return &_instance;
    }
    static FsRWTokenBucket* get_write_instance() {
        static FsRWTokenBucket _instance;
        return &_instance;
    }

    void reset_rate(uint64_t new_rate) {
        BAIDU_SCOPED_LOCK(_mutex);
        if (new_rate > 0 && new_rate < 1024 * 1024) {
            new_rate = 1024 * 1024;
        }
        if (new_rate != _rate) {
            DB_WARNING("update fs rw limit: old rate = %llu to new %llu", _rate, new_rate);
            _rate = new_rate;
        }
    }

private:
    void refill() {
        auto t = _last_update.get_time();
        if (t > 1200 * 1000) {
            t = 1200 * 1000;
        }
        uint64_t add_tokens = t * _rate / 1000 / 1000;
        _tokens = std::min(_tokens + add_tokens, _rate * 12 / 10);
        _last_update.reset();
    }

    uint64_t _rate;       // 持续速率 (bits/s)
    uint64_t _tokens;           // 当前令牌数
    TimeCost _last_update;
    bthread::Mutex  _mutex;
};

} // end namepsace baikaldb
