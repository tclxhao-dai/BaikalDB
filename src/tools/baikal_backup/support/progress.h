//
// Created by user on 2025/9/24.
//
#ifndef BAIKALDB_PROGRESS_H
#define BAIKALDB_PROGRESS_H

#include <atomic>
#include <string>
#include <chrono>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "bvar/bvar.h"
#include "bthread/bthread.h"
#include "type.h"
#include "dump/dump.h"
#include "dump/dump.h"

namespace backup_tool {

class ProgressFile {
public:
    ProgressFile(std::string name,
                 std::string filename,
                 uint64_t total,
                 bvar::Adder<uint>* done_bvar_ptr)
        : _name(std::move(name)),
          _filename(std::move(filename)),
          _total(total),
          _done_bvar(done_bvar_ptr),
          _running(false) {
        if (!_filename.empty()) {
            fs::path path =fs::path(_name)/_filename;
            _ofs.open(path, std::ios::out | std::ios::trunc);
        }else {
            DB_FATAL("progress file is empty");
            exit(1);
        }
    }

    ProgressFile(const ProgressFile&) = delete;
    ProgressFile& operator=(const ProgressFile&) = delete;
    ProgressFile(ProgressFile&&) = delete;
    ProgressFile& operator=(ProgressFile&&) = delete;

    ~ProgressFile() {
        DB_WARNING("%s" ,report().c_str());
        stop();           // 确保先停后台线程
        flush_now();      // 有文件则写最后一行
        if (_ofs.is_open()) _ofs.close();
    }

    // —— 仅 total / done 两类数据 ——
    void set_total(uint64_t total) { _total.store(total, std::memory_order_relaxed); }

    void done(uint64_t delta = 1) {
        if (_done_bvar && delta > 0) {
            *_done_bvar << static_cast<int64_t>(delta); // bvar::Adder 接口是带符号整型
        }
    }

    uint64_t total() const { return _total.load(std::memory_order_relaxed); }
    uint64_t done_value() const { return _done_bvar ? _done_bvar->get_value() : 0ULL; }

    std::string report() const {
        const uint64_t t = total();
        const uint64_t d = done_value();
        const double pct = (t > 0) ? (100.0 * (double)d / (double)t) : 0.0;
        char buf[160];
        snprintf(buf, sizeof(buf),
                 "[%s] done=%" PRIu64 "/%" PRIu64 " (%.2f%%)",
                 _name.c_str(), d, t, pct);
        return std::string(buf);
    }

    // —— 落地控制（可选） ——
    // 立即写一行（epoch, total, done）
    void flush_now() {
        if (!_ofs.is_open()) return;
        std::lock_guard<std::mutex> lk(_ofs_mtx);
        const auto ts = std::chrono::duration_cast<std::chrono::seconds>(
                            std::chrono::system_clock::now().time_since_epoch()).count();
        _ofs.seekp(0);
        //这个字符串会越来越长，所以不用担心尾部会残留上一次写入的数据
        _ofs << ts << "," << total() << "," << done_value() << "\n";
        _ofs.flush();
    }

    // 启动/停止后台周期写（更灵活，适合普通对象按需用）
    void start(int interval_seconds) {
        if (!_ofs.is_open() || interval_seconds <= 0) return;
        if (_running.exchange(true)) return;
        _bg = std::thread([this, interval_seconds] {
            while (_running.load(std::memory_order_acquire)) {
                flush_now();
                std::this_thread::sleep_for(std::chrono::seconds(interval_seconds));
            }
        });
    }

    void stop() {
        if (!_running.exchange(false)) return;
        if (_bg.joinable()) _bg.join();
    }

private:
    std::string _name;
    std::string _filename;

    std::atomic<uint> _total{0};
    bvar::Adder<uint>* _done_bvar{nullptr}; // 非 owning；由外部保证其生命周期≥本对象

    std::ofstream _ofs;
    std::mutex _ofs_mtx;

    std::atomic<bool> _running;
    std::thread _bg;
};

} // backup_tool

#endif //BAIKALDB_PROGRESS_H