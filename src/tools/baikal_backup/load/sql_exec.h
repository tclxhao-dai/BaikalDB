#pragma once
#include <string>
#include <vector>
#include <queue>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <stdexcept>
#include <sstream>
#include <iostream>
#include <cassert>
#include <mysql.h>

namespace backup_tool {
class SQLExec {
public:
    SQLExec(MySQLConfig cfg,
            size_t pool_size,
            std::string insert_values_template, // e.g. "REPLACE INTO db.tbl(a,b,c) VALUES"
            std::string create_table_sql)
        : _cfg(std::move(cfg)),
          pool_size_(pool_size),
          insert_tpl_(std::move(insert_values_template)),
          create_table_sql_(std::move(create_table_sql)) {
        if (pool_size_ == 0) pool_size_ = 1;
        col_count_ = parse_column_count(insert_tpl_);
        if (col_count_ <= 0) {
            throw std::invalid_argument("Failed to parse column count from insert template: " + insert_tpl_);
        }
    }

    ~SQLExec() {
        shutdown();
    }

    // 1) 初始化连接池（线程安全，仅第一次有效）
    bool init() {
        std::call_once(lib_init_flag_, [](){
            mysql_library_init(0, nullptr, nullptr);
        });

        std::unique_lock<std::mutex> lk(m_);
        if (inited_) return true;

        for (size_t i = 0; i < pool_size_; ++i) {
            MYSQL* c = create_one_connection_unlocked();
            if (!c) {
                last_error_ = "create connection failed: " + std::string(mysql_error(last_conn_attempt_ ? last_conn_attempt_ : nullptr));
                // 清理已建
                while(!pool_.empty()) { mysql_close(pool_.front()); pool_.pop(); }
                return false;
            }
            pool_.push(c);
        }
        inited_ = true;
        return true;
    }

    // 2) 线程安全执行任意 SQL（不带结果集，适用于 DDL/DML）
    bool exec_sql(const std::string& sql) {
        auto guard = acquire();
        if (!guard) { last_error_ = "acquire connection failed"; return false; }

        if (mysql_query(guard.get(), sql.c_str()) != 0) {
            last_error_ = mysql_error(guard.get());
            return false;
        }
        // consume result if any
        MYSQL_RES* res = mysql_store_result(guard.get());
        if (res) mysql_free_result(res);
        return true;
    }

    // 5) 执行建表 SQL
    bool ensure_table() {
        if (create_table_sql_.empty()) return true;
        return exec_sql(create_table_sql_);
    }

    // 3) + 4) 将 rows 组装为多值占位符的 prepared statement 并执行批量插入
    // rows: 每一行大小必须 == 由模板解析出来的列数
    // batch_size: 一次 statement 中的行数（避免 SQL 太长或参数过多）
    bool insert_rows(const std::vector<std::vector<std::pair<bool,std::string>>>& rows, size_t batch_size = 1000) {
        if (rows.empty()) return true;
        if (!inited_ && !init()) return false;

        // 基本校验
        for (const auto& r : rows) {
            if (r.size() != static_cast<size_t>(col_count_)) {
                last_error_ = "row column size mismatch: expected " + std::to_string(col_count_)
                              + ", got " + std::to_string(r.size());
                return false;
            }
        }

        const size_t max_params_per_stmt = 65535; // MySQL/MariaDB 参数个数上限
        size_t safe_batch = std::min(batch_size, max_params_per_stmt / (size_t)col_count_);
        if (safe_batch == 0) safe_batch = 1;

        size_t i = 0;
        while (i < rows.size()) {
            size_t n = std::min(safe_batch, rows.size() - i);
            if (!insert_rows_one_stmt({rows.begin() + i, rows.begin() + i + n})) {
                return false;
            }
            i += n;
        }
        return true;
    }

    const std::string& last_error() const { return last_error_; }

    // 手动关闭（可选）
    void shutdown() {
        std::unique_lock<std::mutex> lk(m_);
        if (!shut_) {
            while(!pool_.empty()) { mysql_close(pool_.front()); pool_.pop(); }
            inited_ = false;
            shut_ = true;
        }
    }

private:
    // ----- 连接池 RAII -----
    struct ConnGuard {
        ConnGuard(SQLExec* owner, MYSQL* c) : owner_(owner), c_(c) {}
        ConnGuard() = default;
        ConnGuard(ConnGuard&& o) noexcept : owner_(o.owner_), c_(o.c_) { o.owner_ = nullptr; o.c_ = nullptr; }
        ConnGuard& operator=(ConnGuard&& o) noexcept {
            if (this != &o) {
                release();
                owner_ = o.owner_; c_ = o.c_;
                o.owner_ = nullptr; o.c_ = nullptr;
            }
            return *this;
        }
        ~ConnGuard() { release(); }
        MYSQL* get() const { return c_; }
        explicit operator bool() const { return owner_ && c_; }
    private:
        void release() {
            if (owner_ && c_) owner_->release_(c_);
            owner_ = nullptr; c_ = nullptr;
        }
        SQLExec* owner_ = nullptr;
        MYSQL*   c_     = nullptr;
    };

    ConnGuard acquire() {
        std::unique_lock<std::mutex> lk(m_);
        if (!inited_) {
            lk.unlock();
            if (!init()) return {};
            lk.lock();
        }
        cv_.wait(lk, [&]{ return !pool_.empty(); });
        MYSQL* c = pool_.front(); pool_.pop();
        return ConnGuard(this, c);
    }

    void release_(MYSQL* c) {
        std::unique_lock<std::mutex> lk(m_);
        if (shut_) { mysql_close(c); return; }
        pool_.push(c);
        lk.unlock();
        cv_.notify_one();
    }

    MYSQL* create_one_connection_unlocked() {
        last_conn_attempt_ = mysql_init(nullptr);
        if (!last_conn_attempt_) return nullptr;

        mysql_options(last_conn_attempt_, MYSQL_SET_CHARSET_NAME, "utf8");

            my_bool reconnect = 1;
            mysql_options(last_conn_attempt_, MYSQL_OPT_RECONNECT, &reconnect);
        if (_cfg._timeout_sec > 0) {
            unsigned to = _cfg._timeout_sec;
            mysql_options(last_conn_attempt_, MYSQL_OPT_CONNECT_TIMEOUT, &to);
        }

        MYSQL* conn = mysql_real_connect(
            last_conn_attempt_,
            _cfg._host.c_str(),
            _cfg._user.c_str(),
            _cfg._password.c_str(),
            _cfg._database.empty() ? nullptr : _cfg._database.c_str(),
            _cfg._port,
            nullptr,
            0 /* client_flag */
        );
        if (!conn) {
            // last_conn_attempt_ is same pointer; mysql_error available
            mysql_close(last_conn_attempt_);
            last_conn_attempt_ = nullptr;
            return nullptr;
        }
        return conn;
    }

    // 解析模板中的列数：寻找第一个 (...)，按逗号计数
    static int parse_column_count(const std::string& tpl) {
        auto l = tpl.find('(');
        auto r = (l == std::string::npos) ? std::string::npos : tpl.find(')', l + 1);
        if (l == std::string::npos || r == std::string::npos || r <= l + 1) return -1;
        int cnt = 1;
        for (size_t i = l + 1; i < r; ++i) {
            if (tpl[i] == ',') ++cnt;
        }
        return cnt;
    }

    static std::string make_values_placeholders(const size_t rows, const int cols) {
        // returns: "VALUES (?,?...),(?,?...),..."
        std::ostringstream oss;
        for (size_t i = 0; i < rows; ++i) {
            if (i) oss << ',';
            oss << '(';
            for (int c = 0; c < cols; ++c) {
                if (c) oss << ',';
                oss << '?';
            }
            oss << ')';
        }
        return oss.str();
    }

    bool insert_rows_one_stmt(const std::vector<std::vector<std::pair<bool,std::string>>>& batch) {
        auto guard = acquire();
        if (!guard) { last_error_ = "acquire connection failed"; return false; }

        // 组装完整 SQL： "<tpl> VALUES (?,?),(?,?)..."
        std::string sql = insert_tpl_;
        // 允许模板结尾带/不带 "VALUES"（都兼容）
        auto lowered = [](std::string s){
            for (auto& ch : s) ch = (char)std::tolower((unsigned char)ch);
            return s;
        }(sql);
        if (lowered.find("values") == std::string::npos) {
            sql += " ";
        }
        sql += make_values_placeholders(batch.size(), col_count_);

        MYSQL_STMT* stmt = mysql_stmt_init(guard.get());
        if (!stmt) { last_error_ = "mysql_stmt_init failed"; return false; }

        auto cleanup_stmt = [&](){ mysql_stmt_close(stmt); };

        if (mysql_stmt_prepare(stmt, sql.c_str(), (unsigned long)sql.size()) != 0) {
            last_error_ = mysql_stmt_error(stmt);
            cleanup_stmt();
            return false;
        }

        const size_t param_count = (size_t)col_count_ * batch.size();
        std::vector<MYSQL_BIND> binds(param_count);
        std::vector<unsigned long> lengths(param_count);
        std::vector<char> is_null(param_count, 0);

        // 注意：需要保证 buffer 在 execute 时仍然有效；我们直接指向 std::string 的内部数据即可
        size_t p = 0;
        for (const auto& row : batch) {
            for (int c = 0; c < col_count_; ++c) {
                if (row[c].first) {
                    is_null[p] = 0;
                }
                const std::string& v = row[c].second;
                MYSQL_BIND &b = binds[p];
                memset(&b, 0, sizeof(MYSQL_BIND));
                b.buffer_type   = MYSQL_TYPE_STRING;
                b.buffer        = const_cast<char*>(v.data());
                lengths[p]      = (unsigned long)v.size();
                b.length        = &lengths[p];
                b.is_null       = reinterpret_cast<my_bool*>(&is_null[p]);
                // b.buffer_length 可不设，对输入参数非必须
                ++p;
            }
        }
        assert(p == param_count);

        if (mysql_stmt_bind_param(stmt, binds.data()) != 0) {
            last_error_ = mysql_stmt_error(stmt);
            cleanup_stmt();
            return false;
        }

        if (mysql_stmt_execute(stmt) != 0) {
            last_error_ = mysql_stmt_error(stmt);
            cleanup_stmt();
            return false;
        }

        // 可选：检查受影响行数
        // my_ulonglong affected = mysql_stmt_affected_rows(stmt);

        cleanup_stmt();
        DB_WARNING("affected %lld rows",mysql_stmt_affected_rows(stmt));
        return true;
    }

    MySQLConfig _cfg;
    size_t pool_size_;
    std::string insert_tpl_;
    std::string create_table_sql_;
    int col_count_ = -1;

    std::mutex m_;
    std::condition_variable cv_;
    std::queue<MYSQL*> pool_;
    std::atomic<bool> inited_{false};
    std::atomic<bool> shut_{false};
    std::string last_error_;

    MYSQL* last_conn_attempt_ = nullptr;
    inline static std::once_flag lib_init_flag_;
};
}