//
// Created by user on 25-7-31.
//

#ifndef TYPE_H
#define TYPE_H

#include <string>
#include <mysql.h>

namespace backup_tool {
enum class StatusCode {
    kOk = 0,
    kCancelled,
    kInvalidArgument,
    kNotFound,
    kAlreadyExists,
    kDeadlineExceeded,
    kResourceExhausted,
    kFailedPrecondition,
    kAborted,
    kOutOfRange,
    kUnimplemented,
    kInternal,
    kUnavailable,
    kDataLoss,
    kUnauthenticated,
    kNoData,
    kSameLogIndex,
    kNotEnoughData, // Not enough data to process
    kFileError,
};

class Status {
public:
    // OK constructor
    Status(const StatusCode code = StatusCode::kOk) noexcept : code_(code) {}

    // Error constructor
    Status(StatusCode code, std::string msg)
        : code_(code), msg_(std::move(msg)) {}

    // Factory helpers
    static Status OK() noexcept { return Status(); }

    static Status Unavailable(std::string msg) {
        return {StatusCode::kUnavailable, std::move(msg)};
    }

    static Status Internal(std::string msg) {
        return {StatusCode::kInternal, std::move(msg)};
    }

    static Status InvalidArgument(std::string msg) {
        return {StatusCode::kInvalidArgument, std::move(msg)};
    }

    // Query
    bool ok() const noexcept { return code_ == StatusCode::kOk; }
    StatusCode code() const noexcept { return code_; }
    const std::string& message() const noexcept { return msg_; }

    // Retryable heuristic (network / temporary errors)
    bool is_retryable() const noexcept {
        switch (code_) {
        case StatusCode::kUnavailable:
        case StatusCode::kInternal:
        case StatusCode::kDeadlineExceeded:
        case StatusCode::kAborted:
        case StatusCode::kResourceExhausted:
            return true;
        default:
            return false;
        }
    }

    std::string ToString() const {
        return ok() ? "OK" : CodeToString(code_) + ": " + msg_;
    }

private:
    StatusCode code_;
    std::string msg_;

    static std::string CodeToString(StatusCode code) {
        switch (code) {
        case StatusCode::kOk: return "OK";
        case StatusCode::kCancelled: return "Cancelled";
        case StatusCode::kInvalidArgument: return "Invalid argument";
        case StatusCode::kNotFound: return "Not found";
        case StatusCode::kAlreadyExists: return "Already exists";
        case StatusCode::kDeadlineExceeded: return "Deadline exceeded";
        case StatusCode::kResourceExhausted: return "Resource exhausted";
        case StatusCode::kFailedPrecondition: return "Failed precondition";
        case StatusCode::kAborted: return "Aborted";
        case StatusCode::kOutOfRange: return "Out of range";
        case StatusCode::kUnimplemented: return "Unimplemented";
        case StatusCode::kInternal: return "Internal";
        case StatusCode::kUnavailable: return "Unavailable";
        case StatusCode::kDataLoss: return "Data loss";
        case StatusCode::kUnauthenticated: return "Unauthenticated";
        case StatusCode::kNoData: return "No data return";
        case StatusCode::kSameLogIndex: return "Same log index, no need to backup";
        case StatusCode::kNotEnoughData: return "Not enough data to process";
        case StatusCode::kFileError: return "File operation error";
        default: return "Unknown";
        }
    }
};

// ---------------------------------------------------------------------------
// Templated StatusOr<T> helper – holds either a value or an error Status.
// ---------------------------------------------------------------------------

template <typename T>
class StatusOr {
public:
    // Error state ctor
    StatusOr(Status s) : _status(std::move(s)), _has_value(false) {}

    // Success state ctor
    StatusOr(T v) : _value(std::move(v)), _has_value(true) {}

    bool ok() const noexcept { return _has_value; }

    const T& value() const { return _value; } // caller should have checked ok()
    T& value() { return _value; }

    const Status& status() const {
        static Status ok = Status::OK();
        return _has_value ? ok : _status;
    }

private:
    T _value{}; // default init even if unused
    Status _status = Status::OK();
    bool _has_value = false;
};

struct MetaReqType {
    static constexpr std::string_view QuerySchema = "QUERY_SCHEMA";
    static constexpr std::string_view QueryRegion = "QUERY_REGION";
};

enum DumpBalanceType {
    Machine = 0,
    Instance
};

enum LoadType {
    LoadSQL = 0,
    LoadSST
};

struct MySQLConfig {
    std::string _host;
    int _port = 0;
    std::string _user;
    std::string _password;
    std::string _database;
    int _timeout_sec = 0;
};

auto bind_mysql_value(const baikaldb::ExprValue& v, MYSQL_BIND* b) {
    auto set_len = [&](unsigned long n) {
        if (b->length) *b->length = n; // 仅在调用方提供 length 指针时写入
        b->buffer_length = n; // 输入参数时作为缓冲区大小
    };
    auto set_not_null = [&]() {
        if (b->is_null) *b->is_null = 0;
    };
    auto set_is_null = [&]() {
        if (b->is_null) *b->is_null = 1;
        b->buffer = nullptr;
        b->buffer_length = 0;
        // length 留空即可
    };
    if (v.is_null()
        || v.type == baikaldb::pb::NULL_TYPE
        || v.type == baikaldb::pb::INVALID_TYPE
        || v.type == baikaldb::pb::PLACE_HOLDER
        || v.type == baikaldb::pb::MAXVALUE_TYPE) {
        b->buffer_type = MYSQL_TYPE_NULL;
        set_is_null();
        b->is_unsigned = 0;
        return;
    }

    switch (v.type) {
    case baikaldb::pb::BOOL: {
        const auto* p = &v._u.bool_val;
        b->buffer_type = MYSQL_TYPE_TINY;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 0;
        b->buffer_length = sizeof(*p);
        break;
    }
    case baikaldb::pb::INT8: {
        const auto* p = &v._u.int8_val;
        b->buffer_type = MYSQL_TYPE_TINY;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 0;
        b->buffer_length = sizeof(*p);
    }
    case baikaldb::pb::UINT8: {
        const auto* p = &v._u.uint8_val;
        b->buffer_type = MYSQL_TYPE_TINY;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 1;
        b->buffer_length = sizeof(*p);
        break;
    }
    case baikaldb::pb::INT16: {
        const auto* p = &v._u.int16_val;
        b->buffer_type = MYSQL_TYPE_SHORT;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 0;
        b->buffer_length = sizeof(*p);
        break;
    }
    case baikaldb::pb::UINT16: {
        const auto* p = &v._u.uint16_val;
        b->buffer_type = MYSQL_TYPE_SHORT;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 1;
        b->buffer_length = sizeof(*p);
        break;
    }

    case baikaldb::pb::INT32: {
        const auto* p = &v._u.int32_val;
        b->buffer_type = MYSQL_TYPE_LONG;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 0;
        b->buffer_length = sizeof(*p);
        break;
    }
    case baikaldb::pb::UINT32: {
        const auto* p = &v._u.uint32_val;
        b->buffer_type = MYSQL_TYPE_LONG;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 1;
        b->buffer_length = sizeof(*p);
    }
    case baikaldb::pb::INT64: {
        const auto* p = &v._u.int64_val;
        b->buffer_type = MYSQL_TYPE_LONGLONG;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 0;
        b->buffer_length = sizeof(*p);
        break;
    }
    case baikaldb::pb::UINT64: {
        const auto* p = &v._u.uint64_val;
        b->buffer_type = MYSQL_TYPE_LONGLONG;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 1;
        b->buffer_length = sizeof(*p);
        break;
    }
    case baikaldb::pb::FLOAT: {
        const auto* p = &v._u.float_val;
        b->buffer_type = MYSQL_TYPE_FLOAT;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 0;
        b->buffer_length = sizeof(*p);
        break;
    }
    case baikaldb::pb::DOUBLE: {
        const auto* p = &v._u.double_val;
        b->buffer_type = MYSQL_TYPE_DOUBLE;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 0;
        b->buffer_length = sizeof(*p);
        break;
    }
    case baikaldb::pb::STRING:
    case baikaldb::pb::HLL:
    case baikaldb::pb::BITMAP:
    case baikaldb::pb::TDIGEST:
    case baikaldb::pb::JSON: {
        const std::string& s = v.get_string();
        b->buffer_type = MYSQL_TYPE_STRING;
        b->buffer = const_cast<char*>(s.data());
        b->is_unsigned = 0;
        set_len(s.size());
        break;
    }
    case baikaldb::pb::DATETIME: {
        const auto* p = &v._u.uint64_val;
        b->buffer_type = MYSQL_TYPE_DATETIME;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 1;
        b->buffer_length = sizeof(*p);
        break;
    }
    case baikaldb::pb::TIMESTAMP: {
        const auto* p = &v._u.uint32_val;
        b->buffer_type = MYSQL_TYPE_TIMESTAMP;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 1;
        b->buffer_length = sizeof(*p);
        break;
    }
    case baikaldb::pb::DATE: {
        const auto* p = &v._u.uint32_val;
        b->buffer_type = MYSQL_TYPE_DATE;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 1;
        b->buffer_length = sizeof(*p);
        break;
    }
    case baikaldb::pb::TIME: {
        const auto* p = &v._u.int32_val;
        b->buffer_type = MYSQL_TYPE_TIME;
        b->buffer = const_cast<void*>(static_cast<const void*>(p));
        b->is_unsigned = 0;
        b->buffer_length = sizeof(*p);
        break;
    }
        default: {
        b->buffer_type = MYSQL_TYPE_NULL;
        set_is_null();
        b->is_unsigned = 0;
        break;
    }
    }
}
} // backup_tool

#endif //TYPE_H
