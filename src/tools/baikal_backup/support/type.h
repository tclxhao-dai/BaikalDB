//
// Created by user on 25-7-31.
//

#ifndef TYPE_H
#define TYPE_H

#include <string>

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
    StatusCode   code_;
    std::string  msg_;

    static std::string CodeToString(StatusCode code) {
        switch (code) {
        case StatusCode::kOk:                return "OK";
        case StatusCode::kCancelled:         return "Cancelled";
        case StatusCode::kInvalidArgument:   return "Invalid argument";
        case StatusCode::kNotFound:          return "Not found";
        case StatusCode::kAlreadyExists:     return "Already exists";
        case StatusCode::kDeadlineExceeded:  return "Deadline exceeded";
        case StatusCode::kResourceExhausted: return "Resource exhausted";
        case StatusCode::kFailedPrecondition:return "Failed precondition";
        case StatusCode::kAborted:           return "Aborted";
        case StatusCode::kOutOfRange:        return "Out of range";
        case StatusCode::kUnimplemented:     return "Unimplemented";
        case StatusCode::kInternal:          return "Internal";
        case StatusCode::kUnavailable:       return "Unavailable";
        case StatusCode::kDataLoss:          return "Data loss";
        case StatusCode::kUnauthenticated:   return "Unauthenticated";
        case StatusCode::kNoData:            return "No data return";
        case StatusCode::kSameLogIndex:      return "Same log index, no need to backup";
        case StatusCode::kNotEnoughData:     return "Not enough data to process";
        case StatusCode::kFileError: return "File operation error";
        default:                             return "Unknown";
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

    const T& value() const { return _value; }  // caller should have checked ok()
    T& value()       { return _value; }

    const Status& status() const {
        static Status ok = Status::OK();
        return _has_value ? ok : _status;
    }

private:
    T       _value{};   // default init even if unused
    Status  _status = Status::OK();
    bool    _has_value = false;
};

struct  MetaReqType {
    static constexpr  std::string_view QuerySchema = "QUERY_SCHEMA";
    static constexpr  std::string_view QueryRegion = "QUERY_REGION";
};

enum DumpBalanceType {
    Machine =0 ,
    Instance
};

enum LoadType {
    LoadSQL =0 ,
    LoadSST
};

struct MySQLConfig {
    std::string _host;
    int _port = 0;
    std::string _user;
    std::string _password;
    std::string _database;
    int _timeout_sec =0;
};

} // backup_tool

#endif //TYPE_H
