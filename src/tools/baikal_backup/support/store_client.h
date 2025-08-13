//
// Created by user on 25-8-5.
//

#ifndef STORE_CLIENT_H
#define STORE_CLIENT_H
#include <file_system.h>
#include <string>
#include <utility>

namespace backup_tool {

class StoreClient {
public:
  StoreClient(const std::string &host, const int port,
              std::shared_ptr<RegionFile> rf)
      : _host(host), _port(port), _rf(std::move(rf)) {}
  StoreClient(const std::string &addr, std::shared_ptr<RegionFile> rf)
      : _addr(addr), _rf(std::move(rf)) {}
  auto DownloadRegion(const int64_t &log_index) const -> Status {

    const std::string prefix = "http://";
    // http://${store_ip}:${store_port}/StoreService/backup_region/download/${region_id}/data/${log_index}
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = "http";
    options.timeout_ms = 30000;
    options.connect_timeout_ms = 1000;
    options.max_retry = 3;
    std::string addr;
    if (_addr.empty()) {
      addr = _host + std::to_string(_port);
    } else {
      addr = _addr;
    }
    if (channel.Init(addr.c_str(), &options) != 0) {
      DB_FATAL("channel init fail. addr: %s", addr.c_str());
      return Status::Internal("channel init fail");
    }
    brpc::Controller cntl;
    cntl.http_request().uri() = "/StoreService/backup_region/download/" +
                                std::to_string(_rf->get_region_id()) +
                                "/data/" + std::to_string(log_index);
    channel.CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr);
    if (cntl.Failed()) {
      DB_FATAL("process http download fail. addr: %s, region_id: %ld, "
               "log_index: %ld, err: %s",
               addr.c_str(), _rf->get_region_id(), log_index,
               cntl.ErrorText().c_str())
      return {StatusCode::kInternal};
    }
    switch (cntl.http_response().status_code()) {
    case brpc::HTTP_STATUS_OK: {
      break;
    }
    case brpc::HTTP_STATUS_NO_CONTENT: {
      DB_WARNING("region_id %ld not changed, log_index: %ld",
                 _rf->get_region_id(), log_index);
      return StatusCode::kOk;
    }
    case brpc::HTTP_STATUS_PARTIAL_CONTENT: {
      DB_WARNING("region_id %ld not data in store, log_index: %ld",
                 _rf->get_region_id(), log_index);
      return StatusCode::kOk;
    }
    default: {
      DB_WARNING("region_id %ld download fail, log_index: %ld, status_code: %d",
                 _rf->get_region_id(), log_index,
                 cntl.http_response().status_code());
      break;
    }
    }
    return _rf->write(std::move(cntl.response_attachment()));
  }

private:
  std::string _host;
  int _port = 0;
  std::string _addr;
  std::shared_ptr<RegionFile> _rf;
};
} // namespace backup_tool

#endif // STORE_CLIENT_H
