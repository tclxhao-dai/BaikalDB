//
// Created by user on 25-7-29.
//
#pragma once

#include <config.h>
#include <filesystem>
#include <google/protobuf/stubs/port.h>
#include <meta_server_interact.hpp>
#include <queue>
#include <retry.h>
#include <store_client.h>
#include <string>
#include <task.h>
#include <type.h>
#include <vector>
#include <debug.h>

#include "progress.h"

static  bvar::Adder<uint> g_dump_success_count("dump_success_count");
static  bvar::Adder<uint> g_dump_fail_count("dump_fail_count");
static  bvar::Adder<uint> g_dump_total_count("dump_total_count");
static bvar::Adder<uint> g_dump_processing_count("dump_processing_count");
namespace backup_tool {
class DumpWorker {
public:
  DumpWorker(const std::string& key, const int concurrency,
             const RetryPolicy &retry = RetryPolicy({})):_key(key),_concurrency(concurrency),_retry_policy(retry) {
  }
  auto Enqueue(RegionTask* region_task) -> void {
    _q.emplace(region_task);
  }
  ~DumpWorker() {
    DB_WARNING("worker %s destruct, concurrency: %d", _key.c_str(), _concurrency);
  }
  auto Done() const -> bool {
    return _q.size()==0;
  }
  auto Dequeue() -> RegionTask* {

    RegionTask* task = _q.front();
    _q.pop();

    return task;
  }
  auto Run() -> Status {
    DB_WARNING("worker %s start, concurrency: %d,addr:%p,size:%d", _key.c_str(), _concurrency,this,_q.size());
    auto bth = baikaldb::ConcurrencyBthread(_concurrency);
    while (!Done()) {
      auto task = Dequeue();
      bth.run([this,task]{
        DB_WARNING("start to process region %ld",task->_region_id);
        g_dump_processing_count << 1;
        if (const auto ret = process_one_task(*task); !ret.ok()) {
          g_dump_fail_count << 1;
        }else {
            g_dump_success_count << 1;
        }
        g_dump_processing_count << -1;
      }
      );
    }
    bth.join();
    return Status::OK();
  }



//private:
  auto process_one_task(RegionTask& task) -> Status {
    const auto rf_ptr = std::make_shared<RegionFile>(task._region_id,task._table_id, task._table_name,
                                              _key, task._path, task._log_index);

    //it's ok to use one StoreClient/a few StoreClients for all task
    //because one worker only process one/a few store,but cost of
    //building a store client is not high
    StoreClient store_client(task._store_addr, rf_ptr);
    Status ret = _retry_policy.WithRetry([&rf_ptr,&store_client]() -> Status {
      return store_client.DownloadRegion(rf_ptr->get_log_index());
      }
    );

    return ret;
  }
  std::string _key; // worker id, ip/ip:port,decided by load balance type
  int _concurrency = 1; // concurrency for each store/machine(decided by _dump_balance_by_machine)
  RetryPolicy _retry_policy; // retry policy for download
  std::queue<RegionTask*> _q;
};


class MetaDumper {
public:
  MetaDumper(const std::shared_ptr<baikaldb::pb::QueryResponse> &schema_pb,
             const std::shared_ptr<baikaldb::pb::QueryResponse> &region_pb,
             const std::string &dump_path)
      : _schema_pb(schema_pb), _region_pb(region_pb),_dump_path(dump_path) {}

  auto write() const -> Status {
    auto dir = std::filesystem::path(_dump_path);
    if (!exists(dir)) {
      DB_FATAL("dump path %s not exist", _dump_path.c_str());
      return {StatusCode::kFileError};
    }
    // write schema info into disk
    {
      std::ofstream ofs(dir/"schema.json", std::ios::out|std::ios::binary);
      if (!ofs || ofs.bad() || ofs.fail()) {
        DB_FATAL("write schema file failed, path: %s", (dir/"schema.json").c_str());
        return {StatusCode::kFileError};
      }
      ofs << baikaldb::pb2json(*_schema_pb);
      if (ofs.bad() || ofs.fail()) {
        DB_FATAL("write schema content failed, path: %s", (dir/"schema.json").c_str());
        return {StatusCode::kFileError};
      }
    }

    // write region info into disk
    {
      std::ofstream ofs(dir/"region.json", std::ios::out|std::ios::binary);
      if (!ofs || ofs.bad() || ofs.fail()) {
        DB_FATAL("write region file failed, path: %s", (dir/"region.json").c_str());
        return {StatusCode::kFileError};
      }
      ofs << baikaldb::pb2json(*_region_pb);
      if (ofs.bad() || ofs.fail()) {
        DB_FATAL("write region content failed, path: %s", (dir/"region.json").c_str());
        return {StatusCode::kFileError};
      }
    }

    return {StatusCode::kOk};

  }

private:
  std::shared_ptr<baikaldb::pb::QueryResponse> _schema_pb;
  std::shared_ptr<baikaldb::pb::QueryResponse> _region_pb;
  std::string _dump_path; // dump path
};



class DumpManager{
public:
  explicit DumpManager(const Config &config){
    _dump_path = config._dump_path;
    _meta_group = config._src_meta_group;
    _meta_interact = baikaldb::MetaServerInteract::get_instance();
    _namespace = config._namespace;
    _database = config._database;
    _tables = config._tables;
    _download_from_leader = config._download_from_leader;
    _balance_by_machine = config._dump_balance_by_machine;
    _concurrency = config._concurrency;
    _filter_tables = config._filter_tables;
  }
  auto update_new_region() -> bool {
    //fetch region info from meta,if have new region,update _table_region_info_map
    return false;
  }
  auto Run() -> void {
    do {
      const int size = static_cast<int>(_worker_map.size());
      baikaldb::ConcurrencyBthread bth(size);
      for (auto const &[id,worker] : _worker_map) {
        bth.run([id,worker] {
          DB_WARNING("run worker %s", id.c_str())
          auto status = worker->Run();
          if (!status.ok()) {
            DB_FATAL("run worker %s failed, err: %s", id.c_str(), status.message().c_str());
          }
          DB_WARNING("run worker %s done", id.c_str())
          bthread_usleep(60*1000);
        });
      }
      bth.join();
    }while (update_new_region());
    DB_WARNING("processing:%d,success:%d,fail:%d,total:%d",g_dump_processing_count.get_value(),
               g_dump_success_count.get_value(),g_dump_fail_count.get_value(),g_dump_total_count.get_value());
    delete _progress;
  }


  auto init() -> void {
    for (const auto& table : _tables) {
      _table_set.insert(table);
    }
    for (const auto& table : _filter_tables) {
      _filter_table_set.insert(table);
    }
    if (_meta_interact->init_internal(_meta_group) != 0) {
      DB_FATAL("init meta interact fail. meta_group: %s", _meta_group.c_str());
      exit(-1);
    }
    RetryPolicy retry({});
    //fetch_schema_info;
    baikaldb::pb::QueryResponse schema_response;
    Status status= retry.WithRetry([this, &schema_response]() -> Status {
      return this->fetch_schema_info(schema_response);
    });
    std::vector<int64_t> table_ids;
    for (const auto& schema_info : schema_response.schema_infos()) {
      table_ids.emplace_back(schema_info.table_id());
    }
    if (!status.ok()) {
      DB_FATAL("fetch schema info fail. err: %s", status.message().c_str());
      exit(-1);
    }
    //fetch region list
    baikaldb::pb::QueryResponse sum_regions;
    baikaldb::pb::QueryResponse region_response;
    for (size_t i =0;i< table_ids.size();i++) {
      const int64_t table_id = table_ids[i];
      status = retry.WithRetry([this,&region_response,&table_id]() -> Status {
        return this->fetch_regions(table_id,region_response);
      });
      if (!status.ok()) {
        DB_FATAL("fetch region info fail. err: %s", status.message().c_str());
        exit(-1);
      }
      if (i==0) {
        sum_regions = region_response;
      }else {
        for (baikaldb::pb::RegionInfo region_info : region_response.region_infos()) {
          sum_regions.mutable_region_infos()->Add(std::move(region_info));
        }
      }
    }
    //create dump path
    if (!std::filesystem::exists(_dump_path)) {
      bool c = std::filesystem::create_directory(_dump_path);
      if (!c) {
        DB_FATAL("create dump path fail. path: %s", _dump_path.c_str());
        exit(-1);
      }
    }
    // store meta info into disk
    const auto m = MetaDumper(std::make_shared<baikaldb::pb::QueryResponse>(schema_response),
                              std::make_shared<baikaldb::pb::QueryResponse>(sum_regions),
                              _dump_path);
    Status s = m.write();
    if (!s.ok()) {
      DB_FATAL("write meta info to disk fail. err: %s", s.message().c_str());
      exit(-1);
    }


    // throw task into workers
    for (const auto& [table_name,vec] : _table_region_info_map) {
      for (const auto& region_info : vec) {
        std::string instance = "";
        if (_download_from_leader) {
          instance = region_info.leader();
        }else {
          for (const std::string& peer : region_info.peers()) {
            if (peer != region_info.leader()) {
              instance = peer;
              break;
            }
          }
        }
        std::string worker_id = instance;
        if (_balance_by_machine) {
          // 从ip端口中取出ip
          const std::string ip = instance.substr(0, instance.find(':'));
          worker_id = ip;
        }
        if (_worker_map.find(worker_id) == _worker_map.end()) {
          auto const worker = new DumpWorker(worker_id, _concurrency);
          _worker_map[worker_id] = worker;
        }
        std::string path = _dump_path + "/" + _table_id_map[region_info.table_id()] + "/" + std::to_string(region_info.region_id());
        const auto task = new RegionTask(region_info.region_id(), region_info.table_id(),region_info.table_name(),
                                  path,instance,0);
        _worker_map[worker_id]->Enqueue(task);

      }
    }_progress = new ProgressFile(_dump_path,"dump",g_dump_total_count.get_value(),&g_dump_success_count);
    _progress->start(10);

DEBUG_ONLY({
      int cnt =0;
      for (auto const [k,v] : _worker_map) {
        DB_WARNING("worker_id: %s, size: %d", k.c_str(), v->_q.size());
        cnt+=v->_q.size();
      };
      DB_WARNING("total task count: %d", cnt);
    }
);
  }

private:
  std::string _dump_path;
  std::string _meta_group;
  baikaldb::MetaServerInteract* _meta_interact;
  std::unordered_map<std::string,std::vector<int64_t>> _table_region_ids;
  std::unordered_map<std::string, std::vector<baikaldb::pb::RegionInfo>> _table_region_info_map;
  std::string _database;
  std::string _namespace;
  std::vector<std::string> _tables;
  std::vector<std::string> _filter_tables;
  std::unordered_set<std::string> _table_set;
  std::unordered_set<std::string> _filter_table_set;
  std::vector<baikaldb::pb::SchemaInfo> _schema_infos;
  std::unordered_map<int64_t,std::string> _table_id_map; // key is table name, value is table id
  std::unordered_map<std::string,baikaldb::pb::SchemaInfo> _schema_info_map;
  std::map<std::string,DumpWorker*> _worker_map; // key is worker id, ip/ip:port,decided by load balance type
  int _concurrency = 1;   // concurrency for each store/machine(decided by _balance_by_machine)

  ProgressFile* _progress;

  bool _download_from_leader = true;
  bool _balance_by_machine =false;


  auto fetch_schema_info(baikaldb::pb::QueryResponse& response) -> Status {
    baikaldb::pb::QueryRequest request;
    request.set_op_type(baikaldb::pb::QUERY_SCHEMA);
    request.set_namespace_name(_namespace);
    request.set_database(_database);
    if (baikaldb::MetaServerInteract::get_instance()->send_request("query",request, response) !=0) {
      DB_WARNING("send fetch schema request failed,request:%s,response:%s",request.ShortDebugString().c_str(),response.ShortDebugString().c_str())
      return Status::Unavailable("failed to request schema info from meta server");
    }
    for (const auto& schema_info : response.schema_infos()) {
      if (!_table_set.empty() && _table_set.find(schema_info.table_name()) == _table_set.end()) {
        continue;
      }
      if (_filter_table_set.find(schema_info.table_name()) != _filter_table_set.end()) {
        continue;
      }
      _schema_infos .emplace_back(schema_info);
      _table_id_map[schema_info.table_id()]=schema_info.table_name();
    }
    return Status::OK();
  }

  // fetch_regions QUERY_REGION doesn't support specify namespace and database now
  auto fetch_regions(int64_t table_id,baikaldb::pb::QueryResponse& response) -> Status {
    baikaldb::pb::QueryRequest request;
    request.set_op_type(baikaldb::pb::QUERY_REGION);
    request.set_table_id(table_id);
    //request.set_namespace_name(_namespace);
    //request.set_database(_database);
    if (baikaldb::MetaServerInteract::get_instance()->send_request("query",request,response) != 0) {
      DB_WARNING("send fetch region request failed,request:%s,response:%s",request.ShortDebugString().c_str(),response.ShortDebugString().c_str());
      return Status::Unavailable("failed to request region info from meta server");
    }
    for (const auto& region_info : response.region_infos()) {
      if (_table_id_map.find(region_info.table_id())==_table_id_map.end() &&
          _table_id_map.find(region_info.main_table_id()) == _table_id_map.end()) {
        continue;
      }
      std::string table_key = build_key(region_info.table_name());
      _table_region_ids[table_key].emplace_back(region_info.region_id());
      _table_region_info_map[table_key].emplace_back(region_info);
      g_dump_total_count << 1;
    }

DEBUG_ONLY({
    for (auto pair : _table_region_ids) {
      std::cout<<"table_name: "<<pair.first<<", region_ids: "<<pair.second.size()<<std::endl;
    }
});
    return Status::OK();

  }
  auto build_key(const std::string& table) const -> std::string {
    return _namespace + "\001" + _database + "\001" +table;
  }
};




}