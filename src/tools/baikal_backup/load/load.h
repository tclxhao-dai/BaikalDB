//
// Created by user on 25-8-22.
//

#ifndef LOAD_H
#define LOAD_H
#include <baikal_heartbeat.h>
#include <config.h>
#include <cstdint>
#include <map>
#include <task.h>
#include <type.h>
#include <json2pb/json_to_pb.h>
#include <filesystem>

#include "parser.h"
#include <load/sql_exec.h>

namespace backup_tool {
namespace fs = std::filesystem;

typedef std::vector<std::pair<bool, std::string>> SQLRecord;

DEFINE_uint32(load_concurrency, 1, "load concurrency for each table");
DEFINE_uint32(receive_queue_num, 1, "receive queue num for each table");
DEFINE_uint32(sql_batch_size, 1000, "sql batch size for each insert");
DEFINE_uint32(max_record_queue_size, 100000, "max record queue size for each table");

DEFINE_string(dest_host, "", "destination mysql host");
DEFINE_int32(dest_port, 3306, "destination mysql port");
DEFINE_string(dest_user, "root", "destination mysql user");
DEFINE_string(dest_password, "", "destination mysql password");
DEFINE_string(dest_database_name, "", "destination mysql database name");


class LoadWorker {
public:
    LoadWorker(const int64_t& table_id, const baikaldb::TableInfo& table_info,
               const std::unordered_map<int64_t, baikaldb::SmartIndex>& index_info_map, std::mutex* mutex,
               std::queue<SQLRecord>* q, RegionTask& task) : _table_id(table_id),
                                                             _table_info(table_info), _index_info_map(index_info_map),
                                                             _mutex(mutex),
                                                             _q(q), _task(task) {}

    auto process() -> Status {
        init();
        SSTParser parser = SSTParser(_task._path, _index_info_map, _table_info, _pk_index_info, pk_fields);
        auto status = parser.init();
        if (!status.ok()) {
            DB_FATAL("init sst parser fail, path: %s, err: %s", _task._path.c_str(), status.message().c_str());
            return status;
        }
        int all = 0;
        while (true) {
            std::vector<std::vector<std::pair<bool, std::string>>> rows;
            size_t row_cnt = parser.fetch_records(_batch_size, rows);
            enqueue_record(rows);
            DB_WARNING("enqueue %d rows", row_cnt);
            all += row_cnt;
            if (row_cnt < _batch_size) {
                std::cout << all << std::endl;
                break;
            }
            DB_WARNING("region %ld process %ld rows", _task._region_id, all);
        }

        return StatusCode::kOk;
    }

private:
    auto enqueue_record(std::vector<SQLRecord>& record) -> void {
        DB_WARNING("queue size: %d", _q->size());
        while (_q->size() > FLAGS_max_record_queue_size) {
            usleep(1000 * 100);
        }
        _mutex->lock();
        for (size_t i = 0; i < record.size(); i++) {
            _q->emplace(record[i]);
        }
        _mutex->unlock();
    }

    auto init() -> Status {
        int64_t pk_id = _table_info.id;
        const auto& pkInfo = _index_info_map.find(pk_id);
        if (pkInfo == _index_info_map.end()) {
            DB_FATAL("index info not found, table_id: %ld, pk_id: %ld", _table_id, pk_id);
            exit(1);
        }
        _pk_index_info = pkInfo->second;
        for (const auto& field : pkInfo->second->fields) {
            pk_fields.insert(field.pb_idx);
        }
        return StatusCode::kOk;
    }

    int64_t _table_id = 0;
    size_t _batch_size = 64;
    const baikaldb::TableInfo& _table_info;
    const std::unordered_map<int64_t, baikaldb::SmartIndex>& _index_info_map;
    baikaldb::SmartIndex _pk_index_info;
    std::unordered_set<int64_t> pk_fields;
    RegionTask& _task;
    std::queue<SQLRecord>* _q;
    std::mutex* _mutex;
};


class TableScheduler {
public:
    TableScheduler(int64_t table_id, std::string table_name, std::string rewrite_name,
                   const baikaldb::TableInfo& table_info, std::vector<int64_t>& regions, std::queue<RegionTask>& tasks,
                   const std::unordered_map<int64_t, baikaldb::SmartIndex>& index_info_map,
                   SQLExec* exec) : _table_id(table_id),
                                    _table_name(table_name), _rewrite_name(rewrite_name), _table_info(table_info),
                                    _index_info_map(index_info_map),
                                    _regions(regions), _tasks(tasks), _sql_exec(exec) {
        _load_concurrency = FLAGS_load_concurrency;
        _receive_queue_num = FLAGS_receive_queue_num;
        _sql_batch_size = FLAGS_sql_batch_size;
    }

    auto process_one_region() -> Status {
        task_mutex.lock();
        if (_tasks.empty()) {
            _done = true;
            task_mutex.unlock();
            return StatusCode::kOk;
        }
        auto task = _tasks.front();
        _tasks.pop();
        task_mutex.unlock();
        LoadWorker worker(task._table_id, _table_info, _index_info_map, &_queue_mutex, &_exec_queue, task);
        worker.process();
        return StatusCode::kOk;
    }

    auto process() -> void {
        baikaldb::Bthread bth;
        bth.run([this] {
            this->exec_sql_thread();
        });
        baikaldb::ConcurrencyBthread cbth(_load_concurrency);
        while (!_tasks.empty()) {
            cbth.run([this] {
                DB_WARNING("xxx")
                this->process_one_region();
            });
        }
        cbth.join();
        bth.join();
    }


    auto exec_sql_thread() -> void {
        while (!_done) {
            if (_exec_queue.empty()) {
                usleep(1000 * 100);
                continue;
            }
            std::vector<SQLRecord> records;
            _queue_mutex.lock();
            for (size_t i = 0; i < _sql_batch_size && !_exec_queue.empty(); i++) {
                records.emplace_back(_exec_queue.front());
                _exec_queue.pop();
            }
            _queue_mutex.unlock();
            _sql_exec->insert_rows(records, _sql_batch_size);
        }
    }

private:
    int64_t _table_id = 0;
    std::string _table_name;
    std::string _rewrite_name;
    const baikaldb::TableInfo& _table_info;
    const std::unordered_map<int64_t, baikaldb::SmartIndex>& _index_info_map;
    size_t _load_concurrency = 1;
    size_t _receive_queue_num = 1;
    size_t _sql_batch_size = 500;

    std::mutex _queue_mutex;
    std::queue<SQLRecord> _exec_queue;

    std::vector<int64_t>& _regions;

    std::mutex task_mutex;
    std::queue<RegionTask>& _tasks;

    bool _done = false;
    SQLExec* _sql_exec = nullptr;
    std::string _create_sql;
};

class LoadManager {
public:
    explicit LoadManager(const Config& cfg) {
        _max_region_concurrency = cfg._max_region_concurrency;
        _mysql_exec_concurrency = cfg._mysql_exec_concurrency;
        _mysql_batch_size = cfg._mysql_batch_size;
        _load_path = cfg._load_path;
        _table_rewrite = cfg._table_rewrite;
        for (const std::string& table : cfg._tables) {
            _table_rewrite[table] = table;
        }
        for (const auto& [ori,dst] : cfg._table_rewrite) {
            _table_rewrite[ori] = dst;
        }
        for (auto const& [k,v] : _table_rewrite) {
            _src_tables.emplace_back(k);
            _dst_tables.emplace_back(v);
        }
        _src_ns_name = cfg._namespace;
        _src_database_name = cfg._database;
        _src_tables = cfg._tables;
    }

    auto init() -> Status {
        fs::path load_path(_load_path);
        //load schema info
        const auto ret = load_table_info();
        if (!ret.ok()) {
            DB_FATAL("load table info fail");
            exit(1);
        }
        //iterate table path
        std::error_code err_table;
        if (!fs::exists(load_path)) {
            DB_FATAL("load path isn't exist");
            exit(1);
        }
        for (fs::directory_iterator table_it(load_path, err_table), end; table_it != end; table_it.
             increment(err_table)) {
            std::string table_name = table_it->path().filename();
            if (err_table) {
                DB_FATAL("iterate path %s err:%s", table_it->path().c_str(), err_table.message().c_str());
                exit(1);
            }
            //only process directory
            if (!table_it->is_directory()) {
                continue;
            }
            //iterate every region of the table
            int64_t table_id = _full_name_id_mp[build_key(table_name)];
            table_region_map[table_id] = std::vector<int64_t>{};
            _tasks[table_id] = std::queue<RegionTask>{};
            std::error_code err_region;
            for (fs::directory_iterator region_it(table_it->path(), err_region), end2; region_it != end2; region_it.
                 increment(err_region)) {
                if (err_region) {
                    DB_FATAL("iterate path %s err:%s", region_it->path().c_str(), err_region.message().c_str());
                }
                int64_t region_id = atoi(region_it->path().filename().c_str());
                table_region_map[table_id].emplace_back(table_id);
                RegionTask t(region_id, _full_name_id_mp[build_key(table_name)], table_name, region_it->path(), "", 0);
                _tasks[table_id].push(t);
                DB_WARNING("push region %d into work queue", region_id);
            }
        }
        return StatusCode::kOk;
    }

    auto run() -> Status {
        for (const auto& [table_id,table_info] : _table_info_map) {
            MySQLConfig config;
            config._host = FLAGS_dest_host;
            config._port = FLAGS_dest_port;
            config._user = FLAGS_dest_user;
            config._password = FLAGS_dest_password;
            config._database = FLAGS_dest_database_name;
            config._timeout_sec = 5;
            SQLExec exec(config, 100, _table_insert_sql_tpl_map[table_id], "");
            exec.init();
            TableScheduler table_scheduler(table_id, table_info.name, _table_rewrite[table_info.name], table_info,
                                           table_region_map[table_id], _tasks[table_id], _idx_mp, &exec);
            table_scheduler.process();
            exec.shutdown();
        }
        return StatusCode::kOk;
    }

private:
    auto build_key(const std::string& table) const -> std::string {
        return _src_ns_name + "\001" + _src_database_name + "\001" + table;
    }

    auto load_table_info() -> Status {
        const fs::path json_path = fs::path(_load_path) / "schema.json";
        std::cout << json_path << std::endl;
        std::ifstream ifs(json_path, std::ios::in | std::ios::binary);
        if (!ifs.is_open()) {
            DB_WARNING("open log index file %s failed", (_load_path+"/schema.json").c_str());
            return StatusCode::kFileError;
        }
        std::ostringstream oss;
        oss << ifs.rdbuf();
        std::string schema_json = oss.str();
        baikaldb::pb::QueryResponse response;
        std::string err = baikaldb::json2pb(schema_json, &response);
        if (err != "") {
            std::cout << schema_json.length() << std::endl;
            std::cout << schema_json.c_str() << std::endl;
            std::cout << err.c_str() << std::endl;
            DB_FATAL("transfer json:%s to pb error:%s", schema_json.c_str(), err.c_str());
            return StatusCode::kInternal;
        }
        for (auto schema_info : response.schema_infos()) {
            if (schema_info.namespace_name() == _src_ns_name &&
                schema_info.database() == _src_database_name &&
                _table_rewrite.find(schema_info.table_name()) != _table_rewrite.end()) {
                TableBuilder tb(schema_info.table_id());
                baikaldb::TableInfo table_info = baikaldb::TableInfo();

                tb.build(schema_info, table_info, _idx_mp);
                _table_info_map[schema_info.table_id()] = table_info;
                _full_name_id_mp[build_key(schema_info.table_name())] = schema_info.table_id();
                _table_insert_sql_tpl_map[schema_info.table_id()] = gen_table_insert_tpl(
                    FLAGS_dest_database_name, _table_rewrite[table_info.short_name], table_info);
            }
        }
        return StatusCode::kOk;
    }

    auto get_table_info(int64_t table_id) -> const baikaldb::TableInfo& {
        if (_table_info_map.find(table_id) == _table_info_map.end()) {
            DB_FATAL("table_id: %ld not exist", table_id);
            exit(1);
        }
        return _table_info_map[table_id];
    }

    auto gen_table_insert_tpl(const std::string& dest_database_name, const std::string& dest_table_name,
                              const baikaldb::TableInfo& table_info) -> std::string {
        std::ostringstream oss;
        oss << "REPLACE INTO `" << dest_database_name << "`.`" << dest_table_name << "` (";
        bool first = true;
        for (const auto& field : table_info.fields) {
            if (field.deleted) {
                continue;
            }
            if (!first) {
                oss << ",";
            }
            first = false;
            oss << "`" << field.short_name << "`";
        }
        oss << ") VALUES ";
        return oss.str();
    }

    uint32_t _max_region_concurrency;
    uint32_t _mysql_exec_concurrency;
    uint32_t _mysql_batch_size;
    std::unordered_map<std::string, std::string> _table_rewrite;
    std::unordered_map<std::string, int64_t> _full_name_id_mp;
    std::vector<std::string> _ns_name;
    std::string _load_path;
    std::string _src_ns_name;
    std::string _src_database_name;

    std::vector<std::string> _src_tables;
    std::vector<std::string> _dst_tables;

    std::unordered_map<int64_t, std::queue<RegionTask>> _tasks;
    std::unordered_map<int64_t, std::queue<SQLRecord>> _sql_queue;
    std::unordered_map<int64_t, std::vector<int64_t>> table_region_map;
    std::unordered_map<int64_t, std::mutex> _sql_mutex;


    struct _dst_connection {
        std::string _host;
        uint32_t _port;
        std::string _user;
        std::string _password;
        std::string _database;
    };

    std::unordered_map<int64_t, baikaldb::TableInfo> _table_info_map;
    std::unordered_map<int64_t, std::string> _table_insert_sql_tpl_map;
    std::unordered_map<int64_t, baikaldb::SmartIndex> _idx_mp;
};
}


#endif //LOAD_H
