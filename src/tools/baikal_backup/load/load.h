//
// Created by user on 25-8-22.
//

#ifndef LOAD_H
#define LOAD_H
#include <baikal_heartbeat.h>
#include <support/config.h>
#include <cstdint>
#include <map>
#include <task.h>
#include <type.h>
#include <json2pb/json_to_pb.h>
#include <filesystem>

#include "parse_record.h"
#include <load/sql_exec.h>

namespace backup_tool {
namespace fs = std::filesystem;

typedef std::vector<baikaldb::ExprValue> SQLRecord;

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
    LoadWorker(const int64_t& table_id,
               const baikaldb::TableInfo& table_info,
               const std::unordered_map<int64_t, baikaldb::SmartIndex>& index_info_map,
               std::mutex* queue_mutex,
               std::condition_variable* cv,
               std::queue<SQLRecord>* q,
               RegionTask& task)
        : _table_id(table_id),
          _table_info(table_info),
          _index_info_map(index_info_map),
          _queue_mutex(queue_mutex),
          _cv(cv),
          _q(q),
          _task(task) {}

    auto process() -> Status {
        DB_WARNING("region %ld start to throw record to queue, path: %s",
                   _task._region_id, _task._path.c_str());

        init();

        SSTParser parser(_task._path, _index_info_map, _table_info, _pk_index_info, pk_fields);
        auto status = parser.init();
        if (!status.ok()) {
            DB_FATAL("init sst parser fail, path: %s, err: %s",
                     _task._path.c_str(), status.message().c_str());
            return status;
        }

        int all = 0;
        for (;;) {
            std::vector<std::vector<baikaldb::ExprValue>> rows;
            size_t row_cnt = parser.fetch_records(_batch_size, rows);

            // 将 rows 转成 SQLRecord 批次（请按你的字段/表结构实现）
            // std::vector<SQLRecord> batch;
            // batch.reserve(rows.size());
            // for (auto& row : rows) {
            //     batch.emplace_back(build_sql_record(row)); // TODO: 实现该转换
            // }

            enqueue_records(rows);
            all += static_cast<int>(row_cnt);

            if (row_cnt < _batch_size) break;
        }

        DB_WARNING("region %ld done, total rows: %d", _task._region_id, all);
        return StatusCode::kOk;
    }

private:
    // TODO: 根据你的 TableInfo/IndexInfo 构造 SQLRecord
    inline SQLRecord build_sql_record(const std::vector<std::pair<bool, std::string>>& row) {
        SQLRecord rec;
        // 填充 rec ...
        return rec;
    }

    // 有界队列入队：队满则等待；每 push 一个检查一次容量
    void enqueue_records(std::vector<SQLRecord>& records) {
        std::unique_lock<std::mutex> lk(*_queue_mutex);
        for (auto& r : records) {
            _cv->wait(lk, [&] {
                return _q->size() < FLAGS_max_record_queue_size;
            });
            _q->push(std::move(r));
        }
        lk.unlock();
        _cv->notify_one(); // 唤醒消费者
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

private:
    int64_t _table_id = 0;
    size_t _batch_size = 64;

    const baikaldb::TableInfo& _table_info;
    const std::unordered_map<int64_t, baikaldb::SmartIndex>& _index_info_map;
    baikaldb::SmartIndex _pk_index_info;
    std::unordered_set<int64_t> pk_fields;

    RegionTask& _task;

    std::mutex* _queue_mutex{nullptr};
    std::condition_variable* _cv{nullptr};
    std::queue<SQLRecord>* _q{nullptr};
};

class TableScheduler {
public:
    TableScheduler(int64_t table_id, std::string table_name, std::string rewrite_name,
                   const baikaldb::TableInfo& table_info, std::vector<int64_t>& regions,
                   std::queue<RegionTask>& tasks,
                   const std::unordered_map<int64_t, baikaldb::SmartIndex>& index_info_map,
                   SQLExec* exec)
        : _table_id(table_id),
          _table_name(std::move(table_name)),
          _rewrite_name(std::move(rewrite_name)),
          _table_info(table_info),
          _index_info_map(index_info_map),
          _regions(regions),
          _tasks(tasks),
          _sql_exec(exec) {
        _load_concurrency = FLAGS_load_concurrency;
        _receive_queue_num = FLAGS_receive_queue_num;
        _sql_batch_size = FLAGS_sql_batch_size;
    }

    auto process_one_region() -> Status {
        std::lock_guard<std::mutex> lk(task_mutex);
        if (_tasks.empty()) {
            DB_WARNING("table %s all regions done", _table_name.c_str());
            return StatusCode::kOk;
        }
        auto task = _tasks.front();
        DB_WARNING("start to process region: %ld, remain regions: %zu",
                   task._region_id, _tasks.size());
        _tasks.pop();

        // 生产者：把记录塞到共享队列（使用同一把 mutex + cv）
        LoadWorker worker(task._table_id, _table_info, _index_info_map,
                          &_queue_mutex, &_cv, &_exec_queue, task);
        worker.process();

        DB_WARNING("process region: %ld done", task._region_id);
        return StatusCode::kOk;
    }

    Status process_one_region_nolock(RegionTask task) {
        LoadWorker worker(task._table_id, _table_info, _index_info_map,
                          &_queue_mutex, &_cv, &_exec_queue, task);
        auto st = worker.process();
        DB_WARNING("process region: %ld done", task._region_id);
        return st;
    }

    auto process() -> void {
        DB_WARNING("table: %s start, region num: %zu", _table_name.c_str(), _regions.size());

        baikaldb::Bthread bth;
        bth.run([this] { this->exec_sql_thread(); });

        baikaldb::ConcurrencyBthread cbth(_load_concurrency);
        for (;;) {
            std::lock_guard<std::mutex> lk(task_mutex);
            if (_tasks.empty()) break;
            RegionTask task = _tasks.front();
            _tasks.pop();
            DB_WARNING("start to process region: %ld, remain regions: %zu", task._region_id, _tasks.size());

            cbth.run([this, task] { this->process_one_region_nolock(task); });
        }

        cbth.join(); // 先等所有生产者结束
        mark_done(); // 再宣布 done，消费者会 drain 干净后退出
        bth.join();

        DB_WARNING("table: %s done, region num: %zu", _table_name.c_str(), _regions.size());
    }

    //（可选）若别处也要直接 push，可保留此接口
    void push_record(SQLRecord r) {
        {
            std::lock_guard<std::mutex> lk(_queue_mutex);
            if (_done) {
                DB_WARNING("push after done ignored");
                return;
            }
            _exec_queue.push(std::move(r));
        }
        _cv.notify_one();
    }

    void mark_done() {
        {
            std::lock_guard<std::mutex> lk(_queue_mutex);
            _done = true;
        }
        _cv.notify_all();
    }

    void exec_sql_thread() {
        DB_WARNING("exec sql thread start");
        std::vector<SQLRecord> batch;
        batch.reserve(_sql_batch_size);

        for (;;) {
            batch.clear();

            std::unique_lock<std::mutex> lk(_queue_mutex);
            _cv.wait(lk, [&] {
                return !_exec_queue.empty() || _done;
            });

            // 只有“空 && done”才退出；确保把剩余都处理完
            if (_exec_queue.empty() && _done) {
                break;
            }

            size_t before = _exec_queue.size();
            while (!_exec_queue.empty() && batch.size() < _sql_batch_size) {
                batch.emplace_back(std::move(_exec_queue.front()));
                _exec_queue.pop();
            }
            size_t after = _exec_queue.size();
            lk.unlock();

            // 队列变小了，唤醒可能在等“队列不满”的生产者
            _cv.notify_all();

            DB_WARNING("get %zu records from queue, queue: %zu -> %zu",
                       batch.size(), before, after);

            _sql_exec->insert_rows(batch, _sql_batch_size);
        }

        DB_WARNING("exec sql thread done");
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

    // 共享队列 & 同步原语
    std::mutex _queue_mutex;
    std::condition_variable _cv;
    std::queue<SQLRecord> _exec_queue;
    bool _done = false;

    // 任务列表
    std::mutex task_mutex;
    std::queue<RegionTask>& _tasks;
    std::vector<int64_t>& _regions;

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
        //if no table rules, load all tables without rewrite
        if (cfg._tables.size() == 0 && cfg._table_rewrite.size() == 0) {
            _all_tables = true;
        }
        else {
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
            //if (table_name == "check_record") continue;
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
        MySQLConfig config;
        config._host = FLAGS_dest_host;
        config._port = FLAGS_dest_port;
        config._user = FLAGS_dest_user;
        config._password = FLAGS_dest_password;
        config._database = FLAGS_dest_database_name;
        config._timeout_sec = 5;
        SQLExec exec(config, 100, "", "");
        exec.init();
        for (const auto& [table_id,table_info] : _table_info_map) {
            //if (table_info.short_name=="check_record") continue;
            DB_WARNING("---------");
            DB_WARNING("start to run table %s", table_info.name.c_str());
            DB_WARNING("start create table %s", table_info.name.c_str());
            exec.set_create_table_sql(_table_create_sql_map[table_id]);
            exec.set_insert_tpl(_table_insert_sql_tpl_map[table_id]);
            bool create = exec.ensure_table();
            if (!create) {
                DB_FATAL("ensure table %s failed", table_info.name.c_str());
                exit(1);
            }
            DB_WARNING("create table %s done", table_info.name.c_str());
            //usleep(10*1000 * 1000);
            TableScheduler table_scheduler(table_id, table_info.name, _table_rewrite[table_info.name], table_info,
                                           table_region_map[table_id], _tasks[table_id], _idx_mp, &exec);

            table_scheduler.process();
            //exec.shutdown();
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
            if (_all_tables) {
                _table_rewrite[schema_info.table_name()] = schema_info.table_name();
                _src_tables.emplace_back(schema_info.table_name());
                _dst_tables.emplace_back(schema_info.table_name());
            }
            if (_all_tables || (
                    schema_info.namespace_name() == _src_ns_name &&
                    schema_info.database() == _src_database_name &&
                    _table_rewrite.find(schema_info.table_name()) != _table_rewrite.end()
                )
            ) {
                TableBuilder tb(schema_info.table_id());
                baikaldb::TableInfo table_info = baikaldb::TableInfo();

                tb.build(schema_info, table_info, _idx_mp);
                _table_info_map[schema_info.table_id()] = table_info;
                _full_name_id_mp[build_key(schema_info.table_name())] = schema_info.table_id();
                _table_insert_sql_tpl_map[schema_info.table_id()] = gen_table_insert_tpl(
                    FLAGS_dest_database_name, _table_rewrite[table_info.short_name], table_info);
                _table_create_sql_map[schema_info.table_id()] = gen_create_table_sql(table_info);
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

    auto gen_create_table_sql(const baikaldb::TableInfo& table_info) -> std::string {
        std::ostringstream oss;
        std::string rewrite_table_name = _table_rewrite[table_info.short_name];
        baikaldb::ShowHelper::_build_create_table_sql(oss, rewrite_table_name, table_info, _idx_mp, true);
        std::string ret = oss.str();
        //const size_t pos = ret.find("`");
        // if (pos == std::string::npos) {
        //     DB_FATAL("gen create table sql fail, table: %s", table_info.name.c_str());
        //     exit(1);
        // }
        //ret.insert(pos, "IF NOT EXISTS ");
        return ret;
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

    bool _all_tables = false;
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
    std::unordered_map<int64_t, std::string> _table_create_sql_map;
};
}


#endif //LOAD_H
