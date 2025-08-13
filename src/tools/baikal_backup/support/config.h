//
// Created by user on 25-7-28.
//
#pragma once

#include <algorithm>
#include <gflags/gflags.h>
#include <iostream>
#include <numeric>
#include <sstream>
#include <string>
#include <unordered_map>
#include <vector>

namespace backup_tool {

// ------------------------------ Flag Declarations ---------------------------
// Dump‑side flags
DEFINE_string(src_meta_group, "", "Comma‑separated list of source MetaServer addresses <ip:port>[,<ip:port>...]");
DEFINE_string(namespace_name, "", "Namespace to dump (required)");
DEFINE_string(database, "", "Database to dump (required)");
DEFINE_string(tables, "", "Comma‑separated list of tables to dump (empty = all tables)");
DEFINE_string(dump_path, "./backup", "Directory where region‑sst & meta files are written");
DEFINE_bool(dump_balance_by_machine,true,"dump load balance by machine, false means balance by instance");
DEFINE_bool(dump_from_leader, true, "Download region SST from leader only (default = true)");
DEFINE_int32(concurrency, 1, "Concurrency for each store/machine (decided by --dump_balance_by_machine)");

// Load‑side flags
DEFINE_string(load_path, "", "Path that contains files to load (required for load tool)");
DEFINE_string(table_rewrite, "", "Table rename mapping when loading. Format: old1:new1,old2:new2");
DEFINE_string(load_type, "local_sst", "{local_sst|sst|database} – how to load backup");
DEFINE_string(dest_db_host, "", "Destination MySQL host when load_type==database");
DEFINE_int32(dest_db_port, 3306, "Destination MySQL port");
DEFINE_string(dest_db_user, "", "Destination MySQL user");
DEFINE_string(dest_db_pwd,  "", "Destination MySQL password");
DEFINE_string(dest_meta_group, "", "Comma‑separated MetaServer list of target cluster (for physical load)");
DEFINE_string(dest_namespace, "", "Target namespace (default = src)");
DEFINE_string(dest_database,  "", "Target database   (default = src)");
DEFINE_string(dest_resource_tag, "", "Target resource_tag (default = src)");
DEFINE_bool(debug,false,"print debug info");
DEFINE_uint32(max_region_concurrency, 20, "max concurrency for region sst file");
DEFINE_uint32(mysql_exec_concurrency, 10, "max concurrency for mysql execute");
DEFINE_uint32(mysql_batch_size,1000, "mysql batch size for replace");
// ----------------------------------------------------------------------------

struct Config {
    // Common/dump
    std::string _src_meta_group;
    std::string _namespace;           // "namespace" is a C++ keyword; use ns instead
    std::string _database;
    std::vector<std::string> _tables;
    std::string _dump_path;
    bool _dump_balance_by_machine = true; // true = by machine, false = by instance
    int _concurrency = 1; // concurrency for each store/machine(decided by _dump_balance_by_machine)

    bool _download_from_leader = true;


    // Load
    std::string _load_path;
    std::unordered_map<std::string, std::string> _table_rewrite;
    std::string _load_type;
    uint32_t _max_region_concurrency = 20;
    uint32_t _mysql_exec_concurrency = 10; // max concurrency for mysql execute
    uint32_t _mysql_batch_size = 1000; // mysql batch size for replace

    // DB‑level load (SQL)
    std::string _dest_db_host;
    int         _dest_db_port = 0;
    std::string _dest_db_user;
    std::string _dest_db_pwd;

    // Cluster‑level load (physical)
    std::string _dest_meta_group;
    std::string _dest_namespace;
    std::string _dest_database;
    std::string _dest_resource_tag;

    bool valid = true; // set false if mandatory opts are missing

    auto to_string() -> std::string {
        std::stringstream output;
        output << "config:\n";
        output  << "meta_group: "<< _src_meta_group << "\n"
                << "namespace: " << _namespace << "\n"
                << "database: " << _database << "\n"
                << "tables: " << std::accumulate(_tables.begin(), _tables.end(), std::string(",")) << "\n"
                << "table_rewrite: ";
        for (const auto &[k, v] : _table_rewrite) {
            output << k << ":" << v << "\n";
        }
        output  << "dump_path: " << _dump_path << "\n"
                << "load_path: " << _load_path << "\n";
        output << "load_type: "<< _load_type << "\n";
        if (_load_type == "database") {
            output  << "dest_db_host: " << _dest_db_host << "\n"
                    << "dest_db_port: " << _dest_db_port << "\n"
                    << "dest_db_user: " << _dest_db_user << "\n"
                    << "dest_db_pwd: "  << _dest_db_pwd << "\n";
        } else if (_load_type == "sst") {
            output  << "dest_meta_group: " << _dest_meta_group << "\n"
                    << "dest_namespace: " << _dest_namespace << "\n"
                    << "dest_database: "  << _dest_database << "\n"
                    << "dest_resource_tag: " << _dest_resource_tag << "\n";
        }
        return output.str();
    }
};

// ---------------------------- Helper Functions ------------------------------
inline std::vector<std::string> Split(const std::string& s, char delim) {
    std::vector<std::string> out;
    std::stringstream ss(s);
    std::string item;
    while (std::getline(ss, item, delim)) {
        if (!item.empty()) out.push_back(item);
    }
    return out;
}

inline std::unordered_map<std::string, std::string> ParseMapping(const std::string& s) {
    std::unordered_map<std::string, std::string> out;
    for (const auto& pair : Split(s, ',')) {
        auto pos = pair.find(':');
        if (pos == std::string::npos || pos == 0 || pos + 1 == pair.size()) continue;
        out.emplace(pair.substr(0, pos), pair.substr(pos + 1));
    }
    return out;
}

// ---------------------------- ParseConfig() ---------------------------------
inline Config ParseConfig(int* argc, char*** argv) {

    google::SetUsageMessage("BaikalDB backup/load tool – see flags for details");
    google::ParseCommandLineFlags(argc, argv, true);

    Config cfg;

    // Common / dump
    cfg._src_meta_group = FLAGS_src_meta_group;
    cfg._namespace            = FLAGS_namespace_name;
    cfg._database      = FLAGS_database;
    cfg._tables        = Split(FLAGS_tables, ',');
    cfg._dump_path     = FLAGS_dump_path;
    cfg._dump_balance_by_machine = FLAGS_dump_balance_by_machine;
    cfg._download_from_leader = FLAGS_dump_from_leader;
    cfg._concurrency = FLAGS_concurrency;

    // Load
    cfg._load_path       = FLAGS_load_path;
    cfg._table_rewrite   = ParseMapping(FLAGS_table_rewrite);
    cfg._load_type       = FLAGS_load_type;

    cfg._dest_db_host    = FLAGS_dest_db_host;
    cfg._dest_db_port    = FLAGS_dest_db_port;
    cfg._dest_db_user    = FLAGS_dest_db_user;
    cfg._dest_db_pwd     = FLAGS_dest_db_pwd;

    cfg._dest_meta_group = FLAGS_dest_meta_group;
    cfg._dest_namespace  = FLAGS_dest_namespace.empty() ? cfg._namespace       : FLAGS_dest_namespace;
    cfg._dest_database   = FLAGS_dest_database .empty() ? cfg._database : FLAGS_dest_database;
    cfg._dest_resource_tag = FLAGS_dest_resource_tag;

    cfg._max_region_concurrency = FLAGS_max_region_concurrency;
    cfg._mysql_exec_concurrency = FLAGS_mysql_exec_concurrency;
    cfg._mysql_batch_size = FLAGS_mysql_batch_size;


    // Minimal validation – require dump‑side essentials.
    if (cfg._src_meta_group.empty() || cfg._namespace.empty() || cfg._database.empty()) {
        std::cerr << "[init] --src_meta_group, --namespace and --database are required." << std::endl;
        cfg.valid = false;
    }


    // If running in load mode, some extra checks
    if (!cfg._load_path.empty()) {
        if (cfg._load_type == "database" && (cfg._dest_db_host.empty() || cfg._dest_db_user.empty())) {
            std::cerr << "[init] --dest_db_host and --dest_db_user are required when load_type=database." << std::endl;
            cfg.valid = false;
        }
        if (cfg._load_type == "sst" && cfg._dest_meta_group.empty()) {
            std::cerr << "[init] --dest_meta_group is required when load_type=sst." << std::endl;
            cfg.valid = false;
        }
    }

    return cfg;
}

} // namespace backup_tool

