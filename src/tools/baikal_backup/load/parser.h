//
// Created by user on 25-8-26.
//

#ifndef PARSER_H
#define PARSER_H

#include <bthread/bthread.h>
#include <log.h>
#include <network_server.h>
#include <rocksdb/db.h>
#include <rocksdb/iterator.h>
#include <rocksdb/options.h>
#include <type.h>

#include <schema_factory.h>

#include "expr_value.h"
#include "expr_value.h"
#include "expr_value.h"

using google::protobuf::FileDescriptor;

namespace backup_tool {
class SSTParser {
public:
    SSTParser(const std::string& sst_path, const std::string& tmp_path,
              const  std::unordered_map<int64_t, baikaldb::SmartIndex>& idx_map,
              const baikaldb::TableInfo& table_info, baikaldb::SmartIndex pk_index_info,
              const std::unordered_set<int64_t>& pk_fields)
        : _sst_path(sst_path), _tmp_path(tmp_path + "/tmp"), _idx_mp(idx_map), _table_info(table_info),
          _pk_index_info(pk_index_info), _pk_fields(pk_fields) {}

    SSTParser(const std::string& sst_path,
              const std::unordered_map<int64_t, baikaldb::SmartIndex>& idx_map,
              const baikaldb::TableInfo& table_info, baikaldb::SmartIndex pk_index_info,
              const std::unordered_set<int64_t>& pk_fields) : _sst_path(sst_path), _idx_mp(idx_map),
                                                              _table_info(table_info), _pk_index_info(pk_index_info),
                                                              _pk_fields(pk_fields) {
        _tmp_path = sst_path + "/tmp";
    }

    ~SSTParser() {}

    auto init() -> Status {
        rocksdb::Options opt;
        opt.disable_auto_compactions = true;
        opt.create_if_missing = true;
        opt.comparator = rocksdb::BytewiseComparator();
        {
            rocksdb::DB* raw = nullptr;
            auto s = rocksdb::DB::Open(opt, _tmp_path, &raw);
            if (!s.ok()) {
                DB_WARNING("open sst file %s failed, %s", _tmp_path.c_str(),
                           s.ToString().c_str());
                return StatusCode::kFileError;
            }
            _db.reset(raw);
        }
        rocksdb::IngestExternalFileOptions ifo;
        ifo.move_files = false; // 同盘可用：不复制，直接 rename，最省
        ifo.allow_global_seqno = true; // 给整文件打全局 seq（读不受影响）
        ifo.ingest_behind = true; // 空库可开；避免与未来写入交错
        ifo.ingest_behind = false;
        auto s = _db->IngestExternalFile({_sst_path + "/data.sst"}, ifo);
        if (!s.ok()) {
            DB_WARNING("ingest sst file %s failed, %s", _sst_path.c_str(),
                       s.ToString().c_str());
            return StatusCode::kFileError;
        }
        rocksdb::ReadOptions ro;
        ro.fill_cache = false;
        ro.verify_checksums = false;
        ro.readahead_size = 4 * 1024 * 1024;
        _it.reset(_db->NewIterator(ro));
        _it->SeekToFirst();
        return Status::OK();
    }

    auto fetch_records(const size_t rows,
                       std::vector<std::vector<std::pair<bool, std::string>>>& res)
        -> size_t {
        size_t cnt = 0;
        for (; _it->Valid(); _it->Next()) {
            std::vector<std::pair<bool, std::string>> tmp;
            _it2col_val(tmp);
            res.emplace_back(tmp);
            ++cnt;
            if (cnt >= rows) {
                break;
            }
        }
        return cnt;
    }

private:
    auto _it2col_val(std::vector<baikaldb::ExprValue>& res) -> bool {
        //process pk of record;
        baikaldb::ExprValue
        Message* k_msg = _table_info.msg_proto->New();
        baikaldb::TableKey pk(_it->key());
        int pos = sizeof(int64_t) * 2;
        baikaldb::TableRecord pk_record(k_msg);
        pk.extract_index(*_pk_index_info, &pk_record, pos);
        //process value of record
        Message* v_msg = _table_info.msg_proto->New();
        baikaldb::TableRecord value_record(v_msg);
        value_record.decode(_it->value().data_, _it->value().size_);

        for (size_t i = 0; i < _table_info.fields.size(); i++) {
            const baikaldb::FieldInfo field_info = _table_info.fields[i];
            if (field_info.deleted) {
                continue;
            }
            std::string val;
            bool is_null = false;
            if (_pk_fields.find(field_info.pb_idx) != _pk_fields.end()) {
                pk_record.field_to_string(field_info, &val, &is_null);
            }
            else {
                value_record.field_to_string(field_info, &val, &is_null);
            }
            res.emplace_back(std::make_pair(is_null, val));
        }
        return true;
    }

    std::unique_ptr<rocksdb::DB> _db;
    std::unique_ptr<rocksdb::Iterator> _it;
    std::string _sst_path;
    std::string _tmp_path;

    const std::unordered_map<int64_t, baikaldb::SmartIndex>& _idx_mp;
    baikaldb::SmartIndex _pk_index_info;
    const std::unordered_set<int64_t>& _pk_fields;
    const baikaldb::TableInfo& _table_info;
};

class TableBuilder {
public:
    TableBuilder(int64_t table_id) : _table_id(table_id) {}

    auto build(const baikaldb::pb::SchemaInfo& table, baikaldb::TableInfo& tbl_info,
               std::unordered_map<int64_t, baikaldb::SmartIndex>& idx_mp) -> Status {
        const std::string& _db_name = table.database();
        const std::string& _tbl_name = table.table_name();
        const std::string& _namespace = table.namespace_name();
        std::string table_name("table_" + std::to_string(_table_id));

        int64_t database_id = table.database_id();
        int64_t table_id = table.table_id();
        std::unordered_set<int64_t> last_indics;
        //clear
        // tbl_info.file_proto->Clear();
        // tbl_info.fields.clear();
        // tbl_info.indices.clear();
        // tbl_info.dists.clear();
        // tbl_info.reverse_fields.clear();
        // tbl_info.arrow_reverse_fields.clear();
        // tbl_info.has_global_not_none = false;
        // tbl_info.has_rollup_index = false;
        // tbl_info.has_index_write_only_or_write_local = false;
        // tbl_info.sign_blacklist.clear();
        // tbl_info.sign_forcelearner.clear();
        // tbl_info.sign_rolling.clear();
        // tbl_info.sign_forceindex.clear();
        // tbl_info.fields_need_sum.clear();
        // tbl_info.sign_exec_type.clear();
        // tbl_info.has_version = false;
        // tbl_info.snapshot_blacklist.clear();
        //build table info
        tbl_info.file_proto = new(std::nothrow)google::protobuf::FileDescriptorProto;
        tbl_info.file_proto->mutable_options()->set_cc_enable_arenas(true);
        tbl_info.file_proto->set_name(std::to_string(database_id) + ".proto");
        tbl_info.tbl_proto = tbl_info.file_proto->add_message_type();

        tbl_info.id = table.table_id();
        tbl_info.db_id = table.database_id();
        tbl_info.version = table.version();
        tbl_info.partition_num = table.partition_num();
        if (!table.has_byte_size_per_record() || table.byte_size_per_record() < 1) {
            tbl_info.byte_size_per_record = 1;
        }
        else {
            tbl_info.byte_size_per_record = table.byte_size_per_record();
        }
        if (table.has_region_split_lines() && table.region_split_lines() != 0) {
            tbl_info.region_split_lines = table.region_split_lines();
        }
        else {
            tbl_info.region_split_lines =
                table.region_size() / tbl_info.byte_size_per_record;
        }
        int field_cnt = table.fields_size();
        tbl_info.auto_inc_field_id = -1;
        //build tableInfo.schema_conf
        if (table.has_schema_conf()) {
            baikaldb::update_schema_conf_common(
                table.table_name(), table.schema_conf(), &tbl_info.schema_conf);
            auto bk = table.schema_conf().backup_table();
            if (bk == baikaldb::pb::BT_AUTO) {
                tbl_info.have_backup = true;
            }
            else if (bk == baikaldb::pb::BT_READ) {
                tbl_info.have_backup = true;
                tbl_info.need_read_backup = true;
            }
            else if (bk == baikaldb::pb::BT_WRITE) {
                tbl_info.have_backup = true;
                tbl_info.need_write_backup = true;
            }
            else if (bk == baikaldb::pb::BT_LEARNER) {
                tbl_info.need_learner_backup = true;
            }
            else {
                tbl_info.have_backup = false;
                tbl_info.need_read_backup = false;
                tbl_info.need_write_backup = false;
                tbl_info.need_learner_backup = false;
            }
            if (tbl_info.schema_conf.has_sign_blacklist() &&
                tbl_info.schema_conf.sign_blacklist() != "") {
                std::vector<std::string> vec;
                boost::split(vec, tbl_info.schema_conf.sign_blacklist(),
                             boost::is_any_of(","));
                for (auto& sign_str : vec) {
                    uint64_t sign_num = strtoull(sign_str.c_str(), nullptr, 10);
                    tbl_info.sign_blacklist.emplace(sign_num);
                }
            }
            if (tbl_info.schema_conf.has_sign_forcelearner() &&
                tbl_info.schema_conf.sign_forcelearner() != "") {
                std::vector<std::string> vec;
                boost::split(vec, tbl_info.schema_conf.sign_forcelearner(),
                             boost::is_any_of(","));
                for (auto& sign_str : vec) {
                    uint64_t sign_num = strtoull(sign_str.c_str(), nullptr, 10);
                    tbl_info.sign_forcelearner.emplace(sign_num);
                }
            }
            if (tbl_info.schema_conf.has_sign_rolling() &&
                tbl_info.schema_conf.sign_rolling() != "") {
                std::vector<std::string> vec;
                boost::split(vec, tbl_info.schema_conf.sign_rolling(),
                             boost::is_any_of(","));
                for (auto& sign_str : vec) {
                    uint64_t sign_num = strtoull(sign_str.c_str(), nullptr, 10);
                    tbl_info.sign_rolling.emplace(sign_num);
                }
            }
            if (tbl_info.schema_conf.has_sign_forceindex() &&
                tbl_info.schema_conf.sign_forceindex() != "") {
                std::vector<std::string> vec;
                boost::split(vec, tbl_info.schema_conf.sign_forceindex(),
                             boost::is_any_of(","));
                for (auto& sign_str : vec) {
                    tbl_info.sign_forceindex.emplace(sign_str);
                }
            }
            if (tbl_info.schema_conf.has_sign_exec_type() &&
                tbl_info.schema_conf.sign_exec_type() != "") {
                std::vector<std::string> vec;
                boost::split(vec, tbl_info.schema_conf.sign_exec_type(),
                             boost::is_any_of(","));
                for (auto& sign_str : vec) {
                    tbl_info.sign_exec_type.emplace(sign_str);
                }
            }
            if (tbl_info.schema_conf.has_snapshot_blacklist() &&
                tbl_info.schema_conf.snapshot_blacklist() != "") {
                std::vector<std::string> vec;
                boost::split(vec, tbl_info.schema_conf.snapshot_blacklist(),
                             boost::is_any_of(","));
                for (auto& snapshot_str : vec) {
                    uint64_t snapshot_num = strtoull(snapshot_str.c_str(), nullptr, 10);
                    tbl_info.snapshot_blacklist.emplace(snapshot_num);
                }
            }
            if (tbl_info.schema_conf.auto_inc_rand_max() > 0) {
                tbl_info.auto_inc_rand_max = tbl_info.schema_conf.auto_inc_rand_max();
            }
        }

        tbl_info.version = table.version();
        tbl_info.name = _db_name + "." + _tbl_name;
        tbl_info.short_name = _tbl_name;
        tbl_info.tbl_proto->set_name(table_name);
        tbl_info.namespace_ = _namespace;
        tbl_info.resource_tag = table.resource_tag();
        tbl_info.main_logical_room = table.main_logical_room();
        if (table.has_comment()) {
            tbl_info.comment = table.comment();
        }
        tbl_info.charset = table.charset();
        tbl_info.engine = baikaldb::pb::ROCKSDB;
        if (table.has_engine()) {
            tbl_info.engine = table.engine();
        }
        if (table.has_replica_num()) {
            tbl_info.replica_num = table.replica_num();
        }

        if (table.has_region_num()) {
            tbl_info.region_num = table.region_num();
        }

        if (table.has_ttl_duration()) {
            tbl_info.ttl_info.ttl_duration_s = table.ttl_duration();
            if (table.has_online_ttl_expire_time_us()) {
                tbl_info.ttl_info.online_ttl_expire_time_us = table.online_ttl_expire_time_us();
            }

            DB_WARNING("table:%s ttl_duration:%ld, online_ttl_expire_time_us:%ld, %s",
                       tbl_info.name.c_str(), tbl_info.ttl_info.ttl_duration_s,
                       tbl_info.ttl_info.online_ttl_expire_time_us,
                       baikaldb::timestamp_to_str(tbl_info.ttl_info.online_ttl_expire_time_us / 1000000).c_str());
        }

        tbl_info.learner_resource_tags.clear();
        for (auto& learner_resource : table.learner_resource_tags()) {
            tbl_info.learner_resource_tags.emplace_back(learner_resource);
        }
        for (auto& dist : table.dists()) {
            baikaldb::DistInfo dist_info;
            dist_info.resource_tag = dist.resource_tag();
            dist_info.logical_room = dist.logical_room();
            dist_info.physical_room = dist.physical_room();
            dist_info.count = dist.count();
            tbl_info.dists.push_back(dist_info);
        }
        std::unique_ptr<DescriptorPool> tmp_pool(new(std::nothrow)DescriptorPool);
        if (tmp_pool == nullptr) {
            DB_FATAL("create FileDescriptorProto failed");
            StatusCode::kInternal;
        }
        std::unique_ptr<DynamicMessageFactory> tmp_factory(
            new(std::nothrow)DynamicMessageFactory(tmp_pool.get()));
        if (tmp_factory == nullptr) {
            DB_FATAL("create DynamicMessageFactory failed");
            return StatusCode::kInternal;
        }


        //process fields info
        std::ostringstream new_fields_sign;
        int pb_idx = 0;
        //auto inc可以被取消
        tbl_info.auto_inc_field_id = -1;
        std::map<std::string, baikaldb::pb::FieldInfo> field_name_map;
        for (int idx = 0; idx < field_cnt; ++idx) {
            const baikaldb::pb::FieldInfo& field = table.fields(idx);
            if (field.deleted()) {
                continue;
            }
            field_name_map[field.field_name()] = field;
        }

        for (int idx = 0; idx < field_cnt; idx++) {
            const baikaldb::pb::FieldInfo& field = table.fields(idx);
            if (field.deleted()) {
                continue;
            }
            if (!field.has_field_id()
                || !field.has_mysql_type()
                || !field.has_field_name()) {
                DB_FATAL("missing field id (type or name)");
                return StatusCode::kInternal;
            }
            if (field.auto_increment()) {
                tbl_info.auto_inc_field_id = field.field_id();
            }
            FieldDescriptorProto* field_proto = tbl_info.tbl_proto->add_field();
            if (!field_proto) {
                DB_FATAL("add field failed: %d", field.has_field_id());
                return StatusCode::kInternal;
            }
            if (field.mysql_type() == baikaldb::pb::TDIGEST) {
                DB_WARNING("%s is TDIGEST", tbl_info.name.c_str());
            }
            field_proto->set_name(field.field_name());
            //FieldDescriptorProto::Type proto_type;
            int proto_type = baikaldb::primitive_to_proto_type(field.mysql_type());
            if (proto_type == -1) {
                DB_FATAL("mysql_type %d not supported.", field.mysql_type());
                return StatusCode::kInternal;
            }
            field_proto->set_type((FieldDescriptorProto::Type)proto_type);
            field_proto->set_number(field.field_id());
            field_proto->set_label(FieldDescriptorProto::LABEL_OPTIONAL);
            new_fields_sign << field.field_id() << ":";
            new_fields_sign << proto_type << ";";

            baikaldb::FieldInfo field_info;
            field_info.id = field.field_id();
            tbl_info.max_field_id = std::max(tbl_info.max_field_id, field_info.id);
            field_info.pb_idx = pb_idx++;
            field_info.table_id = _table_id;
            field_info.name = tbl_info.name + "." + field.field_name();
            field_info.short_name = field.field_name();
            field_info.lower_short_name.resize(field_info.short_name.size());
            std::transform(field_info.short_name.begin(), field_info.short_name.end(),
                           field_info.lower_short_name.begin(), ::tolower);
            field_info.lower_name = tbl_info.name + "." + field_info.lower_short_name;
            field_info.type = field.mysql_type();
            field_info.flag = field.flag();
            field_info.can_null = field.can_null();
            field_info.auto_inc = field.auto_increment();
            field_info.deleted = field.deleted();
            field_info.comment = field.comment();
            field_info.noskip = boost::algorithm::icontains(field_info.comment, "noskip");
            field_info.default_value = field.default_value();
            field_info.on_update_value = field.on_update_value();
            field_info.is_unique_indicator = field.is_unique_indicator();
            if (field.has_default_value()) {
                field_info.default_expr_value.type = baikaldb::pb::STRING;
                field_info.default_expr_value.str_val = field_info.default_value;
                if (field_info.default_value == "(current_timestamp())" && field.has_default_literal()) {
                    field_info.default_expr_value.str_val = field.default_literal();
                }
                field_info.default_expr_value.cast_to(field_info.type);
            }
            if (field_info.type == baikaldb::pb::STRING || field_info.type == baikaldb::pb::HLL
                || field_info.type == baikaldb::pb::BITMAP || field_info.type == baikaldb::pb::TDIGEST || field_info.
                type == baikaldb::pb::JSON) {
                field_info.size = -1;
            }
            else {
                field_info.size = baikaldb::get_num_size(field_info.type);
                if (field_info.size == -1) {
                    DB_FATAL("get_num_size type %d not supported.", field.mysql_type());
                    return StatusCode::kInternal;
                }
            }
            if (field.has_float_total_len()) {
                field_info.float_total_len = field.float_total_len();
            }
            if (field.has_float_precision_len()) {
                field_info.float_precision_len = field.float_precision_len();
            }
            if (field.has_generate_info()) {
                field_info.generate_expr = field.generate_info().generate_expr();
                field_info.generate_str = field.generate_info().generate_str();
                field_info.is_generated = true;
                tbl_info.has_generated_fields = true;
                auto set_expr_type_func = [&field_name_map, &field_info](baikaldb::pb::Expr& expr) -> int {
                    for (size_t i = 0; i < expr.nodes_size(); i++) {
                        auto node = expr.mutable_nodes(i);
                        if (node->has_derive_node() && node->derive_node().has_field_name()) {
                            auto field_name = node->derive_node().field_name();
                            auto iter = field_name_map.find(field_name);
                            if (iter != field_name_map.end()) {
                                auto& f = iter->second;
                                node->set_col_type(f.mysql_type());
                                node->mutable_derive_node()->set_field_id(f.field_id());
                                node->set_col_flag(f.flag());
                                field_info.generate_from_id = f.field_id();
                            }
                            else {
                                return -1;
                            }
                        }
                    }
                    return 0;
                };
                if (set_expr_type_func(field_info.generate_expr)) {
                    DB_FATAL("generated column set expr faild! table: %s, field: %s", tbl_info.name.c_str(),
                             field_info.name.c_str());
                    return StatusCode::kInternal;
                }
            }
            tbl_info.fields.push_back(field_info);
        }

        tbl_info.link_field_map.clear();
        tbl_info.binlog_target_ids.clear();
        tbl_info.is_linked = false;
        tbl_info.binlog_id = 0;
        tbl_info.binlog_ids.clear();
        if (table.has_binlog_info()) {
            auto& binlog_info = table.binlog_info();
            if (binlog_info.has_binlog_table_id()) {
                DB_WARNING("table %s,link binlog %ld", tbl_info.name.c_str(), binlog_info.binlog_table_id());
                tbl_info.is_linked = true;
                tbl_info.binlog_id = binlog_info.binlog_table_id();
                if (table.has_partition_is_same_hint()) {
                    tbl_info.binlog_ids[binlog_info.binlog_table_id()] = table.partition_is_same_hint();
                }
                else {
                    tbl_info.binlog_ids[binlog_info.binlog_table_id()] = true;
                }
            }
            for (auto target_id : table.binlog_info().target_table_ids()) {
                tbl_info.binlog_target_ids.insert(target_id);
            }
            if (table.has_link_field()) {
                for (int i = 0; i < tbl_info.fields.size(); i++) {
                    if (tbl_info.fields[i].id == table.link_field().field_id()) {
                        tbl_info.link_field_map[binlog_info.binlog_table_id()] = tbl_info.fields[i];
                        break;
                    }
                }
            }
        }
        if (table.binlog_infos_size() > 0) {
            for (int i = 0; i < table.binlog_infos_size(); i++) {
                auto& binlog_info = table.binlog_infos(i);
                if (binlog_info.has_binlog_table_id()) {
                    tbl_info.is_linked = true;
                    tbl_info.binlog_id = tbl_info.binlog_id == 0 ? binlog_info.binlog_table_id() : tbl_info.binlog_id;
                    if (binlog_info.has_partition_is_same_hint()) {
                        tbl_info.binlog_ids[binlog_info.binlog_table_id()] = binlog_info.partition_is_same_hint();
                    }
                    else {
                        tbl_info.binlog_ids[binlog_info.binlog_table_id()] = true;
                    }
                    DB_WARNING("table %s,link binlog %ld", tbl_info.name.c_str(), binlog_info.binlog_table_id());
                }
                for (auto target_id : binlog_info.target_table_ids()) {
                    tbl_info.binlog_target_ids.insert(target_id);
                }
                if (binlog_info.has_link_field()) {
                    for (int j = 0; j < tbl_info.fields.size(); j++) {
                        if (tbl_info.fields[j].id == binlog_info.link_field().field_id()) {
                            tbl_info.link_field_map[binlog_info.binlog_table_id()] = tbl_info.fields[j];
                            break;
                        }
                    }
                }
            }
        }
        tbl_info.is_binlog = (table.engine() == baikaldb::pb::BINLOG);
        std::string new_sign_str = new_fields_sign.str();
        bool pb_need_update = tbl_info.fields_sign != new_sign_str;
        DB_NOTICE("double_buffer_write pb_need_update:%d, old:%s new:%s table:%s ", pb_need_update,
                  tbl_info.fields_sign.c_str(), new_sign_str.c_str(), table.ShortDebugString().c_str());
        tbl_info.fields_sign = new_sign_str;
        // 先不处理分区，看看如何
        // if (table.has_partition_info()) {
        //     DB_NOTICE("update partition info table_%ld table_info[%s].", _table_id,
        //         table.ShortDebugString().c_str());
        //     tbl_info.partition_info.CopyFrom(table.partition_info());
        //     if (table.partition_info().type() == baikaldb::pb::PT_RANGE) {
        //         tbl_info.is_range_partition = true;
        //         tbl_info.partition_ptr.reset(new baikaldb::RangePartition);
        //         if (tbl_info.partition_ptr->init(table.partition_info(), tbl_info_ptr, table.partition_num()) != 0) {
        //             DB_WARNING("init RangePartition error.");
        //             return -1;
        //         }
        //     } else if (table.partition_info().type() == pb::PT_HASH) {
        //         tbl_info.partition_ptr.reset(new HashPartition);
        //         if (tbl_info.partition_ptr->init(table.partition_info(), tbl_info_ptr, table.partition_num()) != 0) {
        //             DB_WARNING("init HashPartition error.");
        //             return -1;
        //         }
        //     } else {
        //         DB_WARNING("unknown partition type.");
        //         return -1;
        //     }
        // }

        if (pb_need_update) {
            const FileDescriptor* db_desc = tmp_pool->BuildFile(*tbl_info.file_proto);
            if (db_desc == nullptr) {
                std::cout << tbl_info.file_proto->DebugString().c_str();
                DB_FATAL("build proto_file [%ld] failed, %s", _table_id, tbl_info.file_proto->DebugString().c_str());
                return StatusCode::kInternal;
            }
            const Descriptor* descriptor = db_desc->FindMessageTypeByName(table_name);
            if (descriptor == nullptr) {
                DB_FATAL("FindMessageTypeByName [%ld] failed.", _table_id);
                return StatusCode::kInternal;
            }
            auto del_pool = tbl_info.pool;
            auto del_factory = tbl_info.factory;
            tbl_info.pool = tmp_pool.release();
            tbl_info.factory = tmp_factory.release();
            tbl_info.tbl_desc = descriptor;
            tbl_info.msg_proto = tbl_info.factory->GetPrototype(tbl_info.tbl_desc);

            if (del_pool != nullptr || del_factory != nullptr) {
                baikaldb::BthreadTimer bth;
                bth.run(3600 * 1000, [ del_pool, del_factory]() {
                    // 延迟删除
                    delete del_factory;
                    delete del_pool;
                });
            }
        }
        std::string _db_table(baikaldb::to_lower(_namespace + "." + _db_name + "." + _tbl_name));

        size_t index_cnt = table.indexs_size();
        const baikaldb::pb::IndexInfo* pk_index = nullptr;
        for (size_t idx = 0; idx < index_cnt; ++idx) {
            const baikaldb::pb::IndexInfo& cur = table.indexs(idx);
            if (cur.index_id() == table_id) {
                pk_index = &cur;
                break;
            }
        }
        if (pk_index == nullptr && index_cnt > 0) {
            DB_FATAL("find pk_index failed: %ld, %ld", database_id, table_id);
            return StatusCode::kInternal;
        }
        for (size_t idx = 0; idx < index_cnt; ++idx) {
            const baikaldb::pb::IndexInfo& cur = table.indexs(idx);
            int64_t index_id = cur.index_id();
            DB_WARNING("schema_factory_update_index: %ld", index_id);
            last_indics.erase(index_id);
            update_index(tbl_info, cur, pk_index, idx_mp);
            if (cur.index_type() == baikaldb::pb::I_PRIMARY
                || cur.is_global() == true) {
                if (cur.is_global() && cur.state() != baikaldb::pb::IS_NONE && cur.hint_status() !=
                    baikaldb::pb::IHS_VIRTUAL) {
                    tbl_info.has_global_not_none = true;
                }
            }
            if (cur.index_type() == baikaldb::pb::I_FULLTEXT) {
                tbl_info.has_fulltext = true;
            }
            else if (cur.index_type() == baikaldb::pb::I_ROLLUP) {
                tbl_info.has_rollup_index = true;
            }
            else if (cur.index_type() == baikaldb::pb::I_VECTOR) {
                tbl_info.has_vector_index = true;
            }
            if (!cur.is_global() && (cur.state() == baikaldb::pb::IS_WRITE_ONLY || cur.state() ==
                baikaldb::pb::IS_WRITE_LOCAL)) {
                tbl_info.has_index_write_only_or_write_local = true;
            }
            if (cur.state() != baikaldb::pb::IS_PUBLIC) {
                DB_WARNING("table:%s index:%s not public", _db_table.c_str(), cur.index_name().c_str());
            }
            tbl_info.indices.push_back(index_id);
        }
        std::set<int32_t> pk_field_ids;
        tbl_info.fields_need_sum.clear();
        tbl_info.has_version = false;
        if (pk_index != nullptr) {
            for (int32_t id : pk_index->field_ids()) {
                pk_field_ids.insert(id);
            }
            for (const baikaldb::FieldInfo& f : tbl_info.fields) {
                if (pk_field_ids.count(f.id) <= 0 && !f.is_unique_indicator && baikaldb::is_int(f.type)) {
                    if (f.lower_short_name == "__version__") {
                        tbl_info.version_field = f;
                        tbl_info.has_version = true;
                    }
                    else {
                        tbl_info.fields_need_sum.emplace_back(f);
                    }
                }
            }
        }
        return StatusCode::kOk;
    }

    auto update_index(baikaldb::TableInfo& table_info, const baikaldb::pb::IndexInfo& index,
                      const baikaldb::pb::IndexInfo* pk_index,
                      std::unordered_map<int64_t, baikaldb::SmartIndex>& index_info_mapping) -> void {
        using namespace baikaldb;
        baikaldb::SmartIndex idx_info_ptr = std::make_shared<baikaldb::IndexInfo>();
        std::string old_idx_name;
        //如果存在，需要清空里面的内容
        bool index_first_init = true;


        IndexInfo& idx_info = *idx_info_ptr;
        if (index.is_global()) {
            idx_info.is_global = true;
        }
        if (index.index_type() == pb::I_PRIMARY || index.is_global()) {
            idx_info.is_partitioned = table_info.partition_num > 1 || table_info.is_range_partition;
        }
        idx_info.version = table_info.version;
        idx_info.pk = table_info.id;
        idx_info.id = index.index_id();
        std::string lower_index_name = index.index_name();
        std::transform(lower_index_name.begin(), lower_index_name.end(),
                       lower_index_name.begin(), ::tolower);
        idx_info.name = table_info.name + "." + lower_index_name;
        // idx_info.short_name = lower_index_name;
        idx_info.short_name = index.index_name();
        idx_info.type = index.index_type();
        idx_info.state = index.state();
        idx_info.max_field_id = table_info.max_field_id;
        idx_info.publish_timestamp = index.publish_timestamp();
        if (idx_info.state == pb::IS_WRITE_ONLY) {
            auto time = butil::gettimeofday_us();
            DB_DEBUG("update write_only timestamp %ld", time);
            idx_info.write_only_time = time;
        }
        else {
            idx_info.write_only_time = -1;
        }
        idx_info.segment_type = index.segment_type();
        if (index.has_hint_status()) {
            if (!index_first_init && idx_info.state == pb::IS_PUBLIC) {
                if (idx_info.index_hint_status != pb::IHS_NORMAL &&
                    index.hint_status() == pb::IHS_NORMAL) {
                    idx_info.restore_time = butil::gettimeofday_us();
                    DB_WARNING("table_id: %ld, index_id: %ld, restore time: %ld",
                               idx_info.pk, idx_info.id, idx_info.restore_time);
                }

                if (idx_info.index_hint_status != pb::IHS_DISABLE &&
                    index.hint_status() == pb::IHS_DISABLE) {
                    idx_info.disable_time = butil::gettimeofday_us();
                    DB_WARNING("table_id: %ld, index_id: %ld, disable time: %ld",
                               idx_info.pk, idx_info.id, idx_info.disable_time);
                }
            }
            idx_info.index_hint_status = index.hint_status();
        }
        if (index.has_storage_type()) {
            idx_info.storage_type = index.storage_type();
        }
        idx_info.vector_description = index.vector_description();
        idx_info.dimension = index.dimension();
        if (index.has_metric_type()) {
            idx_info.metric_type = index.metric_type();
        }
        if (index.has_nprobe()) {
            idx_info.nprobe = index.nprobe();
        }
        if (index.has_efsearch()) {
            idx_info.efsearch = index.efsearch();
        }
        if (index.has_efconstruction()) {
            idx_info.efconstruction = index.efconstruction();
        }
        int field_cnt = index.field_ids_size();
        if (index.index_type() == pb::I_ROLLUP) {
            idx_info.rollup_type = index.rollup_type();
        }
        //用于构建 std::vector<std::pair<int,int> > pk_pos;
        std::unordered_map<int32_t, int32_t> id_map;

        idx_info.has_nullable = false;
        if (idx_info.type == pb::I_KEY || idx_info.type == pb::I_UNIQ) {
            idx_info.length = 1; //nullflag
        }
        else {
            idx_info.length = 0;
        }
        for (int idx = 0; idx < field_cnt; ++idx) {
            FieldInfo* info = table_info.get_field_ptr(index.field_ids(idx));
            if (info == nullptr) {
                DB_FATAL("table %ld index %ld field %d not exist",
                         table_info.id, idx_info.id, index.field_ids(idx));
                return;
            }
            idx_info.fields.push_back(*info);

            //记录field_id在index_bytes中的对应位置
            id_map.insert(std::make_pair(info->id, idx_info.length));
            //DB_WARNING("index:%ld, field:%d, length:%d", idx_info.id, info.id, idx_info.length);
            if (info->can_null) {
                idx_info.has_nullable = true;
            }
            if (info->size == -1) {
                idx_info.length = -1;
            }
            else if (idx_info.length != -1) {
                idx_info.length += info->size;
            }
            if (idx_info.type == pb::I_FULLTEXT) {
                DB_NOTICE("table %ld:%s index %ld insert reverse field %d, type:%s",
                          table_info.id, table_info.name.c_str(), idx_info.id, info->id,
                          StorageType_Name(idx_info.storage_type).c_str());
                if (idx_info.storage_type == pb::ST_PROTOBUF_OR_FORMAT1) {
                    table_info.reverse_fields[info->id] = idx_info.id;
                }
                else {
                    table_info.arrow_reverse_fields[info->id] = idx_info.id;
                }
            }
        }
        if (idx_info.has_nullable) {
            idx_info.length = -1;
        }
        //DB_WARNING("index:%ld, index_length:%d", idx_info.id, idx_info.length);

        //只有二级索引需要保存pk_fields
        if (idx_info.type == pb::I_KEY || idx_info.type == pb::I_UNIQ) {
            //如果有变长或nullable字段，则不进行压缩
            if (idx_info.length == -1) {
                id_map.clear();
            }
            int32_t pk_length = 0;
            for (int idx = 0; idx < pk_index->field_ids_size(); ++idx) {
                int32_t field_id = pk_index->field_ids(idx);
                FieldInfo* info = table_info.get_field_ptr(field_id);
                if (info == nullptr) {
                    DB_FATAL("table %ld index %ld pk field %d not exist",
                             table_info.id, idx_info.id, field_id);
                    return;
                }
                if (info->size == -1) {
                    pk_length = -1;
                    break;
                }
                if (id_map.count(field_id) != 0) {
                    //重建pk时，该field从二级索引读取
                    idx_info.pk_pos.push_back(std::make_pair(1, id_map[field_id]));
                }
                else {
                    //重建pk时，该field从主键读取
                    idx_info.pk_fields.push_back(*info);
                    idx_info.pk_pos.push_back(std::make_pair(-1, pk_length == -1 ? 0 : pk_length));
                    pk_length += info->size;
                }
            }
            //pk中有变长字段，则不进行主键压缩
            if (pk_length == -1) {
                idx_info.pk_fields.clear();
                idx_info.pk_pos.clear();
                for (int idx = 0; idx < pk_index->field_ids_size(); ++idx) {
                    int32_t field_id = pk_index->field_ids(idx);
                    FieldInfo* info = table_info.get_field_ptr(field_id);
                    if (info != nullptr) {
                        idx_info.pk_fields.push_back(*info);
                    }
                }
            }

            //判断index和pk是否有overlap
            //length=-1时overlap一定为false, length>0时overlap仍可能为false）
            if (idx_info.length == -1) {
                idx_info.overlap = false;
            }
            else if (idx_info.pk_fields.size() == (uint32_t)pk_index->field_ids_size()) {
                idx_info.overlap = false;
            }
            else {
                idx_info.overlap = true;
            }
        }
        // _split_index_map.modify([idx_info](butil::FlatMap<int64_t, IndexInfo*>& map) {
        //     if (map.seek(idx_info.id) == nullptr) {
        //         // index的字段等信息不会修改，修改也是按照新增删除索引方式做的
        //         // 范围判断只需要这些不会修改的字段
        //         IndexInfo* new_info = new IndexInfo;
        //         *new_info = idx_info;
        //         map[idx_info.id] = new_info;
        //     }
        // });

        index_info_mapping[idx_info.id] = idx_info_ptr;
        std::string fullname = idx_info.name;
        // if (!old_idx_name.empty() && old_idx_name != fullname) {
        //     index_name_id_mapping.erase(old_idx_name);
        // }
        fullname = table_info.namespace_ + "." + idx_info.name;
        DB_WARNING("index full name:%s, %ld, %d", fullname.c_str(), idx_info.id, idx_info.overlap);
        //index_name_id_mapping[fullname] = idx_info.id;
    }

private:
    int64_t _table_id = 0;
};
} // namespace backup_tool
#endif // PARSER_H
