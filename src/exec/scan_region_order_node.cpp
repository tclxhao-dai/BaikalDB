// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "runtime_state.h"
#include "scan_region_order_node.h"
#include "network_socket.h"

namespace baikaldb {
//DEFINE_int32(region_per_batch, 4, "request region number in a batch");

int ScanRegionOrderNode::init(const pb::PlanNode& node) {
    int ret = 0;
    ret = ExecNode::init(node);
    if (ret < 0) {
        DB_WARNING("ExecNode::init fail, ret:%d", ret);
        return ret;
    }
    _op_type = pb::OP_SELECT;
    return 0;
}

void ScanRegionOrderNode::get_next_partition(RuntimeState* state) {
    _region_infos.clear();
    _start_key_sort.clear();
    _region_id_sort.clear();
    auto iter = _origin_region_infos.begin();
    while (iter != _origin_region_infos.end()) {
        _current_partition = iter->first;
        _region_infos.insert(iter->second.begin(), iter->second.end());
        for (auto& pair : iter->second) {
            auto& info = pair.second;
            _start_key_sort[info.partition_id()][info.start_key()] = info.region_id();
            _region_id_sort[info.region_id()] = info.region_id();
        }
        if (state->start_region_id != -1) {
            _origin_region_infos.erase(iter);
            iter ++;
            continue;
        }
        _origin_region_infos.erase(iter);
        break;
    }
    if (state->start_region_id > 0) {
        for (auto& pair : _region_id_sort) {
            if (state->start_region_id > pair.first) {
                continue;
            }
            _send_region_ids.emplace_back(pair.first);
        }
    } else {
        for (auto& partition_pair : _start_key_sort) {
            for (auto& pair : partition_pair.second) {
                _send_region_ids.emplace_back(pair.second);
            }
        }
    }
}

int ScanRegionOrderNode::open(RuntimeState* state) {
    auto client_conn = state->client_conn();
    if (client_conn == nullptr) {
        DB_WARNING("connection is nullptr: %lu", state->txn_id);
        return -1;
    }
    if (_return_empty) {
        return 0;
    }
    client_conn->seq_id++;
    state->seq_id = client_conn->seq_id;
    _scan_node = static_cast<RocksdbScanNode*>(get_node(pb::SCAN_NODE));
    if (state->start_region_id > 0) {
        if (get_all_regions(state) != 0) {
            return -1;
        }
    }
    for (auto& pair : _region_infos) {
        _origin_region_infos[pair.second.partition_id()][pair.first] = pair.second;
    }
    get_next_partition(state);
    DB_WARNING("region_count:%ld _current_partition:%ld ", _send_region_ids.size(), _current_partition);
    return 0;
}

int ScanRegionOrderNode::get_all_regions(RuntimeState* state) {
    ScanIndexInfo scan_index_info = *(_scan_node->main_scan_index());
    if (scan_index_info.router_index->ranges_size() == 0) {
        return 0;
    } 
    SchemaFactory* schema_factory = SchemaFactory::get_instance();
    int64_t main_table_id = _scan_node->table_id();
    auto index_ptr = schema_factory->get_index_info_ptr(scan_index_info.router_index_id);
    if (index_ptr == nullptr) {
        DB_WARNING("index info not found index_id:%ld", scan_index_info.router_index_id);
        return -1;
    }
    int ret = schema_factory->get_region_by_key(main_table_id,
            *index_ptr, nullptr,
            _region_infos,
            nullptr,
            _scan_node->get_partition(),
            true);
    if (ret < 0) {
        DB_WARNING("get_region_by_key:fail :%d", ret);
        return ret;
    }
    return 0;
}

bool ScanRegionOrderNode::get_batch(RowBatch* batch) {
    auto iter = _fetcher_store.start_key_sort.begin();
    if (iter != _fetcher_store.start_key_sort.end()) {
        // _fetcher_store内部region_batch修改
        auto iter2 = _fetcher_store.region_batch.find(iter->second);
        _fetcher_store.start_key_sort.erase(iter);
        if (iter2 != _fetcher_store.region_batch.end()) {
            // region merge了，拿下一个
            if (iter2->second == nullptr) {
                return true;
            }
            batch->swap(*iter2->second);
            _fetcher_store.region_batch.erase(iter2);
            return true;
        } else {
            return false;
        }
    } else {
        return false;
    }
}

int ScanRegionOrderNode::get_next(RuntimeState* state, RowBatch* batch, bool* eos) {
    if (state->is_cancelled()) {
        DB_WARNING_STATE(state, "cancelled");
        state->set_eos();
        *eos = true;
        return 0;
    }
    if (_return_empty) {
        state->set_eos();
        *eos = true;
        return 0;
    }
    if (reached_limit()) {
        *eos = true;
        return 0;
    }

    if (get_batch(batch)) {
        _num_rows_returned += batch->size();
        if (reached_limit()) {
            *eos = true;
            _num_rows_returned = _limit;
            return 0;
        }
        return 0;
    }

    uint64_t log_id = state->log_id();

    if (state->scan_region_count > 0 && state->scan_region_count <= send_region_count) {
        DB_WARNING("process full select done log_id:%lu because scan region count reach %d", log_id, send_region_count);
        state->set_eos();
        *eos = true;
        return 0;
    }

    if (_send_region_ids.empty()) {
        if (_origin_region_infos.empty()) {
            state->set_eos();
            DB_WARNING("process full select done log_id:%lu", log_id);
            *eos = true;
            return 0;
        }
        get_next_partition(state);
        DB_WARNING("region_count:%ld _current_partition:%ld", _send_region_ids.size(), _current_partition);
        if (_send_region_ids.empty()) {
            state->set_eos();
            DB_WARNING("process full select done log_id:%lu", log_id);
            *eos = true;
            return 0;
        }
    }
    
    _fetcher_store.scan_rows = 0;
    state->memory_limit_release_all();
    std::map<int64_t, pb::RegionInfo> infos;
    int64_t region_id = _send_region_ids.front();
    _send_region_ids.pop_front();
    auto iter = _region_infos.find(region_id);
    if (iter != _region_infos.end()) {
        infos[region_id] = iter->second;
    } else {
        return 0;
    }
    if (infos.empty()) {
        return 0;
    }
    int64_t last_region_id = region_id;
    state->start_region_id = -1;
    DB_WARNING("send region_id:%ld log_id:%lu", region_id, log_id);
    send_region_count ++;
    int ret = _fetcher_store.run(state, infos, _children[0], state->seq_id, state->seq_id, _op_type);
    if (ret < 0) {
        DB_FATAL("fetcher_store run fail:%d", ret);
        return -1;
    }
    // 有split或merge，更新region信息
    if (_fetcher_store.start_key_sort.size() > 1) {
        for (auto& pair : _fetcher_store.start_key_sort) {
            last_region_id = pair.second;
            if (state->client_conn()->region_infos.count(last_region_id) != 0) {
                _region_infos[last_region_id] = state->client_conn()->region_infos[last_region_id];
            }
        }
    }
    if (_limit > 0) {
        bool match = false;
        std::vector<int64_t> match_region_ids;
        auto iter = _fetcher_store.start_key_sort.begin();
        while (iter != _fetcher_store.start_key_sort.end()) {
            last_region_id = iter->second;
            if (match) {
                iter = _fetcher_store.start_key_sort.erase(iter);
                _fetcher_store.region_batch.erase(last_region_id);
                match_region_ids.emplace_back(last_region_id);
                continue;
            }
            auto iter2 = _fetcher_store.region_batch.find(last_region_id);
            if (iter2 != _fetcher_store.region_batch.end()) {
                // region merge了
                if (iter2->second == nullptr) {
                    iter = _fetcher_store.start_key_sort.erase(iter);
                    _fetcher_store.region_batch.erase(last_region_id);
                    continue;
                } else if (iter2->second->size() >= _limit) {
                    //当前region还有数据，下轮循环从last_key开始
                    match = true;
                    match_region_ids.emplace_back(last_region_id);
                }
            }
            ++iter;
        }
        //待继续的region重新加入队列
        if (match_region_ids.size() > 0) {
            _send_region_ids.insert(_send_region_ids.begin(), match_region_ids.begin(), match_region_ids.end());
        }
    }
    return 0;
}
} 

/* vim: set ts=4 sw=4 sts=4 tw=100 */
