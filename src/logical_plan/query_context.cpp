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

#include "query_context.h"
#include "exec_node.h"

namespace baikaldb {
DEFINE_bool(default_2pc, false, "default enable/disable 2pc for autocommit queries");
DECLARE_bool(use_dynamic_timeout);
QueryContext::~QueryContext() {
    if (need_destroy_tree) {
        ExecNode::destroy_tree(root);
        root = nullptr;
    }
}

int QueryContext::create_plan_tree() {
    need_destroy_tree = true;
    return ExecNode::create_tree(plan, &root);
}

int QueryContext::destroy_plan_tree() {
    need_destroy_tree = false;
    ExecNode::destroy_tree(root);
    root = nullptr;
    return 0;
}

void QueryContext::update_ctx_stat_info(RuntimeState* state, int64_t query_total_time) {
    stat_info.num_returned_rows += state->num_returned_rows();
    stat_info.num_affected_rows += state->num_affected_rows();
    stat_info.num_scan_rows += state->num_scan_rows();
    stat_info.read_disk_size += state->read_disk_size();
    stat_info.num_filter_rows += state->num_filter_rows();
    stat_info.region_count += state->region_count;
    stat_info.rocks_get_count += state->rocks_get_count();
    stat_info.rocks_multiget_count += state->rocks_multiget_count();
    stat_info.rocks_seek_count += state->rocks_seek_count();
    stat_info.rocks_scan_count += state->rocks_scan_count();
    stat_info.get_primary_count += state->get_primary_count();
    stat_info.lock_cost += state->get_lock_cost();
    stat_info.wait_cost += state->get_wait_cost();
    if (FLAGS_use_dynamic_timeout && stmt_type == parser::NT_SELECT && stat_info.error_code == 1000 && state->sign != 0) {
        auto sql_info = SchemaFactory::get_instance()->get_sql_stat(state->sign);
        if (sql_info == nullptr) {
            sql_info = SchemaFactory::get_instance()->create_sql_stat(state->sign);
        }
        if (state->need_statistics) {
            sql_info->update(query_total_time, stat_info.num_scan_rows);
        }
    }
}

int64_t QueryContext::get_ctx_total_time() {
    gettimeofday(&(stat_info.end_stamp), NULL);
    stat_info.total_time = timestamp_diff(stat_info.start_stamp, stat_info.end_stamp);
    return stat_info.total_time;
}
}
