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

#pragma once

#include "query_context.h"
namespace baikaldb {
class FieldFilterBlacklist {
public:
    /* 
     * 计算自增id
     */
    int analyze(QueryContext* ctx) {
        ExecNode* root = ctx->root;
        std::vector<ExecNode*> scan_nodes;
        auto stat_info = &(ctx->stat_info);
        root->get_node(pb::SCAN_NODE, scan_nodes);
        if (scan_nodes.size() == 0) {
            return 0;
        }
        for (auto& scan_node_ptr : scan_nodes) {
            if (static_cast<ScanNode*>(scan_node_ptr)->engine() == pb::INFORMATION_SCHEMA) {
                continue;
            }
            int64_t table_id = static_cast<ScanNode*>(scan_node_ptr)->table_id();
            SchemaFactory* _factory = SchemaFactory::get_instance();
            auto table_info = _factory->get_table_info_ptr(table_id);
            if (table_info == nullptr) {
                continue;
            }
            if (table_info->filter_blacklist.empty()) {
                continue;
            }
            ExecNode* parent_node_ptr = scan_node_ptr->get_parent();
            if (parent_node_ptr == NULL) {
                continue;
            }

            FilterNode* filter_node = nullptr;
            if (parent_node_ptr->node_type() == pb::WHERE_FILTER_NODE
                    || parent_node_ptr->node_type() == pb::TABLE_FILTER_NODE) {
                filter_node = static_cast<FilterNode*>(parent_node_ptr);
            }
            if (filter_node == nullptr) {
                continue;
            }
            std::vector<ExprNode*>* conjuncts = filter_node->mutable_conjuncts();
            if (conjuncts == nullptr) {
                continue;
            }
            for (auto expr : *conjuncts) {
                std::string hit_result;
                if (expr->hit_filter_blacklist(table_info->filter_blacklist, hit_result)) {
                    ctx->stat_info.error_code = ER_SQL_REFUSE;
                    ctx->stat_info.error_msg << "filter  " << hit_result << " in filter blacklist";
                    DB_WARNING("sql in filter blacklist! %s logid [%llu]", hit_result.c_str(), stat_info->log_id);
                    return -1;
                }
            }
        }
    };
};
}

/* vim: set ts=4 sw=4 sts=4 tw=100 */
