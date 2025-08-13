//
// Created by user on 25-8-6.
//

#ifndef TASK_H
#define TASK_H

namespace backup_tool {

class RegionTask  {
public:
  RegionTask(const int64_t& region_id,const int64_t& table_id, const std::string& table_name,
             const std::string& path, const std::string& store_addr, const int64_t log_index)
      : _region_id(region_id), _table_id(table_id), _table_name(table_name),
        _path(path), _store_addr(store_addr), _log_index(log_index) {}

    int64_t _region_id =0 ;
    int64_t _table_id =0 ;
    std::string _table_name;
    std::string _path;          // region dir path
    std::string _store_addr;    // load not need
    int64_t _log_index =0 ;     // load not need

};

} // backup_tool

#endif //TASK_H
