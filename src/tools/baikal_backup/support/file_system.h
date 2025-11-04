//
// Created by user on 25-8-5.
//

#ifndef FILE_SYSTEM_H
#define FILE_SYSTEM_H
#include <filesystem>

namespace backup_tool {

namespace  fs = std::filesystem;

class RegionFile {
public:
    RegionFile(int64_t region_id,int64_t table_id, const std::string& table_name,std::string key,
               std::string path, int64_t& log_index)
        : _region_id(region_id), _table_id(table_id), _table_name(table_name),
          _key(std::move(key)), _path(std::move(path)), _log_index(log_index) {
        if (log_index ==0) {
            read_log_index(log_index);
            _log_index = log_index;
        }
    }
    auto get_log_index() const ->int64_t    { return _log_index; }
    auto get_region_id() const ->int64_t    { return _region_id; }
    auto set_region_id(int64_t region_id) -> void { _region_id = region_id; }

    // TODO add load return feature
    auto read_log_index(int64_t& log_index) const ->Status {
      const fs::path dir = fs::path(_path);
        if (!exists(dir)) {
            return Status::OK();
        }
        fs::path log_index_path = dir / "log_index";
        if (!exists(log_index_path)) {
            log_index = 0;
            DB_WARNING("log index file %s not exists,set log_index = 0 region_id: %ld", log_index_path.c_str(), _region_id);
            return StatusCode::kOk;
        }
        std::ifstream ifs(log_index_path, std::ios::in | std::ios::binary);
        if (!ifs.is_open()) {
            DB_WARNING("open log index file %s failed", log_index_path.c_str());
            return StatusCode::kFileError;
        }
        ifs >> log_index;
        ifs.close();
        return StatusCode::kOk;
    }
    auto write(butil::IOBuf&& buf) const -> Status{
        // create directory if not exists
        fs::path dir = fs::path(_path);
        if (!fs::exists(_path)) {
            std::error_code err;
            fs::create_directories(_path,err);
            if (err) {
                DB_WARNING("create directory %s failed, error: %s", _path.c_str(), err.message().c_str());
                return StatusCode::kFileError;
            }
        }
        // write log index
        fs::path log_index_path = dir / "log_index";
        std::ofstream ofs(log_index_path, std::ios::out | std::ios::binary);
        int64_t log_index=0;
        if (buf.cutn(&log_index, sizeof(int64_t)) != sizeof(int64_t)) {
            DB_WARNING("region_%ld don't have enough data contains log index", _region_id);
            return StatusCode::kNotEnoughData;
        }
        DB_WARNING("region_%ld write log index %ld,buf size = %d", _region_id, log_index,buf.size());
        ofs << log_index;
        ofs.close();
        // write meta data
        int8_t file_num;
        buf.cutn(&file_num,sizeof(int8_t));
        size_t meta_len =0 ;
        if (buf.cutn(&meta_len, sizeof(int64_t)) != sizeof(int64_t) || meta_len <= 0) {
            DB_WARNING("region_%ld don't have enough data contains meta data size", _region_id);
            return StatusCode::kNotEnoughData;
        }
        fs::path meta_path = dir / "meta.sst";
        std::ofstream meta_ofs(meta_path, std::ios::out | std::ios::binary);
        butil::IOBuf meta_buf;
        if (buf.cutn(&meta_buf, meta_len) != meta_len) {
            DB_WARNING("region_%ld don't have enough data contains meta data", _region_id);
            return StatusCode::kNotEnoughData;
        }
        meta_ofs << meta_buf;
        meta_ofs.close();
        if (file_num == 2) {
            size_t data_len =0 ;
            if (buf.cutn(&data_len, sizeof(int64_t)) != sizeof(int64_t) || data_len <= 0) {
                DB_WARNING("region_%ld don't have enough data contains data size", _region_id);
                return StatusCode::kNotEnoughData;
            }
            fs::path data_path = dir / "data.sst";
            std::ofstream data_ofs(data_path, std::ios::out | std::ios::binary);
            butil::IOBuf data_buf;
            if (buf.cutn(&data_buf, data_len) != data_len) {
                DB_WARNING("region_%ld don't have enough data contains data", _region_id);
                return StatusCode::kNotEnoughData;
            }
            data_ofs << data_buf;
            data_ofs.close();
        }
        return StatusCode::kOk;
    }
private:
    int64_t _region_id=0;
    int64_t _table_id=0;
    std::string _table_name;
    std::string _key;
    std::string _path;
    std::int64_t _log_index=0;

};

auto ensure_dir_exist(std::string dir)-> bool {
    if (dir.empty()) {
        return true;
    }
    std::error_code ec;
    if (!fs::exists(dir,ec)) {
        if (!fs::create_directories(dir,ec)) {
            std::cout << "create directory " << dir << " failed, error: " << ec.message() << std::endl;
            return false;
        }
    }else if (!fs::is_directory(dir,ec)){
        std::cout<< "path " << dir << " is not a directory" << std::endl;
        return false;
    }
    return true;
}

}

#endif //FILE_SYSTEM_H
