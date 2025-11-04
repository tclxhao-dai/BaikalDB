//
// Created by user on 25-8-1.
//
//
// Created by user on 25-7-28.
//
#include "support/config.h"
#include "dump.h"
#include "support/progress.h"
#include "support/retry.h"
#include "dump.h"
#include <support/file_system.h>
#include "support/store_client.h"
#include "support/task.h"

backup_tool::Config config;

int main(int argc, char **argv) {
    gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);
  bool log_ok = backup_tool::ensure_dir_exist(FLAGS_log_dir);
  if (!log_ok) {
      fprintf(stderr, "log dir %s create failed.", FLAGS_log_dir.c_str());
      exit(-1);
  }
  config = backup_tool::ParseConfig(&argc,&argv);
  backup_tool::DumpManager dump_manager(config);
  dump_manager.init();
  dump_manager.Run();

}
