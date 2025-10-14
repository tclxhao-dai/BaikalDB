//
// Created by user on 25-8-22.
//

#include <config.h>
#include <load/load.h>
#include <load/parse_record.h>
#include <table_record.h>

int main(int argc, char *argv[]) {
  backup_tool::Config config = backup_tool::ParseConfig(&argc,&argv);
  backup_tool::LoadManager load_manager(config);
  load_manager.init();
  load_manager.run();
}
