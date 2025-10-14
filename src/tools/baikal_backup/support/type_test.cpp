#include "type.h"
#include<gtest/gtest.h>

TEST(BasicTests, TestStatusCode) {
    backup_tool::StatusCode code = backup_tool::StatusCode::kOk;
    EXPECT_TRUE(code == backup_tool::StatusCode::kOk);
}