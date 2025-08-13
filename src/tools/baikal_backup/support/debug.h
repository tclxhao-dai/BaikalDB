//
// Created by user on 25-8-11.
//

#ifndef DEBUG_H
#define DEBUG_H

#if defined(_DEBUG) || !defined(NDEBUG)
    #define DEBUG_MODE 1
#else
    #define DEBUG_MODE 0
#endif

// 宏封装：整段代码只在 Debug 模式下编译
#if DEBUG_MODE
    #define DEBUG_ONLY(code) do { code } while(0)
#else
    #define DEBUG_ONLY(code) do { } while(0)
#endif

#endif //DEBUG_H
