add_library(farmhash
	farmhash.cc
	farmhash.h)

if (MSVC)
    target_compile_definitions (farmhash PRIVATE FARMHASH_NO_BUILTIN_EXPECT=1)
endif ()

if (MINGW)
    target_compile_definitions (farmhash PRIVATE _MSC_VER=MINGW)
endif (MINGW)

target_include_directories (farmhash PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
