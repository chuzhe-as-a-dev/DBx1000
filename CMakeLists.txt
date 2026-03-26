cmake_minimum_required(VERSION 3.16)

project(DBx1000 LANGUAGES CXX)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

# Global compile flags — apply to all targets
add_compile_options(-Wall -Werror)
add_compile_definitions(NOGRAPHITE=1)

option(DBX_TEST_MODE "Use smaller table sizes for testing" OFF)
if(DBX_TEST_MODE)
    add_compile_definitions(MAX_TXN_PER_PART=100 SYNTH_TABLE_SIZE=40960)
endif()

# Platform: librt
if(UNIX AND NOT APPLE)
    find_library(RT_LIBRARY rt)
endif()

# jemalloc: prefer bundled static lib, fall back to system
set(DBX_LOCAL_JEMALLOC "${PROJECT_SOURCE_DIR}/libs/libjemalloc.a")
if(EXISTS "${DBX_LOCAL_JEMALLOC}")
    add_library(dbx_jemalloc STATIC IMPORTED GLOBAL)
    set_target_properties(dbx_jemalloc PROPERTIES IMPORTED_LOCATION "${DBX_LOCAL_JEMALLOC}")
    set(DBX_USE_LOCAL_JEMALLOC TRUE)
else()
    find_library(JEMALLOC_LIBRARY NAMES jemalloc REQUIRED)
    add_library(dbx_jemalloc INTERFACE IMPORTED GLOBAL)
    set_target_properties(dbx_jemalloc PROPERTIES INTERFACE_LINK_LIBRARIES "${JEMALLOC_LIBRARY}")
    set(DBX_USE_LOCAL_JEMALLOC FALSE)
endif()

# Shared include dirs — INTERFACE library so all OBJECT libs can link it
set(DBX_INCLUDE_DIRS
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/benchmarks
    ${PROJECT_SOURCE_DIR}/concurrency_control
    ${PROJECT_SOURCE_DIR}/storage
    ${PROJECT_SOURCE_DIR}/system
)
add_library(dbx_includes INTERFACE)
target_include_directories(dbx_includes INTERFACE ${DBX_INCLUDE_DIRS})

# Build matrix: CC algorithms × workloads
set(DBX_ALGS "DL_DETECT;NO_WAIT;HEKATON;SILO;TICTOC")
set(DBX_WORKLOADS "YCSB;TPCC;TEST")

# Root-level common OBJECT lib
add_library(config_common OBJECT config.cpp)
target_link_libraries(config_common PRIVATE dbx_includes)

# Subdirectories define their own OBJECT libs
add_subdirectory(benchmarks)
add_subdirectory(concurrency_control)
add_subdirectory(storage)
add_subdirectory(system)

# Assemble executables from OBJECT libs
foreach(alg IN LISTS DBX_ALGS)
    string(TOLOWER "${alg}" alg_lower)
    foreach(wl IN LISTS DBX_WORKLOADS)
        string(TOLOWER "${wl}" wl_lower)
        set(target "rundb_${alg_lower}_${wl_lower}")

        # Check if this algorithm has CC-specific objects
        set(cc_objs)
        if(TARGET cc_${alg_lower})
            set(cc_objs $<TARGET_OBJECTS:cc_${alg_lower}>)
        endif()

        add_executable(${target}
            $<TARGET_OBJECTS:config_common>
            $<TARGET_OBJECTS:benchmarks_common>
            $<TARGET_OBJECTS:cc_common>
            $<TARGET_OBJECTS:storage_common>
            $<TARGET_OBJECTS:system_common>
            ${cc_objs}
            $<TARGET_OBJECTS:benchmarks_${wl_lower}_${alg_lower}>
            $<TARGET_OBJECTS:storage_${alg_lower}>
            $<TARGET_OBJECTS:system_${alg_lower}_${wl_lower}>
        )

        target_link_libraries(${target} PRIVATE Threads::Threads dbx_jemalloc)
        if(RT_LIBRARY)
            target_link_libraries(${target} PRIVATE ${RT_LIBRARY})
        endif()
        if(DBX_USE_LOCAL_JEMALLOC AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
            target_link_options(${target} PRIVATE -no-pie)
        endif()
    endforeach()
endforeach()
