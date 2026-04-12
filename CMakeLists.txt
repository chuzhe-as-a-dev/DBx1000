cmake_minimum_required(VERSION 3.16)

project(DBx1000 LANGUAGES CXX)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_STANDARD 23)
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

# Optional overrides for config.h compile-time constants.
# Propagate as -D<name>=<value> so #ifndef guards in config.h pick them up.
foreach(_var MAX_TXN_PER_PART WARMUP SYNTH_TABLE_SIZE)
    if(DEFINED ${_var})
        add_compile_definitions(${_var}=${${_var}})
    endif()
endforeach()

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
# PER_OP is included for subdirectory object libraries but executables are
# built separately below (one per hook variant: 2PL, OCC, MVCC).
set(DBX_ALGS "DL_DETECT;NO_WAIT;HEKATON;SILO;TICTOC;PER_OP")
set(DBX_WORKLOADS "YCSB;TPCC;TEST")

# PER_OP hook variants — each links a different cc_hooks_*.cpp
set(DBX_PER_OP_VARIANTS "NOOP;NO_WAIT;OCC;MVCC;PJ_TPCC_4WH")

# Root-level common OBJECT lib
add_library(config_common OBJECT config.cpp)
target_link_libraries(config_common PRIVATE dbx_includes)

# Subdirectories define their own OBJECT libs
add_subdirectory(benchmarks)
add_subdirectory(concurrency_control)
add_subdirectory(storage)
add_subdirectory(system)

# Helper macro for linking common libs to an executable
macro(dbx_link_common target)
    target_link_libraries(${target} PRIVATE Threads::Threads dbx_jemalloc)
    if(RT_LIBRARY)
        target_link_libraries(${target} PRIVATE ${RT_LIBRARY})
    endif()
    if(DBX_USE_LOCAL_JEMALLOC AND CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_link_options(${target} PRIVATE -no-pie)
    endif()
endmacro()

# Assemble executables: baseline algorithms (PER_OP handled separately below)
foreach(alg IN LISTS DBX_ALGS)
    if(alg STREQUAL "PER_OP")
        continue()
    endif()
    string(TOLOWER "${alg}" alg_lower)
    foreach(wl IN LISTS DBX_WORKLOADS)
        string(TOLOWER "${wl}" wl_lower)
        set(target "rundb_${alg_lower}_${wl_lower}")

        set(cc_objs)
        if(TARGET cc_${alg_lower})
            set(cc_objs $<TARGET_OBJECTS:cc_${alg_lower}>)
        endif()

        add_executable(${target}
            $<TARGET_OBJECTS:config_common>
            $<TARGET_OBJECTS:benchmarks_common>
            $<TARGET_OBJECTS:storage_common>
            $<TARGET_OBJECTS:system_common>
            ${cc_objs}
            $<TARGET_OBJECTS:benchmarks_${wl_lower}_${alg_lower}>
            $<TARGET_OBJECTS:storage_${alg_lower}>
            $<TARGET_OBJECTS:system_${alg_lower}_${wl_lower}>
        )
        dbx_link_common(${target})

        # Also build a PJ-ordered TPCC variant for baseline comparison
        # (apples-to-apples against per_op_pj_tpcc_4wh).
        if(wl STREQUAL "TPCC")
            set(target_pj "rundb_${alg_lower}_tpcc_pj")
            add_executable(${target_pj}
                $<TARGET_OBJECTS:config_common>
                $<TARGET_OBJECTS:benchmarks_common>
                $<TARGET_OBJECTS:storage_common>
                $<TARGET_OBJECTS:system_common>
                ${cc_objs}
                $<TARGET_OBJECTS:benchmarks_tpcc_${alg_lower}_pj>
                $<TARGET_OBJECTS:storage_${alg_lower}>
                $<TARGET_OBJECTS:system_${alg_lower}_${wl_lower}>
            )
            dbx_link_common(${target_pj})
        endif()
    endforeach()
endforeach()

# Assemble executables: PER_OP variants (skip TEST workload)
foreach(variant IN LISTS DBX_PER_OP_VARIANTS)
    string(TOLOWER "${variant}" var_lower)
    foreach(wl IN LISTS DBX_WORKLOADS)
        if(wl STREQUAL "TEST")
            continue()
        endif()
        string(TOLOWER "${wl}" wl_lower)

        # PJ variants use a Polyjuice-ordered TPCC implementation; name the
        # binary with a _pj suffix so bench.py can treat it as a separate
        # "workload" alongside the standard tpcc binaries.
        if(var_lower MATCHES "^pj_" AND wl STREQUAL "TPCC")
            set(target "rundb_per_op_${var_lower}_${wl_lower}_pj")
            set(_bench_objs $<TARGET_OBJECTS:benchmarks_tpcc_per_op_pj>)
        else()
            set(target "rundb_per_op_${var_lower}_${wl_lower}")
            set(_bench_objs $<TARGET_OBJECTS:benchmarks_${wl_lower}_per_op>)
        endif()

        add_executable(${target}
            $<TARGET_OBJECTS:config_common>
            $<TARGET_OBJECTS:benchmarks_common>
            $<TARGET_OBJECTS:storage_common>
            $<TARGET_OBJECTS:system_common>
            $<TARGET_OBJECTS:cc_per_op_${var_lower}>
            ${_bench_objs}
            $<TARGET_OBJECTS:storage_per_op>
            $<TARGET_OBJECTS:system_per_op_${wl_lower}>
        )
        dbx_link_common(${target})
    endforeach()
endforeach()
