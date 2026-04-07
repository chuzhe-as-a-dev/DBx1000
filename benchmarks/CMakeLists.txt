# Truly macro-independent (pure math, no CC_ALG/WORKLOAD guards)
add_library(benchmarks_common OBJECT tpcc_helper.cpp)
target_link_libraries(benchmarks_common PRIVATE dbx_includes)

# Per-algorithm-per-workload objects.
# txn/wl/query files include txn.h, whose struct layout depends on CC_ALG
# (e.g. HEKATON adds history_entry, shifting all subsequent fields by 8 bytes).
# Compiling without CC_ALG causes ODR violations and silent memory corruption.
foreach(alg IN LISTS DBX_ALGS)
    string(TOLOWER "${alg}" alg_lower)
    foreach(wl IN LISTS DBX_WORKLOADS)
        string(TOLOWER "${wl}" wl_lower)
        if(wl STREQUAL "YCSB")
            set(_srcs ycsb_query.cpp ycsb_txn.cpp ycsb_wl.cpp)
        elseif(wl STREQUAL "TPCC")
            set(_srcs tpcc_query.cpp tpcc_txn.cpp tpcc_wl.cpp)
        elseif(wl STREQUAL "TEST")
            set(_srcs test_txn.cpp test_wl.cpp)
        else()
            message(FATAL_ERROR "Unknown workload: ${wl}")
        endif()
        add_library(benchmarks_${wl_lower}_${alg_lower} OBJECT ${_srcs})
        target_link_libraries(benchmarks_${wl_lower}_${alg_lower} PRIVATE dbx_includes)
        target_compile_definitions(benchmarks_${wl_lower}_${alg_lower} PRIVATE CC_ALG=${alg} WORKLOAD=${wl})
    endforeach()
endforeach()

# Polyjuice-ordered TPCC variant: uses tpcc_txn_pj.cpp instead of tpcc_txn.cpp
# to match the operation order that Polyjuice's learned policy was trained against.
add_library(benchmarks_tpcc_per_op_pj OBJECT
    tpcc_query.cpp tpcc_txn_pj.cpp tpcc_wl.cpp)
target_link_libraries(benchmarks_tpcc_per_op_pj PRIVATE dbx_includes)
target_compile_definitions(benchmarks_tpcc_per_op_pj PRIVATE
    CC_ALG=PER_OP WORKLOAD=TPCC)
