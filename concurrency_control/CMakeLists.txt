# Macro-independent (compiled once)
add_library(cc_common OBJECT
    dl_detect.cpp plock.cpp row_occ.cpp row_ts.cpp row_vll.cpp
)
target_link_libraries(cc_common PRIVATE dbx_includes)

# Per-algorithm CC objects
foreach(alg IN LISTS DBX_ALGS)
    set(_srcs)

    if(alg STREQUAL "TICTOC")
        list(APPEND _srcs tictoc.cpp row_tictoc.cpp)
    elseif(alg STREQUAL "SILO")
        list(APPEND _srcs silo.cpp row_silo.cpp)
    elseif(alg STREQUAL "HEKATON")
        list(APPEND _srcs hekaton.cpp row_hekaton.cpp)
    elseif(alg STREQUAL "MVCC")
        list(APPEND _srcs row_mvcc.cpp)
    elseif(alg STREQUAL "VLL")
        list(APPEND _srcs vll.cpp)
    elseif(alg STREQUAL "OCC")
        list(APPEND _srcs occ.cpp)
    endif()

    if(alg STREQUAL "NO_WAIT" OR alg STREQUAL "WAIT_DIE" OR alg STREQUAL "DL_DETECT")
        list(APPEND _srcs row_lock.cpp)
    endif()

    if(_srcs)
        string(TOLOWER "${alg}" alg_lower)
        add_library(cc_${alg_lower} OBJECT ${_srcs})
        target_link_libraries(cc_${alg_lower} PRIVATE dbx_includes)
        target_compile_definitions(cc_${alg_lower} PRIVATE CC_ALG=${alg})
    endif()
endforeach()
