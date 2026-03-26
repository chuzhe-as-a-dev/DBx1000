# Macro-independent (compiled once)
add_library(system_common OBJECT
    helper.cpp manager.cpp mem_alloc.cpp parser.cpp stats.cpp
)
target_link_libraries(system_common PRIVATE dbx_includes)

# Macro-dependent (compiled per alg×workload)
foreach(alg IN LISTS DBX_ALGS)
    string(TOLOWER "${alg}" alg_lower)
    foreach(wl IN LISTS DBX_WORKLOADS)
        string(TOLOWER "${wl}" wl_lower)
        add_library(system_${alg_lower}_${wl_lower} OBJECT
            global.cpp main.cpp query.cpp thread.cpp txn.cpp wl.cpp
        )
        target_link_libraries(system_${alg_lower}_${wl_lower} PRIVATE dbx_includes)
        target_compile_definitions(system_${alg_lower}_${wl_lower} PRIVATE
            CC_ALG=${alg} WORKLOAD=${wl})
    endforeach()
endforeach()
