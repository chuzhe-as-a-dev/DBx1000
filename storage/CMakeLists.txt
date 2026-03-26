# Macro-independent (compiled once)
add_library(storage_common OBJECT
    catalog.cpp index_hash.cpp index_btree.cpp table.cpp
)
target_link_libraries(storage_common PRIVATE dbx_includes)

# Macro-dependent (compiled per algorithm)
foreach(alg IN LISTS DBX_ALGS)
    string(TOLOWER "${alg}" alg_lower)
    add_library(storage_${alg_lower} OBJECT row.cpp)
    target_link_libraries(storage_${alg_lower} PRIVATE dbx_includes)
    target_compile_definitions(storage_${alg_lower} PRIVATE CC_ALG=${alg})
endforeach()
