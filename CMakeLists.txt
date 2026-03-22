cmake_minimum_required(VERSION 3.16)

project(DBx1000 LANGUAGES CXX)

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING "Build type" FORCE)
endif()

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_executable(rundb config.cpp)

target_include_directories(rundb PRIVATE
    ${PROJECT_SOURCE_DIR}
    ${PROJECT_SOURCE_DIR}/benchmarks
    ${PROJECT_SOURCE_DIR}/concurrency_control
    ${PROJECT_SOURCE_DIR}/storage
    ${PROJECT_SOURCE_DIR}/system
)

target_compile_definitions(rundb PRIVATE NOGRAPHITE=1)
target_compile_options(rundb PRIVATE -Wall -Werror)
target_link_libraries(rundb PRIVATE Threads::Threads)

add_subdirectory(benchmarks)
add_subdirectory(concurrency_control)
add_subdirectory(storage)
add_subdirectory(system)

if(UNIX AND NOT APPLE)
    find_library(RT_LIBRARY rt)
    if(RT_LIBRARY)
        target_link_libraries(rundb PRIVATE ${RT_LIBRARY})
    endif()
endif()

set(DBX_LOCAL_JEMALLOC "${PROJECT_SOURCE_DIR}/libs/libjemalloc.a")
if(EXISTS "${DBX_LOCAL_JEMALLOC}")
    add_library(jemalloc STATIC IMPORTED GLOBAL)
    set_target_properties(jemalloc PROPERTIES IMPORTED_LOCATION "${DBX_LOCAL_JEMALLOC}")
    target_link_libraries(rundb PRIVATE jemalloc)

    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        target_link_options(rundb PRIVATE -no-pie)
    endif()
else()
    find_library(JEMALLOC_LIBRARY NAMES jemalloc REQUIRED)
    target_link_libraries(rundb PRIVATE ${JEMALLOC_LIBRARY})
endif()
