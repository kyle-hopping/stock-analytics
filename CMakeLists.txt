cmake_minimum_required(VERSION 3.20)
project(StockAnalytics VERSION 1.0.0 LANGUAGES CXX)

# --- C++ Standard -------------------------------------------------------------
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# --- Build type default -------------------------------------------------------
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

message(STATUS "Build type: ${CMAKE_BUILD_TYPE}")
message(STATUS "Compiler:   ${CMAKE_CXX_COMPILER_ID} ${CMAKE_CXX_COMPILER_VERSION}")

# --- Force correct vcpkg toolchain --------------------------------------------
# Prevents CMake from picking up the vcpkg bundled with Visual Studio Build Tools.
# Priority: VCPKG_ROOT env var → C:/vcpkg fallback
if(NOT DEFINED CMAKE_TOOLCHAIN_FILE)
  if(DEFINED ENV{VCPKG_ROOT})
    set(CMAKE_TOOLCHAIN_FILE "$ENV{VCPKG_ROOT}/scripts/buildsystems/vcpkg.cmake"
        CACHE STRING "vcpkg toolchain")
  elseif(EXISTS "C:/vcpkg/scripts/buildsystems/vcpkg.cmake")
    set(CMAKE_TOOLCHAIN_FILE "C:/vcpkg/scripts/buildsystems/vcpkg.cmake"
        CACHE STRING "vcpkg toolchain")
  else()
    message(FATAL_ERROR
      "vcpkg not found. Either:\n"
      "  1. Set the VCPKG_ROOT environment variable to your vcpkg root\n"
      "  2. Install vcpkg to C:/vcpkg\n"
      "  See README.md for setup instructions."
    )
  endif()
  message(STATUS "vcpkg toolchain: ${CMAKE_TOOLCHAIN_FILE}")
endif()

# --- Output directories -------------------------------------------------------
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# --- Find dependencies --------------------------------------------------------
find_package(Threads REQUIRED)

# librdkafka — vcpkg: vcpkg install librdkafka:x64-windows
find_package(RdKafka CONFIG REQUIRED)

# nlohmann JSON (header-only)
find_package(nlohmann_json CONFIG REQUIRED)

# spdlog (async logging)
find_package(spdlog CONFIG REQUIRED)

# CURL (HTTP for Alpha Vantage / Massive / Spark REST)
find_package(CURL REQUIRED)

# Boost (Asio for async I/O)
find_package(Boost 1.80 REQUIRED COMPONENTS system)

# uWebSockets — vcpkg: vcpkg install uwebsockets
find_package(unofficial-uwebsockets CONFIG REQUIRED)

# influxdb-cxx — vcpkg: vcpkg install influxdb-cxx
# Note: target is InfluxData::InfluxDB not InfluxDB::InfluxDB
find_package(InfluxDB CONFIG REQUIRED)

# --- Compiler flags -----------------------------------------------------------
if(MSVC)
  add_compile_options(
    /W4
    /WX-
    /MP
    /permissive-
  )
else()
  add_compile_options(
    -Wall
    -Wextra
    -Wpedantic
    -O2
    -Wno-unused-parameter
  )
endif()

# --- Source files -------------------------------------------------------------
set(INGESTION_SOURCES
  src/ingestion/kafka_producer.cpp
  src/ingestion/kafka_consumer.cpp
  src/ingestion/stock_fetcher.cpp
)

set(PROCESSING_SOURCES
  src/processing/stream_processor.cpp
  src/processing/parallel_pipeline.cpp
  src/processing/thread_pool.cpp
  src/processing/indicators/moving_average.cpp
  src/processing/indicators/rsi_calculator.cpp
  src/processing/indicators/vwap_calculator.cpp
  src/processing/spark_bridge/spark_rest_client.cpp
)

set(STORAGE_SOURCES
  src/storage/influxdb_writer.cpp
  src/storage/timeseries_buffer.cpp
)

set(DASHBOARD_SOURCES
  src/dashboard/websocket_server.cpp
  src/dashboard/query_handler.cpp
)

# --- Main executable ----------------------------------------------------------
add_executable(stock_analytics
  src/main.cpp
  ${INGESTION_SOURCES}
  ${PROCESSING_SOURCES}
  ${STORAGE_SOURCES}
  ${DASHBOARD_SOURCES}
)

# --- Include paths ------------------------------------------------------------
target_include_directories(stock_analytics PRIVATE
  ${CMAKE_SOURCE_DIR}/src
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/config
  ${Boost_INCLUDE_DIRS}
)

# --- Link libraries -----------------------------------------------------------
target_link_libraries(stock_analytics PRIVATE
  Threads::Threads
  RdKafka::rdkafka++
  nlohmann_json::nlohmann_json
  spdlog::spdlog
  CURL::libcurl
  Boost::system
  InfluxData::InfluxDB
  unofficial::uwebsockets::uwebsockets
)

# --- Tests --------------------------------------------------------------------
option(BUILD_TESTS "Build unit tests" ON)

if(BUILD_TESTS)
  enable_testing()
  find_package(GTest CONFIG REQUIRED)

  set(TEST_SOURCES
    tests/test_kafka_consumer.cpp
    tests/test_stream_processor.cpp
    tests/test_parallel_pipeline.cpp
    tests/test_influxdb_writer.cpp
  )

  add_executable(stock_analytics_tests ${TEST_SOURCES})

  target_include_directories(stock_analytics_tests PRIVATE
    ${CMAKE_SOURCE_DIR}/src
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_SOURCE_DIR}/config
  )

  target_link_libraries(stock_analytics_tests PRIVATE
    GTest::gtest
    GTest::gtest_main
    Threads::Threads
    RdKafka::rdkafka++
    nlohmann_json::nlohmann_json
    spdlog::spdlog
    CURL::libcurl
    Boost::system
    InfluxData::InfluxDB
    unofficial::uwebsockets::uwebsockets
  )

  include(GoogleTest)
  gtest_discover_tests(stock_analytics_tests)
endif()

# --- Summary ------------------------------------------------------------------
message(STATUS "------------------------------------------")
message(STATUS " Stock Analytics build configuration")
message(STATUS "  C++ standard : ${CMAKE_CXX_STANDARD}")
message(STATUS "  Build type   : ${CMAKE_BUILD_TYPE}")
message(STATUS "  Tests        : ${BUILD_TESTS}")
message(STATUS "  Output dir   : ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}")
message(STATUS "------------------------------------------")