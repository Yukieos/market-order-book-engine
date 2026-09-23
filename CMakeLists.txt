cmake_minimum_required(VERSION 3.20)
project(market_order_book_engine VERSION 0.1.0 LANGUAGES CXX)

option(MARKET_ENABLE_WEBSOCKET "Build the optional Boost.Beast TLS WebSocket connector" OFF)
option(MARKET_ENABLE_IO_URING "Build the optional Linux io_uring UDP datagram source (needs liburing)" OFF)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

find_package(Threads REQUIRED)

add_library(market_engine
    src/Affinity.cpp
    src/CSVReplayConnector.cpp
    src/ItchFileConnector.cpp
    src/MoldUdp64Connector.cpp
    src/MultiSymbolBook.cpp
    src/OrderBook.cpp
    src/SoupBinTcpConnector.cpp
)
target_include_directories(market_engine PUBLIC include)
target_link_libraries(market_engine PUBLIC Threads::Threads)
target_compile_options(market_engine PRIVATE
    $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall -Wextra -Wpedantic -Wconversion>
)

if(MARKET_ENABLE_WEBSOCKET)
    find_package(Boost 1.74 REQUIRED)
    find_package(OpenSSL REQUIRED)
    add_library(market_websocket src/WebSocketConnector.cpp)
    target_include_directories(market_websocket PUBLIC include)
    target_include_directories(market_websocket SYSTEM PRIVATE ${Boost_INCLUDE_DIRS})
    target_compile_definitions(market_websocket PRIVATE BOOST_ERROR_CODE_HEADER_ONLY)
    target_link_libraries(market_websocket
        PUBLIC market_engine
        PRIVATE OpenSSL::SSL OpenSSL::Crypto Threads::Threads
    )
    target_compile_options(market_websocket PRIVATE
        $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall -Wextra -Wpedantic -Wconversion>
    )
endif()

if(MARKET_ENABLE_IO_URING)
    find_path(LIBURING_INCLUDE_DIR liburing.h REQUIRED)
    find_library(LIBURING_LIBRARY uring REQUIRED)
    add_library(market_io_uring src/IoUringDatagramSource.cpp)
    target_include_directories(market_io_uring PUBLIC include)
    target_include_directories(market_io_uring SYSTEM PRIVATE ${LIBURING_INCLUDE_DIR})
    target_link_libraries(market_io_uring
        PUBLIC market_engine
        PRIVATE ${LIBURING_LIBRARY} Threads::Threads
    )
    target_compile_definitions(market_io_uring PUBLIC MARKET_HAVE_IO_URING=1)
    target_compile_options(market_io_uring PRIVATE
        $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall -Wextra -Wpedantic -Wconversion>
    )

    add_executable(io_uring_pipeline_bench benchmarks/IoUringPipelineBench.cpp)
    target_link_libraries(io_uring_pipeline_bench PRIVATE market_io_uring Threads::Threads)
    target_compile_options(io_uring_pipeline_bench PRIVATE
        $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall -Wextra -Wpedantic -Wconversion>
    )
endif()

add_executable(market_replay src/Main.cpp)
target_link_libraries(market_replay PRIVATE market_engine Threads::Threads)

add_executable(itch_replay src/ItchReplay.cpp)
target_link_libraries(itch_replay PRIVATE market_engine Threads::Threads)
target_compile_options(itch_replay PRIVATE
    $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall -Wextra -Wpedantic -Wconversion>
)

add_executable(itch_features src/ItchFeatures.cpp)
target_link_libraries(itch_features PRIVATE market_engine Threads::Threads)
target_compile_options(itch_features PRIVATE
    $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall -Wextra -Wpedantic -Wconversion>
)

include(CTest)
if(BUILD_TESTING)
    add_executable(market_tests tests/MarketEngineTests.cpp)
    target_link_libraries(market_tests PRIVATE market_engine Threads::Threads)
    target_compile_definitions(market_tests PRIVATE MARKET_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/data")
    add_test(NAME market_tests COMMAND market_tests)

    if(MARKET_ENABLE_IO_URING)
        add_executable(io_uring_smoke tests/IoUringSmoke.cpp)
        target_link_libraries(io_uring_smoke PRIVATE market_io_uring Threads::Threads)
        target_compile_options(io_uring_smoke PRIVATE
            $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall -Wextra -Wpedantic -Wconversion>
        )
        add_test(NAME io_uring_smoke COMMAND io_uring_smoke)
    endif()
endif()

add_executable(market_benchmark benchmarks/Benchmark.cpp)
target_link_libraries(market_benchmark PRIVATE market_engine Threads::Threads)
target_compile_definitions(market_benchmark PRIVATE
    MARKET_BENCHMARK_FLAGS="${CMAKE_CXX_FLAGS_RELEASE}"
)
target_compile_options(market_benchmark PRIVATE
    $<$<CXX_COMPILER_ID:AppleClang,Clang,GNU>:-Wall -Wextra -Wpedantic -Wconversion>
)
