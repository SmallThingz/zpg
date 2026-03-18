#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
#include <iostream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <libpq-fe.h>

namespace {

enum class Mode {
    latency,
    throughput,
};

struct Options {
    Mode mode = Mode::latency;
    std::string url;
    std::string sql = "select 1";
    std::size_t iterations = 10000;
    std::size_t warmup = 0;
    std::size_t requests = 0;
    std::size_t connections = 1;
    std::size_t pipeline_depth = 1;
};

struct Summary {
    std::string driver = "libpq-pipeline-shim";
    std::string variant = "pipeline";
    Mode mode = Mode::latency;
    std::size_t requests = 0;
    std::size_t warmup = 0;
    std::size_t workers = 0;
    std::size_t pool = 0;
    std::size_t connections = 0;
    std::size_t pipeline_depth = 1;
    std::uint64_t wall_ns = 0;
    double qps = 0.0;
    std::uint64_t min_ns = 0;
    std::uint64_t avg_ns = 0;
    std::uint64_t p50_ns = 0;
    std::uint64_t p95_ns = 0;
    std::uint64_t p99_ns = 0;
    std::uint64_t max_ns = 0;
};

struct WorkerResult {
    std::vector<std::uint64_t> samples;
};

std::string mode_name(Mode mode) {
    return mode == Mode::latency ? "latency" : "throughput";
}

std::size_t parse_size(const char* text) {
    return static_cast<std::size_t>(std::stoull(text));
}

void require_ok(bool ok, const std::string& message) {
    if (!ok) {
        throw std::runtime_error(message);
    }
}

PGconn* connect_or_throw(const std::string& url) {
    PGconn* conn = PQconnectdb(url.c_str());
    if (conn == nullptr) {
        throw std::runtime_error("PQconnectdb returned null");
    }
    if (PQstatus(conn) != CONNECTION_OK) {
        std::string message = PQerrorMessage(conn);
        PQfinish(conn);
        throw std::runtime_error("connection failed: " + message);
    }
    return conn;
}

void drain_batch(PGconn* conn, std::size_t batch_size, std::vector<std::uint64_t>* samples, std::size_t sample_offset, const std::vector<std::chrono::steady_clock::time_point>& send_times) {
    std::size_t completed = 0;
    while (true) {
        require_ok(PQconsumeInput(conn) == 1, "PQconsumeInput failed: " + std::string(PQerrorMessage(conn)));
        if (PQisBusy(conn) != 0) {
            std::this_thread::yield();
            continue;
        }

        PGresult* result = PQgetResult(conn);
        if (result == nullptr) {
            std::this_thread::yield();
            continue;
        }

        const ExecStatusType status = PQresultStatus(result);
        switch (status) {
            case PGRES_TUPLES_OK:
            case PGRES_COMMAND_OK:
            case PGRES_EMPTY_QUERY:
                if (completed >= batch_size) {
                    PQclear(result);
                    throw std::runtime_error("received too many command results");
                }
                if (samples != nullptr) {
                    const auto now = std::chrono::steady_clock::now();
                    (*samples)[sample_offset + completed] = static_cast<std::uint64_t>(
                        std::chrono::duration_cast<std::chrono::nanoseconds>(now - send_times[completed]).count()
                    );
                }
                ++completed;
                break;
            case PGRES_PIPELINE_SYNC:
                if (completed != batch_size) {
                    PQclear(result);
                    throw std::runtime_error("pipeline sync arrived before all command results");
                }
                PQclear(result);
                return;
                break;
            default: {
                std::string message = PQresultErrorMessage(result);
                PQclear(result);
                throw std::runtime_error("unexpected pipeline result: " + message);
            }
        }
        PQclear(result);
    }

    if (completed != batch_size) {
        throw std::runtime_error("pipeline batch did not return the expected number of results");
    }
}

void flush_or_throw(PGconn* conn) {
    while (true) {
        const int flush = PQflush(conn);
        if (flush == 0) {
            return;
        }
        if (flush == -1) {
            throw std::runtime_error("PQflush failed: " + std::string(PQerrorMessage(conn)));
        }
    }
}

void run_batches(PGconn* conn, const std::string& sql, std::size_t request_count, std::size_t pipeline_depth, std::vector<std::uint64_t>* samples) {
    std::size_t sent = 0;
    while (sent < request_count) {
        const std::size_t batch_size = std::min(pipeline_depth, request_count - sent);
        std::vector<std::chrono::steady_clock::time_point> send_times;
        if (samples != nullptr) {
            send_times.reserve(batch_size);
        }

        for (std::size_t i = 0; i < batch_size; ++i) {
            if (samples != nullptr) {
                send_times.push_back(std::chrono::steady_clock::now());
            }
            const int ok = PQsendQueryParams(conn, sql.c_str(), 0, nullptr, nullptr, nullptr, nullptr, 0);
            require_ok(ok == 1, "PQsendQueryParams failed: " + std::string(PQerrorMessage(conn)));
        }

        require_ok(PQpipelineSync(conn) == 1, "PQpipelineSync failed: " + std::string(PQerrorMessage(conn)));
        flush_or_throw(conn);
        drain_batch(conn, batch_size, samples, sent, send_times);
        sent += batch_size;
    }
}

WorkerResult run_worker(const Options& options, std::size_t measured_requests, std::atomic<std::size_t>* ready, std::atomic<bool>* start_flag) {
    PGconn* conn = connect_or_throw(options.url);
    try {
        require_ok(PQenterPipelineMode(conn) == 1, "PQenterPipelineMode failed: " + std::string(PQerrorMessage(conn)));

        run_batches(conn, options.sql, options.warmup, options.pipeline_depth, nullptr);

        if (ready != nullptr && start_flag != nullptr) {
            ready->fetch_add(1, std::memory_order_acq_rel);
            while (!start_flag->load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
        }

        WorkerResult result;
        result.samples.resize(measured_requests);
        run_batches(conn, options.sql, measured_requests, options.pipeline_depth, &result.samples);

        require_ok(PQexitPipelineMode(conn) == 1, "PQexitPipelineMode failed: " + std::string(PQerrorMessage(conn)));
        PQfinish(conn);
        return result;
    } catch (...) {
        PQfinish(conn);
        throw;
    }
}

std::uint64_t percentile(const std::vector<std::uint64_t>& sorted, std::size_t numerator, std::size_t denominator) {
    if (sorted.empty()) {
        return 0;
    }
    const std::size_t index = ((sorted.size() - 1) * numerator) / denominator;
    return sorted[index];
}

Summary summarize(std::vector<std::uint64_t> samples, std::uint64_t wall_ns, const Options& options, std::size_t workers) {
    Summary summary;
    summary.mode = options.mode;
    summary.requests = samples.size();
    summary.warmup = options.warmup;
    summary.workers = workers;
    summary.connections = options.connections;
    summary.pipeline_depth = options.pipeline_depth;
    summary.wall_ns = wall_ns;
    summary.qps = wall_ns == 0 ? 0.0 : (static_cast<double>(samples.size()) * 1'000'000'000.0) / static_cast<double>(wall_ns);

    if (samples.empty()) {
        return summary;
    }

    std::sort(samples.begin(), samples.end());
    unsigned long long total_ns = 0;
    for (std::uint64_t sample : samples) {
        total_ns += sample;
    }

    summary.min_ns = samples.front();
    summary.avg_ns = static_cast<std::uint64_t>(total_ns / samples.size());
    summary.p50_ns = percentile(samples, 50, 100);
    summary.p95_ns = percentile(samples, 95, 100);
    summary.p99_ns = percentile(samples, 99, 100);
    summary.max_ns = samples.back();
    return summary;
}

void print_summary(const Summary& summary) {
    std::cout
        << "bench driver=" << summary.driver
        << " mode=" << mode_name(summary.mode)
        << " variant=" << summary.variant
        << " requests=" << summary.requests
        << " warmup=" << summary.warmup
        << " workers=" << summary.workers
        << " pool=" << summary.pool
        << " connections=" << summary.connections
        << " pipeline_depth=" << summary.pipeline_depth
        << " wall_ms=" << (summary.wall_ns / 1'000'000ULL)
        << " qps=" << summary.qps
        << " min_us=" << (summary.min_ns / 1'000ULL)
        << " avg_us=" << (summary.avg_ns / 1'000ULL)
        << " p50_us=" << (summary.p50_ns / 1'000ULL)
        << " p95_us=" << (summary.p95_ns / 1'000ULL)
        << " p99_us=" << (summary.p99_ns / 1'000ULL)
        << " max_us=" << (summary.max_ns / 1'000ULL)
        << '\n';
}

Options parse_args(int argc, char** argv) {
    Options options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "--mode") {
            if (++i >= argc) {
                throw std::runtime_error("missing value for --mode");
            }
            const std::string value = argv[i];
            if (value == "latency") {
                options.mode = Mode::latency;
            } else if (value == "throughput") {
                options.mode = Mode::throughput;
            } else {
                throw std::runtime_error("invalid --mode value");
            }
        } else if (arg == "--url") {
            if (++i >= argc) {
                throw std::runtime_error("missing value for --url");
            }
            options.url = argv[i];
        } else if (arg == "--sql") {
            if (++i >= argc) {
                throw std::runtime_error("missing value for --sql");
            }
            options.sql = argv[i];
        } else if (arg == "--iterations") {
            if (++i >= argc) {
                throw std::runtime_error("missing value for --iterations");
            }
            options.iterations = parse_size(argv[i]);
        } else if (arg == "--warmup") {
            if (++i >= argc) {
                throw std::runtime_error("missing value for --warmup");
            }
            options.warmup = parse_size(argv[i]);
        } else if (arg == "--requests") {
            if (++i >= argc) {
                throw std::runtime_error("missing value for --requests");
            }
            options.requests = parse_size(argv[i]);
        } else if (arg == "--connections") {
            if (++i >= argc) {
                throw std::runtime_error("missing value for --connections");
            }
            options.connections = std::max<std::size_t>(parse_size(argv[i]), 1);
        } else if (arg == "--pipeline-depth") {
            if (++i >= argc) {
                throw std::runtime_error("missing value for --pipeline-depth");
            }
            options.pipeline_depth = std::max<std::size_t>(parse_size(argv[i]), 1);
        } else {
            throw std::runtime_error("unknown argument: " + arg);
        }
    }

    if (options.url.empty()) {
        throw std::runtime_error("--url is required");
    }
    return options;
}

Summary run_latency(const Options& options) {
    Options worker_options = options;
    worker_options.connections = 1;
    worker_options.pipeline_depth = 1;

    const auto started = std::chrono::steady_clock::now();
    WorkerResult result = run_worker(worker_options, worker_options.iterations, nullptr, nullptr);
    const auto wall_ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started).count()
    );

    std::vector<std::uint64_t> samples = std::move(result.samples);
    worker_options.requests = samples.size();
    return summarize(std::move(samples), wall_ns, worker_options, 1);
}

Summary run_throughput(const Options& options) {
    const std::size_t connections = std::max<std::size_t>(options.connections, 1);
    std::vector<WorkerResult> results(connections);
    std::vector<std::thread> threads;
    threads.reserve(connections);

    std::atomic<std::size_t> ready{0};
    std::atomic<bool> start_flag{false};

    for (std::size_t i = 0; i < connections; ++i) {
        const std::size_t base = options.requests / connections;
        const std::size_t extra = i < (options.requests % connections) ? 1 : 0;
        const std::size_t worker_requests = base + extra;

        threads.emplace_back([&, i, worker_requests]() {
            results[i] = run_worker(options, worker_requests, &ready, &start_flag);
        });
    }

    while (ready.load(std::memory_order_acquire) != connections) {
        std::this_thread::yield();
    }

    const auto started = std::chrono::steady_clock::now();
    start_flag.store(true, std::memory_order_release);

    for (auto& thread : threads) {
        thread.join();
    }

    std::vector<std::uint64_t> samples;
    samples.reserve(options.requests);
    for (auto& result : results) {
        samples.insert(samples.end(), result.samples.begin(), result.samples.end());
    }

    const auto wall_ns = static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(std::chrono::steady_clock::now() - started).count()
    );

    return summarize(std::move(samples), wall_ns, options, connections);
}

} // namespace

int main(int argc, char** argv) {
    try {
        const Options options = parse_args(argc, argv);
        const Summary summary = options.mode == Mode::latency ? run_latency(options) : run_throughput(options);
        print_summary(summary);
        return 0;
    } catch (const std::exception& ex) {
        std::cerr << ex.what() << '\n';
        return 1;
    }
}
