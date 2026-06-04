#pragma once
#include <nlohmann/json.hpp>
#include <spdlog/spdlog.h>
#include <curl/curl.h>
#include <future>
#include <string>
#include <vector>

// SparkRestClient submits batch aggregation jobs to an Apache Spark cluster
// via its REST submission API (port 6066). This bridges the C++ pipeline
// to Spark's distributed processing capabilities for heavy batch workloads
// that would be too expensive to run inline on the stream processor.

// Typical use cases:
//   - Hourly OHLCV candle aggregation across all tickers
//   - Daily performance summary generation
//   - Cross-ticker correlation matrix computation
//   - Large historical backtest runs

// The REST API submits a JAR file containing the Spark job. The JAR is
// a separate Scala/Java program that reads from Kafka and writes to InfluxDB.
// This client just triggers the submission and monitors the job status.
class SparkRestClient {
public:
    // Job submission result returned by submit_job()
    struct JobResult {
        std::string submission_id;  // Spark's unique ID for this submission
        std::string status;         // "success", "failed", or "running"
        std::string message;        // human-readable status message
        bool        success{ false }; // true if submission was accepted
    };

    // Constructs the client pointed at the given Spark master REST endpoint.
    // spark_url — Spark master REST URL e.g. "http://localhost:6066"
    // Throws std::runtime_error if libcurl cannot be initialised.
    explicit SparkRestClient(const std::string& spark_url);

    // Destructor cleans up the libcurl session.
    ~SparkRestClient();

    // Submits a Spark batch job for OHLCV candle aggregation.
    // jar_path   — path to the compiled Spark job JAR on the cluster
    // app_args   — arguments passed to the Spark application main()
    // Returns a JobResult with the submission ID and initial status.
    JobResult submit_aggregation_job(const std::string& jar_path,
                                     const std::vector<std::string>& app_args);

    // Polls the Spark REST API for the current status of a submitted job.
    // submission_id — ID returned by submit_aggregation_job()
    // Returns the current JobResult with updated status.
    JobResult get_job_status(const std::string& submission_id);

    // Submits a job and blocks until it completes or times out.
    // poll_interval_ms — how often to poll for status updates
    // timeout_ms       — maximum time to wait before returning timeout status
    JobResult submit_and_wait(const std::string& jar_path,
                              const std::vector<std::string>& app_args,
                              int poll_interval_ms = 2000,
                              int timeout_ms = 300000);

    // Returns true if the Spark master is reachable and responding.
    bool is_available();

private:
    // Performs a blocking HTTP POST request with a JSON body.
    // Returns the response body as a string, or empty on failure.
    std::string http_post(const std::string& url, const nlohmann::json& body);

    // Performs a blocking HTTP GET request.
    // Returns the response body as a string, or empty on failure.
    std::string http_get(const std::string& url);

    // Parses a Spark REST API response into a JobResult.
    JobResult parse_submission_response(const std::string& response);

    // Parses a Spark status API response into a JobResult.
    JobResult parse_status_response(const std::string& submission_id,
                                    const std::string& response);

    // libcurl write callback — appends response chunks to a std::string.
    static size_t curl_write_callback(char* ptr, size_t size,
                                      size_t nmemb, void* userdata);

    CURL*       curl_;      // libcurl easy handle
    std::string spark_url_; // Spark master REST base URL
};