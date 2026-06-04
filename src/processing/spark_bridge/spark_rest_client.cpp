#include "spark_rest_client.hpp"
#include <stdexcept>
#include <thread>
#include <chrono>

SparkRestClient::SparkRestClient(const std::string& spark_url)
    : spark_url_(spark_url) {

    curl_global_init(CURL_GLOBAL_DEFAULT);

    curl_ = curl_easy_init();
    if (!curl_) {
        throw std::runtime_error("SparkRestClient: failed to initialise libcurl");
    }

    // Follow redirects and timeout after 10 seconds
    curl_easy_setopt(curl_, CURLOPT_FOLLOWLOCATION, 1L);
    curl_easy_setopt(curl_, CURLOPT_TIMEOUT, 10L);
    curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, curl_write_callback);

    spdlog::info("SparkRestClient: connected to Spark master at {}", spark_url);
}

SparkRestClient::~SparkRestClient() {
    if (curl_) {
        curl_easy_cleanup(curl_);
        curl_global_cleanup();
    }
}

SparkRestClient::JobResult SparkRestClient::submit_aggregation_job(
    const std::string& jar_path,
    const std::vector<std::string>& app_args) {

    // Build the Spark REST submission payload
    // Spark's REST API expects a JSON body with the job class, JAR location,
    // and any arguments to pass to the application's main() method.
    nlohmann::json payload = {
        { "action", "CreateSubmissionRequest" },
        { "mainClass", "com.stockanalytics.aggregation.Main" },
        { "appResource", jar_path }, { "appArgs", app_args },
        { "sparkProperties",   {
            { "spark.master", spark_url_ },
            { "spark.app.name", "StockAnalyticsAggregation" },
            { "spark.executor.memory", "1g" },
            { "spark.driver.memory", "512m" },
            { "spark.executor.cores", "2" }
        }},
        { "clientSparkVersion", "3.5.0" }
    };

    spdlog::info("SparkRestClient: submitting aggregation job — JAR: {}", jar_path);

    const std::string url = spark_url_ + "/v1/submissions/create";
    const std::string response = http_post(url, payload);

    if (response.empty()) {
        return { "", "failed", "Empty response from Spark master", false };
    }

    return parse_submission_response(response);
}

SparkRestClient::JobResult SparkRestClient::get_job_status(
    const std::string& submission_id) {

    const std::string url = spark_url_ + "/v1/submissions/status/" + submission_id;
    const std::string response = http_get(url);

    if (response.empty()) {
        return { submission_id, "unknown", "Could not reach Spark master", false };
    }

    return parse_status_response(submission_id, response);
}

SparkRestClient::JobResult SparkRestClient::submit_and_wait(
    const std::string& jar_path,
    const std::vector<std::string>& app_args,
    int poll_interval_ms,
    int timeout_ms) {

    // Submit the job and get the initial result
    JobResult result = submit_aggregation_job(jar_path, app_args);

    if (!result.success) {
        spdlog::error("SparkRestClient: job submission failed — {}", result.message);
        return result;
    }

    spdlog::info("SparkRestClient: job submitted — ID: {}, polling every {}ms",
        result.submission_id, poll_interval_ms);

    // Poll until completion or timeout
    const auto start = std::chrono::steady_clock::now();

    while (true) {
        std::this_thread::sleep_for(std::chrono::milliseconds(poll_interval_ms));

        result = get_job_status(result.submission_id);
        spdlog::info("SparkRestClient: job {} status: {}",
            result.submission_id, result.status);

        // Job finished — either success or failure
        if (result.status == "FINISHED" || result.status == "FAILED" ||
            result.status == "KILLED") {
            break;
        }

        // Check if we have exceeded the timeout
        const auto elapsed = std::chrono::steady_clock::now() - start;
        if (std::chrono::duration_cast<std::chrono::milliseconds>(elapsed).count()
                >= timeout_ms) {
            spdlog::warn("SparkRestClient: job {} timed out after {}ms",
                result.submission_id, timeout_ms);
            result.status  = "timeout";
            result.message = "Job did not complete within the timeout period";
            result.success = false;
            break;
        }
    }

    return result;
}

bool SparkRestClient::is_available() {
    // Ping the Spark master's status endpoint — returns 200 if reachable
    const std::string response = http_get(spark_url_ + "/v1/submissions/status");
    return !response.empty();
}

std::string SparkRestClient::http_post(const std::string& url,
                                        const nlohmann::json& body) {
    std::string response_body;
    const std::string json_str = body.dump();

    // Set up headers for JSON content type
    struct curl_slist* headers = nullptr;
    headers = curl_slist_append(headers, "Content-Type: application/json");

    curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_, CURLOPT_POSTFIELDS, json_str.c_str());
    curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headers);
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response_body);

    const CURLcode res = curl_easy_perform(curl_);
    curl_slist_free_all(headers);

    // Reset to GET for subsequent requests
    curl_easy_setopt(curl_, CURLOPT_HTTPGET, 1L);

    if (res != CURLE_OK) {
        spdlog::error("SparkRestClient: POST failed: {}", curl_easy_strerror(res));
        return {};
    }

    return response_body;
}

std::string SparkRestClient::http_get(const std::string& url) {
    std::string response_body;

    curl_easy_setopt(curl_, CURLOPT_URL, url.c_str());
    curl_easy_setopt(curl_, CURLOPT_HTTPGET, 1L);
    curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response_body);

    const CURLcode res = curl_easy_perform(curl_);
    if (res != CURLE_OK) {
        spdlog::error("SparkRestClient: GET failed: {}", curl_easy_strerror(res));
        return {};
    }

    return response_body;
}

SparkRestClient::JobResult SparkRestClient::parse_submission_response(
    const std::string& response) {

    nlohmann::json json;
    try {
        json = nlohmann::json::parse(response);
    } catch (const nlohmann::json::parse_error& e) {
        spdlog::error("SparkRestClient: failed to parse submission response: {}", e.what());
        return { "", "failed", "JSON parse error", false };
    }

    JobResult result;
    result.submission_id = json.value("submissionId", "");
    result.success       = json.value("success", false);
    result.status        = result.success ? "submitted" : "failed";
    result.message       = json.value("message", "");

    if (result.success) {
        spdlog::info("SparkRestClient: job accepted — submission ID: {}",
            result.submission_id);
    } else {
        spdlog::error("SparkRestClient: job rejected — {}", result.message);
    }

    return result;
}

SparkRestClient::JobResult SparkRestClient::parse_status_response(
    const std::string& submission_id,
    const std::string& response) {

    nlohmann::json json;
    try {
        json = nlohmann::json::parse(response);
    } catch (const nlohmann::json::parse_error& e) {
        spdlog::error("SparkRestClient: failed to parse status response: {}", e.what());
        return { submission_id, "unknown", "JSON parse error", false };
    }

    JobResult result;
    result.submission_id = submission_id;
    result.status        = json.value("driverState", "UNKNOWN");
    result.success       = (result.status == "FINISHED");
    result.message       = json.value("message", "");

    return result;
}

size_t SparkRestClient::curl_write_callback(char* ptr, size_t size,
                                             size_t nmemb, void* userdata) {
    const size_t total_bytes = size * nmemb;
    auto* response = static_cast<std::string*>(userdata);
    response->append(ptr, total_bytes);
    return total_bytes;
}