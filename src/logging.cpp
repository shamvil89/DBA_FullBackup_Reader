#include "bakread/logging.h"

#include <chrono>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <sstream>

namespace bakread {

static const char* level_str(LogLevel l) {
    switch (l) {
        case LogLevel::Trace: return "TRACE";
        case LogLevel::Debug: return "DEBUG";
        case LogLevel::Info:  return "INFO ";
        case LogLevel::Warn:  return "WARN ";
        case LogLevel::Error: return "ERROR";
        case LogLevel::Fatal: return "FATAL";
    }
    return "?????";
}

Logger& Logger::instance() {
    static Logger s;
    return s;
}

Logger::Logger()  = default;
Logger::~Logger() = default;

void Logger::set_level(LogLevel level)   { level_ = level; }
void Logger::set_verbose(bool v)         { verbose_ = v; if (v) level_ = LogLevel::Debug; }

void Logger::set_log_file(const std::string& path) {
    std::lock_guard<std::mutex> lk(mu_);
    file_.open(path, std::ios::out | std::ios::trunc);
    if (!file_.is_open()) {
        std::cerr << "[bakread] WARNING: cannot open log file: " << path << "\n";
    }
}

void Logger::log(LogLevel level, const char* fmt, ...) {
    va_list args;
    va_start(args, fmt);
    log_impl(level, fmt, args);
    va_end(args);
}

void Logger::trace(const char* fmt, ...) {
    if (level_ > LogLevel::Trace) return;
    va_list a; va_start(a, fmt); log_impl(LogLevel::Trace, fmt, a); va_end(a);
}
void Logger::debug(const char* fmt, ...) {
    if (level_ > LogLevel::Debug) return;
    va_list a; va_start(a, fmt); log_impl(LogLevel::Debug, fmt, a); va_end(a);
}
void Logger::info(const char* fmt, ...) {
    if (level_ > LogLevel::Info) return;
    va_list a; va_start(a, fmt); log_impl(LogLevel::Info, fmt, a); va_end(a);
}
void Logger::warn(const char* fmt, ...) {
    if (level_ > LogLevel::Warn) return;
    va_list a; va_start(a, fmt); log_impl(LogLevel::Warn, fmt, a); va_end(a);
}
void Logger::error(const char* fmt, ...) {
    if (level_ > LogLevel::Error) return;
    va_list a; va_start(a, fmt); log_impl(LogLevel::Error, fmt, a); va_end(a);
}
void Logger::fatal(const char* fmt, ...) {
    va_list a; va_start(a, fmt); log_impl(LogLevel::Fatal, fmt, a); va_end(a);
}

void Logger::log_impl(LogLevel level, const char* fmt, va_list args) {
    if (level < level_) return;

    char buf[4096];
    vsnprintf(buf, sizeof(buf), fmt, args);

    auto now   = std::chrono::system_clock::now();
    auto tt    = std::chrono::system_clock::to_time_t(now);
    auto ms    = std::chrono::duration_cast<std::chrono::milliseconds>(
                     now.time_since_epoch()) % 1000;
    struct tm local_tm;
#ifdef _WIN32
    localtime_s(&local_tm, &tt);
#else
    localtime_r(&tt, &local_tm);
#endif

    char ts[64];
    snprintf(ts, sizeof(ts), "%04d-%02d-%02d %02d:%02d:%02d.%03d",
             local_tm.tm_year + 1900, local_tm.tm_mon + 1, local_tm.tm_mday,
             local_tm.tm_hour, local_tm.tm_min, local_tm.tm_sec,
             static_cast<int>(ms.count()));

    std::lock_guard<std::mutex> lk(mu_);

    auto& out = (level >= LogLevel::Warn) ? std::cerr : std::cout;
    out << "[" << ts << "] [" << level_str(level) << "] " << buf << std::endl;

    if (file_.is_open()) {
        file_ << "[" << ts << "] [" << level_str(level) << "] " << buf << "\n";
        file_.flush();
    }
}

}  // namespace bakread
