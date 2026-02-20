#pragma once

#include <cstdarg>
#include <fstream>
#include <memory>
#include <mutex>
#include <string>

namespace bakread {

enum class LogLevel { Trace, Debug, Info, Warn, Error, Fatal };

class Logger {
public:
    static Logger& instance();

    void set_level(LogLevel level);
    void set_verbose(bool v);
    void set_log_file(const std::string& path);

    void log(LogLevel level, const char* fmt, ...);

    void trace(const char* fmt, ...);
    void debug(const char* fmt, ...);
    void info(const char* fmt, ...);
    void warn(const char* fmt, ...);
    void error(const char* fmt, ...);
    void fatal(const char* fmt, ...);

private:
    Logger();
    ~Logger();
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;

    void log_impl(LogLevel level, const char* fmt, va_list args);

    LogLevel           level_    = LogLevel::Info;
    bool               verbose_  = false;
    std::ofstream      file_;
    mutable std::mutex mu_;
};

// Convenience macros
#define LOG_TRACE(...)  ::bakread::Logger::instance().trace(__VA_ARGS__)
#define LOG_DEBUG(...)  ::bakread::Logger::instance().debug(__VA_ARGS__)
#define LOG_INFO(...)   ::bakread::Logger::instance().info(__VA_ARGS__)
#define LOG_WARN(...)   ::bakread::Logger::instance().warn(__VA_ARGS__)
#define LOG_ERROR(...)  ::bakread::Logger::instance().error(__VA_ARGS__)
#define LOG_FATAL(...)  ::bakread::Logger::instance().fatal(__VA_ARGS__)

}  // namespace bakread
