#ifndef MAIN_H
#define MAIN_H

#include <cstdint>
#include <queue>
#include <vector>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <chrono>
#include <istream>
#include <streambuf>
#include <stdexcept>
#include <zlib.h>


struct packet_256 {
    static constexpr size_t TOTAL_SIZE = 256;
    static constexpr size_t DATA_SIZE = TOTAL_SIZE - 2;
    char data[DATA_SIZE];  // минус 2 байта под size (< (TOTAL_SIZE - 2)) и compressor_id
    uint8_t size;
    uint8_t compressor_id;
};


template<typename T>
class SafeQueue {
private:
    std::queue<T> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    bool finished_ = false;

public:
    void push(T&& item) {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            queue_.push(std::move(item));
        }
        cv_.notify_one();
    }

    bool pop(T& item) {
        std::unique_lock<std::mutex> lock(mutex_);
        cv_.wait(lock, [&]() { return !queue_.empty() || finished_; });

        if (queue_.empty() && finished_)
            return false;

        item = std::move(queue_.front());
        queue_.pop();
        return true;
    }

    void setFinished() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            finished_ = true;
        }
        cv_.notify_one();
    }
};



class Measure {
    std::chrono::steady_clock::time_point start_time_;

    int original_size_total;
    int compressed_size_total;

public:
    Measure() : original_size_total(0), compressed_size_total(0) {};

    void startMeasure() {
        start_time_ = std::chrono::steady_clock::now();
    }

    std::chrono::milliseconds finishMeasure() const {
        auto end_time = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_);
    }

    void addOriginalSize(int bytesRead) {
        original_size_total += bytesRead;
    }
    void addCompressedlSize(int bytesRead) {
        compressed_size_total += bytesRead;
    }

    double CalculateCompressPercent() const
    {
        return ((static_cast<double>(compressed_size_total) / static_cast<double>(original_size_total)) * 100.0);
    }

};

class MemoryBuffer : public std::streambuf {
public:
    MemoryBuffer(const char* base, std::size_t size) {
        char* p = const_cast<char*>(base);
        this->setg(p, p, p + size);
    }
};


class MemoryInputStream : public std::istream {
public:
    MemoryInputStream(const char* base, std::size_t size)
        : std::istream(&buffer_), buffer_(base, size) {
    }

private:
    MemoryBuffer buffer_;
};

#endif // MAIN_H