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
//#include <array>
#include <zlib.h>


struct packet_256 {
    static constexpr size_t TOTAL_SIZE = 256;
    static constexpr size_t DATA_SIZE = TOTAL_SIZE - 2;
    char data[DATA_SIZE];  // минус 2 байта под size (< (TOTAL_SIZE - 2)) и compressor_id
    uint8_t size;
    uint8_t compressor_id;
};


struct DecompressorData {
    std::queue<packet_256> packetQueue;
    std::mutex mtx;
    std::condition_variable cv;
};

struct parts_and_size {
    uInt partSize;
    uInt residue;
    uint8_t ThreadsNum;
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
        cv_.notify_all();
    }
};



class Measure {
    std::chrono::steady_clock::time_point start_time_;

    uInt original_size_total = 0;
    uInt compressed_size_total = 0;

public:
//   static constexpr std::array<uInt, 16> CorrectCounters = { 11634, 11635, 11632, 11634, 
                                                       //11632, 11634, 11636, 11633, 
                                                       //11635, 11634, 11637, 11635, 
                                                       //11634, 11635, 11633, 11634 };

//    Measure(uInt threads) {
//        DecompressCounter.resize(threads);
    Measure() = default;

    void startMeasure() {
        start_time_ = std::chrono::steady_clock::now();
    }

    std::chrono::milliseconds finishMeasure() const {
        auto end_time = std::chrono::steady_clock::now();
        return std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time_);
    }

    void addOriginalSize(uInt bytesRead) {
        original_size_total += bytesRead;
    }
    void addCompressedSize(uInt bytesRead) {
        compressed_size_total += bytesRead;
    }

    double CalculateCompressPercent() const
    {
        return ((static_cast<double>(compressed_size_total) / static_cast<double>(original_size_total)) * 100.0);
    }

    //bool containsNZeros(const std::vector<char>& buffer, int n) {
    //    return std::count(buffer.begin(), buffer.end(), 0) > n;
    //}
};

class MemoryInBuffer : public std::streambuf {
public:
    MemoryInBuffer(const char* base, std::size_t size) {
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
    MemoryInBuffer buffer_;
};



class MemoryOutBuffer : public std::streambuf {
public:
    MemoryOutBuffer(char* base, std::size_t size) {
        setp(base, base + size);
    }

    // Получить указатель на начало буфера записи:
    char* data() const { return pbase(); }

    // Получить сколько байт записано:
    std::size_t size() const { return static_cast<size_t>(pptr() - pbase()); }
};



class MemoryOutputStream : public std::ostream {
public:
    MemoryOutputStream(char* base, std::size_t size)
        : std::ostream(&buffer_), buffer_(base, size) {
    }

   char* data() const { return buffer_.data(); }
   std::size_t size() const { return buffer_.size(); }

private:
    MemoryOutBuffer buffer_;
};



#endif // MAIN_H