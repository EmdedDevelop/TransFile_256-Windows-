#include <algorithm>
#include <iostream>
#include <fstream>
#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <zlib.h>
#include <iomanip>
#include "main.h"


using namespace std;

constexpr uInt CHUNK = packet_256::TOTAL_SIZE - 2;

uInt packet_index = 0;


// --- Модифицированный compressor ---
void produce(const string& inputFile, SafeQueue<packet_256>& queue, Measure& Measurement, bool compression) {
    vector<char> buffer;
    ifstream in(inputFile, ios::binary | std::ios::ate);
    if (!in) {
        cerr << "Failed to open input file!" << endl;
        queue.setFinished();
        return;
    }

    streamsize size = in.tellg();  
    if (size <= 0) {
        std::cerr << "Файл пустой или ошибка определения размера: " << inputFile << "\n";
        return;
    }
    in.seekg(0, std::ios::beg);          

    buffer.resize(size);                   
    if (!in.read(buffer.data(), size)) { 
        std::cerr << "Ошибка чтения файла: " << inputFile << "\n";
        return;
    }

    MemoryInputStream memStream(buffer.data(), buffer.size());

    Measurement.startMeasure();

    if (!compression) {

    while (true) {
        vector<char> chunk(CHUNK);

        memStream.read(chunk.data(), CHUNK);
        streamsize bytesRead = memStream.gcount();

        if (bytesRead <= 0)
            break;
       
        chunk.resize(bytesRead);

        packet_256 pkt;
        uint8_t copy_size = static_cast<uint8_t>(min(chunk.size(), size_t(CHUNK)));
        copy(chunk.begin(), chunk.begin() + copy_size, pkt.data);
        pkt.size = copy_size;  
        pkt.index_of_packet = packet_index;
        queue.push(move(pkt));
    }
        queue.setFinished();
        return;
    }

    vector<char> inBuf(CHUNK);
    vector<char> outBuf(CHUNK);

    z_stream def_strm{};
    if (deflateInit(&def_strm, 4) != Z_OK) {
        cerr << "deflateInit failed!" << endl;
        queue.setFinished();
        return;
    }

    int flush;
    do {
        memStream.read(inBuf.data(), CHUNK);
        def_strm.next_in = reinterpret_cast<Bytef*>(inBuf.data());
        def_strm.avail_in = static_cast<uInt>(memStream.gcount());
        Measurement.addOriginalSize(def_strm.avail_in);

        flush = memStream.eof() ? Z_FINISH : Z_NO_FLUSH;

        do {
            def_strm.next_out = reinterpret_cast<Bytef*>(outBuf.data());
            def_strm.avail_out = CHUNK;

            int ret = deflate(&def_strm, flush);
            if (ret == Z_STREAM_ERROR) {
                cerr << "deflate error" << endl;
                deflateEnd(&def_strm);
                queue.setFinished();
                return;
            }

            uInt have_out = CHUNK - def_strm.avail_out;
            if (have_out > 0) {
                Measurement.addCompressedlSize(have_out);
                vector<char> compressedChunk(outBuf.begin(), outBuf.begin() + have_out);

                packet_256 pkt;
                uint8_t copy_size = static_cast<uint8_t>(min(compressedChunk.size(), size_t(CHUNK)));
                copy(compressedChunk.begin(), compressedChunk.begin() + copy_size, pkt.data);
                pkt.size = copy_size;
                pkt.index_of_packet = packet_index;
                queue.push(move(pkt));
            }
        } while (def_strm.avail_out == 0);

    } while (flush != Z_FINISH);

    deflateEnd(&def_strm);
    queue.setFinished();
}


// --- Модифицированный decompressor ---
void consume(const string& outputFile, SafeQueue<packet_256>& queue, Measure& Measurement, bool compression) {
    ofstream outFile(outputFile, ios::binary);
    if (!outFile) {
        cerr << "Failed to open output file!" << endl;
        return;
    }

    if (!compression) {
        // Просто читаем блоки из очереди и пишем в файл 
        packet_256 chunk;
        while (queue.pop(chunk)) {
            outFile.write(chunk.data, chunk.size);
            if (!outFile) {
                std::cerr << "Error writing output file!" << std::endl;
                return;
            }
        }

        cout << "Полное время от начала до конца передачи (несжатых данных): " << Measurement.finishMeasure() << "\n";
        return;
    }

    z_stream strm{};
    if (inflateInit(&strm) != Z_OK) {
        cerr << "inflateInit failed!" << endl;
        return;
    }
        
    packet_256 compressedChunk;
    vector<char> outBuf(CHUNK);

    while (queue.pop(compressedChunk)) {
        strm.next_in = reinterpret_cast<Bytef*>(compressedChunk.data);
        strm.avail_in = static_cast<uInt>(compressedChunk.size);

        do {
            strm.next_out = reinterpret_cast<Bytef*>(outBuf.data());
            strm.avail_out = CHUNK;

            int ret = inflate(&strm, Z_NO_FLUSH);
            if (ret == Z_STREAM_ERROR || ret == Z_DATA_ERROR || ret == Z_MEM_ERROR) {
                cerr << "inflate error: " << ret << endl;
                inflateEnd(&strm);
                return;
            }

            size_t have_out = CHUNK - strm.avail_out;
            if (have_out > 0) {
                outFile.write(outBuf.data(), have_out);
                if (!outFile) {
                    cerr << "Error writing decompressed data!" << endl;
                    inflateEnd(&strm);
                    return;
                }
            }

            if (ret == Z_STREAM_END) {
                cout << "Полное время от начала сжатия до завершения разжатия: " << Measurement.finishMeasure() << " \n";
                break;
            }
        } while (strm.avail_out == 0);
    }

    inflateEnd(&strm);
}




int main(int argc, char* argv[]) {

    setlocale(LC_ALL, "ru");

    if (argc != 4) {
        cerr << "Ошибка: укажите входной, выходной файлы и установите флаг для компрессии \n";
        cerr << "Пример использования: TransFile_256 [source_file] [consumer_file] [compression_flag]  \n";
        cerr << "[compression flag: 1 - использовать компрессию, 0 - пересылать данные без сжатия] \n";
        return 1;
    }
    else
    {
        string inputFile = argv[1];
        string outputFile = argv[2];
        bool compression = (stoi(argv[3]) != 0) ? true : false;

        SafeQueue<packet_256> queue;

        Measure Measurement;

        // Запускаем компрессор и декомпрессор в отдельных потоках
        thread producer(produce, ref(inputFile), ref(queue), ref(Measurement), compression);
        thread consumer(consume, ref(outputFile), ref(queue), ref(Measurement), compression);

        producer.join();
        consumer.join();

        if (compression)
        {
            cout << fixed << setprecision(1);
            cout << "Процент сжатия = " << Measurement.CalculateCompressPercent() << "%" << endl;
        }

    }
    return 0;
}