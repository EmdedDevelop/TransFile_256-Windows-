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
#include <future>
#include <streambuf>
#include "main.h"


using namespace std;

constexpr uInt CHUNK = packet_256::DATA_SIZE;
constexpr uint8_t totalParts = 1; // пока жёстко тут, но позже должно вычисляться в runtime
bool decompressor_busy[totalParts] = {};
bool done = false; 
vector<DecompressorData> decompressorQueues(totalParts);




static void compressor(int compressor_id, MemoryInputStream& memStream, vector<char>& inBuf, vector<char>& outBuf, z_stream& def_strm, int& flush,
    SafeQueue<packet_256>& queue, Measure& Measurement)
{
    
    flush = memStream.eof() ? Z_FINISH : Z_NO_FLUSH;
    def_strm.next_in = reinterpret_cast<Bytef*>(inBuf.data());
    def_strm.avail_in = static_cast<uInt>(memStream.gcount());
    Measurement.addOriginalSize(def_strm.avail_in);

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
            pkt.compressor_id = compressor_id;
            queue.push(move(pkt));
        }
    } while (def_strm.avail_out == 0);
}



static void compress_thread(int compressor_id, MemoryInputStream& memStream, z_stream& def_strm,
    SafeQueue<packet_256>& queue, Measure& Measurement)
{

    vector<char> inBuf(CHUNK);
    vector<char> outBuf(CHUNK);
    int flush;
    do {
        memStream.read(inBuf.data(), CHUNK);
        compressor(compressor_id, memStream, inBuf, outBuf, def_strm, ref(flush),
            queue, Measurement);
    } while (flush != Z_FINISH);
}



static void produce(const string& inputFile, SafeQueue<packet_256>& queue, Measure& Measurement, bool compression, promise<streamsize> sizePromise) {

    ifstream in(inputFile, ios::binary | ios::ate);
    if (!in) {
        cerr << "Failed to open input file!" << endl;
        queue.setFinished();
        return;
    }

    vector<vector<char>> buffers;
    vector<thread> compressors;

    streamsize size = in.tellg() / totalParts;
    if (size <= 0) {
        cerr << "Файл пустой или ошибка определения размера: " << inputFile << endl;
        return;
    }
    sizePromise.set_value(size);
    in.seekg(0, ios::beg);
    buffers.resize(totalParts, vector<char>(size));


    z_stream def_strm[totalParts]{};        
    vector<unique_ptr<MemoryInputStream>> memStream;
    memStream.reserve(totalParts); 

    Measurement.startMeasure();

    for (uint8_t portionNumber = 0; portionNumber < totalParts; portionNumber++)
    {

        if (!in.read(buffers[portionNumber].data(), size)) {
            cerr << "Ошибка чтения файла: " << inputFile << endl;
            return;
        }

        memStream.emplace_back(make_unique<MemoryInputStream>(buffers[portionNumber].data(), 
        buffers[portionNumber].size()));

        if (deflateInit(&def_strm[portionNumber], 4) != Z_OK) {
            cerr << "deflateInit failed!" << endl;
            queue.setFinished();
            return;
        }

        compressors.emplace_back(compress_thread, portionNumber, ref(*memStream[portionNumber]),
            ref(def_strm[portionNumber]), ref(queue), std::ref(Measurement));
    }

    in.close();


    for (auto& t : compressors) {
        if (t.joinable())
            t.join();
    }
    

#if 0
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
            queue.push(move(pkt));
        }
        queue.setFinished();
        return;
    }
#endif

    queue.setFinished();
}



static void decompress(packet_256& packet, z_stream& strm, vector<char>& outBuf, MemoryOutputStream& memOutStream)
{
    strm.next_in = reinterpret_cast<Bytef*>(packet.data);
    strm.avail_in = static_cast<uInt>(packet.size);

    do {
        strm.next_out = reinterpret_cast<Bytef*>(outBuf.data());
        strm.avail_out = CHUNK;

        int ret = inflate(&strm, Z_NO_FLUSH);
        if (ret == Z_STREAM_ERROR || ret == Z_DATA_ERROR || ret == Z_MEM_ERROR) {
            cerr << "inflate error: " << ret << endl;
            inflateEnd(&strm);
            return;
        }

        uint8_t have_out = CHUNK - strm.avail_out;
        if (have_out > 0) {
            //outFile.write(outBuf.data(), have_out);
            memOutStream.write(outBuf.data(), have_out);

            //if (!outFile) {
            //    cerr << "Error writing decompressed data!" << endl;
            //    inflateEnd(&strm);
            //    return;
            //}
        }

        if (ret == Z_STREAM_END) {
            break;
        }
    } while (strm.avail_out == 0);

}



static void decompress_thread(int decompressor_id, z_stream& strm, MemoryOutputStream& memOutStream)
{

    auto& data = decompressorQueues[decompressor_id];
    vector<char> outBuf(CHUNK);

    while (!done)
    {
        std::unique_lock<std::mutex> lock(data.mtx);
        data.cv.wait(lock, [&]() { return done || !data.packetQueue.empty(); });

        if (data.packetQueue.empty() && done)
            break;

        packet_256 packet = data.packetQueue.front();
        data.packetQueue.pop();
        lock.unlock();

        decompress(packet, strm, outBuf, memOutStream);
    }
    inflateEnd(&strm);
}


static void consume(const string& outputFile, SafeQueue<packet_256>& queue, Measure& Measurement, bool compression, streamsize size) {
    ofstream outFile(outputFile, ios::binary);
    if (!outFile) {
        cerr << "Failed to open output file!" << endl;
        return;
    }

    if (!compression) {
        // Тупо читаем блоки из очереди и пишем в файл 
        packet_256 chunk;
        while (queue.pop(chunk)) {
            outFile.write(chunk.data, chunk.size);
            if (!outFile) {
                cerr << "Error writing output file!" << endl;
                return;
            }
        }

        cout << "Полное время от начала до конца передачи (несжатых данных): " << Measurement.finishMeasure() << endl;
        return;
    }

        
    packet_256 compressedChunk;
    vector<thread>decompressors;
    vector<unique_ptr<MemoryOutputStream>> memStream;
    memStream.reserve(totalParts);

    uInt partSize = static_cast<uInt>(size / totalParts);

    vector<vector<char>> buffers(totalParts, vector<char>(partSize));

    z_stream strm[totalParts]{};
    for (uint8_t portionNumber = 0; portionNumber < totalParts; portionNumber++)
    {     

        if (inflateInit(&strm[portionNumber]) != Z_OK) {
            cerr << "inflateInit failed!" << endl;
            return;
        }
        memStream.emplace_back(make_unique<MemoryOutputStream>(buffers[portionNumber].data(), partSize));

        decompressors.emplace_back(decompress_thread, portionNumber, ref(strm[portionNumber]), ref(*memStream[portionNumber]));
    }

    while (queue.pop(compressedChunk))
    {
        uint8_t id = compressedChunk.compressor_id;
        {
            lock_guard<mutex> lock(decompressorQueues[id].mtx);
            decompressorQueues[id].packetQueue.push(compressedChunk);
        }
        decompressorQueues[id].cv.notify_one();
    }

	done = true;
	for (uint8_t id = 0; id < totalParts; id++)
	{
		lock_guard<mutex> lock(decompressorQueues[id].mtx);
        decompressorQueues[id].cv.notify_all();
	}    
    

    for (auto& t : decompressors) {
        if (t.joinable())
            t.join();
    }

    cout << "Полное время от начала сжатия до завершения разжатия: " << Measurement.finishMeasure() << " \n";

    for (uint8_t id = 0; id < totalParts; id++)
    {
        outFile.write(memStream[id]->data(), memStream[id]->size());
    }
        
    outFile.close();
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
        std::promise<streamsize> sizePromise;
        std::future<streamsize> sizeFuture = sizePromise.get_future();

        Measure Measurement;

        thread producer(produce, ref(inputFile), ref(queue), ref(Measurement), compression, move(sizePromise));

        streamsize OriginalSize = sizeFuture.get();

        thread consumer(consume, ref(outputFile), ref(queue), ref(Measurement), compression, OriginalSize);

        producer.join();
        consumer.join();

        if (compression)
        {
            cout << fixed << setprecision(1);
            cout << "Процент сжатия = " << Measurement.CalculateCompressPercent() << "% \n";
        }

    }
    return 0;
}

