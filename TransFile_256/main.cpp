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
constexpr uint8_t WishThreads = 16;
mutex AddComp_mtx, AddOrig_mtx;
parts_and_size parts_size = {};
bool done(false);


static void compressor(int compressor_id, MemoryInputStream& memStream, vector<char>& inBuf, vector<char>& outBuf, z_stream& def_strm, int& flush,
    SafeQueue<packet_256>& queue, Measure& Measurement)
{
    
    flush = memStream.eof() ? Z_FINISH : Z_NO_FLUSH;
    def_strm.next_in = reinterpret_cast<Bytef*>(inBuf.data());
    def_strm.avail_in = static_cast<uInt>(memStream.gcount());
    {
        lock_guard <mutex> lock(AddOrig_mtx);
        Measurement.addOriginalSize(def_strm.avail_in);
    }

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
            {   
                lock_guard <mutex> lock(AddComp_mtx);
                Measurement.addCompressedlSize(have_out);
            }

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



static void produce(const string& inputFile, SafeQueue<packet_256>& queue, Measure& Measurement, bool compression, promise<parts_and_size> sizePromise, uint8_t ThreadsNum) {

    ifstream in(inputFile, ios::binary | ios::ate);
    if (!in) {
        cerr << "Failed to open input file!" << endl;
        queue.setFinished();
        return;
    }

    vector<vector<char>> buffers;
    vector<thread> compressors;

    streamsize origSize = in.tellg();

    if (origSize <= 0) {
        cerr << "Файл пустой или ошибка определения размера: " << inputFile << endl;
        return;
    }

    in.seekg(0, ios::beg);


    if (!compression) {
        Measurement.startMeasure();
        while (true) {
            vector<char> chunk(CHUNK);

            in.read(chunk.data(), CHUNK);
            streamsize bytesRead = in.gcount();

            if ((bytesRead == 0) || in.eof())
                break;

            if (in.fail() && !in.eof()) {
                cerr << "Ошибка чтения файла: " << inputFile << endl;
                return;
            }

            chunk.resize(bytesRead);

            packet_256 pkt;
            uint8_t copy_size = static_cast<uint8_t>(min(chunk.size(), size_t(CHUNK)));
            copy(chunk.begin(), chunk.begin() + copy_size, pkt.data);
            pkt.size = copy_size;
            queue.push(move(pkt));
        }
        in.close();
        queue.setFinished();
        return;
    }


    uInt portion_size = static_cast<uInt>(origSize / ThreadsNum);
    uint8_t residue = origSize % ThreadsNum;
    parts_size.ThreadsNum = (residue == 0) ? ThreadsNum : (ThreadsNum + 1);
    parts_size.partSize = static_cast<uInt>(portion_size);
    parts_size.residue = (residue == 0) ? portion_size : residue;

    sizePromise.set_value(parts_size);
    buffers.resize(parts_size.ThreadsNum, vector<char>(parts_size.partSize));


    vector <z_stream> def_strm(parts_size.ThreadsNum);
    vector<unique_ptr<MemoryInputStream>> memStream;
    memStream.reserve(parts_size.ThreadsNum);

    Measurement.startMeasure();

    for (uint8_t portionNumber = 0; portionNumber < parts_size.ThreadsNum; portionNumber++)
    {
        streamsize size;
        if (portionNumber != (parts_size.ThreadsNum - 1))
        {
            size = parts_size.partSize;
        }
        else
        {
            size = parts_size.residue;
        }

        if (!in.read(buffers[portionNumber].data(), size)) {
            cerr << "Ошибка чтения файла: " << inputFile << endl;
            return;
        }

        size_t mem_size = buffers[portionNumber].size();

        if (portionNumber == (parts_size.ThreadsNum - 1))
        {
            if (parts_size.residue != parts_size.partSize)
            {
                mem_size = parts_size.residue;
            }
        }
        memStream.emplace_back(make_unique<MemoryInputStream>(buffers[portionNumber].data(),
            mem_size));

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
            memOutStream.write(outBuf.data(), have_out);
        }

        if (ret == Z_STREAM_END) {
            break;
        }
    } while (strm.avail_out == 0);

}



static void decompress_thread(int decompressor_id, z_stream& strm, MemoryOutputStream& memOutStream, vector<DecompressorData>& decompressorQueues)
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


static void consume(const string& outputFile, SafeQueue<packet_256>& queue, Measure& Measurement, bool compression, 
    parts_and_size partsAndSize, vector<DecompressorData>& decompressorQueues) {
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
    memStream.reserve(partsAndSize.ThreadsNum);

    vector<vector<char>> buffers(partsAndSize.ThreadsNum, vector<char>(partsAndSize.partSize));

    vector <z_stream> strm(partsAndSize.ThreadsNum);

    for (uint8_t portionNumber = 0; portionNumber < partsAndSize.ThreadsNum; portionNumber++)
    {  
        if (inflateInit(&strm[portionNumber]) != Z_OK) {
            cerr << "inflateInit failed!" << endl;
            return;
        }
        memStream.emplace_back(make_unique<MemoryOutputStream>(buffers[portionNumber].data(), partsAndSize.partSize));

        decompressors.emplace_back(decompress_thread, portionNumber, ref(strm[portionNumber]), ref(*memStream[portionNumber]), ref(decompressorQueues));
    }

    while (queue.pop(compressedChunk))
    {
        uint8_t compress_id = compressedChunk.compressor_id;
        {
            lock_guard<mutex> lock(decompressorQueues[compress_id].mtx);
            decompressorQueues[compress_id].packetQueue.push(compressedChunk);
        }
        decompressorQueues[compress_id].cv.notify_one();
    }

	done = true;
	for (uint8_t id = 0; id < partsAndSize.ThreadsNum; id++)
	{
		lock_guard<mutex> lock(decompressorQueues[id].mtx);
        decompressorQueues[id].cv.notify_all();
	}    
    

    for (auto& t : decompressors) {
        if (t.joinable())
            t.join();
    }

    cout << "Полное время от начала сжатия до завершения разжатия: " << Measurement.finishMeasure() << " \n";

    for (uint8_t id = 0; id < partsAndSize.ThreadsNum; id++)
    {
        if (id != partsAndSize.ThreadsNum - 1)
        {
            outFile.write(memStream[id]->data(), memStream[id]->size());
        }
        else
        {
            outFile.write(memStream[id]->data(), parts_size.residue);
        }
        if (!outFile) {
            cerr << "Error writing decompressed data!" << endl;
            return;
        }       
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
        promise<parts_and_size> sizePromise;
        future<parts_and_size> sizeFuture = sizePromise.get_future();
        uint8_t ThreadsNum = WishThreads;

        Measure Measurement;

        thread producer(produce, ref(inputFile), ref(queue), ref(Measurement), compression, move(sizePromise), ThreadsNum);

        if (compression)
        {
            parts_and_size parts_size = sizeFuture.get();
        }  
        vector<DecompressorData> decompressorQueues(parts_size.ThreadsNum);

        thread consumer(consume, ref(outputFile), ref(queue), ref(Measurement), compression, parts_size, ref(decompressorQueues));

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

