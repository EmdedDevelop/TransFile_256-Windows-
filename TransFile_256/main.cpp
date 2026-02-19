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

constexpr uInt CHUNK = packet_256::DATA_SIZE;

vector <packet_256> notInOrderBuffer;
mutex mtx;
bool compressor_busy[2] = {};
bool data_ready(false);
condition_variable cvRead, cvCompress;
bool done = false; 



// Считали пол-файла - запустили свой объект для компрессора 1, в нём помечаем что это первый компрессор
// Считали вторую половину файла - запустили другой объект для компрессора 2, в нём помечаем, что это второй

// На приёме - запустили сразу два потока декомпрессора. Первый обрабатывает только пакеты с признаком первого
// Второй - только пакеты с признаком второго

// Первый поток сразу пишет в файл, второй поток пишет в память, а как только закончит
// проверяет, что первый поток записал файл и после этого пишет вторую часть


static void compressor(int compressor_id, MemoryInputStream& memStream, vector<char>& inBuf, z_stream& def_strm, int& flush,
    SafeQueue<packet_256>& queue, vector<char>& outBuf, Measure& Measurement)
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



static void compress_thread(int compressor_id, MemoryInputStream& memStream, vector<char>& inBuf, z_stream& def_strm,
    SafeQueue<packet_256>& queue, vector<char>& outBuf, Measure& Measurement)
{

    int flush;
    do {
        memStream.read(inBuf.data(), CHUNK);
        compressor(compressor_id, memStream, inBuf, def_strm, ref(flush),
            queue, outBuf, Measurement);
    } while (flush != Z_FINISH);



#if 0
    unique_lock<mutex> lock(mtx);

    while (!done)
    {
        cvRead.wait(lock, [] { return data_ready || done; });
        if (done) break;

        data_ready = false;
        compressor_busy[0] = true;
        lock.unlock();
        compressor(memStream, inBuf, def_strm, flush, queue, outBuf, Measurement);          
        lock.lock();
        //cout << "Compressor is free again\n";
        compressor_busy[0] = false;
        cvCompress.notify_one();
    }    
#endif
}



static void produce(const string& inputFile, SafeQueue<packet_256>& queue, Measure& Measurement, bool compression) {
    constexpr uint8_t totalParts = 1;

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
    in.seekg(0, ios::beg);
    buffers.resize(totalParts, vector<char>(size));
    z_stream def_strm[totalParts]{};
    //int flush[totalParts];
        
    vector<vector<char>> inBuf(totalParts, vector<char>(CHUNK));
    vector<vector<char>> outBuf(totalParts, vector<char>(CHUNK));
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

        compressors.emplace_back(compress_thread, portionNumber, ref(*memStream[portionNumber]), ref(inBuf[portionNumber]),
            ref(def_strm[portionNumber]), ref(queue), ref(outBuf[portionNumber]), std::ref(Measurement));
    }      


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

#if 0
    vector<char> inBuf(CHUNK);
    vector<char> outBuf(CHUNK);

    z_stream def_strm{};
    if (deflateInit(&def_strm, 4) != Z_OK) {
        cerr << "deflateInit failed!" << endl;
        queue.setFinished();
        return;
    }

    int flush;
    int thread_id = 0;
	thread Compressor(compress_thread, thread_id, ref(memStream), ref(inBuf), ref(def_strm), ref(flush), ref(queue), ref(outBuf), ref(Measurement));
	do {
		{
			unique_lock<mutex> lock(mtx);
			cvCompress.wait(lock, [] { return (!compressor_busy[0]) && (!data_ready); });
		}
		memStream.read(inBuf.data(), CHUNK);
		{
			lock_guard<mutex> lock(mtx);
			data_ready = true;
		}
		cvRead.notify_one();
	} while (flush != Z_FINISH);

	{
		lock_guard<mutex> lock(mtx);
		done = true;
	}
	cvRead.notify_one();
    Compressor.join();


    deflateEnd(&def_strm);

#endif

    queue.setFinished();
}



static void decompressor(packet_256& packet, z_stream& strm, vector<char>& outBuf, ofstream& outFile, Measure& Measurement)
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


#if 0
static void processPendedPackets(z_stream& strm, vector<char>& outBuf, ofstream& outFile, Measure& Measurement) {
    bool foundNext;
    do {
        foundNext = false;
        for (auto it = notInOrderBuffer.begin(); it != notInOrderBuffer.end(); ) {
            //if (it->index_of_packet == expected_packet_index) {

                decompressor(*it, strm, outBuf, outFile, Measurement);
                it = notInOrderBuffer.erase(it);
                //++expected_packet_index;
                foundNext = true;
            //}
            //else {
            //    ++it;
            //}
        }
    } while (foundNext); 
}

#endif

void consume(const string& outputFile, SafeQueue<packet_256>& queue, Measure& Measurement, bool compression) {
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

    z_stream strm{};
    if (inflateInit(&strm) != Z_OK) {
        cerr << "inflateInit failed!" << endl;
        return;
    }
        
    packet_256 compressedChunk;
    vector<char> outBuf(CHUNK);

	while (queue.pop(compressedChunk)) {
		//if (expected_packet_index == compressedChunk.index_of_packet)
		//{
            decompressor(compressedChunk, strm, outBuf, outFile, Measurement);
            //expected_packet_index++;
            //processPendedPackets(strm, outBuf, outFile, Measurement);
		//}
        //else
        //{
        //    notInOrderBuffer.push_back(compressedChunk);
       // }
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

        thread producer(produce, ref(inputFile), ref(queue), ref(Measurement), compression);
        thread consumer(consume, ref(outputFile), ref(queue), ref(Measurement), compression);

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