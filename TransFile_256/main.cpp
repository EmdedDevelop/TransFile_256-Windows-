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
#include <cassert>


using namespace std;

static constexpr uInt CHUNK = packet_256::DATA_SIZE;
static constexpr uint8_t WishThreads = 16;
static mutex AddComp_mtx, AddOrig_mtx;
static parts_and_size parts_size = {};
static bool done(false);
static size_t FileSize;



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
                Measurement.addCompressedSize(have_out);
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


static void SimplyCopier(istream& in, const string& inputFile, SafeQueue<packet_256>& queue, const streamsize origSize)
{
    vector <char> buffer(origSize);

    if (!in.read(buffer.data(), origSize)) { // считать данные в буфер
        std::cerr << "Ошибка чтения файла: " << inputFile << "\n";
        return;
    }

    MemoryInputStream memStream(buffer.data(), buffer.size());
    vector<char> chunk(CHUNK);

    while (true) {

        memStream.read(chunk.data(), CHUNK);
        streamsize bytesRead = memStream.gcount();
        //            in.read(chunk.data(), CHUNK);
        //            streamsize bytesRead = in.gcount();

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
}



static void produce(const string& inputFile, SafeQueue<packet_256>& queue, Measure& Measurement, bool compression, promise<parts_and_size> sizePromise, uint8_t ThreadsNum) {

    ifstream in(inputFile, ios::binary | ios::ate);
    if (!in) {
        cerr << "Failed to open input file!" << endl;
        queue.setFinished();
        return;
    }

    FileSize = in.tellg();

    if (FileSize <= 0) {
        cerr << "Исходный файл пустой или ошибка определения размера: " << inputFile << endl;
        in.close();

        parts_and_size empty_parts_size{};
        empty_parts_size.ThreadsNum = 0;
        empty_parts_size.partSize = 0;
        empty_parts_size.residue = 0;
        sizePromise.set_value(empty_parts_size);

        Measurement.startMeasure();
        queue.setFinished();
        return;
    }

    in.seekg(0, ios::beg);
    
    if (!compression) {
        parts_and_size non_compressed_size{};
        non_compressed_size.ThreadsNum = 1;
        non_compressed_size.partSize = static_cast<uInt>(FileSize);
        non_compressed_size.residue = static_cast<uInt>(FileSize);
        // Передаем значение, чтобы убрать future_error в main
        sizePromise.set_value(non_compressed_size);

        Measurement.startMeasure();
        SimplyCopier(in, inputFile, queue, FileSize);
        return;
    }


    vector<vector<char>> buffers;
    vector<thread> compressors;
    uInt portion_size = static_cast<uInt>(FileSize / ThreadsNum);
    uint8_t residue = FileSize % ThreadsNum;
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
        size_t size = parts_size.partSize;

        if (portionNumber == (parts_size.ThreadsNum - 1))
        {
             size = parts_size.residue;
        }

        if (!in.read(buffers[portionNumber].data(), size)) {
            cerr << "Ошибка чтения файла: " << inputFile << endl;
            return;
        }

        memStream.emplace_back(make_unique<MemoryInputStream>(buffers[portionNumber].data(),
            size));

        if (deflateInit(&def_strm[portionNumber], 4) != Z_OK) {
            cerr << "deflateInit failed!" << endl;
            queue.setFinished();
            return;
        }

        compressors.emplace_back(compress_thread, portionNumber, ref(*memStream[portionNumber]),
            ref(def_strm[portionNumber]), ref(queue), ref(Measurement));
    }

    in.close();


    for (auto& t : compressors) {
        if (t.joinable())
            t.join();
    }
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


static void decompress_thread(const uInt decompressor_id, MemoryOutputStream& memOutStream, vector<DecompressorData>& decompressorQueues)
{

    auto& data = decompressorQueues[decompressor_id];
    vector<char> outBuf(CHUNK);
    z_stream strm{};
    if (inflateInit(&strm) != Z_OK) {
        cerr << "inflateInit failed!" << endl;
        return;
    }

    while (true)
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


static void SimplyPaster(SafeQueue<packet_256>& queue, ofstream& outFile)
{
    // Тупо читаем блоки из очереди и пишем в файл 
    packet_256 chunk;
    while (queue.pop(chunk)) {
        outFile.write(chunk.data, chunk.size);
        if (!outFile) {
            cerr << "Error writing output file!" << endl;
            return;
        }
    }
}


static void consume(const string& outputFile, SafeQueue<packet_256>& queue, Measure& Measurement, bool compression, 
    parts_and_size partsAndSize, vector<DecompressorData>& decompressorQueues) {
    ofstream outFile(outputFile, ios::binary);
    if (!outFile) {
        cerr << "Failed to open output file!" << endl;
        return;    
    }


    if (!compression) {
        SimplyPaster(queue, outFile);
        auto duration = Measurement.finishMeasure();
        cout << "[TIME] Полное время от начала передачи до конца приёма несжатых данных: " << duration.count() << "мс \n";
        return;
    }
        
    packet_256 compressedChunk;
    vector<thread>decompressors;
    vector<unique_ptr<MemoryOutputStream>> memStream;
    memStream.reserve(partsAndSize.ThreadsNum);

    vector<vector<char>> buffers(partsAndSize.ThreadsNum, vector<char>(partsAndSize.partSize));

    for (uint8_t portionNumber = 0; portionNumber < partsAndSize.ThreadsNum; portionNumber++)
    {  

        memStream.emplace_back(make_unique<MemoryOutputStream>(buffers[portionNumber].data(), partsAndSize.partSize));

        decompressors.emplace_back(decompress_thread, portionNumber, ref(*memStream[portionNumber]), ref(decompressorQueues));
    }

    while (queue.pop(compressedChunk))
    {
        uint8_t compress_id = compressedChunk.compressor_id;
        const uint8_t dataSize = CHUNK;
        //bool allZero = all_of(compressedChunk.data, compressedChunk.data + dataSize, [](char c) { return c == 0; });
        {
            lock_guard<mutex> lock(decompressorQueues[compress_id].mtx);
            decompressorQueues[compress_id].packetQueue.push(move(compressedChunk));
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

    uInt memSize = parts_size.partSize;
    for (uint8_t id = 0; id < partsAndSize.ThreadsNum; id++)
    {
        if (id == partsAndSize.ThreadsNum - 1)
        { 
            memSize = parts_size.residue;
        }

        outFile.write(reinterpret_cast<const char*>(memStream[id]->data()), memSize);

        // Проверка ошибок записи
        if (!outFile)
        {
            std::cerr << "Error writing decompressed data!" << std::endl;
            return;
        }
    }    
        
    outFile.close();

    auto duration = Measurement.finishMeasure();
    cout << "[TIME] Полное время от начала сжатия до завершения разжатия: " << duration.count() << " мс \n";
}



static void verifyFiles(string& inputFile, string& outputFile, Measure& Measurement)
{
    cout << "[VERIFY] ";

    ifstream sourcefile(inputFile, ios::binary | ios::ate);
    if (!sourcefile) {
        cerr << "Failed to open input file!" << endl;
        return;
    }

    uInt origSize = static_cast<uInt>(sourcefile.tellg());

    vector<char> buffer_in(origSize);

    sourcefile.seekg(ios::beg);

    if (!sourcefile.read(buffer_in.data(), origSize)) { // считать данные в буфер
        std::cerr << "Ошибка чтения файла: " << inputFile << "\n";
        return;
    }


    ifstream outfile(outputFile, ios::binary | ios::ate);
    if (!outfile) {
        cerr << "Failed to open input file!" << endl;
        return;
    }

    if (outfile.tellg() != origSize)
    {
        cout << "Размеры входного и выходного файла отличаются! \n";
    }     
    else
    {
        outfile.seekg(ios::beg);
        vector<char> buffer_out(origSize);
        if (!outfile.read(buffer_out.data(), origSize)) { // считать данные в буфер
            std::cerr << "Ошибка чтения файла: " << inputFile << "\n";
            return;
        }

        bool areEqual = (buffer_in == buffer_out);
        cout << "Исходный и выходной файл ";
        if (areEqual)
            cout << "идентичны :) \n";
        else
        {            
            cout << "отличаются :( \n";
        }
    }

    sourcefile.close();
    outfile.close();
}


static void perfomanceStats(bool compression, Measure &Measurement, SafeQueue<packet_256> &queue)
{
    size_t peak_packets = queue.get_max_utilization();
    size_t peak_bytes = peak_packets * 256;
    size_t wait_chunks = queue.get_consumer_wait_count();

    if (Measurement.total_time_sec > 0) {
        double file_size_mb = static_cast<double>(FileSize) / (1024.0 * 1024.0);

        // Вычисляем чистую скорость обработки потока
        double throughput = file_size_mb / Measurement.total_time_sec;

        std::cout << "[PERF] Скорость обработки потока: " << std::fixed << std::setprecision(2)
            << throughput << " МБ/сек\n";
    }

    std::cout << "[MEMORY] Пиковая загрузка очереди: " << peak_packets << " пакетов ";
    std::cout << "(использовано ОЗУ: " << peak_bytes << " байт)\n";
    std::cout << "[THREAD] Поток Потребителя блокировался (ждал данные): " << wait_chunks << " раз(а)\n";

    if (compression)
    {
        cout << fixed << setprecision(1);
        cout << "[STATS] Процент сжатия = " << Measurement.CalculateCompressPercent() << "% \n";
    }
}


static void runInternalTests()
{
	std::cout << "[TEST] --------------------------------------------------\n";
	std::cout << "[TEST] Шаг 1: Тестирование SafeQueue на пограничные режимы\n";
	{
		SafeQueue<int> test_queue;

		// Тест 1.1: Проверка корректности push/pop в рамках одного потока
		test_queue.push(42);
		test_queue.push(100);

		int value1 = 0;
		int value2 = 0;

		assert(test_queue.pop(value1) == true);
		assert(value1 == 42); // Очередь должна вернуть первый зашедший элемент (FIFO)

		assert(test_queue.pop(value2) == true);
		assert(value2 == 100);

		// Тест 1.2: Проверка корректного закрытия очереди через setFinished
		test_queue.setFinished();
		int dummy = 0;
		// После setFinished() и опустошения очереди метод pop должен возвращать false
		assert(test_queue.pop(dummy) == false);

		std::cout << "[TEST] Базовые тесты SafeQueue: УСПЕШНО\n";
	}

	std::cout << "[TEST] --------------------------------------------------\n";
	std::cout << "[TEST] Шаг 2: Тестирование сквозной потоковой компрессии zlib\n";
	{
		// Имитируем наш 256-байтовый пакет данных
		std::string original_text = "Senior Embedded Engineer. STM32, C++, Real-Time Systems. Fryazino 2026.";

		// Структуры zlib для deflate (сжатие)
		z_stream def_strm;
		def_strm.zalloc = Z_NULL;
		def_strm.zfree = Z_NULL;
		def_strm.opaque = Z_NULL;

		// Инициализируем компрессор (используем уровень 4, как в вашем боевом коде)
		int init_res = deflateInit(&def_strm, 4);
		assert(init_res == Z_OK);

		char compress_buffer[512];
		def_strm.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(original_text.data()));
		def_strm.avail_in = original_text.size();
		def_strm.next_out = reinterpret_cast<Bytef*>(compress_buffer);
		def_strm.avail_out = sizeof(compress_buffer);

		// Сжимаем данные
		int def_res = deflate(&def_strm, Z_FINISH);
		assert(def_res == Z_STREAM_END); // Алгоритм обязан успешно дойти до конца потока

		size_t compressed_size = sizeof(compress_buffer) - def_strm.avail_out;
		deflateEnd(&def_strm);

		assert(compressed_size > 0); // Проверяем, что сжатый буфер не пустой
		std::cout << "[TEST] Потоковый deflateInit и deflate: УСПЕШНО\n";

		// ---------------------------------------------------------------------
		// Теперь имитируем разжатие (inflate) на приёмной стороне
		z_stream inf_strm;
		inf_strm.zalloc = Z_NULL;
		inf_strm.zfree = Z_NULL;
		inf_strm.opaque = Z_NULL;
		inf_strm.avail_in = 0;
		inf_strm.next_in = Z_NULL;

		int inf_init_res = inflateInit(&inf_strm);
		assert(inf_init_res == Z_OK);

		char decompress_buffer[512];
		inf_strm.next_in = reinterpret_cast<Bytef*>(compress_buffer);
		inf_strm.avail_in = compressed_size;
		inf_strm.next_out = reinterpret_cast<Bytef*>(decompress_buffer);
		inf_strm.avail_out = sizeof(decompress_buffer);

		int inf_res = inflate(&inf_strm, Z_FINISH);
		assert(inf_res == Z_STREAM_END); // Проверяем, что разжатие завершилось без битых байт

		size_t decompressed_size = sizeof(decompress_buffer) - inf_strm.avail_out;
		inflateEnd(&inf_strm);

		// Финальный побайтовый контроль совпадения данных бит-в-бит
		assert(decompressed_size == original_text.size());
		std::string recovered_text(decompress_buffer, decompressed_size);
		assert(recovered_text == original_text);

		std::cout << "[TEST] Потоковый inflateInit, inflate и верификация: УСПЕШНО\n";
	}

	std::cout << "[TEST] --------------------------------------------------\n";
	std::cout << "[TEST] Все внутренние изолированные юнит-тесты пройдены успешно!\n";
	std::cout << "[TEST] --------------------------------------------------\n";
}



int main(int argc, char* argv[]) {

    setlocale(LC_ALL, "ru");

    if (argc == 2 && std::string(argv[1]) == "--test") {
        std::cout << "[TEST] Запуск режима самотестирования...\n";

        runInternalTests();
        return 0;
    }

    if (argc != 4) {
        cerr << "Ошибка: укажите входной, выходной файлы и установите флаг для компрессии \n";
        cerr << "Пример использования: TransFile_256.exe [source_file] [output_file] [compression_flag]  \n";
        cerr << "[compression flag: 1 - использовать компрессию, 0 - пересылать данные без сжатия] \n";
        return 1;
    }
    else
    {
        string inputFile = argv[1];
        string outputFile = argv[2];
        int compressionMode = atoi(argv[3]);
        bool compression = (compressionMode == 1);

        promise<parts_and_size> sizePromise;
        SafeQueue<packet_256> queue;
        future<parts_and_size> sizeFuture = sizePromise.get_future();
        uint8_t ThreadsNum = WishThreads;

        Measure Measurement;

        thread producer(produce, ref(inputFile), ref(queue), ref(Measurement), compression, move(sizePromise), ThreadsNum);
        parts_and_size parts_size = sizeFuture.get();

        if (parts_size.ThreadsNum > 0)
        {
            if (compression)
            {
                vector<DecompressorData> decompressorQueues(parts_size.ThreadsNum);
                thread consumer(consume, ref(outputFile), ref(queue), ref(Measurement), compression, parts_size, ref(decompressorQueues));
                producer.join();
                consumer.join();
            }
            else
            {
                vector<DecompressorData> dummy; // пустые данные не нужны
                thread consumer(consume, outputFile, ref(queue), ref(Measurement), compression, parts_size, ref(dummy));
                producer.join();
                consumer.join();
            }

            perfomanceStats(compression, ref(Measurement), ref(queue));
            verifyFiles(inputFile, outputFile, Measurement);
        }
        else
        {
            producer.join();
        }


    }
    return 0;
}

