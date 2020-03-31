def main():
    part = 0
    workers = []
    myQueue = Queue()
    # number of workders
    n = 4
    directory = "C:\Projects\Telematics\Sample_MSG_Count.txt"
    for _ in range(n):
        worker = DownloadWorker()
        workers.append(worker)
#       worker.daemon = True
        worker.start()
    mainStartTime = time.time()
    for f in os.scandir(directory):
        part += 1
        start = time.time()
        with open(f, 'r') as inFile:
            for line in inFile:
                while myQueue.qsize() > 400000:
                    pass
                myQueue.put((line))
        inFile.close()
        # wait for queue to empty
        myQueue.join()
        end = time.time()
        logging.info('Sample ' + str(part) + ' Took: %s', end - start)
    # wait for worker threads to complete
    for _ in range(n):
        myQueue.put(None)
    for worker in workers:
        worker.join()
    #producer.flush()
    #producer.close()
    mainEndTime = time.time()
    totalTime = mainEndTime - mainStartTime
    logging.info('Total Messenger Process Took: %s',totalTime)


if __name__ == '__main__':
    main()
