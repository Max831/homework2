package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import javax.annotation.Nonnull;
import java.io.File;
import java.io.IOException;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Runtime.getRuntime;
import static java.nio.charset.Charset.defaultCharset;
import static org.slf4j.LoggerFactory.getLogger;

public class FileProcessor {
    private static final Logger logger = getLogger(FileProcessor.class);
    public static final int CHUNK_SIZE = 2 * getRuntime().availableProcessors();

    public void process(@Nonnull String processingFileName, @Nonnull String resultFileName) {
        checkFileExists(processingFileName);

        final File file = new File(processingFileName);
        final File resultFile = new File(resultFileName);
        if (!resultFile.exists()) {
            try {
                resultFile.createNewFile();
            } catch (IOException exception) {
                exception.printStackTrace();
            }
        }

        final AtomicInteger countRow = new AtomicInteger(1);
        final Exchanger<Pair<String, Integer>> exchanger = new Exchanger<>();

        final Thread writerThread = new Thread(new FileWriter(exchanger, resultFile, countRow));
        final LineProcessor<Integer> processorCount = new LineCounterProcessor();
        writerThread.setDaemon(true);
        writerThread.start();
        ExecutorService executorService = Executors.newFixedThreadPool(CHUNK_SIZE);

        try (final Scanner scanner = new Scanner(file, defaultCharset())) {
            while (scanner.hasNext()) {
                int atomicCount = countRow.get();
                String nextLine = scanner.nextLine();
                executorService.execute(new Runnable() {
                    int innerCountRow = atomicCount;

                    @Override
                    public void run() {
                        Pair<String, Integer> process = processorCount.process(nextLine);
                        while (innerCountRow != atomicCount) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                e.printStackTrace();
                                return;
                            }
                        }
                        try {
                            exchanger.exchange(process);
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                });
                countRow.getAndIncrement();
              }
        } catch (IOException exception) {
            logger.error("IOException", exception);
        }

        try {
            executorService.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        executorService.shutdown();

        logger.info("Finish main thread {}", Thread.currentThread().getName());
    }

    private void checkFileExists(@Nonnull String fileName) {
        final File file = new File(fileName);
        if (!file.exists() || file.isDirectory()) {
            throw new IllegalArgumentException("File '" + fileName + "' not exists");
        }
    }
}
