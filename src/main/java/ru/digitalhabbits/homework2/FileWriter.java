package ru.digitalhabbits.homework2;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.Exchanger;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Thread.currentThread;
import static org.slf4j.LoggerFactory.getLogger;

public class FileWriter
        implements Runnable {
    private static final Logger logger = getLogger(FileWriter.class);

    private final Exchanger<Pair<String, Integer>> exchanger;
    private final File file;
    private final AtomicInteger rawCounter;

    public FileWriter(Exchanger<Pair<String, Integer>> exchanger, File file, AtomicInteger rawCounter) {
        this.exchanger = exchanger;
        this.file = file;
        this.rawCounter = rawCounter;
    }

    @Override
    public void run() {
        logger.info("Started writer thread {}", currentThread().getName());
        Path path = Paths.get(file.toURI());
        try {
            BufferedWriter bufferedWriter = Files.newBufferedWriter(path);
            while (!Thread.currentThread().isInterrupted()) {
                Pair<String, Integer> exchange = exchanger.exchange(null);
                String message = exchange.toString("%1$s %2$s");
                bufferedWriter.write(message);
                bufferedWriter.newLine();
                bufferedWriter.flush();
                rawCounter.incrementAndGet();
            }
        } catch (IOException exception) {
            exception.printStackTrace();
        } catch (InterruptedException e) {
            logger.info("Thread is interrupted ");
        }

        logger.info("Finish writer thread {}", currentThread().getName());
    }
}
