package com.github.hieuvv.inputstreamdemo;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.S3Object;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.StopWatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Slf4j
@SpringBootApplication
public class S3InputStreamDemoApplication implements CommandLineRunner {

	private static final String BUCKET_NAME = "hieuvo-bucket-demo";
	private static final String OBJECT_KEY = "multipart-ratings.csv";

	private final AmazonS3Client s3Client;

	public S3InputStreamDemoApplication(AmazonS3Client s3Client) {
		this.s3Client = s3Client;
	}

	public void readFileFromS3Object() {
		S3Object s3Object = s3Client.getObject(BUCKET_NAME, OBJECT_KEY);
		try (BufferedReader br = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()))) {
			Stream<String> data = br.lines();
			AtomicInteger counter = new AtomicInteger(0);
			data.forEach(item -> {
				log.info("readFileFromS3Object: {}, {}", counter.incrementAndGet(), item);
			});
			log.info("Total record: {}", counter.get());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void readFileFromS3ObjectParallel() {
		S3Object s3Object = s3Client.getObject(BUCKET_NAME, OBJECT_KEY);

		try (BufferedReader br = new BufferedReader(new InputStreamReader(s3Object.getObjectContent()), 8192 * 4)) {
			Stream<String> data = br.lines();
			AtomicInteger counter = new AtomicInteger(0);
			data.parallel().forEach(item -> {
				log.info("readFileFromS3Object: {}, {}", counter.incrementAndGet(), item);
			});
			log.info("Total record: {}", counter.get());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	void readFileFromDisk() {
		try (BufferedReader br = Files.newBufferedReader(Paths.get("D:\\git\\study\\s3-demos\\s3-transfermanager-demo\\target\\classes\\data\\multipart-ratings.csv"))) {
			Stream<String> data = br.lines();
			AtomicInteger counter = new AtomicInteger(0);
			data.forEach(item -> {
				log.info("readFileFromDisk item: {}, {}", counter.incrementAndGet(), item);
			});
			log.info("Total record: {}", counter.get());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public void run(String... args) throws Exception {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		readFileFromS3Object();
		stopWatch.stop();
		log.info("==================================\nStopWatch: {}", prettyElapsedTime(stopWatch.getLastTaskTimeNanos()));
	}

	public static void main(String[] args) {
		SpringApplication.run(S3InputStreamDemoApplication.class, args);
	}

	private String prettyElapsedTime(long nano) {
		Duration duration = Duration.ofNanos(nano);
		return duration.toSeconds() + "s." + duration.toMillisPart() ;
	}


}
