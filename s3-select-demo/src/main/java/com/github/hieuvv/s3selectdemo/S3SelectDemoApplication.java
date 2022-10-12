package com.github.hieuvv.s3selectdemo;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.CSVInput;
import com.amazonaws.services.s3.model.CSVOutput;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.ExpressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.ScanRange;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.amazonaws.services.s3.model.SelectObjectContentResult;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.StopWatch;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Slf4j
@SpringBootApplication
public class S3SelectDemoApplication implements CommandLineRunner {

	private static final String BUCKET_NAME = "hieuvo-bucket-demo";
	private static final String OBJECT_KEY = "multipart-ratings.csv";
	private static final String QUERY = "select * from S3Object \"multipart-ratings\"";

	private final AmazonS3Client s3Client;

	public S3SelectDemoApplication(AmazonS3Client s3Client) {
		this.s3Client = s3Client;
	}

	void s3FileSelectParallelProcessing() {
		// 104857600 ~ 100 MB
		long chunkBytes = 104857600;
		List<ScanRange> scanRangeList = buildScanRangeList(getS3FileSize(), chunkBytes);
		log.info("Number of list necessary to scan: {}", scanRangeList.size());
		AtomicInteger counter = new AtomicInteger(0);
		scanRangeList.parallelStream().forEach(scanRange -> {
			log.info("Calling get data by scanRange: {}", scanRange);
			SelectObjectContentRequest request = generateBaseCSVRequest(BUCKET_NAME, OBJECT_KEY, QUERY);
			request.setScanRange(scanRange);
			try (SelectObjectContentResult result = s3Client.selectObjectContent(request);
				 BufferedReader br = new BufferedReader(new InputStreamReader(result.getPayload().getRecordsInputStream()))) {
				Stream<String> data = br.lines();
				data.forEach(item -> log.info("s3FileSelectParallelProcessing: {}, {}", counter.incrementAndGet(), item));
			} catch (IOException e) {
				e.printStackTrace();
			}
		});
		log.info("Total record: {}", counter.get());
	}

	void s3SelectFileProcessing() {
		long fileSize = getS3FileSize();
		// 512KB or 0.5MB
		long chunkBytes = 104857600;
		long startBytes = 0;
		long endRange = Long.min(chunkBytes, fileSize);
		AtomicInteger counter = new AtomicInteger(0);
		while (startBytes < fileSize) {
			SelectObjectContentRequest request = generateBaseCSVRequest(BUCKET_NAME, OBJECT_KEY, QUERY);
			ScanRange scanRange = new ScanRange();
			scanRange.setStart(startBytes);
			scanRange.setEnd(endRange);
			log.info("Calling get data by scanRange: {}", scanRange);
			try (SelectObjectContentResult result = s3Client.selectObjectContent(request);
				 BufferedReader br = new BufferedReader(new InputStreamReader(result.getPayload().getRecordsInputStream()))) {
				Stream<String> data = br.lines();
				counter.addAndGet((int) data.count());
//				data.forEach(item -> {
//					log.info("item: {}, {}", counter.getAndIncrement(), item);
//				});
			} catch (IOException e) {
				e.printStackTrace();
			}
			startBytes = endRange;
			endRange = endRange + Long.min(chunkBytes, fileSize - endRange);
		}
		log.info("Total record: {}", counter.get());
	}

	private long getS3FileSize() {
		final S3Object object = s3Client.getObject(BUCKET_NAME, OBJECT_KEY);
		log.info("fileSize: {}", object.getObjectMetadata().getContentLength());
		return object.getObjectMetadata().getContentLength();
	}

	List<ScanRange> buildScanRangeList(long fileSize, long chunkBytes) {
		List<ScanRange> target = new ArrayList<>();
		long startBytes = 0;
		long endRange = Long.min(chunkBytes, fileSize);
		while (startBytes < fileSize) {
			ScanRange scanRange = new ScanRange();
			scanRange.setStart(startBytes);
			scanRange.setEnd(endRange);
			target.add(scanRange);
			startBytes = endRange;
			endRange = endRange + Long.min(chunkBytes, fileSize - endRange);
		}
		return target;
	}


	@Override
	public void run(String... args) throws Exception {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		s3FileSelectParallelProcessing();
		stopWatch.stop();
		log.info("==================================\nStopWatch: {}", prettyElapsedTime(stopWatch.getLastTaskTimeNanos()));
	}

	public static void main(String[] args) {
		SpringApplication.run(S3SelectDemoApplication.class, args);
	}

	private static SelectObjectContentRequest generateBaseCSVRequest(String bucket, String key, String query) {
		SelectObjectContentRequest request = new SelectObjectContentRequest();
		request.setBucketName(bucket);
		request.setKey(key);
		request.setExpression(query);
		request.setExpressionType(ExpressionType.SQL);
		InputSerialization inputSerialization = new InputSerialization();
		inputSerialization.setCsv(new CSVInput());
		inputSerialization.setCompressionType(CompressionType.NONE);
		request.setInputSerialization(inputSerialization);
		OutputSerialization outputSerialization = new OutputSerialization();
		outputSerialization.setCsv(new CSVOutput());
		request.setOutputSerialization(outputSerialization);
		return request;
	}

	private String prettyElapsedTime(long nano) {
		Duration duration = Duration.ofNanos(nano);
		return duration.toSeconds() + "s." + duration.toMillisPart();
	}
}
