package com.github.hieuvo.transfermanager;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.transfer.Download;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.util.ResourceUtils;
import org.springframework.util.StopWatch;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

@Slf4j
@SpringBootApplication
public class S3TransferManagerDemo implements CommandLineRunner {

	private static final String BUCKET_NAME = "hieuvo-bucket-demo";
	private static final String OBJECT_KEY_MULTIPART = "multipart-ratings.csv";
	private static final String OBJECT_KEY_NON_MULTIPART = "ratings.csv";
	private static final String FILE_UPLOAD = "D:\\data-set\\ml-20m\\ratings.csv";

	private final TransferManager transferManager;
	private final AmazonS3Client s3Client;

	public S3TransferManagerDemo(AmazonS3Client s3Client) {
		this.s3Client = s3Client;
		transferManager = TransferManagerBuilder.standard().withS3Client(s3Client).build();
	}

	public void uploadFile() {
		 s3Client.putObject(BUCKET_NAME, OBJECT_KEY_NON_MULTIPART, new File(FILE_UPLOAD));
	}

	public void uploadMultipartFileWithListener() {
		try {
			Upload u = transferManager.upload(BUCKET_NAME, OBJECT_KEY_MULTIPART, new File(FILE_UPLOAD));
			// print an empty progress bar...
			printProgressBar(0.0);
			u.addProgressListener(new ProgressListener() {
				public void progressChanged(ProgressEvent e) {
					double pct = e.getBytesTransferred() * 100.0 / e.getBytes();
					eraseProgressBar();
					printProgressBar(pct);
				}
			});
			// block with Transfer.waitForCompletion()
			u.waitForCompletion();
			;
			// print the final state of the transfer.
			Transfer.TransferState xfer_state = u.getState();
			System.out.println(": " + xfer_state);
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
			System.exit(1);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		transferManager.shutdownNow();
		// snippet-end:[s3.java1.s3_xfer_mgr_progress.progress_listener]
	}

	void transferManagerDownloadNonMultipart() throws FileNotFoundException, InterruptedException {
		Path path = Paths.get(ResourceUtils.getFile("classpath:").getPath()).resolve("data").resolve(OBJECT_KEY_NON_MULTIPART);
		Download download = transferManager.download(BUCKET_NAME, OBJECT_KEY_NON_MULTIPART, path.toFile());
		download.addProgressListener(new ProgressListener() {
			@Override
			public void progressChanged(ProgressEvent e) {
				double pct = e.getBytesTransferred() * 100.0 / e.getBytes();
				eraseProgressBar();
				printProgressBar(pct);
			}
		});
		download.waitForCompletion();
		if (download.isDone()) {
			log.info("download  success");
		}
		transferManager.shutdownNow();
	}

	void transferManagerDownloadMultipart() throws FileNotFoundException, InterruptedException {
		Path path = Paths.get(ResourceUtils.getFile("classpath:").getPath()).resolve("data").resolve(OBJECT_KEY_MULTIPART);
		Download download = transferManager.download(BUCKET_NAME, OBJECT_KEY_MULTIPART, path.toFile());
		download.addProgressListener(new ProgressListener() {
			@Override
			public void progressChanged(ProgressEvent e) {
				double pct = e.getBytesTransferred() * 100.0 / e.getBytes();
				eraseProgressBar();
				printProgressBar(pct);
			}
		});
		download.waitForCompletion();

		if (download.isDone()) {
			log.info("download  success");
		}
		transferManager.shutdownNow();
	}

	void transferManagerProcessMultipart() throws IOException, InterruptedException {
		File tempFile = File.createTempFile("D:\\git\\study\\s3-demos\\s3-transfermanager-demo\\target\\classes\\data\\temp-file", "csv");
		Download download = transferManager.download(BUCKET_NAME, OBJECT_KEY_MULTIPART, tempFile);
		download.addProgressListener(new ProgressListener() {
			@Override
			public void progressChanged(ProgressEvent e) {
				double pct = e.getBytesTransferred() * 100.0 / e.getBytes();
				eraseProgressBar();
				printProgressBar(pct);
			}
		});
		download.waitForCompletion();
		if (download.isDone()) {
			log.info("download  success");
		}
		transferManager.shutdownNow();
		// read tem file
		try (BufferedReader br = new BufferedReader(new FileReader(tempFile))) {
			Stream<String> data = br.lines();
			AtomicInteger counter = new AtomicInteger(0);
			data.forEach(item -> {
				log.info("readFileFromS3Object: {}, {}", counter.incrementAndGet(), item);
			});
			log.info("Total record: {}", counter.get());
		}
		log.info("delete tempfile: {}", tempFile.delete());
	}



	// erases the progress bar.
	public static void eraseProgressBar() {
		// erase_bar is bar_size (from printProgressBar) + 4 chars.
		final String erase_bar = "\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b\b";
		System.out.format(erase_bar);
	}

	// prints a simple text progressbar: [#####     ]
	public static void printProgressBar(double pct) {
		// if bar_size changes, then change erase_bar (in eraseProgressBar) to
		// match.
		final int bar_size = 40;
		final String empty_bar = "                                        ";
		final String filled_bar = "########################################";
		int amt_full = (int) (bar_size * (pct / 100.0));
		System.out.format("  [%s%s]", filled_bar.substring(0, amt_full),
				empty_bar.substring(0, bar_size - amt_full));
	}


	@Override
	public void run(String... args) throws Exception {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		transferManagerProcessMultipart();
		stopWatch.stop();
		log.info("==================================\nStopWatch: {}", prettyElapsedTime(stopWatch.getLastTaskTimeNanos()));
	}


	public static void main(String[] args) {

		SpringApplication.run(S3TransferManagerDemo.class, args);
	}

	private String prettyElapsedTime(long nano) {
		Duration duration = Duration.ofNanos(nano);
		return  duration.toSeconds() + "s." + duration.toMillisPart() ;
	}


}
