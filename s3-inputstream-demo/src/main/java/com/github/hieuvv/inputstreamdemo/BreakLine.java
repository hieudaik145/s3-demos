package com.github.hieuvv.inputstreamdemo;

import java.time.Duration;

public class BreakLine {


	public static void main(String[] args) {

		System.out.println(prettyElapsedTime(226838549900l));
	}

	private static String prettyElapsedTime(long nano) {
		Duration duration = Duration.ofNanos(nano);
		return  duration.toSeconds() + "s." + duration.toMillisPart() ;
	}


}
