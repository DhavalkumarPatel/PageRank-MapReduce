package mr.hw3.main;

import java.util.Date;

import org.apache.hadoop.mapreduce.Job;
import org.apache.log4j.Logger;

import mr.hw3.utility.Counters;
import mr.hw3.utility.Utility;

/**
 * This class runs PreProcessingJob, PageRankJob, PageRankCorrectionJob and TopKJob
 * in sequence, logs the running time of each job and measures the convergence in each
 * iteration. 
 * @author dspatel
 *
 */
public class Driver 
{
	public static void main(String[] args) throws Exception
	{
		Logger logger = Logger.getLogger(Driver.class);
		StringBuffer timeLog = new StringBuffer();
		StringBuffer convergenceLog = new StringBuffer();
		
		try
		{
			//START - Initialize the local variables from args argument
			if (args.length != 5) 
			{
				logger.error("Usage: Driver <input path> <output path> <alpha> <No of Iteration> <K for TopK>");
				System.exit(-1);
			}
			String inputPath = args[0];
			String outputPath = args[1];
			Double alpha = Double.parseDouble(args[2]);
			int noOfIteration = Integer.parseInt(args[3]);
			int kForTopK = Integer.parseInt(args[4]);
			//END - Initialize the local variables from args argument
			
			
			//START - PreProcessingJob
			logger.info("PreProcessingJob Started.");
			Date startTime = new Date();
			
			// run the preProcessingJob and get the total number of pages from global counters
			Job preProcessingJob = new PreProcessingJob().runPreProcessingJob(inputPath, outputPath + 0);
			Long numberOfPages = preProcessingJob.getCounters().findCounter(Counters.NUMBER_OF_PAGES).getValue();
			
			Date end_time = new Date();
			logger.info("PreProcessingJob completed successfully with numberOfPages = " + numberOfPages);
			timeLog.append("\nTime taken by PreProcessingJob = " + (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
			//END - PreProcessingJob
			
			
			//START - PageRankJob 
			logger.info("PageRankJobs Started.");
			startTime = new Date();
			Double delta = 0.0;
			Double sumOfPageRankDiff = 0.0;
			
			int i;
			for(i = 1; i <= noOfIteration; i++)
			{
				logger.info("PageRankJob Iteration(" + i + ") Started.");
				
				// run the one iteration of PageRankJob and get the Delta and sum of page rank difference from global counters
				Job pageRankJob = new PageRankJob().runPageRankJob(outputPath + (i - 1), outputPath + i, numberOfPages, alpha, delta);
				delta = Utility.convertLongToDouble(pageRankJob.getCounters().findCounter(Counters.DELTA).getValue());
				sumOfPageRankDiff = Utility.convertLongToDouble(pageRankJob.getCounters().findCounter(Counters.SUM_OF_PAGE_RANK_DIFF_BETWEEN_ITERATIONS).getValue());
				
				// calculate the convergence achieved in this iteration and logs it.
				convergenceLog.append("\nAverage change in pagerank after iteration (" + (i-1) + ") = " + sumOfPageRankDiff/numberOfPages);
				logger.info("PageRankJob Iteration(" + i + ") completed successfully.");
			}
			// Correct the pageRank calculated in last iteration by Map Only Job
			new PageRankCorrectionJob().runPageRankCorrectionJob(outputPath + (i - 1), outputPath + "_AllPageRanks", numberOfPages, alpha, delta);
			
			end_time = new Date();
			logger.info("PageRankJobs completed successfully.");
			timeLog.append("\nTime taken by PageRankJobs = " + (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
			//END - PageRankJob 
			
			
			//START - TopKJob
			logger.info("TopKJob Started.");
			startTime = new Date();
			
			// run the TopKJob
			new TopKJob().runTopKJob(outputPath + "_AllPageRanks", outputPath + "_TopKPageRanks", kForTopK);
			
			end_time = new Date();
			logger.info("TopKJob completed successfully.");
			timeLog.append("\nTime taken by TopKJob = " + (end_time.getTime() - startTime.getTime()) /1000 + " seconds.");
			//END - TopKJob
			
			logger.info("Estimated Converence after each iteration:" + convergenceLog);
			logger.info("Running Time of each job:" + timeLog);
		}
		catch(Exception e)
		{
			logger.error(e.getMessage());
			logger.error("PageRank job sequence failed.");
			throw e;
		}
	}
}
