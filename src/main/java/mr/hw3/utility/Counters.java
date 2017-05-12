package mr.hw3.utility;

/**
 * Counter for calculating Number of total pages, Delta and sum of page rank differences
 * @author dspatel
 */
public enum Counters 
{
	// To keep track of total number of pages 
	NUMBER_OF_PAGES, 
	
	// To keep track of dangling factor (delta)
	DELTA,
	
	// To keep track of total page rank difference between iterations to measure convergence
	SUM_OF_PAGE_RANK_DIFF_BETWEEN_ITERATIONS
}
