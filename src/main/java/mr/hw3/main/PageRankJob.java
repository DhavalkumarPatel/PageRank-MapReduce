package mr.hw3.main;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mr.hw3.datatypes.Node;
import mr.hw3.utility.Counters;
import mr.hw3.utility.EncodingTypes;
import mr.hw3.utility.Utility;

/**
 * PageRank Job: This class executes one iteration of a Page Rank.
 * It calculates the delta for the next iteration and also estimates the 
 * convergence of the page rank values in each iteration.
 * @author dspatel
 */
public class PageRankJob
{
	/**
	 * Mapper class with setup, cleanup and map function
	 * It uses in-mapper combining to calculate the sum of delta and page rank differences
	 * @author dspatel
	 */
	public static class PageRankMapper extends Mapper<Object, Text, Text, Text> 
	{
		private Double deltaLocal;
		private Double sumOfPageRankDiff;
		
		/**
		 * This method initializes the local delta and local sumOfPageRankDiff
		 */
		@Override
		protected void setup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException 
		{
			super.setup(context);
			deltaLocal = 0.0;
			sumOfPageRankDiff = 0.0;
		}
		
		/**
		 * map function
		 * 1. Decodes the Node from Text input
		 * 2. Initializes/Updates the page rank of the Node.
		 * 2. Calculates the difference between old and new page rank and update sumOfPageRankDiff
		 * 3. Emits nodeId with node structure
		 * 4. Emits adjacentNodeId with page rank contribution or update local delta if this is a 
		 * 	  dangling node 
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			// Decode the Node from Text input
			Node node = Utility.decodeNode(value);
			
			// Initialize/Update the page rank of the Node.
			node.updatePageRank(context.getConfiguration());
			
			// Calculates the difference between old and new page rank and update sumOfPageRankDiff
			sumOfPageRankDiff += Math.abs(node.getPageRank() - node.getOldPageRank());
			
			// Emits nodeId with node structure
			context.write(node.getNodeIdAsText(), Utility.encodeNode(node, EncodingTypes.AL_PR));
			
			// Emits adjacentNodeId with page rank contribution or update local delta if this is a dangling node
			String[] adjacencyList = node.getAdjacencyList();
			if(null != adjacencyList && adjacencyList.length > 0)
			{
				Double pageRankContribution = node.getPageRank() / adjacencyList.length;
				
				for(String adjacentNodeId : adjacencyList)
				{
					context.write(new Text(adjacentNodeId), new Text(pageRankContribution.toString()));
				}
			}
			else
			{
				deltaLocal += node.getPageRank();
			}
		}
		
		/**
		 * This function emits the local delta and increments the sum of page rank difference global counter
		 * to measure the convergence
		 */
		@Override
		protected void cleanup(Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException 
		{
			super.cleanup(context);
			
			if(deltaLocal != 0.0)
				context.write(Utility.DELTA, new Text(deltaLocal.toString()));
			
			if(sumOfPageRankDiff != 0.0)
				context.getCounter(Counters.SUM_OF_PAGE_RANK_DIFF_BETWEEN_ITERATIONS).increment(Utility.convertDoubleToLong(sumOfPageRankDiff));
		}
	}
	
	/**
	 * Combiner class
	 * @author dspatel
	 */
	public static class PageRankCombiner extends Reducer<Text, Text, Text, Text> 
	{
		/**
		 * reduce function
		 * 1. Sums the page rank of a given key (nodeId)
		 * 2. Emits the node structure if found
		 * 3. Emits the pageRankSum of a given key (NodeId) after combining
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			Double pageRankSum = 0.0;
			
			for(Text val : values)
			{
				if(Utility.isEncodedNode(val))
				{
					// Emit the node structure
					context.write(key, val);
				}
				else
				{
					if(!"".equals(val.toString()))
					{
						// Combine the pageRank
						pageRankSum += Double.parseDouble(val.toString());
					}
				}
			}
			
			if(pageRankSum != 0.0)
			{
				// Emit the combined PageRank
				context.write(key, new Text(pageRankSum.toString()));
			}
		}
	}
	
	/**
	 * Reducer class
	 * @author dspatel
	 */
	public static class PageRankReducer extends Reducer<Text, Text, Text, NullWritable> 
	{
		/**
		 * reduce function
		 * 1. Sums the page rank of a given key (nodeId)
		 * 2. Gets the node structure and decode it to Node
		 * 3. If key is for delta then set the global counter else emit the encoded node
		 */
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			Double pageRankSum = 0.0;
			Node node = null;
			
			for(Text val : values)
			{
				if(Utility.isEncodedNode(val))
				{
					// Decode the Node from Text
					node = Utility.decodeNode(val,EncodingTypes.AL_PR);
					node.setNodeId(key);
					
					// Current pageRank is assigned to oldPageRank
					node.setOldPageRank(node.getPageRank());
				}
				else
				{
					if(!"".equals(val.toString()))
					{
						// Sum the page rank of a given key (nodeId)
						pageRankSum += Double.parseDouble(val.toString());
					}
				}
			}
			
			if(key.toString().equalsIgnoreCase(Utility.DELTA.toString())) 
			{
				// set delta to global counter to use in next iteration
				context.getCounter(Counters.DELTA).setValue(Utility.convertDoubleToLong(pageRankSum));
			}
			else if(null != node)
			{
				// set the new pageRank
				node.setPageRank(pageRankSum);
				
				// emit (nodeId, adjacencyList, newPageRank(not finalized), oldPageRank(to estimate convergence)) as Text
				context.write(Utility.encodeNode(node, EncodingTypes.NID_AL_PR_OPR), NullWritable.get());
			}
		}
	}
	
	/**
	 * This method executes the PageRankJob and returns job object if the
	 * job is successful else null.
	 * @param inputPath
	 * @param outputPath
	 * @param numberOfPages
	 * @param alpha
	 * @param delta
	 * @param iteration
	 * @return
	 * @throws Exception
	 */
	public Job runPageRankJob(String inputPath, String outputPath, Long numberOfPages, Double alpha, Double delta) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("NUMBER_OF_PAGES", numberOfPages.toString());
		conf.set("ALPHA", alpha.toString());
		conf.set("DELTA", delta.toString());

		Job job = Job.getInstance(conf, "PageRankJob");
		job.setJarByClass(PageRankJob.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(PageRankMapper.class);
		job.setCombinerClass(PageRankCombiner.class);
		job.setReducerClass(PageRankReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		if(job.waitForCompletion(true))
		{
			return job; 
		}
		
		return null;
	}
}