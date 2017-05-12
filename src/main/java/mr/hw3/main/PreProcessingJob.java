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
import org.apache.log4j.BasicConfigurator;

import mr.hw3.datatypes.Node;
import mr.hw3.utility.Bz2WikiParser;
import mr.hw3.utility.Counters;
import mr.hw3.utility.EncodingTypes;
import mr.hw3.utility.Utility;

/**
 * Pre-processing Job: This class parses input Wikipedia data into a graph represented as
 * adjacency lists. The page names are used as node and link IDs. Any path information from the
 * beginning, .html suffix from the end and pages and links containing a tilde (~) character 
 * are discarded.
 * @author dspatel
 *
 */
public class PreProcessingJob
{
	/**
	 * Mapper class with only map function
	 * @author dspatel
	 */
	public static class PreProcessingMapper extends Mapper<Object, Text, Text, Text> 
	{
		private Text emptyAdjacencyList = new Text();
		
		/**
		 * Map function parses each line to nodeId and adjacencyList graph representation.
		 * It emits the node with its adjacencyList as text and additionally it emits each 
		 * adjacent node from the adjacencyList because any of them can be a dangling node and
		 * we need to count them. Adjacent nodes are emitted with empty adjacencyList. 
		 */
		@Override
		public void map(Object key, Text line, Context context) throws IOException, InterruptedException 
		{
			if(null != line)
			{
				Node node = Bz2WikiParser.parse(line.toString());
				
				if(null != node)
				{
					// Emit nodeId with its adjacencyList
					context.write(node.getNodeIdAsText(), node.getAdjacencyListAsText());
						
					// Emit each adjacentNode with empty adjacencyList
					for(String adjacentNodeId : node.getAdjacencyList())
					{
						context.write(new Text(adjacentNodeId), emptyAdjacencyList);
					}
				}
			}
		}
	}
	
	/**
	 * Combiner class with only reduce function
	 * @author dspatel
	 */
	public static class PreProcessingCombiner extends Reducer<Text, Text, Text, Text> 
	{
		/**
		 * This method combines the adjacencyLists of a specific node and emits the 
		 * first non empty adjacencyList if found else emits empty adjacencyList against
		 * nodeId. We can have duplicate documents(lines) with same nodeId in input so only
		 * the first document(line) is considered for adjacencyList.
		 */
		public void reduce(Text nodeId, Iterable<Text> adjacencyListValues, Context context) throws IOException, InterruptedException 
		{
			Text adjacencyList = new Text();
			
			for(Text adjacencyListVal : adjacencyListValues)
			{
				if(!"".equals(adjacencyListVal.toString()))
				{
					// break the loop after getting the first non empty adjacencyListVal
					adjacencyList.set(adjacencyListVal);
					break;
				}
			}

			// emit nodeId with its adjacencyList
			context.write(nodeId, adjacencyList);
		}
	}
	
	/**
	 * Reducer class with setup, reduce and cleanup function
	 * It uses the in-mapper combining concept (in reducer) to calculate the total no of pages
	 * @author dspatel
	 */
	public static class PreProcessingReducer extends Reducer<Text, Text, Text, NullWritable> 
	{
		private long localNumberOfPages;
		
		/**
		 * Initialize the localNumberOfPages for reduce task
		 */
		@Override
		protected void setup(Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException 
		{
			super.setup(context);
			localNumberOfPages = 0;
		}
		
		/**
		 * This method combines the adjacencyLists of a specific node and emits the 
		 * first non empty adjacencyList if found else emits empty adjacencyList along
		 * with nodeId. We can have duplicate documents(lines) with same nodeId in input
		 * so only the first document(line) is considered for adjacencyList.
		 */
		public void reduce(Text nodeId, Iterable<Text> adjacencyListValues, Context context) throws IOException, InterruptedException 
		{
			Text adjacencyList = new Text();
			
			for(Text adjacencyListVal : adjacencyListValues)
			{
				if(!"".equals(adjacencyListVal.toString()))
				{
					adjacencyList.set(adjacencyListVal);
					break;
				}
			}

			// increment the number of pages
			localNumberOfPages++; 
			Node node = new Node(nodeId, adjacencyList);
			
			// encode the Node to Text and emit it.
			context.write(Utility.encodeNode(node, EncodingTypes.NID_AL), NullWritable.get());
		}
		
		/**
		 * This function increments the global counter by localNumberOfPages count.
		 */
		@Override
		protected void cleanup(Reducer<Text, Text, Text, NullWritable>.Context context) throws IOException, InterruptedException 
		{
			super.cleanup(context);
			context.getCounter(Counters.NUMBER_OF_PAGES).increment(localNumberOfPages);
		}
	}

	/**
	 * This method executes the PreProcessingJob and returns the job object if job
	 * is successful else null.
	 * @param inputPath
	 * @param outputPath
	 * @return
	 * @throws Exception
	 */
	public Job runPreProcessingJob(String inputPath, String outputPath) throws Exception
	{
		Configuration conf = new Configuration();
		BasicConfigurator.configure();
		
		Job job = Job.getInstance(conf, "PreProcessingJob");
		job.setJarByClass(PreProcessingJob.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(PreProcessingMapper.class);
		job.setCombinerClass(PreProcessingCombiner.class);
		job.setReducerClass(PreProcessingReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		if(job.waitForCompletion(true))
		{
			return job;
		}
		
		return null;
	}
}