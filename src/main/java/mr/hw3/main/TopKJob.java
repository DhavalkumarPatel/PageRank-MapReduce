package mr.hw3.main;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import mr.hw3.datatypes.MyTreeMap;
import mr.hw3.datatypes.Node;
import mr.hw3.utility.EncodingTypes;
import mr.hw3.utility.Utility;

/**
 * Top-k Job: From the output of the last PageRank iteration (correction job in our case), 
 * this class finds the k(100) pages with the highest PageRank and output them, along with their 
 * ranks, from highest to lowest.
 * @author dspatel
 *
 */
public class TopKJob 
{
	/**
	 * Mapper Class
	 * This class uses in-mapper combining concept to keep track of local topK pages
	 * @author dspatel
	 *
	 */
	public static class TopKMapper extends Mapper<Object, Text, NullWritable, Text> 
	{
		// MyTreeMap is used to keep track of local topK records which keeps two values with 
		// same key in list so handles duplicate page rank case.
		private MyTreeMap topRankNodes;
		private int k = 100;

		/**
		 * This setup function initializes MyTreeMap and k from context configuration
		 */
		@Override
		protected void setup(Mapper<Object, Text, NullWritable, Text>.Context context) throws IOException, InterruptedException 
		{
			super.setup(context);
			topRankNodes = new MyTreeMap();
			k = Integer.parseInt(context.getConfiguration().get("K"));
		}
		
		/**
		 * This map function keeps track of local top k page rank nodes using MyTreeMap.
		 */
		@Override
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
		{
			// Decode Node from Text
			Node node = Utility.decodeNode(value, EncodingTypes.NID_PR);
			topRankNodes.add(node.getPageRank(), node);
			
			if (topRankNodes.size() > k) 
			{
				// removes the lowest page rank node from the map
				topRankNodes.remove();
			}
		}

		/**
		 * cleanup function emits the local top k page rank nodes.
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException 
		{
			for (List<Node> nodes : topRankNodes.getMap().values()) 
			{
				for(Node node : nodes)
				{
					context.write(NullWritable.get(), Utility.encodeNode(node, EncodingTypes.NID_PR));
				}
			}
		}
	}
	
	/**
	 * Reducer Class
	 * @author dspatel
	 *
	 */
	public static class TopKReducer extends Reducer<NullWritable, Text, Text, NullWritable> 
	{
		/**
		 * This function keeps track of top k page rank nodes using MyTreeMap and after
		 * iterating all local map topk lists (values list), it emits the top k page rank 
		 * values (from highest to lowest) with nodeId (page name).
		 */
		@Override
		public void reduce(NullWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
		{
			// MyTreeMap is used to keep track of local topK records which keeps two values with 
			// same key in list so handles duplicate page rank case.
			MyTreeMap topRankNodes = new MyTreeMap();
			
			int k = Integer.parseInt(context.getConfiguration().get("K"));
			
			for (Text value : values) 
			{
				// decode the node from text
				Node node = Utility.decodeNode(value, EncodingTypes.NID_PR);
				topRankNodes.add(node.getPageRank(), node);
				
				if (topRankNodes.size() > k) 
				{
					// removes the lowest page rank node from the map
					topRankNodes.remove();
				}
			}

			// emit the top k page rank values (from highest to lowest) with nodeId (page name)
			for (List<Node> nodes : topRankNodes.getMap().descendingMap().values()) 
			{
				for(Node node : nodes)
				{
					context.write(Utility.encodeNode(node, EncodingTypes.NID_PR), NullWritable.get());
				}
			}
		}
	}

	/**
	 * This method executes the Map Only TopKJob
	 * @param inputPath
	 * @param outputPath
	 * @param k
	 * @return
	 * @throws Exception
	 */
	public boolean runTopKJob(String inputPath, String outputPath, Integer k) throws Exception
	{
		Configuration conf = new Configuration();
		conf.set("K", k.toString());
		
		Job job = Job.getInstance(conf, "TopKJob");
		job.setJarByClass(TopKJob.class);

		FileInputFormat.addInputPath(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setMapperClass(TopKMapper.class);
		job.setReducerClass(TopKReducer.class);
		
		// one reducer only
		job.setNumReduceTasks(1);
		
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		return job.waitForCompletion(true);
	}
}