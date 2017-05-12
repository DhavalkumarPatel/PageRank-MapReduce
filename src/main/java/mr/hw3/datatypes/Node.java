package mr.hw3.datatypes;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;

import mr.hw3.utility.Utility;

/**
 * This class is used to store Node's attributes provides operations to be
 * performed on Node. 
 * @author dspatel
 */
public class Node
{	
	private String nodeId;
	private String adjacencyList;
    private Double pageRank = -1.0;
    
    // oldPageRank is used to calculate the pageRank change per iteration to measure convergence
    private Double oldPageRank = -1.0;
    
    public Node(String nodeId, String adjacencyList)
    {
    	this.nodeId = nodeId;
    	this.adjacencyList = adjacencyList;
    }
    
    public Node(Text nodeId, Text adjacencyList)
    {
    	this(nodeId.toString(), adjacencyList.toString());
    }
    
    public Node(String nodeId, String adjacencyList, Double pageRank)
    {
    	this(nodeId, adjacencyList);
    	this.pageRank = pageRank;
    }
    
    public Node(String nodeId, String adjacencyList, Double pageRank, Double oldPageRank)
    {
    	this(nodeId, adjacencyList, pageRank);
    	this.oldPageRank = oldPageRank;
    }
    
    public void setNodeId(Text nodeId) 
    {
		this.nodeId = nodeId.toString();
	}
    
    public void setPageRank(Double pageRank) 
    {
		this.pageRank = pageRank;
	}
    
    public void setOldPageRank(Double oldPageRank) 
    {
		this.oldPageRank = oldPageRank;
	}
    
    public String getNodeId() 
    {
		return nodeId;
	}
    
    public Text getNodeIdAsText() 
    {
		return new Text(nodeId);
	}
    
    public String getAdjacencyListAsString() 
    {
		return adjacencyList;
	}
    
    public Text getAdjacencyListAsText() 
    {
		return new Text(adjacencyList);
	}
    
    public String[] getAdjacencyList() 
    {
		return Utility.adjacencyListStringToArray(this.adjacencyList);
	}
    
    public Double getPageRank() 
    {
		return pageRank;
	}
    
    public Double getOldPageRank() 
    {
		return oldPageRank;
	}
    
    /**
     * This method used to update the pageRank of a Node.
     * If the pageRank of a Node is default then it initializes the pageRank
     * else it applies pageRank equation (alpha & delta) to the current value.
     * P(x) = alpha/N + (1-alpha)*(delta/N + sumOfIncomingPageRankCountributions) 
     * @param conf
     */
    public void updatePageRank(Configuration conf)
    {
    	// Total no of nodes/pages including dangling nodes
		Long numberOfPages = Long.parseLong(conf.get("NUMBER_OF_PAGES"));
    			
    	if(this.pageRank == -1.0)
    	{
    		// initialize the page rank value for first iteration
			this.pageRank = 1.0/numberOfPages;
			this.oldPageRank = this.pageRank;
    	}
    	else
    	{
    		// correct the page rank value by applying alpha & delta (dangling factor)
			Double alpha = Double.parseDouble(conf.get("ALPHA"));
			Double delta = Double.parseDouble(conf.get("DELTA"));
			this.pageRank = ((alpha/numberOfPages) + (1-alpha)*((delta/numberOfPages) + this.pageRank));
    	}
    }
}