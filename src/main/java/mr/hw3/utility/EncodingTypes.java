package mr.hw3.utility;

/**
 * EncodingTypes are used to encode Node object to String and decode it back.
 * Node has four attributes and passing it to map to reduce and reduce to map in
 * next iteration we do not need all attributes. So using this Types we will specify
 * that what we need to encode and decode.
 * @author dspatel
 */
public enum EncodingTypes 
{
	// NodeId and AdjacencyList
	NID_AL,
	
	// NodeId, AdjacencyList and PageRank
	NID_AL_PR,
	
	// NodeId, AdjacencyList, PageRank and OldPageRank 
	NID_AL_PR_OPR,
	
	// AdjacencyList and PageRank
	AL_PR,
	
	// NodeId and PageRank
	NID_PR
}
