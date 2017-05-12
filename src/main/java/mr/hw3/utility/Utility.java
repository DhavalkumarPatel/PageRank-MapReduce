package mr.hw3.utility;

import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;

import mr.hw3.datatypes.Node;

/**
 * This class provides different utility methods to all other programs.
 * It provides methods for encoding Node to Text and decoding it back and other 
 * conversion methods.
 * @author dspatel
 *
 */
public class Utility 
{
	private static final String TILD = "~";
	private static final String COLONS = "::";
	private static final Double NUMBER_TO_MUL_DIVIDE = 10000000000000.0;
	
	// key for passing delta from all map outputs to group in same reduce call.
	public static final Text DELTA = new Text("DELTA~DANGLING~FACTOR");
	
	/**
	 * This method encodes the given Node to Text as per given encoding type.
	 * @param node
	 * @param type
	 * @return
	 */
	public static Text encodeNode(Node node, EncodingTypes type)
	{
		Text encodedText = new Text();
    	switch (type) 
    	{
			case NID_AL:
				encodedText.set(node.getNodeId() + COLONS + node.getAdjacencyListAsString());
				break;
			case NID_AL_PR:
				encodedText.set(node.getNodeId() + COLONS + node.getAdjacencyListAsString() + COLONS + node.getPageRank());
				break;
			case NID_AL_PR_OPR:
				encodedText.set(node.getNodeId() + COLONS + node.getAdjacencyListAsString() + COLONS + node.getPageRank() + COLONS + node.getOldPageRank());
				break;
			case AL_PR:
				encodedText.set(node.getAdjacencyListAsString() + COLONS + node.getPageRank());
				break;
			case NID_PR:
				encodedText.set(node.getNodeId() + COLONS + node.getPageRank());
				break;
		}
    	return encodedText;
	}
	
	/**
	 * This method decodes the given Text to Node as per given encoding type
	 * @param encodedText
	 * @param type
	 * @return
	 */
	public static Node decodeNode(Text encodedText, EncodingTypes type)
	{
		Node node = null;
		String[] nodeArr = encodedText.toString().split(COLONS, -1);
		switch (type) 
    	{
			case NID_AL:
				node = new Node(nodeArr[0], nodeArr[1]);
				break;
			case NID_AL_PR:
				node = new Node(nodeArr[0], nodeArr[1], Double.parseDouble(nodeArr[2]));
				break;
			case NID_AL_PR_OPR:
				node = new Node(nodeArr[0], nodeArr[1], Double.parseDouble(nodeArr[2]), Double.parseDouble(nodeArr[3]));
				break;
			case AL_PR:
				node = new Node("", nodeArr[0], Double.parseDouble(nodeArr[1]));
				break;
			case NID_PR:
				node = new Node(nodeArr[0], "", Double.parseDouble(nodeArr[1]));
				break;
		}
		return node;
	}
	
	/**
	 * For the first iteration of map, this method helps to find the encoding type
	 * of a given encodedText and as per type it decodes the Node.
	 * @param encodedText
	 * @return
	 */
	public static Node decodeNode(Text encodedText)
	{
		if(encodedText.toString().split(COLONS, -1).length == 2)
		{
			return decodeNode(encodedText, EncodingTypes.NID_AL);
		}
		else
		{
			return decodeNode(encodedText, EncodingTypes.NID_AL_PR_OPR);
		}
	}
	
	/**
	 * This method converts the given adjacencyListString to array
	 * @param adjacencyListString
	 * @return
	 */
	public static String[] adjacencyListStringToArray(String adjacencyListString)
	{
		String[] adjacencyListArray = null;
		if(null != adjacencyListString)
		{
			adjacencyListArray = StringUtils.split(adjacencyListString, TILD);
		}
		return adjacencyListArray;
	}
	
	/**
	 * This method converts the given adjacencyListSet to String
	 * @param adjacencyListSet
	 * @return
	 */
	public static String adjacencyListSetToString(Set<String> adjacencyListSet)
	{
		String adjacencyListString = null;
		if(null != adjacencyListSet)
		{
			adjacencyListString = StringUtils.join(adjacencyListSet, TILD);
		}
		return adjacencyListString;
	}
	
	public static boolean isEncodedNode(Text text)
	{
		return text.toString().contains(COLONS);
	}
	
	/**
	 * This function converts Double to Long by multiplying it with constant value.
	 * I have not used Double's inbuilt method as we can get a right double number
	 * after adding to long conversions of doubles.
	 * @param d
	 * @return
	 */
	public static Long convertDoubleToLong(Double d)
	{
		d = d * NUMBER_TO_MUL_DIVIDE;
		return d.longValue();
	}
	
	/**
	 * This function converts Long to Double by dividing with constant value.
	 * @param l
	 * @return
	 */
	public static Double convertLongToDouble(Long l)
	{
		Double d = 0.0;
		if(null != l)
		{
			d = l.doubleValue()/NUMBER_TO_MUL_DIVIDE;
		}
		return d;
	}
}
