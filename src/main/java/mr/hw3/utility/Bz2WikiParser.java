package mr.hw3.utility;

import java.io.StringReader;
import java.net.URLDecoder;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.xml.sax.Attributes;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

import mr.hw3.datatypes.Node;

/**
 * This class is provided along with HW3 problem and used to parse the input.
 * Below modifications are done in the given parser:
 * 		1. Removed duplicated nodes from adjacencyList.
 * 		2. Removed self links.
 * 		3. Fixed " & " issue while parsing
 * @author MR TA's
 */
public class Bz2WikiParser 
{
	private static Pattern namePattern;
	private static Pattern linkPattern;
	private static SAXParserFactory spf;
	private static SAXParser saxParser;
	private static XMLReader xmlReader;
	
	static 
	{
		// Keep only html pages not containing tilde (~).
		namePattern = Pattern.compile("^([^~]+)$");
		// Keep only html filenames ending relative paths and not containing tilde (~).
		linkPattern = Pattern.compile("^\\..*/([^~]+)\\.html$");
		
		// Configure parser.
		try
		{
			spf = SAXParserFactory.newInstance();
			spf.setFeature("http://apache.org/xml/features/nonvalidating/load-external-dtd", false);
			saxParser = spf.newSAXParser();
			xmlReader = saxParser.getXMLReader();
		}
		catch (Exception e) 
		{
			e.printStackTrace();
		}
	}
	
	/**
	 * It parses the line and returns nodeId and adjacencyList as a Node object.
	 * It returns null if input does not satisfy all requirements as per HW description.
	 * @param line
	 * @return ParsedOutput
	 */
	public static Node parse(String line)
	{
		int delimLoc = line.indexOf(':');
		
		// Parse nodeId
		String nodeId = line.substring(0, delimLoc);
		Matcher matcher = namePattern.matcher(nodeId);
		if (!matcher.find()) 
		{
			// Skip this html file, nodeId contains (~).
			return null;
		}
		
		// Parse adjacencyList
		String html = line.substring(delimLoc + 1);
		html = html.replaceAll(" & ", "&amp;");
		Set<String> adjacencyList = new HashSet<String>();
		xmlReader.setContentHandler(new WikiParser(adjacencyList));
		
		try 
		{
			xmlReader.parse(new InputSource(new StringReader(html)));
		} 
		catch (Exception e) 
		{
			// Discard ill-formatted pages.
			return null;
		}
		
		// remove self links
		adjacencyList.remove(nodeId);
		
		return new Node(nodeId, Utility.adjacencyListSetToString(adjacencyList));
	}

	/** Parses a Wikipage, finding links inside bodyContent div element. */
	private static class WikiParser extends DefaultHandler 
	{
		/** List of linked pages; filled by parser. */
		private Set<String> linkPageNames;
		/** Nesting depth inside bodyContent div element. */
		private int count = 0;

		public WikiParser(Set<String> linkPageNames) 
		{
			super();
			this.linkPageNames = linkPageNames;
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes attributes) throws SAXException 
		{
			super.startElement(uri, localName, qName, attributes);
			if ("div".equalsIgnoreCase(qName) && "bodyContent".equalsIgnoreCase(attributes.getValue("id")) && count == 0) 
			{
				// Beginning of bodyContent div element.
				count = 1;
			} 
			else if (count > 0 && "a".equalsIgnoreCase(qName)) 
			{
				// Anchor tag inside bodyContent div element.
				count++;
				String link = attributes.getValue("href");
				if (link == null) 
				{
					return;
				}
				try 
				{
					// Decode escaped characters in URL.
					link = URLDecoder.decode(link, "UTF-8");
				} 
				catch (Exception e) 
				{
					// Wiki-weirdness; use link as is.
				}
				// Keep only html filenames ending relative paths and not containing tilde (~).
				Matcher matcher = linkPattern.matcher(link);
				if (matcher.find()) 
				{
					linkPageNames.add(matcher.group(1));
				}
			} 
			else if (count > 0) 
			{
				// Other element inside bodyContent div.
				count++;
			}
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException 
		{
			super.endElement(uri, localName, qName);
			if (count > 0) 
			{
				// End of element inside bodyContent div.
				count--;
			}
		}
	}
}
