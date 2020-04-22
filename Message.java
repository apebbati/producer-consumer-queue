package src;

import java.util.HashMap;
import java.util.Map;

public class Message 
{
	private static int id ;
	/**
	 * Main content of the message.
	 */
	private Map json;
	private int messageID;
	private String topicString;
	
	public Message(String topic)
	{
		this.json = new HashMap();
		setMessageID();
		this.topicString = topic;
	}

	public Map getJSON() 
	{
		return json;
	}

	public String getTopicString() {
		return topicString;
	}

	public void setJSON(Map json) 
	{
		this.json = json;
	}
	
	private synchronized void setMessageID()
	{
		this.messageID = getNewID();
	}
	
	private synchronized static int getNewID()
	{
		return id++;
	}

	public int getMessageID() {
		return messageID;
	}
}
