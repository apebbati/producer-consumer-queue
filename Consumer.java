package src;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Consumer
{
	private final int consumerID;
	private Map<String, TopicQueue> topicMap;
	private ExecutorService executorService;
	private boolean isActive=true;
	
	public Consumer(int consumerID)
	{
		this.consumerID = consumerID;
		init();
	}
	
	private void init()
	{
		executorService = Executors.newCachedThreadPool();
		topicMap = new HashMap<String, TopicQueue>();
	}
	
	/**
	 * Subscribes consumer to a topic and creates a thread to listen to the concerned TopicQueue
	 * @param topicQueue
	 */
	public void subscribeTopic(TopicQueue topicQueue)
	{
		synchronized (topicMap) 
		{
			topicMap.put(topicQueue.getTopic(), topicQueue);
			executorService.submit(new ConsumerTopic(this, topicQueue));
		}
	}
	
	public int getConsumerID() 
	{
		return consumerID;
	}

	public boolean isActive() {
		return isActive;
	}

	/**
	 * Sets consumer active/inactive
	 * @param isActive
	 */
	public void setActive(boolean isActive) 
	{
		if (this.isActive == isActive)
		{
			if (isActive)
				System.out.println(this + " already enabled.");
			else
				System.out.println(this + " already disabled.");
			
			return;
		}
		
		this.isActive = isActive;
		for (Map.Entry<String, TopicQueue> entry : topicMap.entrySet())
		{
			if(isActive)
				entry.getValue().subscribeConsumer(this);
			else
				entry.getValue().unsubscribeConsumer(this);
		}
		
		if (isActive)
		{
			synchronized (this) 
			{
				this.notifyAll();
			}
			System.out.println(this + " enabled.");
		}
		else
			System.out.println(this + " disabled.");
	}

	public boolean isRegisteredToTopic(String topic)
	{
		return topicMap.containsKey(topic);
	}

	/**
	 * Terminates all threads, waiting or otherwise.
	 */
	public void shutdown()
	{
		synchronized (this) 
		{
			this.notifyAll();
		}
		executorService.shutdown();
		try {
			executorService.awaitTermination(1, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public String toString() {
		return "Consumer " + this.consumerID;
	}
}
