package src;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Dedicated queue for every topic. 
 * @author apebbati
 *
 */
public class TopicQueue extends LinkedBlockingQueue<Message> implements Runnable
{
	/**
	 * 'history' will contain the messages that are pushed out of the TopicQueue
	 * This will come in handy in case of a consumer failure
	 */
	private List<Message> history;
	private Map<Integer, Consumer> subscribedConsumers;
	private int peekCount=0;
	private String topic;
	private Dependency dependency;
	
	/**
	 * The message ID which is up for consumers to consume.
	 * Defaulted to -1
	 */
	private int activeMessageID=-1;
	
	public TopicQueue(int size, String topic) 
	{
		super(size);
		this.topic=topic;
		init();
	}
	private void init() 
	{
		this.history = new ArrayList<Message>();
		this.subscribedConsumers = new HashMap<Integer, Consumer>();
		this.dependency = null;	
	}
	
	@Override
	public Message poll() 
	{
		Message message = super.poll();
		history.add(message);
		
		return message;
	}
	
	/**
	 * Peek will increment peek count by default.
	 */
	@Override
	public Message peek() {
		return this.peek(true);
	}
	
	/**
	 * @param incrementPeekCount - To specify whether or not to increment peek count
	 * @return
	 */
	public Message peek(boolean incrementPeekCount) 
	{
		if (incrementPeekCount)
			incrementPeekCount();
		
		return super.peek();
	}
	
	public int getActiveMessageID() 
	{
		return activeMessageID;
	}
	
	public Dependency getDependency() {
		return dependency;
	}
	public List<Message> getHistory() 
	{
		return history;
	}
	public synchronized void incrementPeekCount()
	{
		peekCount++;
	}
	
	public String getTopic() {
		return topic;
	}
	
	public void resetStats()
	{
		peekCount=0;
		activeMessageID=-1; 
	}
	
	public void subscribeConsumer(Consumer consumer) 
	{
		synchronized (this) 
		{
			subscribedConsumers.put(consumer.getConsumerID(), consumer);
		}
	}
	
	public void unsubscribeConsumer(Consumer consumer) 
	{
		synchronized (this) 
		{
			subscribedConsumers.remove(consumer.getConsumerID());
		}
	}
	
	public Consumer getSubscribedConsumer(int consumerID)
	{
		return subscribedConsumers.get(consumerID);
	}
	
	public int getSubscriptionCount() 
	{
		synchronized (this) 
		{
			return subscribedConsumers.size();
		}
	}
	
	public void setDependency(String dependencyOrder)
	{
		this.dependency= new Dependency(dependencyOrder, this);
	}
	
	public void removeDependency()
	{
		this.dependency=null;
	}
	
	public boolean hasDependency()
	{
		if (this.dependency!=null && this.dependency.isDependencyActive())
			return true;
		else
			return false;
	}
	
	/**
	 * Checks if the queue head is outdated
	 */
	@Override
	public void run() 
	{
		while (QueueExt.getInstance().isRunning())
		{
			if (activeMessageID != -1)
			{
				synchronized (this) 
				{
					// If number of consumers peeked equals number of subscriptions, queue is polled.
					if (peekCount==this.getSubscriptionCount() && !this.isEmpty())
					{
						this.poll();
						resetStats();
					}
				}
			}
			else
			{
				synchronized (this) 
				{
					if (this.size() != 0)
					{
						// Resetting inactive 
						if (this.hasDependency())
							this.getDependency().resetDependencyStack();
						
						// We do not want to increment peek count yet.
						this.activeMessageID=this.peek(false).getMessageID();
					}
				}
				
			}
			
			try {
				Thread.sleep(QueueExt.getSleepPeriod());
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Override
	public String toString() {
		return this.topic + ": " + super.toString();
	}
}
