package src;

import java.util.List;

/**
 * Thread started by Consumer to listen to each subscribed TopicQueue 
 * @author apebbati
 *
 */
public class ConsumerTopic implements Runnable
{
	private Consumer consumer;
	private TopicQueue topicQueue;
	private int lastReadMessageID=-1;
	
	ConsumerTopic(Consumer consumer, TopicQueue topicQueue)
	{
		this.consumer=consumer;
		this.topicQueue=topicQueue;
	}
	
	/**
	 * Consumes messages that were missed out during down time
	 */
	public void doReadHistory()
	{
		List<Message> history = topicQueue.getHistory();
		int index=0;
		
		if (lastReadMessageID != -1)
		{
			for (index=history.size()-1; index>=0; index--)
			{
				if ((history.get(index)).getMessageID() == lastReadMessageID)
				{
					index++;
					break;
				}
			}
		}
		
		int msgID;
		for (int i=index; i<history.size(); i++)
		{
			msgID=history.get(i).getMessageID();
			System.out.println(consumer + " has consumed message " + history.get(i).getMessageID() + 
					" from topic " + topicQueue.getTopic());
			lastReadMessageID=msgID;
		}
		
		System.out.println(consumer + " successfully consumed missed messages from topic " + topicQueue.getTopic());
	}
	
	/**
	 * Checks if a consumer is a dependent and supposed to run if it's turn to consume
	 * A non dependent consumer is always considered active.
	 * @return
	 */
	public boolean isActiveDependentConsumer()
	{
		if (topicQueue.hasDependency())
		{
			return topicQueue.getDependency().isActiveConsumer(consumer);
		}
		return true;
	}
	
	@Override
	public void run() 
	{
		Message message;
		while(QueueExt.getInstance().isRunning())
		{		
			while (QueueExt.getInstance().isRunning() && !isActiveDependentConsumer())
			{
				synchronized (topicQueue) 
				{
					try {
						topicQueue.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
			if (!consumer.isActive())
			{
				synchronized (consumer) 
				{
					try {
						synchronized (topicQueue) 
						{
							topicQueue.notifyAll();
						}
						consumer.wait();
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					doReadHistory();
				}
			}
			
			// Peeks into the queue only if the active message ID is more than last read message ID
			if (topicQueue.getActiveMessageID()>lastReadMessageID)
			{
				message = topicQueue.peek();
				System.out.println("Consumer " + consumer.getConsumerID() + " has consumed message " + message.getMessageID() + 
						" from topic " + topicQueue.getTopic());
				lastReadMessageID=message.getMessageID();
				if (topicQueue.hasDependency() && isActiveDependentConsumer())
				{
					topicQueue.getDependency().postConsumption(consumer);
					synchronized (topicQueue) 
					{
						topicQueue.notifyAll();
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
	
}
