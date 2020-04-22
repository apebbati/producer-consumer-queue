package src;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * The main queue implementation.
 * @author apebbati
 *
 */
public class QueueExt 
{
	private static QueueExt instance;
	
	/**
	 * Producer will put the incoming messages in here.
	 */
	private BlockingQueue<Message> mainQueue;
	private Map<String, TopicQueue> topicQueues;
	private List<Consumer> consumerList;
	private final String defaultTopic="DEFAULT";
	private int maxSize;
	private ExecutorService executorService;
	private boolean running = true;
	/**
	 * Sleep period constant for all the threads throughout the application
	 */
	private static long sleepPeriod = 50;
	
	private QueueExt(int maxSize)
	{
		this.maxSize=maxSize;
		init();
	}
	
	public static QueueExt getInstance()
	{
		while (instance==null)
		{
			createInstance();
		}
		return instance;
	}
	
	private static void createInstance() 
	{
		Scanner scanner = new Scanner(System.in);
		System.out.println("Enter the size of the queue.");
		try
		{
			int size = scanner.nextInt();
			if (size<1)
				throw new Exception("Value entered is less than or equal to zero");
			instance =  new QueueExt(size);
		} catch (Exception e) {
			System.out.println("Value entered is not valid. Enter an integer greater than zero.");
		}
	}

	public void init()
	{
		mainQueue = new LinkedBlockingQueue<Message>(maxSize);
		topicQueues = new HashMap<String, TopicQueue>();
		executorService = Executors.newCachedThreadPool();
		consumerList = new ArrayList<Consumer>();
		
		// Thread to push new elements in main queue to corresponding topic queues
		executorService.submit(new Runnable() {
			public void run() {
				QueueExt queueInstance = QueueExt.getInstance();
				Message message;
				while(queueInstance.isRunning())
				{
					if (queueInstance.getMainQueueSize()!=0)
					{
						message=queueInstance.pollMainQueue();
						queueInstance.putMessageToTopic(message);
					}
					
					try {
						Thread.sleep(queueInstance.getSleepPeriod());
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		});
	}
	
	public boolean isRunning() 
	{
		return running;
	}

	public static long getSleepPeriod() 
	{
		return sleepPeriod;
	}
	
	public int getMainQueueSize() 
	{
		return mainQueue.size();
	}
	
	public TopicQueue getTopicQueue(String topic)
	{
		synchronized (topicQueues) 
		{
			return topicQueues.get(topic);
		}
	}
	
	/**
	 * Create 'n' consumers iteratively
	 */
	public boolean createConsumers(int n)
	{
		if (n<1)
		{
			System.out.println("Enter a number greater than 0");
			return false;
		}
		
		for (int i=0; i<n; i++)
		{
			createConsumer();
		}
		return true;
	}
	
	/**
	 * Consumer is created and alloted a unique ID.
	 */
	public boolean createConsumer()
	{
		synchronized (consumerList) 
		{
			Consumer consumer = new Consumer(getConsumerCount());
			consumerList.add(consumer);
			
			System.out.println("Consumer with ID " + consumer.getConsumerID() + " created.");
		}
		return true;
	}
	
	/**
	 * Subscribes consumer to given string of topics
	 */
	public void subscribeConsumerTo(int consumerID, String[] topics)
	{
		for (int i=0; i<topics.length; i++)
		{
			subscribeConsumerToTopic(consumerID, topics[i]);
		}
	}
	
	/**
	 * Subscribes consumer to the given topic
	 */
	public void subscribeConsumerToTopic(int consumerID, String topic)
	{
		Consumer consumer = getConsumer(consumerID);
		if (consumer==null)
		{
			return;
		}
		
		if (!this.hasTopic(topic))
		{
			System.out.println("Topic " + topic + " doesn't exist.");
			return;
		}
		
		if (consumer.isRegisteredToTopic(topic))
		{
			System.out.println(consumer + " already registered to topic " + topic +".");
			return;
		}
		
		getTopicQueue(topic).subscribeConsumer(consumer);
		consumer.subscribeTopic(getTopicQueue(topic));
		System.out.println(consumer + " registered to topic '" + topic + "'.");
	}
	
	public void createTopics(String topics[])
	{
		for (int i=0; i<topics.length; i++)
		{
			createTopic(topics[i]);
		}
	}
	
	/**
	 * Creates a topic by creating a TopicQueue and 
	 * a runnable to update queue head 
	 * @param topicString
	 */
	public void createTopic(String topicString)
	{
		synchronized (topicQueues) 
		{
			if (isStringEmpty(topicString))
			{
				System.out.println("Empty string not allowed for topics. Enter a valid topic string.");
				return;
			}
			
			if (topicQueues.containsKey(topicString))
			{
				System.out.println("Topic '" + topicString + "' already exists. Choose a unique topic string.");
				return;
			}
			
			TopicQueue topicQueue = new TopicQueue(getMaxSize(), topicString);
			topicQueues.put(topicString, topicQueue);
			executorService.submit(topicQueue);
			System.out.println("Topic with string '" + topicString + "' created.");
			if (topicQueues.size()==1) // If this is the first topic to be created, create a default topic. Messages that do not match user created topics will go into default topics
			{
				createTopic(defaultTopic);
			}
		}
	}
	
	public Producer getProducer()
	{
		return Producer.getInstance();
	}
	
	public int getConsumerCount()
	{
		synchronized (consumerList) 
		{
			return consumerList.size();
		}
	}
	
	public Consumer getConsumer(int consumerID)
	{
		synchronized (consumerList) 
		{
			if (consumerID<0 || consumerID>=this.getConsumerCount())
			{
				System.out.println("Invalid consumer ID entered.");
				return null;
			}
			else
				return consumerList.get(consumerID);
		}
	}
	
	public Message peekMainQueue()
	{
		return (Message) mainQueue.peek();
	}
	
	public Message pollMainQueue()
	{
		return (Message) mainQueue.poll();
	}
	
	public int getMaxSize()
	{
		return this.maxSize;
	}
	
	public boolean hasTopic(String topic)
	{
		return topicQueues.containsKey(topic);
	}
	
	/**
	 * Producer will call this message to put the incoming messages into the main queue.
	 * @param message
	 * @return
	 */
	public synchronized void pushMessageToMainQueue(Message message)
	{
		try {
			mainQueue.put(message);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}
	
	public void setTopicDependency(String topic, String dependencyOrder)
	{
		if (topic.isEmpty() || !topicQueues.containsKey(topic))
		{
			System.out.println("Enter valid and existing topic.");
			System.out.println("Available Topics are: " + topicQueues.keySet().toString());
			return;
		}
		
		TopicQueue topicQueue = topicQueues.get(topic);
		if (topicQueue.hasDependency())
		{
			System.out.println("Topic " + topic + " already has an active dependency: " + topicQueue.getDependency().getDependencyOrder());
			System.out.println("Press 0 to replace existing dependency, others to not.");
			
			Scanner scan = new Scanner(System.in);
			if (!"0".equals(scan.nextLine().trim()))
					return;
		}
		
		topicQueue.setDependency(dependencyOrder);
		if (topicQueue.hasDependency())
			System.out.println("Topic " + topicQueue.getTopic() + " is set for dependency for order of consumers: " + topicQueue.getDependency().getDependencyOrder());
	}
	
	public boolean putMessageToTopic(Message message)
	{
		if (message!=null)
		{
			String topic = message.getTopicString();
			if (!this.hasTopic(topic)) // If the topic doesn't exists, the queue will put it in a "Default Topic" queue
			{
				topic=this.defaultTopic;
			}
			
			try {
				synchronized (topicQueues) 
				{
					topicQueues.get(topic).put(message);
					return true;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		
		return false;
	}
	
	/**
	 * To simulate fail case scenario
	 */
	public void enableConsumer(int consumerID)
	{
		Consumer consumer = getConsumer(consumerID);
		if (consumer!=null)
			consumer.setActive(true);
	}
	
	/**
	 * To simulate fail case scenario
	 */
	public void disableConsumer(int consumerID)
	{
		Consumer consumer = getConsumer(consumerID);
		if (consumer!=null)
			consumer.setActive(false);
	}

	/**
	 * Terminates all services and threads.
	 */
	public void shutdown()
	{
		this.running=false;
		stopWaitingThreads();
		executorService.shutdown();
		try {
			executorService.awaitTermination(1, TimeUnit.DAYS);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		shutDownConsumers();
	}
	
	/**
	 * Stops any waiting threads owing to dependency
	 */
	private void stopWaitingThreads() 
	{
		TopicQueue topicQueue;
		for (Map.Entry<String, TopicQueue> entry : topicQueues.entrySet())
		{
			topicQueue = entry.getValue();
			if (topicQueue.hasDependency())
			{
				synchronized (topicQueue) 
				{
					topicQueue.notifyAll();
				}
			}
				
		}
	}

	private void shutDownConsumers() 
	{
		for (Consumer consumer : consumerList)
		{
			consumer.shutdown();
		}
	}

	public boolean isStringEmpty(String str)
	{
		if (str == null || str.trim().equals(""))
			return true;
		else
			return false;
	}
}
