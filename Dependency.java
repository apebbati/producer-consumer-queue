package src;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

public class Dependency extends Stack<Set<Consumer>>
{
	private TopicQueue topicQueue;
	private String dependencyOrder;
	private List<Set<Consumer>> dependencyOrderList;
	private Set<Consumer> activeDependencies;
	private boolean isDependencyActive;
	
	private final String PROCESS_AFTER="->";
	private final String SEPARATOR=",";
	
	public Dependency (String dependencyOrder, TopicQueue topicQueue)
	{
		super();
		this.dependencyOrder=dependencyOrder;
		this.topicQueue=topicQueue;
		init();
	}
	
	private void init()
	{
		activeDependencies = new HashSet<Consumer>();
		dependencyOrderList = new ArrayList<Set<Consumer>>();
		createDependencyOrderList();
	}
	
	private void initDependencyOrderList() 
	{
		isDependencyActive=false;
		dependencyOrderList.clear();
		dependencyOrder = dependencyOrder.trim().replace(" ", "");
	}
	
	/**
	 * Creates the dependency order list from the dependency order string.
	 */
	private void createDependencyOrderList() 
	{
		initDependencyOrderList();
		String[] order = dependencyOrder.split(PROCESS_AFTER);
		String[] consumerStr;
		Consumer consumer;
		Set<Consumer> set;
		for (int i=0; i<order.length; i++)
		{
			consumerStr=order[i].split(SEPARATOR);
			set = new HashSet<Consumer>();
			
			try
			{
				for (int j=0; j<consumerStr.length; j++)
				{
					consumer= topicQueue.getSubscribedConsumer(Integer.parseInt(consumerStr[j].trim()));
					if (consumer==null)
						continue;
					set.add(consumer);
				}
				if (!set.isEmpty())
					dependencyOrderList.add(set);
			} catch (Exception e)
			{
				System.out.println("Invalid dependency string entered. Try again.");
				this.isDependencyActive=false;
				return;
			}
		}
		this.isDependencyActive=true;
	}
	
	/**
	 * Initializes stack reset 
	 */
	private boolean initResetDependencyStack()
	{
		activeDependencies.clear();
		this.clear();
		
		if (dependencyOrderList==null)
			createDependencyOrderList();
		
		return isDependencyActive;
	}
	
	/**
	 * Puts 'dependencyOrderList' elements in stack, ignoring any inactive consumers
	 */
	public void resetDependencyStack() 
	{
		if (!initResetDependencyStack())
			return;		
		
		Set<Consumer> consumerSet;
		Consumer consumer;
		Iterator<Consumer> itr;
		for (Set<Consumer> set : dependencyOrderList)
		{
			consumerSet = new HashSet<Consumer>();
			itr = set.iterator();
			while (itr.hasNext())
			{
				consumer=itr.next();
				if (consumer!=null && consumer.isActive())
				{
					consumerSet.add(consumer);
					activeDependencies.add(consumer);
				}
			}
			if (!consumerSet.isEmpty())
				this.push(consumerSet);
		}
		
	}
	
	public String getDependencyOrder() 
	{
		return dependencyOrder;
	}
	
	/**
	 * Returns if a dependency is successful or not.
	 * @return
	 */
	public boolean isDependencyActive() 
	{
		return isDependencyActive;
	}

	public boolean isActiveConsumer(Consumer consumer)
	{
		if (this.isEmpty() || !this.activeDependencies.contains(consumer) || this.getActiveConsumerSet().contains(consumer))
			return true;
		else
			return false;
	}

	private Set<Consumer> getActiveConsumerSet()
	{
		if (this.isEmpty())
			return null;
		else
			return this.peek();
	}
	
	public synchronized void postConsumption(Consumer consumer) 
	{
		Set<Consumer> activeConsumerSet= getActiveConsumerSet();
		if (activeConsumerSet!=null)
		{
			activeConsumerSet.remove(consumer);
			if (activeConsumerSet.isEmpty())
				this.pop();
		}
		activeDependencies.remove(consumer);
	}
}
