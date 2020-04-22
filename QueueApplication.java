package src;

import java.util.Random;
import java.util.Scanner;

/**
 * Driver application with main method.
 * @author apebbati
 *
 */
public class QueueApplication 
{
	public static void main(String[] args) 
	{
		//genericTest();
		genericTestRandomPush();
	}

	private static void genericTest() 
	{
		Scanner scanner = new Scanner(System.in);
		int input;
		String ip;
		QueueExt queue =  QueueExt.getInstance();
		while (true)
		{
			input = -1;
			System.out.println("0 - Exit");
			System.out.println("1 - Create consumer");
			System.out.println("2 - Create Topic");
			System.out.println("3 - Register Consumer to topic");
			System.out.println("4 - Push Message");
			System.out.println("5 - Fail case check: Disable a random consumer");
			System.out.println("6 - Fail case check: Enable the last disabled consumer");
			System.out.println("7 - Set dependency on a topic");
			
			try {
				ip = scanner.next();
				input = Integer.valueOf(ip);
			} catch (Exception e)
			{
				System.out.println("Invalid input");
			}

			if (input == 0)
			{
				queue.shutdown();
				break;
			} 
			else if (input == 1)
			{
				System.out.println("How many consumers do you want to create?");
				ip = scanner.next().trim();
				input = Integer.valueOf(ip);
				queue.createConsumers(input);
			}
			else if (input == 2)
			{
				System.out.println("Enter the topic string. Enter CSVs in case you want to create multiple topics");
				queue.createTopics(scanner.next().trim().split(","));
			}
			else if (input == 3)
			{
				System.out.println("Enter consumer ID");
				input = scanner.nextInt();
				System.out.println("Enter topic. Enter CSVs in case you want to subscribe to multiple topics");
				queue.subscribeConsumerTo(input, scanner.next().trim().split(","));
			}
			else if (input == 4)
			{
				System.out.println("Enter message topic. (If the entered topic doesn't exist, it will be directed to default topic)");
				queue.getProducer().pushMessage(new Message(scanner.next().trim()));
			}
			else if (input == 5)
			{
				System.out.println("Enter consumer's ID to be disabled.");
				input = scanner.nextInt();
				QueueExt.getInstance().disableConsumer(input);
			}
			else if (input == 6)
			{
				System.out.println("Enter consumer's ID to be enabled.");
				input = scanner.nextInt();
				QueueExt.getInstance().enableConsumer(input);
			}
			else if (input == 7)
			{
				System.out.println("Enter topic to set dependency on: ");
				String topic = scanner.next().trim();
				System.out.println("Enter dependency (Ex. 1->2,3 would mean that Consumer 1 should consumer after Consumer 2 and 3)" );
				String dependencyOrder = scanner.next().trim();
				QueueExt.getInstance().setTopicDependency(topic, dependencyOrder);
			}
			else
			{
				System.out.println("Invalid input");
			}
		}
	}

	private static void genericTestRandomPush() 
	{
		QueueExt queue =  QueueExt.getInstance();
		queue.createConsumers(5);
		
		String[] topics = "a,b,c,d,e".split(",");
		queue.createTopics(topics);
		
		queue.subscribeConsumerTo(0, "e,d,".split(","));
		queue.subscribeConsumerTo(1, "e,b".split(","));
		queue.subscribeConsumerTo(2, "e,a,b,c".split(","));
		queue.subscribeConsumerTo(3, "e,b,c".split(","));
		queue.subscribeConsumerTo(4, "e,a,d".split(","));
		//queue.setTopicDependency("e", "1,3->4,2->0");
		queue.setTopicDependency("e", " 4->3 ->2->1->0");
		//queue.setTopicDependency("b", "3->2->1");
		
		Scanner scanner = new Scanner(System.in);
		String ip = "";
		int input;
		Random r = new Random();
		int pushNumber = 10;
		
		while (true)
		{
			System.out.println("0 - Exit");
			System.out.println("1 - Create consumer");
			System.out.println("2 - Create Topic");
			System.out.println("3 - Register Consumer to topic");
			System.out.println("4 - Push Message");
			System.out.println("5 - Fail case check: Disable a consumer");
			System.out.println("6 - Fail case check: Enable a consumer");
			System.out.println("7 - Set dependency on a topic");
			System.out.println("Others - Push " + pushNumber + " messages.");
			
			
			ip = scanner.next().trim();

			if (ip.trim().equalsIgnoreCase("0"))
			{
				queue.shutdown();
				break;
			} 
			else if (ip.trim().equalsIgnoreCase("1"))
			{
				System.out.println("How many consumers do you want to create?");
				ip = scanner.next().trim();
				input = Integer.valueOf(ip);
				queue.createConsumers(input);
			}
			else if (ip.trim().equalsIgnoreCase("2"))
			{
				System.out.println("Enter the topic string. Enter CSVs in case you want to create multiple topics");
				queue.createTopics(scanner.next().trim().split(","));
			}
			else if (ip.trim().equalsIgnoreCase("3"))
			{
				System.out.println("Enter consumer ID");
				input = scanner.nextInt();
				System.out.println("Enter topic. Enter CSVs in case you want to subscribe to multiple topics");
				queue.subscribeConsumerTo(input, scanner.next().trim().split(","));
			}
			else if (ip.trim().equalsIgnoreCase("4"))
			{
				System.out.println("Enter message topic. (If the entered topic doesn't exist, it will be directed to default topic)");
				queue.getProducer().pushMessage(new Message(scanner.next().trim()));
			}
			else if (ip.trim().equalsIgnoreCase("5"))
			{
				System.out.println("Enter consumer's ID to be disabled.");
				input = scanner.nextInt();
				QueueExt.getInstance().disableConsumer(input);
			}
			else if (ip.trim().equalsIgnoreCase("6"))
			{
				System.out.println("Enter consumer's ID to be enabled.");
				input = scanner.nextInt();
				QueueExt.getInstance().enableConsumer(input);
			}
			else if (ip.trim().equalsIgnoreCase("7"))
			{
				System.out.println("Enter topic to set dependency on: ");
				String topic = scanner.next().trim();
				System.out.println("Enter dependency (Ex. 1->2,3 would mean that Consumer 1 should consumer after Consumer 2 and 3)" );
				String dependencyOrder = scanner.next().trim();
				QueueExt.getInstance().setTopicDependency(topic, dependencyOrder);
			}
			else
			{
				for (int i=0; i<pushNumber; i++)
				{
					queue.getProducer().pushMessage(new Message(topics[r.nextInt(topics.length)]));
				}
			}
		}
		
		queue.shutdown();
	}
}
