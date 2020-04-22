package src;

public class Producer 
{
	private static Producer instance;
	
	private Producer()
	{
	}
	
	public static Producer getInstance()
	{
		if (instance==null)
			instance =  new Producer();
		
		return instance;
	}
	
	public void pushMessage(Message message)
	{
		if (message == null)
		{
			System.out.println("Error: Null cannot be passed into the queue.");
			return;
		}
		QueueExt.getInstance().pushMessageToMainQueue(message);
	}
}
