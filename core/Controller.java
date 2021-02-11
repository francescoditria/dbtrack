package core;

public class Controller {

	Engine db=new Engine();
	
	public void scan(String username,String password, String host,String port,String database)
	{
		if(!db.init(username, password, host, port, database))
		{
			System.out.println("Connection error");
			return;
		}
		
		db.findIndividualTracker(database);
		
	}
	
}
