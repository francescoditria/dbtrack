package core;

public class Controller {

	Engine db=new Engine();
	String version="1.4";
	
	public void scan(String username,String password, String host,String port,String database)
	{
		this.printIntro();
		System.out.println("Host " + host + ":" + port);
		System.out.println("User " + username);
		System.out.println("Database " + database);

		if(!db.init(username, password, host, port, database))
		{
			System.out.println("Connection error");
			return;
		}
		
		db.findTrackers(database);
		
	}
	
	private void printIntro()
	{
		System.out.println("dbtrack "+version);

	}
}
