package core;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Parser {

	public void getParameters(String target)
	{
		
		String username = "";
		String password="";
		String host = "";
		String port = "";
		String database = "";
		
		Pattern pattern;
		Matcher matcher;
		String operation="(.+)\\.(.+)@(.+):(.+)/(.+)";
		pattern=Pattern.compile(operation);
		matcher=pattern.matcher(target);
		while(matcher.find()){
			username=matcher.group(1);
			password=matcher.group(2);
			host=matcher.group(3);
			port=matcher.group(4);
			database=matcher.group(5);					
		}
		//System.out.println("Host " + host + ":" + port);
		//System.out.println("User " + username);
		//System.out.println("Database " + database);

		Controller controller=new Controller();
		controller.scan(username, password, host, port, database);

		
	}

}
