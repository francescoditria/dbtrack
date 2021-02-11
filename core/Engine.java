package core;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.mysql.jdbc.Connection;

public class Engine {

	Connection connection;	
	
	public void findIndividualTracker(String database) 
	{
		
        String[] types={"TABLE"};
		ResultSet rs;

		try {
			Statement statement=connection.createStatement();
			//rs = connection.getMetaData().getTables(database, null, "%", types);
	        String query="select TABLE_NAME, TABLE_ROWS from information_schema.TABLES where TABLE_SCHEMA='"+database+"' and TABLE_TYPE='BASE TABLE'";
	      	//System.out.println(query);
	        rs = statement.executeQuery(query);
			
	        int k=2;
	        int i=0;
            while (rs.next()) {
            	
            	String tableName=rs.getString("TABLE_NAME");
            	String tableRows=rs.getString("TABLE_ROWS");
            	//System.out.println(tableName+ "\t" +tableRows);
            	if(!(tableRows==null))
            	{
            		
            		//System.out.println("Target "+tableName+" [#"+tableRows+"]");
            		this.scanTable(database,tableName,tableRows);
            		
            	
            	}

            	/*
            	Long n=Long.parseLong(tableRows);            	
            	if((k<=n/2) && !(k>n/4))
        		{
        			//System.out.println("Target "+tableName+" [#"+tableRows+"]");
        		}
        		else
        		{
        			//System.out.println("Rejected "+tableName+" [#"+tableRows+"]");
        		}
            	*/
            	
            }
            
		}catch (SQLException e) {
            e.printStackTrace();
	 }

	}
	
	private void scanTable(String database,String tableName, String tableRows) throws SQLException
	{
		ResultSet ds = connection.getMetaData().getColumns(database, null,tableName, null);
		ArrayList al=new ArrayList();
		
		while (ds.next()) {
         	String columnName=ds.getString("COLUMN_NAME");
         	String columnType=ds.getString("TYPE_NAME");
         	//System.out.println("\t"+columnName+" ["+columnType+"]");
         	if((columnType.equals("VARCHAR")) && (!this.isNull(tableName, columnName)))
         	{
         			boolean t=this.ratioDistinct(tableName, columnName,tableRows);
         			if(t==true)
         				al.add(columnName);
         				
         	}         	
		}
		
		//find individual tracker
		this.getUniqueRecord(tableName,al);
		al.clear();		
		
	}
	
	private void getUniqueRecord(String tableName,ArrayList al) throws SQLException
	{
		int n=al.size();
		if((n==0) || (n==1)) return;
		
		
		int i;
		String group="";
		
		for(i=0;i<n;i++)
		{
			String columnName=(String) al.get(i);
			group=group+" "+columnName+","; 
		}
		String clause=group.substring(0, group.length()-1);
		//System.out.println(clause);
		
		String query="SELECT "+clause+",count(*) AS n FROM "+tableName+" GROUP BY "+clause+" HAVING n=1";
	    Statement statement=connection.createStatement();
	    ResultSet rs = statement.executeQuery(query);
	    
	    String queryA="";
	    String queryT="";
	    while(rs.next())
	    {
	    	//System.out.println(tableName);
			for(i=0;i<n;i++)
			{
				String fieldName=(String) al.get(i);
				String fieldValue=rs.getString(fieldName);
				//System.out.println("\t"+fieldName+"="+fieldValue);
				if(i==0)
				{
					queryA="select count(*) as n from "+tableName+" where "+fieldName+"='"+fieldValue+"'";
					queryT=queryA+" AND !(";
				}
				if(i>0 && i<n-1)
				{
					queryT=queryT+fieldName+"='"+fieldValue+"' AND ";
				}
				if(i==n-1)
				{
					queryT=queryT+fieldName+"='"+fieldValue+"')";
				}
				
			}
			//System.out.println(queryA);
			//System.out.println(queryT);
			long x=this.testQuery(queryA);
			long z=this.testQuery(queryT);
			if(x-z==1)
			{
				System.out.println("Found tracker:\n"+queryT);				
			}
	    }
		
	}
	
	private long testQuery(String query) throws SQLException
	{
	    Statement statement=connection.createStatement();
	    ResultSet rs = statement.executeQuery(query);
	    
	    rs.next();
		return rs.getLong("n");
	}
	
	private boolean ratioDistinct(String tableName,String columnName, String tableRows) throws SQLException
	{
		//distinct ratio
		
		Long n;
		Long rows=Long.parseLong(tableRows);
		String columnN="";
		
		String query="SELECT count(DISTINCT "+columnName+") as N from "+tableName;
	    Statement statement=connection.createStatement();
	    ResultSet rs = statement.executeQuery(query);
	    rs.next();
	    //String columnValue=rs.getString(columnName);
	    columnN=rs.getString("N");
	    //System.out.println("\t\t"+columnValue+" ["+columnN+"]");
	    n=Long.parseLong(columnN);
	    		
	    if((n>1) && (rows/n)>=2)
	    {
	    	//System.out.println(tableName+"."+columnName+" - distinct:"+n+", rows:"+rows);
	    	return true;
	    }
	    else
	    	return false;
	      	
	}

	
	
	private boolean isNull(String tableName,String columnName) throws SQLException
	{
		String query="select count(*) as n from "+tableName +" where "+columnName +" is null or length("+columnName+")=0";
	    Statement statement=connection.createStatement();
	    ResultSet rs = statement.executeQuery(query);
	    rs.next();
	    Long n=rs.getLong("n");
	    if(n>0)
	    	return true;
	    else
	    	return false;
		
	}

	
	public boolean init(String username,String password, String host,String port,String database)
	{
		String jdbc="jdbc:mysql://"+host+":"+port+"/"+database;

        try 
        {
			Class.forName("com.mysql.jdbc.Driver");
	        connection = (Connection) DriverManager.getConnection(jdbc, username, password);
		
        } catch (ClassNotFoundException | SQLException e) {
			//e.printStackTrace();
			return false;
		}
        
        return true;

	}
	
}
