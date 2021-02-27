package core;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.DatabaseMetaData;

public class Engine {

	Connection connection;	
	
	public void findTrackers(String database) 
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
         	
         	//search for categorical fields
         	if(((columnType.equals("VARCHAR"))||(columnType.equals("CHAR"))) && (!this.isPK(tableName, columnName)) && (!this.isNull(tableName, columnName)) && (this.ratioDistinct(tableName, columnName,tableRows)))
         	{
         				al.add(columnName);
         				
         	}         	
		}
		
	
		this.getIndividualTracker(tableName,al, tableRows);
		this.getGeneralTracker(tableName, al, tableRows);
		this.getDoubleTracker(tableName, al, tableRows);
	
		al.clear();		
		
	}

	

	
	private void getDoubleTracker(String tableName,ArrayList al, String tableRows) throws SQLException
	{
		int n=al.size();
		//devono essere almeno 2
		if(n<2) return;

		Long rows=Long.parseLong(tableRows);
	    long k=2;
	    float count=0;
	    
	    if(k>rows/3) return;

	    /////////////////////////////////////////////
		int i,j;
		String coli;
		String colj;
		String queryi;
		String queryj;
		String valuei;
		String valuej;
		String queryib;
		String queryjb;
		ArrayList<String> ali=new ArrayList();
		ArrayList<String> alj=new ArrayList();
		
		
		for(i=0;i<n-1;i++)
		{
			coli=(String) al.get(i);
			for(j=i+1;j<n;j++)
			{
				//estrai i valori distinti per ogni colonna i j
				colj=(String) al.get(j);
				//System.out.println("\ni="+coli+"\tj="+colj);
				queryi="select distinct "+coli+" from "+tableName;
				queryj="select distinct "+colj+" from "+tableName;
				
			    Statement statementi=connection.createStatement();
			    Statement statementj=connection.createStatement();
			    ResultSet rsi = statementi.executeQuery(queryi);
			    ResultSet rsj = statementj.executeQuery(queryj);
			    			    
			    while(rsi.next())
			    {
			    	valuei=rsi.getString(coli);
			    	//System.out.println(coli+"\t"+valuei);
			    	queryib="select i from (select *,@i:=@i+1 AS i from " +tableName+",(SELECT @i:=0) tmp) as T where "+coli+"='"+valuei+"'";
			    	//System.out.println(queryib);
				    Statement statementib=connection.createStatement();
				    ResultSet rsib = statementib.executeQuery(queryib);
				    ali.clear();
				    while(rsib.next())
				    {
				    	ali.add(rsib.getString("i"));
				    }
			    			
			    	rsj.beforeFirst();
				    while(rsj.next())
				    {
				    	valuej=rsj.getString(colj);
				    	//System.out.println(colj+"\t"+valuej);
				    	queryjb="select i from (select *,@i:=@i+1 AS i from " +tableName+",(SELECT @i:=0) foo) as T where "+colj+"='"+valuej+"'";
				    	//System.out.println(queryib+"\t"+queryjb);
					    Statement statementjb=connection.createStatement();
					    ResultSet rsjb = statementjb.executeQuery(queryjb);
					    alj.clear();
					    while(rsjb.next())
					    {
					    	alj.add(rsjb.getString("i"));
					    }

				    	//test subset
					    boolean t=this.testSubset(ali,alj,k,rows,tableName,coli,valuei,colj,valuej);
					    if(t) count++;
				    }


			    }



			}			
			
		}
		
	    float percent=count/rows*100;
		System.out.println("\nDouble Trackers\t"+tableName+"\t"+ Math.round(percent)+"%");


		
	}
	
	
	private boolean testSubset(ArrayList ali, ArrayList alj,long k, long rows,String tableName,String coli, String valuei, String colj, String valuej) throws SQLException
	{
		//System.out.println(coli+" "+valuei+" "+colj+" "+valuej);		
		
		String T="";
		String U="";
		
		if(ali.containsAll(alj))
		{
			//System.out.println("j C i");
			//System.out.println("j "+alj.toString()+" i "+ali.toString());
			T=colj+"='"+valuej+"'";
			U=coli+"='"+valuei+"'";			
			//System.out.println(T+" "+U);
			
		}		
		else if(alj.containsAll(ali))
		{
			//System.out.println("i C j");
			//System.out.println("i "+ali.toString()+" j "+alj.toString());
			U=colj+"='"+valuej+"'";
			T=coli+"='"+valuei+"'";			
			//System.out.println(T+" "+U);
		}
		else
			return false;

		
		String queryT="select count(*) as n from "+tableName+" where "+T;
		String queryU="select count(*) as n from "+tableName+" where "+U;
		//System.out.println(queryT);
		//System.out.println(queryU);

		long xT=this.testQuery(queryT);
		long xU=this.testQuery(queryU);
			
		long h;
		long k2;
		long h2;
		h=rows-2*k;
		k2=2*k;
		h2=rows-k;
		
		if(xT>=k && xT<=h)
		{
			if(xU>=k2 && xU<=h2)
			{
				long seck=xT+1;
				long sech=(rows-xT)-1;
				String secRange="none";
				if(seck<sech)
				{
					secRange="["+seck+","+sech+"]";
				}
				
				if(k<rows/3 || (k==rows/3 && xT==rows/3 && xU==(2*rows)/3))
				{
					System.out.println("\nDouble Tracker:\t\t["+tableName+"]");
					System.out.println("T:\t\t\t"+T);
					System.out.println("U:\t\t\t"+U);
					System.out.println("Vulnerability range:\t["+(k)+","+(rows-k)+"]");
					System.out.println("Security range:\t\t"+secRange);
					return true;
				}

			}
		}
		return false;
		
		


	}
	
	private void getGeneralTracker(String tableName,ArrayList al, String tableRows) throws SQLException
	{
		int n=al.size();
		if(n<1) return;

		Long rows=Long.parseLong(tableRows);
		float count = 0;
	    long k=2;
	    
	    if(k>rows/4) return;

		int i;	
		for(i=0;i<n;i++)
		{
			String columnName=(String) al.get(i);
			String query="SELECT "+columnName+",COUNT(*) as n from "+tableName+" GROUP BY "+columnName;
			//System.out.println(query);
		    Statement statement=connection.createStatement();
		    ResultSet rs = statement.executeQuery(query);
		    
		    
		    while(rs.next())
		    {
				String num=rs.getString("n");
				String fieldValue=rs.getString(columnName);
				
				Long m=Long.parseLong(num);
				if(m>=2*k && m<=rows-2*k)
				{
					if((k<rows/4) || (k==rows/4 && m==rows/2))
					{
						System.out.println("\nGeneral Tracker:\t["+tableName+"]\t"+columnName+"='"+fieldValue+"'");
						System.out.println("Vulnerability range:\t["+(k)+","+(rows-k)+"]");
						long ki=(m/2)+1;
						long ks=rows-ki;
						count++;
						if(ki<=ks)
						{
							System.out.println("Security range:\t\t["+((m/2)+1)+","+((rows-m/2)-1)+"]");
						}
						else
						{
							System.out.println("Security range:\t\tnone");	
						}
												
					}
				}

		    }
			 
		}
		
	    float percent=count/rows*100;
		System.out.println("\nGeneral Trackers\t"+tableName+"\t"+ Math.round(percent)+"%");

	}	
	
	private void getIndividualTracker(String tableName,ArrayList al, String tableRows) throws SQLException
	{
		int n=al.size();
		if((n==0) || (n==1)) return;
		
		Long rows=Long.parseLong(tableRows);
		float count=0;
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
	    String C="";
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
					C=fieldName+"='"+fieldValue+"' AND ";
				}
				if(i>0 && i<n-1)
				{
					queryT=queryT+fieldName+"='"+fieldValue+"' AND ";
					C=C+fieldName+"='"+fieldValue+"' AND ";					
				}
				if(i==n-1)
				{
					queryT=queryT+fieldName+"='"+fieldValue+"')";
					C=C+fieldName+"='"+fieldValue+"'";;					
				}
				
			}
			//System.out.println(queryA);
			//System.out.println(queryT);
			long x=this.testQuery(queryA);
			long z=this.testQuery(queryT);
			long k=2;
			if((x-z==1) && (x>=k) && (z<=rows-k))
			{
				//System.out.println("\nIndividual Tracker A:\n"+queryA);
				System.out.println("\nIndividual Tracker:\t"+queryT);
				System.out.println("Characteristic:\t\t["+tableName+"]\t"+C);
				this.getSecurityRange(x,z,k,rows);
				count++;
				
			}
	    }
	    
	    float percent=count/rows*100;
		System.out.println("\nIndividual Trackers\t"+tableName+"\t"+ Math.round(percent)+"%");

		
	}
	
	
	private void getSecurityRange(long x, long z, long k, long rows)
	{
		//System.out.println(x+" "+z);
		System.out.println("Vulnerability range:\t["+k+","+(rows-k)+"]");
		long min;
		if(x>z)
			min=z;
		else
			min=x;
		
		if((min+1)>rows/2)
		{
			System.out.println("Security range:\t\tnone");			
		}
		else
		{
			System.out.println("Security range:\t\t["+(min+1)+","+(rows-(min+1))+"]");	
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

	    //verify if the column determines a table partition
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

	
	private boolean isPK(String tableName,String columnName) throws SQLException
	{
		DatabaseMetaData meta;
	 	meta = (DatabaseMetaData) connection.getMetaData();
	 	ResultSet rs=meta.getPrimaryKeys(null, null, tableName);
	 	while(rs.next())
	 	{
	 		String pk= rs.getString(4);
	 		//System.out.println(pk);
	 		if(columnName.equals(pk))
	 		{
	 			return true;
	 		}
	 	}

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
