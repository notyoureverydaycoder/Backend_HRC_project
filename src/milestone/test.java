package milestone;
import java.io.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
//import java.sql.ResultSet;
import java.sql.SQLException;
//import java.sql.Statement;
import java.util.ArrayList;
import java.util.Date;

//import com.Pojo.invoice_details;

import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;



public class test {
	static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";
	static final String DB_URL = "jdbc:mysql://localhost/h2h_internship";
	// Database credentials
	static final String USER = "root";
	static final String PASS = "Plaphoom123";
	public static void main(String[] args) {
		SimpleDateFormat formatter1 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
		SimpleDateFormat formatter2 = new SimpleDateFormat("yyyy-MM-dd");
		String csvFilePath="C:\\Users\\KIIT\\1803045.csv";
		
		
	try{
	
    
            ArrayList<invoice_details> arr = new ArrayList<>();
			BufferedReader lineReader=new BufferedReader(new FileReader(csvFilePath));
			String lineText=null;
			
			 
              lineReader.readLine(); 
 
			while((lineText=lineReader.readLine())!=null)
			{
				invoice_details obj = new invoice_details();
				String[] values=lineText.split(",");
				
				obj.setBusiness_code(values[0]);
				obj.setCust_number(values[1]);
				obj.setName_customer(values[2]);
				if(values[3].length() != 0)
				{
					Date value = formatter1.parse(values[3]);
					obj.setClear_date(value);
				}
				
				double year = Double.parseDouble(values[4]);
				obj.setBuisness_year((int)year);
				double id = Double.parseDouble(values[5]);
				obj.setDoc_id((long)id);
				
				Date value1 = formatter2.parse(values[6]);
				obj.setPosting_date(value1);
				Date value2 = formatter.parse(values[7]);
				obj.setDocument_create_date(value2);
				Date value4 = formatter.parse(values[9]);
				obj.setDue_in_date(value4);
				
				obj.setInvoice_currency(values[10]);	
				obj.setDocument_type(values[11]);
				
				double post = Double.parseDouble(values[12]);
				obj.setPosting_id((int)post);
				
				obj.setArea_business(values[13]);
				
				obj.setTotal_open_amount(Double.parseDouble(values[14]));
				
				Date value5 = formatter.parse(values[15]);
		 		obj.setBaseline_create_date(value5);
		 		
				obj.setCust_payment_terms(values[16]);
				if(values[17].length() != 0)
				{
					double invoice_id = Double.parseDouble(values[17]);
					obj.setInvoice_id((long)invoice_id);
				}
				
				double open = Double.parseDouble(values[18]);
				obj.setIsOpen((int)open);
				arr.add(obj);
			}	
				
			Connection con = null;
			PreparedStatement ps = null;
			String query = "insert into invoice_details values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

			try 
			{
				//STEP 3: Open a connection
				Class.forName("com.mysql.jdbc.Driver");
				con = DriverManager.getConnection(DB_URL, USER, PASS);
				//STEP 4: Execute a query
				ps = con.prepareStatement(query);
				
				int batchsize=400;
				int count=0;
			
				for(invoice_details j : arr)
				{
					ps.setString(1, j.getBusiness_code());
					ps.setString(2, j.getCust_number());
					ps.setString(3, j.getName_customer());
					if(j.getClear_date() != null) 
					{
						java.sql.Date cDate = new java.sql.Date(j.getClear_date().getTime());

						ps.setDate(4, cDate);
					}
					else
					{
						ps.setNull(4,  Types.NULL);
					}
					ps.setInt(5, j.getBuisness_year());
					ps.setLong(6, j.getDoc_id());
					
					java.sql.Date pDate = new java.sql.Date(j.getPosting_date().getTime());
					ps.setDate(7, pDate);
					java.sql.Date dDate = new java.sql.Date(j.getDocument_create_date().getTime());
					ps.setDate(8, dDate);
					java.sql.Date duDate = new java.sql.Date(j.getDue_in_date().getTime());
					ps.setDate(9, duDate);
					
					ps.setString(10, j.getInvoice_currency());
					ps.setString(11, j.getDocument_type());
					
					ps.setInt(12,j.getPosting_id());
					ps.setString(13, j.getArea_business());
					
					ps.setDouble(14,  j.getTotal_open_amount());
					
					java.sql.Date bDate = new java.sql.Date(j.getBaseline_create_date().getTime());
					ps.setDate(15, bDate);
					
					ps.setString(16, j.getCust_payment_terms());
					
					int len = String.valueOf(j.getInvoice_id()).length();
					if(len != 0)
					{
						ps.setLong(17, j.getInvoice_id());
					}
				
					ps.setInt(18, j.getIsOpen());
				
					ps.addBatch();
				
					count=count+1;
					if(count%batchsize==0)
					{
						ps.executeBatch();
						System.out.println("countvalue:"+count);
					
					}
				}
				
				ps.executeBatch();
				System.out.println("Congrats!");
			
			} 
			catch (SQLException e) {
				e.printStackTrace();
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}
			finally
			{
				try {
					ps.close();
					con.close();
				}	 
				catch (SQLException e) {
					e.printStackTrace();
				}
			}
				
				
		}
	catch (FileNotFoundException e) 
	{
		// TODO Auto-generated catch block
		e.printStackTrace();
	} 
	catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	} 
	catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	catch(ArrayIndexOutOfBoundsException e) {
		e.printStackTrace();
    } 
}
}