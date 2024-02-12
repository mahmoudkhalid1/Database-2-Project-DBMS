package ds.bplus;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Properties;

import eminem.DBAppException;

public class OverflowNode implements Serializable {
	public ArrayList <Object> referenceOfKeys;
	int nodeOrder;
	
	public OverflowNode() throws DBAppException {
		try {
			FileReader reader = new FileReader("config\\DBApp.properties");

			Properties p = new Properties();
			p.load(reader);

			nodeOrder = Integer.parseInt(p.getProperty("NodeSize"));
			
		} catch (IOException e) {
			throw new DBAppException("error in finding config file");
		}
		
		referenceOfKeys=new ArrayList<Object>();
	}
	
}
