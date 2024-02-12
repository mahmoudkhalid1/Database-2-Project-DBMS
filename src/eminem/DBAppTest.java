package eminem;

import java.util.Iterator;

import ds.bplus.BTree;

public class DBAppTest {

	public static void main(String[] args) throws DBAppException {
		DBApp dbApp = new DBApp();
		dbApp.init();
		String strTableName = "Student";
//		String strTableName = "DoubleIndex";

//create table tests
// create table with invalid types
// create more than one table
// try each type to be clustering

//		Hashtable<String, String> htblColNameType = new Hashtable();
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("age", "java.lang.Integer");
//		htblColNameType.put("date", "java.util.Date");
//		htblColNameType.put("gpa", "java.lang.Double");
//		htblColNameType.put("shape", "java.awt.Polygon");
//		htblColNameType.put("grad", "java.lang.Boolean");
//		dbApp.createTable(strTableName, "id", htblColNameType);

//insert tests
// insert more than one page
// check if insertion is done correctly
// check that the index is adjusted if any
// insert all types		

//		for (int i = 0; i < 210; i++) {
//		Hashtable htblColNameValue = new Hashtable();
//		htblColNameValue.put("id", new Integer(4));
//		htblColNameValue.put("name", new String("a"));
//		htblColNameValue.put("age", new Integer(10));
//		htblColNameValue.put("date", new Date(3000, 6, 10));
//////		System.out.println((new Date(2020, 11, 11).getClass()));
//////		System.out.println((new Date(2020, 11, 11)).toString());
//
////			htblColNameValue.put("gpa", new Double(2.0));
////		
////			if (4%2==0) {
////					htblColNameValue.put("grad", true);			
////			}
////			else			htblColNameValue.put("grad", false);
//		Polygon p = new Polygon();
//		p.addPoint(2, 3);
//		p.addPoint(1, 3);
////		 System.out.println("n:"+p.npoints);
//		htblColNameValue.put("shape", p);
////////
//		dbApp.insertIntoTable(strTableName, htblColNameValue);
//		}

		// update tests
//		Hashtable<String, Object> hash = new Hashtable();
//
//		Polygon p = new Polygon();
//		p.addPoint(2, 1);
//		p.addPoint(4, 5);
//		hash.put("shape", p);
//
//		hash.put("age", new Integer(99));
//		hash.put("name", new String("c"));
//		////// hash.put("gpa", new Double(0.6));
//		hash.put("date", new Date(1020, 06, 23));
//
//		 dbApp.updateTable(strTableName, "8", hash);
//
		try {
//			dbApp.createBTreeIndex(strTableName, "name");

//////			dbApp.createRTreeIndex(strTableName, "shape");
////////
			BTree a = (BTree) (dbApp.getDeserlaized("data//" + "BTree" + strTableName + "name" + ".class"));
			System.out.println(a.toString());
			a.serializeTree();
////////			RTree r = (RTree) (dbApp.getDeserlaized("data//" + "RTree" + strTableName + "shape" + ".class"));
////////			System.out.println(r.toString());
////////			r.serializeTree();
		} catch (Exception e) {
			System.out.println("error");
		}

//select tests
//		Polygon p = new Polygon();
//		p.addPoint(2, 4);
//		p.addPoint(1, 4);

		SQLTerm[] arrSQLTerms;
		arrSQLTerms = new SQLTerm[1];
		for (int i = 0; i < arrSQLTerms.length; i++) {
			arrSQLTerms[i] = new SQLTerm();
		}
		arrSQLTerms[0]._strTableName = strTableName;
		arrSQLTerms[0]._strColumnName = "name";
		arrSQLTerms[0]._strOperator = ">=";
		arrSQLTerms[0]._objValue = "a";

//		arrSQLTerms[1]._strTableName = strTableName;
//		arrSQLTerms[1]._strColumnName = "name";
//		arrSQLTerms[1]._strOperator = "=";
//		arrSQLTerms[1]._objValue = "d";

//		arrSQLTerms[2]._strTableName = strTableName;
//		arrSQLTerms[2]._strColumnName = "id";
//		arrSQLTerms[2]._strOperator = ">=";
//		arrSQLTerms[2]._objValue = new Integer(3);

		String[] strarrOperators = new String[0];
		// strarrOperators[0] = "XOR";
		// strarrOperators[1] = "AND";
//////////////////		// select * from Student where name = “John Noor” or gpa = 1.5; 

		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
		while (resultSet.hasNext()) {
			System.out.print(resultSet.next() + " ");
			System.out.println();
		}
		//////
		dbApp.displayTableContent(strTableName);
	}

//delete tests
	// check if the last tuple in the page is deleted then the whole page is deleted
	// check that the index in adjusted if any

//create index tests
	// if the col type is polygon then only an R tree index can be created on it

}
