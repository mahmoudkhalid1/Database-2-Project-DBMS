package eminem;

import java.awt.Polygon;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Properties;
import java.util.Vector;

import ds.Rtree.RTree;
import ds.Rtree.RTreeOverflowNode;
import ds.Rtree.RTreeReferenceValues;
import ds.bplus.BTree;
import ds.bplus.OverflowNode;
import ds.bplus.ReferenceValues;

//import ds.bplus.bptree.SearchResult;
//import ds.bplus.util.InvalidBTreeStateException;

public class DBApp {

	public static int maxPageSize = initializePageSize();

	public static int initializePageSize() {
		try {
			int n = Page.getPageMaxSize();
			return n;
		} catch (DBAppException e) {
			System.out.print(e.getMessage());
			;
			return 0;
		}
	}

	public static int getNodeSize() throws DBAppException {
		int num;

		try {
			FileReader reader = new FileReader("config\\DBApp.properties");

			Properties p = new Properties();
			p.load(reader);

			return num = Integer.parseInt(p.getProperty("NodeSize"));
			// System.out.println(p.getProperty("password")); }
		} catch (IOException e) {
			throw new DBAppException("error in finding config file");
		}

	}

	// this method produces an array of column names with corresponding data types
	public static ArrayList<String> getArrayOfColoumnDataTyoe(String tableName) throws DBAppException {

		String csvFile = "data/metadata.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		ArrayList<String> arrColumn = new ArrayList<String>();
		try {

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

				// use comma as separator
				String[] d = line.split(cvsSplitBy);
				if (d[0].equals(tableName)) {

					arrColumn.add(d[1] + "," + d[2]);
				}
			}
			br.close();

		} catch (Exception e) {
			throw new DBAppException("error in getting column data types");
		}

//		System.out.println(arrColumn.toString());
		return arrColumn;
	}

//  this  does  whatever  initialization  you  would  like 
// or leave it empty if there is no code you want to 
// execute at application startup
	public void init() {

		try {
			maxPageSize = Page.getPageMaxSize();
			FileWriter writer = new FileWriter("data//metadata.csv", true);

			writer.append("Table Name");
			writer.append(',');
			writer.append("Column Name");
			writer.append(',');
			writer.append("Column Type");
			writer.append(',');
			writer.append("ClusteringKey");
			writer.append(',');
			writer.append("Indexed");
			writer.append(',');

			writer.append('\n');

		} catch (Exception e) {
			System.out.println("error in initialization");
		}
	}

	// this method check if there exists a table with this name
	public static boolean checkIfTableFound(String tableName) {

		File folder = new File("data");
		File[] listOfFiles = folder.listFiles();
		boolean flag = false;

		for (File file : listOfFiles) {
			if (file.isFile()) {
				if (file.getName().equals(tableName + ".class")) {
					flag = true;
					break;

				}
			}
		}
		return flag;
	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {

		boolean foundFile = checkIfTableFound(strTableName);
		if (foundFile == true) {
			throw new DBAppException("Table already existing");
		} else {
			Table t = new Table(strTableName, strClusteringKeyColumn, htblColNameType);

		}

	}

	public static Object getDeserlaized(String path) throws DBAppException {
		try {
			// Creating stream to read the object
			// System.out.println(path);
			FileInputStream file = new FileInputStream(path);
			ObjectInputStream in = new ObjectInputStream(file);
			Object a = in.readObject();
			in.close();
			file.close();
			// System.out.println("check");
			return a;
		} catch (Exception e) {
			throw new DBAppException("error in deserialization");
		}

	}

	public static String getPage(BTree a, Tuple nTuple) {
		String res = "";
		ArrayList<String> Strings1 = a.rangeMinSearchKeys((Comparable) nTuple.vtrTupleObj.get(nTuple.index));
		if (Strings1.size() != 0) {
			res = Strings1.get(0);
		}
		if (Strings1.size() == 0) {
			ArrayList<String> Strings2 = a.rangeMaxSearchKeys((Comparable) nTuple.vtrTupleObj.get(nTuple.index));
			res = Strings2.get(0);
		}
		return res;
	}

	public static String getPageR(RTree a, Tuple nTuple) {
		String res = "";
		ArrayList<String> Strings1 = a.rangeMinSearchKeys((Polygon) nTuple.vtrTupleObj.get(nTuple.index));
		if (Strings1.size() != 0) {
			res = Strings1.get(0);
		}
		if (Strings1.size() == 0) {
			ArrayList<String> Strings2 = a.rangeMaxSearchKeys((Polygon) nTuple.vtrTupleObj.get(nTuple.index));
			res = Strings2.get(0);
		}
		return res;
	}

	public static int neededPage(String pageName, String strTableName, Tuple nTuple) throws DBAppException {
		int page = -1;
		Table toBeInstertedIn = (Table) getDeserlaized("data//" + strTableName + ".class");
		int start = 0;
		for (int i = 0; i < toBeInstertedIn.usedPagesNames.size(); i++) {
			String pageNameCompare = toBeInstertedIn.usedPagesNames.get(i) + " ";
			if (pageNameCompare.contains(pageName)) {
				start = i;
				break;
			}
		}
		for (int i = start; i < toBeInstertedIn.usedPagesNames.size(); i++) {
			Page pageToBeInstertedIn = (Page) (getDeserlaized(
					"data//" + toBeInstertedIn.usedPagesNames.get(i) + ".class"));

			Vector<Tuple> Tuples = pageToBeInstertedIn.vtrTuples;

			int compare1 = (pageToBeInstertedIn.vtrTuples.lastElement()).compareTo(nTuple);

			int compare2 = (pageToBeInstertedIn.vtrTuples.get(0)).compareTo(nTuple);

			if (i == 0 && (compare2 >= 0)) {
				page = 0;
				break;
			}
			if (compare1 >= 0 && compare2 >= 0) {
				page = i;
				break;
			}
			if (compare1 >= 0 && compare2 <= 0) {
				page = i;
				break;
			}

		}
		// if nTuple does not fit in any page range if the last page is not full then
		// insert in it else create new page
		if (page == -1) {
			Page pageToBeInstertedIn = (Page) (getDeserlaized(
					"data//" + toBeInstertedIn.usedPagesNames.lastElement() + ".class"));

			if (pageToBeInstertedIn.vtrTuples.size() == maxPageSize) {
				toBeInstertedIn.createPage();
				page = toBeInstertedIn.usedPagesNames.size() - 1;
			} else {
				page = toBeInstertedIn.usedPagesNames.size() - 1;
			}

		}
		return page;
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {

		boolean TableFound = checkIfTableFound(strTableName);

		if (!TableFound) {
			throw new DBAppException("Table does not exist");
		} else {
			try {
				checkTypeSize(htblColNameValue, strTableName);

				Tuple nTuple = createTuple(htblColNameValue, strTableName);

				Table toBeInstertedIn = (Table) getDeserlaized("data//" + strTableName + ".class");

				Boolean Indexed = false;

				// checking if the table has an index
				if (toBeInstertedIn.usedIndicesNames.size() != 0 || toBeInstertedIn.usedRtreeNames.size() != 0) {
					Indexed = true;
				}
				Boolean ClusteringIndexed = false;

				// checking if one of the indexes is on the clustering column

				if (toBeInstertedIn.usedIndicescols.contains(getClusteringKey(strTableName))
						|| toBeInstertedIn.usedRtreeCols.contains(getClusteringKey(strTableName))) {
					ClusteringIndexed = true;
				}

				if (toBeInstertedIn.usedPagesNames.isEmpty()) {
					toBeInstertedIn.createPage();

					if (Indexed) {
						if (toBeInstertedIn.usedIndicesNames.size() != 0) {
							ArrayList<String> columns = getColNames(strTableName);
							for (int i = 0; i < toBeInstertedIn.usedIndicesNames.size(); i++) {
								BTree toUpdate = (BTree) getDeserlaized(
										"data//" + toBeInstertedIn.usedIndicesNames.elementAt(i) + ".class");
								String colName = toBeInstertedIn.usedIndicescols.elementAt(i);
								int colIndex = columns.indexOf(colName);
								Object key = nTuple.vtrTupleObj.get(colIndex);

								toUpdate.insert((Comparable) key, toBeInstertedIn.usedPagesNames.get(0));
								toUpdate.serializeTree();

							}
						}
						if (toBeInstertedIn.usedRtreeNames.size() != 0) {
							ArrayList<String> columns = getColNames(strTableName);
							for (int i = 0; i < toBeInstertedIn.usedRtreeNames.size(); i++) {
								RTree toUpdate = (RTree) getDeserlaized(
										"data//" + toBeInstertedIn.usedRtreeNames.elementAt(i) + ".class");
								String colName = toBeInstertedIn.usedRtreeCols.elementAt(i);
								int colIndex = columns.indexOf(colName);
								Object key = nTuple.vtrTupleObj.get(colIndex);

								toUpdate.insert((Polygon) key, toBeInstertedIn.usedPagesNames.get(0));
								toUpdate.serializeTree();

							}
						}

					}

					Page pageToBeInstertedIn = (Page) getDeserlaized(
							"data//" + toBeInstertedIn.usedPagesNames.get(0) + ".class");

					pageToBeInstertedIn.vtrTuples.add(nTuple);

					FileOutputStream f = new FileOutputStream(
							"data//" + toBeInstertedIn.usedPagesNames.get(0) + ".class");

					ObjectOutputStream bin = new ObjectOutputStream(f);

					bin.writeObject(pageToBeInstertedIn);
					bin.flush();

					bin.close();

					f.close();
				}

				else {

					Vector<String> usedPages = toBeInstertedIn.usedPagesNames;
					int page = -1;
					// System.out.println(ClusteringIndexed);
					if (ClusteringIndexed) {
						ArrayList<String> columns = getColNames(strTableName);
						int index = nTuple.index;
						String colname = columns.get(index);

						if (toBeInstertedIn.usedIndicescols.contains(colname)) {
							int i = toBeInstertedIn.usedIndicescols.indexOf(colname);
							BTree a = (BTree) getDeserlaized(
									"data//" + toBeInstertedIn.usedIndicesNames.elementAt(i) + ".class");
							String pagebyindex = getPage(a, nTuple);
							page = neededPage(pagebyindex, strTableName, nTuple);
							// System.out.println(pagebyindex+" here");
							System.out.println("page from btree: " + pagebyindex + ", page from neededpage: " + page);
						}
						if (toBeInstertedIn.usedRtreeCols.contains(colname)) {
							int i = toBeInstertedIn.usedRtreeCols.indexOf(colname);
							RTree a = (RTree) getDeserlaized(
									"data//" + toBeInstertedIn.usedRtreeNames.elementAt(i) + ".class");
							String pagebyindex = getPageR(a, nTuple);
							page = neededPage(pagebyindex, strTableName, nTuple);
							// System.out.println(pagebyindex+" here");
						}

					}
					// System.out.println(page);
					// searching in which page the nTuple will fit in it's range
					for (int i = 0; i < usedPages.size() && page == -1; i++) {
						Page pageToBeInstertedIn = (Page) (getDeserlaized(
								"data//" + toBeInstertedIn.usedPagesNames.get(i) + ".class"));

						Vector<Tuple> Tuples = pageToBeInstertedIn.vtrTuples;

						int compare1 = (pageToBeInstertedIn.vtrTuples.lastElement()).compareTo(nTuple);

						int compare2 = (pageToBeInstertedIn.vtrTuples.get(0)).compareTo(nTuple);

						// System.out.println(compare1);
						// System.out.println(compare2);
						if (i == 0 && (compare2 >= 0)) {
							page = 0;
							break;
						}
						if (compare1 >= 0 && compare2 >= 0) {
							page = i;
							break;
						}
						if (compare1 >= 0 && compare2 <= 0) {
							page = i;
							break;
						}

					}
					// System.out.println(page);
					// if nTuple does not fit in any page range if the last page is not full then
					// insert in it else create new page
					if (page == -1) {
						Page pageToBeInstertedIn = (Page) (getDeserlaized(
								"data//" + toBeInstertedIn.usedPagesNames.lastElement() + ".class"));

						if (pageToBeInstertedIn.vtrTuples.size() == maxPageSize) {
							toBeInstertedIn.createPage();
							page = toBeInstertedIn.usedPagesNames.size() - 1;
						} else {
							page = toBeInstertedIn.usedPagesNames.size() - 1;
						}

					}
//					System.out.println(page);

					// if the table has index insert the nTuple with the page found in the index
					if (Indexed) {
						if (toBeInstertedIn.usedIndicesNames.size() != 0) {
							ArrayList<String> columns = getColNames(strTableName);
							for (int i = 0; i <= toBeInstertedIn.usedIndicesNames.size() - 1; i++) {
								BTree toUpdate = (BTree) getDeserlaized(
										"data//" + toBeInstertedIn.usedIndicesNames.elementAt(i) + ".class");
								String colName = toBeInstertedIn.usedIndicescols.elementAt(i);
								int colIndex = columns.indexOf(colName);
								Object key = nTuple.vtrTupleObj.get(colIndex);
								System.out.println(key + " in page " + page);
								if (toBeInstertedIn.usedPagesNames.size() == page) {
									toBeInstertedIn.createPage();
								}
								toUpdate.insert((Comparable) key, toBeInstertedIn.usedPagesNames.get(page));

								toUpdate.serializeTree();

							}
						}
						if (toBeInstertedIn.usedRtreeNames.size() != 0) {
							ArrayList<String> columns = getColNames(strTableName);
							for (int i = 0; i <= toBeInstertedIn.usedRtreeNames.size() - 1; i++) {
								RTree toUpdate = (RTree) getDeserlaized(
										"data//" + toBeInstertedIn.usedRtreeNames.elementAt(i) + ".class");
								String colName = toBeInstertedIn.usedRtreeCols.elementAt(i);
								int colIndex = columns.indexOf(colName);
								Object key = nTuple.vtrTupleObj.get(colIndex);
								if (toBeInstertedIn.usedPagesNames.size() == page) {
									toBeInstertedIn.createPage();
								}
								toUpdate.insert((Polygon) key, toBeInstertedIn.usedPagesNames.get(page));

								toUpdate.serializeTree();
							}
						}
					}

					Page pageToBeInstertedIn0 = (Page) (getDeserlaized(
							"data//" + toBeInstertedIn.usedPagesNames.get(page) + ".class"));

					Vector<Tuple> Tuples0 = pageToBeInstertedIn0.vtrTuples;

					// get the index in page in which tuple should be inserted binary search
					int tupleindex = TuplebinarySearch(pageToBeInstertedIn0, 0,
							(pageToBeInstertedIn0.vtrTuples.size()) - 1, nTuple);

					// get the index in page in which tuple should be inserted non binary if not
					// found binary
					if (tupleindex == -1) {
						int j = 0;

						for (int i = 0; i <= Tuples0.size() - 1; i++) {
							Tuple TuplesinPage = Tuples0.get(i);

							int compare = TuplesinPage.compareTo(nTuple);

							if (compare < 0) {
								j++;
							}
						}
						tupleindex = j;
					}

					if (tupleindex > 0) {
						tupleindex = tupleindex - 1;
					}
					// inserting the new tuple in it's place and bubbling the max tuple in page
					for (int j = tupleindex; j <= Tuples0.size() - 1; j++) {
						Tuple TuplesinPage = Tuples0.get(j);

						int compare = TuplesinPage.compareTo(nTuple);

						if (compare > 0) {
							Tuple temp = Tuples0.get(j);

							pageToBeInstertedIn0.vtrTuples.remove(j);

							pageToBeInstertedIn0.vtrTuples.insertElementAt(nTuple, j);

							nTuple = temp;
						}
					}
					boolean flag2 = true;

					// if there is a place in the same page no ref will be updated
					if (pageToBeInstertedIn0.vtrTuples.size() < maxPageSize) {
						pageToBeInstertedIn0.vtrTuples.add(nTuple);

						flag2 = false;
					}
					// if there is no place the last tuple ref should change to the next page
					if (flag2) {
						if (Indexed) {
							if (toBeInstertedIn.usedIndicesNames.size() != 0) {
								ArrayList<String> columns = getColNames(strTableName);
								for (int i = 0; i < toBeInstertedIn.usedIndicesNames.size(); i++) {
									BTree toUpdate = (BTree) getDeserlaized(
											"data//" + toBeInstertedIn.usedIndicesNames.elementAt(i) + ".class");
									String colName = toBeInstertedIn.usedIndicescols.elementAt(i);
									int colIndex = columns.indexOf(colName);
									Object key = nTuple.vtrTupleObj.get(colIndex);
									if (toBeInstertedIn.usedPagesNames.size() - 1 != page)
										toUpdate.update((Comparable) key, toBeInstertedIn.usedPagesNames.get(page),
												toBeInstertedIn.usedPagesNames.get(page + 1));
									toUpdate.serializeTree();

								}
							}
						}
						if (toBeInstertedIn.usedRtreeNames.size() != 0) {
							ArrayList<String> columns = getColNames(strTableName);
							for (int i = 0; i < toBeInstertedIn.usedRtreeNames.size(); i++) {
								RTree toUpdate = (RTree) getDeserlaized(
										"data//" + toBeInstertedIn.usedRtreeNames.elementAt(i) + ".class");
								String colName = toBeInstertedIn.usedRtreeCols.elementAt(i);
								int colIndex = columns.indexOf(colName);
								Object key = nTuple.vtrTupleObj.get(colIndex);
								if (toBeInstertedIn.usedPagesNames.size() - 1 != page)
									toUpdate.update((Polygon) key, toBeInstertedIn.usedPagesNames.get(page),
											toBeInstertedIn.usedPagesNames.get(page + 1));
								toUpdate.serializeTree();

							}
						}
					}
					FileOutputStream f1 = new FileOutputStream("data//" + pageToBeInstertedIn0.pageName + ".class");

					ObjectOutputStream bin1 = new ObjectOutputStream(f1);

					bin1.writeObject(pageToBeInstertedIn0);

					FileOutputStream f3 = new FileOutputStream("data//" + strTableName + ".class");

					ObjectOutputStream bin3 = new ObjectOutputStream(f3);

					bin3.writeObject(toBeInstertedIn);

					if (flag2) {
						int flag = 0;

						for (int i = page + 1; i <= usedPages.size() - 1 && flag == 0; i++) {

							Page pageToBeInstertedIn = (Page) (getDeserlaized(
									"data//" + toBeInstertedIn.usedPagesNames.get(i) + ".class"));

							Vector<Tuple> Tuples = pageToBeInstertedIn.vtrTuples;

							for (int j = 0; j < Tuples.size(); j++) {
								Tuple TuplesinPage = Tuples.get(j);

								int compare = TuplesinPage.compareTo(nTuple);

								if (compare > 0) {
									Tuple temp = Tuples.get(j);

									pageToBeInstertedIn.vtrTuples.remove(j);

									pageToBeInstertedIn.vtrTuples.insertElementAt(nTuple, j);

									nTuple = temp;
								}
							}

							if (pageToBeInstertedIn.vtrTuples.size() < maxPageSize
									&& i != toBeInstertedIn.usedPagesNames.size() - 1) {
								Page nextPage = (Page) (getDeserlaized(
										"data//" + toBeInstertedIn.usedPagesNames.get(i + 1) + ".class"));

								int compare = (nextPage.vtrTuples.get(0)).compareTo(nTuple);

								if (compare >= 0) {
									pageToBeInstertedIn.vtrTuples.add(nTuple);

									nTuple = null;

									flag = 1;
								}

							}
							{
								if (flag == 0) {
									if (Indexed) {
										if (toBeInstertedIn.usedIndicesNames.size() != 0) {
											ArrayList<String> columns = getColNames(strTableName);
											for (int k = 0; k < toBeInstertedIn.usedIndicesNames.size(); k++) {
												BTree toUpdate = (BTree) getDeserlaized("data//"
														+ toBeInstertedIn.usedIndicesNames.elementAt(k) + ".class");
												String colName = toBeInstertedIn.usedIndicescols.elementAt(k);
												int colIndex = columns.indexOf(colName);
												Object key = nTuple.vtrTupleObj.get(colIndex);
												if (i != usedPages.size() - 1) {
													toUpdate.update((Comparable) key,
															toBeInstertedIn.usedPagesNames.get(i),
															toBeInstertedIn.usedPagesNames.get(i + 1));
													toUpdate.serializeTree();
												}

											}
										}
									}
									if (toBeInstertedIn.usedRtreeNames.size() != 0) {
										ArrayList<String> columns = getColNames(strTableName);
										for (int k = 0; k < toBeInstertedIn.usedRtreeNames.size(); k++) {
											RTree toUpdate = (RTree) getDeserlaized(
													"data//" + toBeInstertedIn.usedRtreeNames.elementAt(k) + ".class");
											String colName = toBeInstertedIn.usedRtreeCols.elementAt(k);
											int colIndex = columns.indexOf(colName);
											Object key = nTuple.vtrTupleObj.get(colIndex);
											if (i != usedPages.size() - 1) {
												toUpdate.update((Polygon) key, toBeInstertedIn.usedPagesNames.get(i),
														toBeInstertedIn.usedPagesNames.get(i + 1));
												toUpdate.serializeTree();
											}

										}
									}
								}
							}
							ObjectOutputStream bin = new ObjectOutputStream(
									new FileOutputStream("data//" + toBeInstertedIn.usedPagesNames.get(i) + ".class"));

							bin.writeObject(pageToBeInstertedIn);

							bin.flush();

							bin.close();
						}

						if (nTuple != null) {
							Page lastPage = (Page) (getDeserlaized("data//"
									+ toBeInstertedIn.usedPagesNames.get(toBeInstertedIn.usedPagesNames.size() - 1)
									+ ".class"));

							if (lastPage.vtrTuples.size() < maxPageSize) {
								lastPage.vtrTuples.add(nTuple);

								lastPage.vtrTuples.sort(null);

								ObjectOutputStream bin = new ObjectOutputStream(new FileOutputStream("data//"
										+ toBeInstertedIn.usedPagesNames.get(toBeInstertedIn.usedPagesNames.size() - 1)
										+ ".class"));

								bin.writeObject(lastPage);

								bin.flush();

								bin.close();

							} else if (lastPage.vtrTuples.size() == maxPageSize) {
								toBeInstertedIn.createPage();

								Page p = (Page) (getDeserlaized("data//"
										+ toBeInstertedIn.usedPagesNames.get(toBeInstertedIn.usedPagesNames.size() - 1)
										+ ".class"));

								p.vtrTuples.add(nTuple);
								if (Indexed) {
									if (toBeInstertedIn.usedIndicesNames.size() != 0) {
										ArrayList<String> columns = getColNames(strTableName);
										for (int k = 0; k < toBeInstertedIn.usedIndicesNames.size(); k++) {
											BTree toUpdate = (BTree) getDeserlaized("data//"
													+ toBeInstertedIn.usedIndicesNames.elementAt(k) + ".class");
											String colName = toBeInstertedIn.usedIndicescols.elementAt(k);
											int colIndex = columns.indexOf(colName);
											Object key = nTuple.vtrTupleObj.get(colIndex);
											toUpdate.update((Comparable) key,
													toBeInstertedIn.usedPagesNames.get(usedPages.size() - 2),
													toBeInstertedIn.usedPagesNames.get(usedPages.size() - 1));
											toUpdate.serializeTree();

										}
									}
									if (toBeInstertedIn.usedRtreeNames.size() != 0) {
										ArrayList<String> columns = getColNames(strTableName);
										for (int k = 0; k < toBeInstertedIn.usedRtreeNames.size(); k++) {
											RTree toUpdate = (RTree) getDeserlaized(
													"data//" + toBeInstertedIn.usedRtreeNames.elementAt(k) + ".class");
											String colName = toBeInstertedIn.usedRtreeCols.elementAt(k);
											int colIndex = columns.indexOf(colName);
											Object key = nTuple.vtrTupleObj.get(colIndex);
											toUpdate.update((Polygon) key,
													toBeInstertedIn.usedPagesNames.get(usedPages.size() - 2),
													toBeInstertedIn.usedPagesNames.get(usedPages.size() - 1));
											toUpdate.serializeTree();

										}
									}
								}

								ObjectOutputStream bin = new ObjectOutputStream(new FileOutputStream("data//"
										+ toBeInstertedIn.usedPagesNames.get(toBeInstertedIn.usedPagesNames.size() - 1)
										+ ".class"));

								bin.writeObject(p);

								bin.flush();

								bin.close();
							}
						}
					}
				}
				FileOutputStream f2 = new FileOutputStream("data//" + strTableName + ".class");

				ObjectOutputStream bin2 = new ObjectOutputStream(f2);

				bin2.writeObject(toBeInstertedIn);

			} catch (IOException e) {
				throw new DBAppException("error in insertion");
			}
		}
	}

	public static int TuplebinarySearch(Page p, int first, int last, Tuple key) {
		int mid = (first + last) / 2;
		int pos = -1;
		while (first <= last) {
			int compare = (p.vtrTuples.get(mid)).compareTo(key);
			if (compare < 0) {
				first = mid + 1;
			} else if (compare == 0) {
				// System.out.println("Element is found at index: " + mid);
				pos = mid;
				break;
			} else {
				last = mid - 1;
			}
			mid = (first + last) / 2;
		}
		if (first > last) {
			// System.out.println("Element is not found!");

		}
		return pos;
	}

	// following method inserts one row at a time
	public void insertIntoTable2(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {

		boolean found = checkIfTableFound(strTableName);

		if (!found) {
			System.out.print("Table does not exist");
		} else {
			try {

				checkTypeSize(htblColNameValue, strTableName);
				Tuple nTuple = createTuple(htblColNameValue, strTableName);
				// System.out.println(nTuple.vtrTupleObj.toString());
				Table toBeInstertedIn = (Table) getDeserlaized("data//" + strTableName + ".class");
				if (toBeInstertedIn.usedPagesNames.isEmpty()) {
					// System.out.println("wpw");
					toBeInstertedIn.createPage();
					Page pageToBeInstertedIn = (Page) getDeserlaized(
							"data//" + toBeInstertedIn.usedPagesNames.get(0) + ".class");
					pageToBeInstertedIn.vtrTuples.add(nTuple);

					// System.out.print(pageToBeInstertedIn.vtrTuples.get(0).vtrTupleObj);
					FileOutputStream f = new FileOutputStream(
							"data//" + toBeInstertedIn.usedPagesNames.get(0) + ".class");
					ObjectOutputStream bin = new ObjectOutputStream(f);
					// System.out.print(nTuple);
					bin.writeObject(pageToBeInstertedIn);
					bin.flush();
					bin.close();
					f.close();
				} else {
					Vector<String> usedPages = toBeInstertedIn.usedPagesNames;

					System.out.println(" this is the index");
					boolean flag2 = false;
					int i = 0;
					for (i = 0; i < toBeInstertedIn.usedPagesNames.size(); i++) {

						if (i == toBeInstertedIn.usedPagesNames.size() - 1) {
							// System.out.println("hello1");

							Page pageToBeInstertedIn = (Page) (getDeserlaized(
									"data//" + toBeInstertedIn.usedPagesNames.get(i) + ".class"));

							Vector<Tuple> Tuples = pageToBeInstertedIn.vtrTuples;
							// System.out.println(maxPageSize);
							if (Tuples.size() < maxPageSize) {
								Tuples.add(nTuple);
								Tuples.sort(null);

								FileOutputStream f = new FileOutputStream(
										"data//" + pageToBeInstertedIn.pageName + ".class");
								ObjectOutputStream bin = new ObjectOutputStream(f);
								bin.writeObject(pageToBeInstertedIn);

								break;

							} else if (maxPageSize == Tuples.size()) {

								Tuples.add(nTuple);
								Tuples.sort(null);
								nTuple = Tuples.remove(Tuples.size() - 1);

								toBeInstertedIn.createPage();
								Page newPage = (Page) (getDeserlaized("data//"
										+ toBeInstertedIn.usedPagesNames.get(toBeInstertedIn.usedPagesNames.size() - 1)
										+ ".class"));
								newPage.vtrTuples.add(nTuple);

								FileOutputStream f = new FileOutputStream("data//" + newPage.pageName + ".class");
								ObjectOutputStream bin = new ObjectOutputStream(f);
								bin.writeObject(newPage);

								FileOutputStream f1 = new FileOutputStream(
										"data//" + pageToBeInstertedIn.pageName + ".class");
								ObjectOutputStream bin1 = new ObjectOutputStream(f1);
								bin1.writeObject(pageToBeInstertedIn);

								break;

							}
						} else {

							Page pageToBeInstertedIn = (Page) (getDeserlaized(
									"data//" + toBeInstertedIn.usedPagesNames.get(i) + ".class"));

							Vector<Tuple> Tuples = pageToBeInstertedIn.vtrTuples;
							if (Tuples.size() < maxPageSize) {
								Tuples.add(nTuple);
								Tuples.sort(null);

								FileOutputStream f = new FileOutputStream(
										"data//" + pageToBeInstertedIn.pageName + ".class");
								ObjectOutputStream bin = new ObjectOutputStream(f);
								bin.writeObject(pageToBeInstertedIn);
								flag2 = true;
								break;

							} else if (Tuples.size() == maxPageSize) {
								Tuples.add(nTuple);
								Tuples.sort(null);
								nTuple = Tuples.remove(Tuples.size() - 1);
								FileOutputStream f = new FileOutputStream(
										"data//" + pageToBeInstertedIn.pageName + ".class");
								ObjectOutputStream bin = new ObjectOutputStream(f);
								bin.writeObject(pageToBeInstertedIn);

							}
						}

					}
				}

				FileOutputStream f1 = new FileOutputStream("data//" + strTableName + ".class");
				ObjectOutputStream bin1 = new ObjectOutputStream(f1);
				bin1.writeObject(toBeInstertedIn);
				// System.out.println(toBeInstertedIn.usedPagesNames + "this are the names");

			} catch (IOException e) {
				e.printStackTrace();
			}

		}
	}

	public static int getFirstOccurrenceIndex(Vector<Tuple> tuples, Tuple keyTuple) {

		// search space is arr[low..high]
		int low = 0, high = tuples.size() - 1;

		// initialize the result by -1
		int result = -1;

		// iterate till search space contains at-least one element
		while (low <= high) {
			// find the mid value in the search space and
			// compares it with target value
			int mid = (low + high) / 2;

			// if target is found, update the result and
			// go on searching towards left (lower indices)

			// System.out.println(mid);
			if (keyTuple.compareTo(tuples.get(mid)) == 0) {
				result = mid;
				high = mid - 1;
			}

			// if target is less than the mid element, discard right half
			else if (keyTuple.compareTo(tuples.get(mid)) < 0)
				high = mid - 1;

			// if target is more than the mid element, discard left half
			else
				low = mid + 1;
		}

		// return the leftmost index or -1 if the element is not found
		return result;

	}

	public ArrayList<String> getListOfIndicesNames(Hashtable<String, Object> htblColNameValue, String strTableName)
			throws DBAppException {
		Enumeration<String> keys = htblColNameValue.keys();
		Enumeration<Object> values = htblColNameValue.elements();
		ArrayList<String> listOfAvailableIndices = new ArrayList<String>();
		while (keys.hasMoreElements()) {
			String colName = keys.nextElement();
			Object value = values.nextElement();
			boolean flag = isIndexed(strTableName, colName);

			if (flag && !(value instanceof Polygon)) {
				listOfAvailableIndices.add(colName);
			}
		}
		return listOfAvailableIndices;

	}

	public ArrayList<String> getListRTreeNames(Hashtable<String, Object> htblColNameValue, String strTableName)
			throws DBAppException {
		Enumeration<String> keys = htblColNameValue.keys();
		Enumeration<Object> values = htblColNameValue.elements();
		ArrayList<String> listOfAvailableIndices = new ArrayList<String>();
		while (keys.hasMoreElements()) {
			String colName = keys.nextElement();
			Object value = values.nextElement();
			boolean flag = isIndexed(strTableName, colName);

			if (flag && (value instanceof Polygon)) {
				listOfAvailableIndices.add(colName);
			}
		}
		return listOfAvailableIndices;

	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, InterruptedException {

		Table newTable = (Table) getDeserlaized("data//" + strTableName + ".class");
		// String type = clusteringKeyType(newTable);
		String key = getClusteringKey(strTableName);
		boolean hasKey = htblColNameValue.containsKey(key) ? true : false;

		Object[] dTupleArray = getArrayToDelete(htblColNameValue, strTableName);

		// System.out.println(dTupleArray[3]);
		Vector<String> usedPages = newTable.usedPagesNames;

		// System.out.println(pageToBeInstertedInIndex + " this is the index");
		ArrayList<String> listOfAvailableIndices = getListOfIndicesNames(htblColNameValue, strTableName);

		System.out.println(listOfAvailableIndices.toString());
		ArrayList<String> listOfRTreeNames = getListRTreeNames(htblColNameValue, strTableName);
		boolean flagBtree = false;
		for (int i = 0; i < listOfAvailableIndices.size(); i++) {
			if (htblColNameValue.containsKey(listOfAvailableIndices.get(i))) {
				flagBtree = true;
				break;
			}

		}
		boolean flagRtree = false;
		for (int i = 0; i < listOfRTreeNames.size(); i++) {
			if (htblColNameValue.containsKey(listOfRTreeNames.get(i))) {
				flagRtree = true;
				break;
			}

		}

//		System.out.println(listOfAvailableIndices.size());
//		System.out.println(listOfRTreeNames.size());
		boolean flag2 = false;
		int i = 0;
		int[] compareTuple = getDeleteIndexOfArray(dTupleArray);
		if (listOfAvailableIndices.isEmpty() && listOfRTreeNames.isEmpty()) {

			for (i = 0; i < newTable.usedPagesNames.size(); i++) {

				Page pageToBeDeleteFrom = (Page) (getDeserlaized("data//" + newTable.usedPagesNames.get(i) + ".class"));

				Vector<Tuple> tuples = pageToBeDeleteFrom.vtrTuples;

				Vector temp = new Vector();
				temp.add(htblColNameValue.get(key));

				// System.out.println(htblColNameValue.get(key)+"keyvalue");
				Tuple keyTuple = new Tuple(temp, 0);

				if (!hasKey) {
					for (int j = 0; j < tuples.size(); j++) {
						Tuple t = tuples.get(j);

						boolean flag = true;
						for (int k = 0; k < compareTuple.length; k++) {
							// System.out.println(k);
							// System.out.println(dTupleArray[compareTuple[k]] +"");
							// System.out.println(t.vtrTupleObj.get(compareTuple[k]));
							//////////////////////////// deleting based on a polygon
							if (dTupleArray[compareTuple[k]] instanceof Polygon) {
								myPolygon p1 = new myPolygon((Polygon) dTupleArray[compareTuple[k]]);
								myPolygon p2 = new myPolygon((Polygon) t.vtrTupleObj.get(compareTuple[k]));
								if (!p1.equals(p2)) {
									flag = false;
									break;
								}
							}
							///////////////////////////////
							else if (!dTupleArray[compareTuple[k]].equals(t.vtrTupleObj.get(compareTuple[k]))) {
								flag = false;
								System.out.println("check6");

								break;
							}

						}
						if (flag) {

							Tuple removedTuple = tuples.remove(j);

							ArrayList<String> columnNames = getColNames(strTableName);
							for (int u = 0; u < columnNames.size(); u++) {
								if (isIndexed(strTableName, columnNames.get(u))) {
									if (removedTuple.vtrTupleObj.get(u) instanceof Polygon) {
										RTree r = (RTree) getDeserlaized(
												"data//" + "RTree" + strTableName + columnNames.get(u) + ".class");
										r.delete((Polygon) removedTuple.vtrTupleObj.get(u),
												newTable.usedPagesNames.get(i));
										r.serializeTree();
										System.out.println("No");
									} else {
										System.out.println("yes");

										BTree b = (BTree) getDeserlaized(
												"data//" + "BTree" + strTableName + columnNames.get(u) + ".class");
//										System.out.println(b.toString());
										b.delete((Comparable) removedTuple.vtrTupleObj.get(u),
												newTable.usedPagesNames.get(i));
										b.serializeTree();
//										System.out.println(b.toString());
									}
								}
							}

							j--;
							if (tuples.size() == 0) {
								// delete page and from table
								File file = new File("data//" + newTable.usedPagesNames.get(i) + ".class");

								if (!file.delete()) {
									// wait a bit then retry on Windows
									if (file.exists()) {
										for (int s = 0; s < 6; s++) {
											Thread.sleep(500);
											System.gc();
											if (file.delete())
												break;
										}
									}
								}
								newTable.usedPagesNames.remove(i);

								i--;

							}
						}

					}
				} else if (getFirstOccurrenceIndex(tuples, keyTuple) != -1) {

					int indexOfFirstOcc = getFirstOccurrenceIndex(tuples, keyTuple);
					// System.out.println(indexOfFirstOcc+"LOL");

					for (int j = indexOfFirstOcc; j < tuples.size(); j++) {
						Tuple t = tuples.get(j);

						if (t.compareTo(keyTuple) != 0) {
							break;
						}
						boolean flag = true;
						for (int k = 0; k < compareTuple.length; k++) {
							// System.out.println(k);
							// System.out.println(dTupleArray[compareTuple[k]] +"");
							// System.out.println(t.vtrTupleObj.get(compareTuple[k]));
							//////////////////////////// deleting based on a polygon
							if (dTupleArray[compareTuple[k]] instanceof Polygon) {
								myPolygon p1 = new myPolygon((Polygon) dTupleArray[compareTuple[k]]);
								myPolygon p2 = new myPolygon((Polygon) t.vtrTupleObj.get(compareTuple[k]));
								if (!p1.equals(p2)) {
									flag = false;
									break;
								}
							}
							///////////////////////////////
							else if (!dTupleArray[compareTuple[k]].equals(t.vtrTupleObj.get(compareTuple[k]))) {
								flag = false;
								System.out.println("check6");

								break;
							}

						}

						if (flag) {

							Tuple removedTuple = tuples.remove(j);
							ArrayList<String> columnNames = getColNames(strTableName);
							for (int u = 0; u < columnNames.size(); u++) {
								if (isIndexed(strTableName, columnNames.get(u))) {
									if (removedTuple.vtrTupleObj.get(u) instanceof Polygon) {
										RTree r = (RTree) getDeserlaized(
												"data//" + "RTree" + strTableName + columnNames.get(u) + ".class");
										r.delete((Polygon) removedTuple.vtrTupleObj.get(u),
												newTable.usedPagesNames.get(i));
										r.serializeTree();
									} else {
										BTree b = (BTree) getDeserlaized(
												"data//" + "BTree" + strTableName + columnNames.get(u) + ".class");
										b.delete((Comparable) removedTuple.vtrTupleObj.get(u),
												newTable.usedPagesNames.get(i));
										b.serializeTree();
									}
								}
							}

							j--;
							if (tuples.size() == 0) {
								// delete page and from table
								File file = new File("data//" + newTable.usedPagesNames.get(i) + ".class");

								if (!file.delete()) {
									// wait a bit then retry on Windows
									if (file.exists()) {
										for (int s = 0; s < 6; s++) {
											Thread.sleep(500);
											System.gc();
											if (file.delete())
												break;
										}
									}
								}
								newTable.usedPagesNames.remove(i);

								i--;

							}
						}

					}

				}

				if (tuples.size() != 0) {
					try {

						String n = pageToBeDeleteFrom.pageName;
						ObjectOutputStream bin = new ObjectOutputStream(new FileOutputStream("data//" + n + ".class"));

						bin.writeObject(pageToBeDeleteFrom);
						bin.flush();
						bin.close();
					} catch (Exception e) {
						throw new DBAppException("error in serializing file");
					}
				}

			}
		} else {
			ArrayList<Integer> listOfColNum = new ArrayList<Integer>();

			for (int k = 0; k < listOfAvailableIndices.size(); k++) {

				listOfColNum.add(getColNumber(strTableName, listOfAvailableIndices.get(k)));

			}
			ArrayList<Integer> listOfColNumRtree = new ArrayList<Integer>();

			for (int k = 0; k < listOfRTreeNames.size(); k++) {
				listOfColNumRtree.add(getColNumber(strTableName, listOfRTreeNames.get(k)));

			}

//			for(int l=0;l<listOfColNum.size();l++)
//				System.out.println(listOfColNum.get(l));

			ArrayList<String> listOfColName = getColNames(strTableName);

			ArrayList<String> intersect = new ArrayList<String>();

			for (int k = 0; k < listOfColNum.size(); k++) { // looping over col that has an index

				if (dTupleArray[listOfColNum.get(k)] != null) {

					BTree btree = (BTree) getDeserlaized(
							"data//" + "BTree" + strTableName + listOfColName.get(listOfColNum.get(k)) + ".class");

					ReferenceValues ref = (ReferenceValues) btree.search((Comparable) dTupleArray[listOfColNum.get(k)]);

					if (ref == null) {
						throw new DBAppException("No matching records found");
					}

					ArrayList<OverflowNode> lstofn = ref.getOverflowNodes(); // getting list of overflow nodes

					ArrayList<String> listOfFlattenReference = new ArrayList<String>();

					// code for flattening
					for (int j = 0; j < lstofn.size(); j++) {
						System.out.println("here");
						OverflowNode ofn = lstofn.get(j);
						for (int m = 0; m < ofn.referenceOfKeys.size(); m++) {
							System.out.println("line 1131: " + ofn.referenceOfKeys.get(m));
							String reference = (ofn.referenceOfKeys.get(m)).toString();
							listOfFlattenReference.add(reference);
						}
					}
					// end of flattening now I have a list of references [page1,page2,page3] ..
					if (intersect.isEmpty()) {
						intersect = listOfFlattenReference;
					} else {
						intersect = intersection(listOfFlattenReference, intersect);
					}
				}
			}
			for (int k = 0; k < listOfColNumRtree.size(); k++) { // looping over col that has an rtree

				if (dTupleArray[listOfColNumRtree.get(k)] != null) {
//***********listOfColName.get(listOfColNumRtree.get(k))
					RTree rtree = (RTree) getDeserlaized(
							"data//" + "RTree" + strTableName + listOfColName.get(listOfColNumRtree.get(k)) + ".class");
//***********RTreeReferenceValues
					RTreeReferenceValues ref = (RTreeReferenceValues) rtree
							.search((Polygon) dTupleArray[listOfColNumRtree.get(k)]);
					if (ref == null) {
						throw new DBAppException("No matching records found");
					}
//******** RTreeOverflow
					ArrayList<RTreeOverflowNode> lstofn = ref.getRTreeOverflowNodes(); // getting list of overflow nodes

					ArrayList<String> listOfFlattenReference = new ArrayList<String>();

					// code for flattening
					for (int j = 0; j < lstofn.size(); j++) {
						RTreeOverflowNode ofn = lstofn.get(j);
						for (int m = 0; m < ofn.referenceOfKeys.size(); m++) {
							// System.out.println(ofn.referenceOfKeys.get(m));
							String reference = (ofn.referenceOfKeys.get(m)).toString();
							listOfFlattenReference.add(reference);
						}
					}
					// end of flattening now I have a list of references [page1,page2,page3] ..
					if (intersect.isEmpty()) {
						intersect = listOfFlattenReference;
					} else {
						intersect = intersection(listOfFlattenReference, intersect);
					}
				}
			}
			intersect = removeDuplicates(intersect);
			boolean intersectEmpty = intersect.isEmpty() ? true : false;
			if (intersectEmpty) {
				for (int s = 0; s < usedPages.size(); s++) {
					intersect.add(usedPages.get(s));
					System.out.println("here");
				}
			}

			for (i = 0; i < intersect.size(); i++) {
				Page pageToBeDeleteFrom = (Page) (getDeserlaized("data//" + intersect.get(i) + ".class"));
				Vector<Tuple> tuples = pageToBeDeleteFrom.vtrTuples;

				for (int j = 0; j < tuples.size(); j++) {
					Tuple t = tuples.get(j);

					boolean flag = true;
					for (int k = 0; k < compareTuple.length; k++) {
						// System.out.println(k);
						// System.out.println(dTupleArray[compareTuple[k]] +"");
						// System.out.println(t.vtrTupleObj.get(compareTuple[k]));
						//////////////////////////// deleting based on a polygon
						if (dTupleArray[compareTuple[k]] instanceof Polygon) {
							myPolygon p1 = new myPolygon((Polygon) dTupleArray[compareTuple[k]]);
							myPolygon p2 = new myPolygon((Polygon) t.vtrTupleObj.get(compareTuple[k]));
							if (!p1.equals(p2)) {
								flag = false;
								break;
							}
						}
						///////////////////////////////
						else if (!dTupleArray[compareTuple[k]].equals(t.vtrTupleObj.get(compareTuple[k]))) {
							flag = false;
							System.out.println("check6");

							break;
						}

					}
					if (flag) {

						Tuple removedTuple = tuples.remove(j);
						ArrayList<String> columnNames = getColNames(strTableName);

						for (int u = 0; u < columnNames.size(); u++) {
							if (isIndexed(strTableName, columnNames.get(u))) {
								if (removedTuple.vtrTupleObj.get(u) instanceof Polygon) {
									RTree r = (RTree) getDeserlaized(
											"data//" + "RTree" + strTableName + columnNames.get(u) + ".class");
									r.delete((Polygon) removedTuple.vtrTupleObj.get(u), intersect.get(i));
									r.serializeTree();
								} else {
									BTree b = (BTree) getDeserlaized(
											"data//" + "BTree" + strTableName + columnNames.get(u) + ".class");
									b.delete((Comparable) removedTuple.vtrTupleObj.get(u), intersect.get(i));
									b.serializeTree();
								}
							}
						}

						j--;

						if (tuples.size() == 0) {
							// delete page and from table

							// fix hack
							File file = new File("data//" + intersect.get(i) + ".class");
							if (!file.delete()) {
								// wait a bit then retry on Windows
								if (file.exists()) {
									for (int s = 0; s < 6; s++) {
										Thread.sleep(500);
										System.gc();
										if (file.delete())
											break;
									}
								}
							}

							// not fixed
//							Path path= Paths.get("data//" + intersect.get(i) + ".class");
//							Files.delete(path);

							newTable.usedPagesNames.remove(intersect.get(i));
							intersect.remove(i);

//							if (file.delete()) {
////								newTable.usedPagesNames.remove(intersect.get(i));
////								intersect.remove(i);
//								System.out.println("File deleted successfully");
//							} else {
////								System.out.println(intersect.get(i)+"]]]]]]][[[[[[[[  " +j);
//								System.out.println("Failed to delete the file");
//							}

							i--;

						}
					}

					if (tuples.size() != 0) {
						try {

							String n = pageToBeDeleteFrom.pageName;
							ObjectOutputStream bin = new ObjectOutputStream(
									new FileOutputStream("data//" + n + ".class"));

							bin.writeObject(pageToBeDeleteFrom);
							bin.flush();
							bin.close();
						} catch (Exception e) {
							throw new DBAppException("error in serializing file");
						}
					}

				}

			}

		}
		try {

			ObjectOutputStream bin1 = new ObjectOutputStream(new FileOutputStream("data//" + newTable.name + ".class"));

			bin1.writeObject(newTable);
			bin1.flush();
			bin1.close();
		} catch (Exception e) {
			throw new DBAppException("error in serializing file");
		}

	}

	public ArrayList<String> intersection(ArrayList<String> list1, ArrayList<String> list2) {
		ArrayList<String> list = new ArrayList<String>();

		for (String t : list1) {
			if (list2.contains(t)) {
				list.add(t);
			}
		}

		return list;
	}

	public ArrayList<String> removeDuplicates(ArrayList<String> list) {

		// Create a new ArrayList
		ArrayList<String> newList = new ArrayList<String>();

		// Traverse through the first list
		for (String element : list) {

			// If this element is not present in newList
			// then add it
			if (!newList.contains(element)) {

				newList.add(element);
			}
		}

		// return the new list
		return newList;
	}

	public int[] getDeleteIndexOfArray(Object[] o) {
		int count = 0;
		for (int i = 0; i < o.length; i++) {
//			System.out.println(o[i]);
			if (!(o[i] == null)) {

				count++;
			}

		}
		int[] a = new int[count];
		int j = 0;
		for (int i = 0; i < o.length; i++) {
			if (o[i] != null) {
				a[j] = i;
				j++;

			}

		}
		return a;
	}

	// do not forget to serialize everything back

	public static boolean checkType(Hashtable<String, Object> h, String tableName) throws DBAppException {

		Enumeration type = h.keys();
		Enumeration value = h.elements();
		boolean flag = false;
		ArrayList<String> arrColoumnData = getArrayOfColoumnDataTyoe(tableName);
		try {
			while (type.hasMoreElements()) {
				String key = (String) type.nextElement();
				Object obj = value.nextElement();
				flag = false;
				for (int i = 0; i < arrColoumnData.size(); i++) {
					String[] d = arrColoumnData.get(i).split(",");
					if (d[0].equals(key)) {
						String x = "" + obj.getClass();
						if (x.contains(d[1])) {
							flag = true;
							break;
						} else {
							flag = false;
							throw new DBAppException("Wrong Data Type");
						}
					}
				}
			}
			return flag;
		} catch (Exception e) {
			throw new DBAppException("error in entered types");
		}

	}

	public static boolean checkTypeSize(Hashtable<String, Object> h, String tableName) throws DBAppException {
		try {
			Enumeration type = h.keys();
			Enumeration value = h.elements();
			boolean flag = false;
			ArrayList<String> arrColoumnData = getArrayOfColoumnDataTyoe(tableName);
			if (arrColoumnData.size() == h.size() + 1) {
				while (type.hasMoreElements()) {
					String key = (String) type.nextElement();
					Object obj = value.nextElement();
					boolean flag2 = false;
					flag = false;
					int count = 0;
					for (int i = 0; i < arrColoumnData.size(); i++) {
						String[] d = arrColoumnData.get(i).split(",");
						if (d[0].equals(key)) {
							flag2 = true;
							String x = "" + obj.getClass();
							if (x.contains(d[1])) {
								flag = true;
								break;
							} else {
								flag = false;
								throw new DBAppException("Wrong Data Type for column ");
							}
						}
						count++;
					}
					if (!flag2) {
						throw new DBAppException("Wrong column name");
					}
					if (!flag && count == arrColoumnData.size()) {
						throw new DBAppException("Column missing");
					}
				}
				return flag;

			} else {
				throw new DBAppException("A column does not exist");
			}
		} catch (Exception e) {
			throw new DBAppException("error in size or types entered");
		}
	}

	public static boolean checkType2(Hashtable<String, Object> h, String tableName) throws DBAppException {
		try {
			Enumeration type = h.keys();
			Enumeration value = h.elements();
			boolean flag = false;

			ArrayList<String> arrColoumnData = getArrayOfColoumnDataTyoe(tableName);
			while (type.hasMoreElements()) {
				boolean flag2 = false;
				String key = (String) type.nextElement();
				Object obj = value.nextElement();
				flag = false;
				int count = 0;
				for (int i = 0; i < arrColoumnData.size(); i++) {
					String[] d = arrColoumnData.get(i).split(",");
					if (d[0].equals(key)) {

						flag2 = true;
						String x = "" + obj.getClass();
						if (x.contains(d[1])) {
							System.out.println("check5");
							flag = true;
							break;
						} else {
							flag = false;
							throw new DBAppException("Wrong Data Type");
						}
					}
				}
				if (!flag2) {
					return false;
				}
			}
			return true;
		} catch (Exception e) {
			throw new DBAppException("error in type");
		}
	}

	public static String getClusteringKey(String strTableName) throws DBAppException {

		try {
			BufferedReader br = new BufferedReader(new FileReader("data//metadata.csv"));
			String line;
			String result = "";
			while ((line = br.readLine()) != null) {
				String[] values = line.split(",");
				if (values[0].equals(strTableName)) {
					if (values[3].equals("true")) {
						result = values[1];
						break;
					}
				}
			}
			br.close();
			return result;
		} catch (IOException e) {
			throw new DBAppException("error in getting clustering key");
		}
	}

	public static Tuple createTuple(Hashtable<String, Object> h, String strTableName) {
		Enumeration type = h.keys();
		Enumeration value = h.elements();
		Vector<Object> tupObj = new Vector<Object>();
		int index = 0;
		try {
			String clusterKey = getClusteringKey(strTableName);
			SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
			Date date = new Date();
			tupObj.add(formatter.format(date));
			while (type.hasMoreElements()) {
				String key = (String) type.nextElement();
				Object obj = value.nextElement();
				if (key.equals(clusterKey)) {
					index = (tupObj.size());
					tupObj.add(obj);
				} else {
					tupObj.add(obj);
				}
			}
		} catch (Exception e) {
			System.out.print(e.getMessage());
		}
		Tuple t = new Tuple(tupObj, index);
		return t;
	}

	public void updateTable(String strTableName, String strClusteringKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {
		try {
			boolean found = checkIfTableFound(strTableName);
			if (!found) {
				System.out.print("Table does not exist ");
			} else {
				boolean checkType = checkType(htblColNameValue, strTableName);

				if (checkType) {
					// these are the col names with the same order of the tuple
					ArrayList<String> colNames = getColNames(strTableName);

					// in the tuple with the clustering key I have to get the index of the columns
					// with the same name in the hashtable
					Table toBeUpdatedIn = (Table) getDeserlaized("data//" + strTableName + ".class");

					Enumeration type = htblColNameValue.keys();
					Enumeration value = htblColNameValue.elements();
					ArrayList<String> colToBeUpdated = new ArrayList<String>();
					ArrayList<Object> valuesToBeUpdated = new ArrayList<Object>();
					boolean enough = false;

					while (type.hasMoreElements()) {
						String key = (String) type.nextElement();
						Object obj = value.nextElement();
						if (key.equals(toBeUpdatedIn.strClusteringKeyColumn)) {
							throw new DBAppException("you cannot update clustering key");
						} else {
							colToBeUpdated.add(key);
							valuesToBeUpdated.add(obj);
						}
					}

					int indexToBeUpdated;

					boolean done = false;
					String keyType = clusteringKeyType(toBeUpdatedIn);
					Object enteredKey = new Object();

					if (keyType.equals("java.util.Date")) {
						String[] enteredArr = strClusteringKey.split("-");
						Date date = new Date(Integer.parseInt(enteredArr[0]), Integer.parseInt(enteredArr[1]),
								Integer.parseInt(enteredArr[2]));
						enteredKey = date;
					} else if (keyType.equals("java.lang.Integer")) {
						enteredKey = Integer.parseInt(strClusteringKey);
					} else if (keyType.equals("java.lang.String")) {
						enteredKey = strClusteringKey;
					} else if (keyType.equals("java.lang.Boolean")) {
						enteredKey = Boolean.parseBoolean(strClusteringKey);
					} else if (keyType.equals("java.lang.Double")) {
						enteredKey = Double.parseDouble(strClusteringKey);
					} else if (keyType.equals("java.awt.Polygon")) {
						String[] bracketedPoints = strClusteringKey.split("\\)");
						Polygon p = new Polygon();
						for (int i = 0; i < bracketedPoints.length; i++) {
							// System.out.println(bracketedPoints[i]);
							if ((bracketedPoints[i].charAt(0)) == ',') {
								bracketedPoints[i] = bracketedPoints[i].substring(1);
							}
							String[] points = bracketedPoints[i].split(",");
							int p1 = Integer.parseInt(points[0].substring(1));
							// System.out.println(p1);
							int p2 = Integer.parseInt(points[1]);
							// System.out.println(p2);
							p.addPoint(p1, p2);
						}
						enteredKey = p;
					}

					int startPageIndex;
					if (isIndexed(strTableName, toBeUpdatedIn.strClusteringKeyColumn)) {
						if (toBeUpdatedIn.usedIndicescols.contains(toBeUpdatedIn.strClusteringKeyColumn)) {
							// the index is a btree
							BTree b = (BTree) getDeserlaized("data//" + "BTree" + strTableName
									+ toBeUpdatedIn.strClusteringKeyColumn + ".class");
							// System.out.println(b.toString());
							Comparable comparableKey = (Comparable) enteredKey;

							ReferenceValues ref = (ReferenceValues) b.search(comparableKey);
							if (!(ref.getOverflowNodes().isEmpty())) {
								OverflowNode n = ref.getOverflowNodes().get(0);
								String pageName = n.referenceOfKeys.get(0) + "";
								Page p = (Page) getDeserlaized("data//" + pageName + ".class");
								startPageIndex = p.number;

								// System.out.println(startPageIndex);

							} else {
								startPageIndex = -1;
								throw new DBAppException("key not found");
							}
							b.serializeTree();
						} else if (toBeUpdatedIn.usedRtreeCols.contains(toBeUpdatedIn.strClusteringKeyColumn)) {
							// it is an RTree index

							RTree r = (RTree) getDeserlaized("data//" + "RTree" + strTableName
									+ toBeUpdatedIn.strClusteringKeyColumn + ".class");
							Polygon pol = (Polygon) enteredKey;
							// myPolygon enteredKeyComp = new myPolygon(pol);
							// Comparable comparableKey = (Comparable) enteredKeyComp;
							RTreeReferenceValues ref = (RTreeReferenceValues) r.search(pol);
							if (!(ref.getRTreeOverflowNodes().isEmpty())) {
								// System.out.println("check");
								RTreeOverflowNode n = ref.getRTreeOverflowNodes().get(0);
								String pageName = n.referenceOfKeys.get(0) + "";
								Page p = (Page) getDeserlaized("data//" + pageName + ".class");
								startPageIndex = p.number;
								// System.out.println(startPageIndex);

							} else {
								startPageIndex = -1;
								throw new DBAppException("key not found");
							}
							r.serializeTree();
						} else {
							startPageIndex = -1;
							throw new DBAppException("key not found");
						}
					} else {
						startPageIndex = getPageToBeInsertedIndexUsingClusteringKey(toBeUpdatedIn, strClusteringKey);
					}
					// System.out.println(startPageIndex);

					String startPageName = toBeUpdatedIn.usedPagesNames.get(startPageIndex);
					Page startPage = (Page) getDeserlaized("data//" + startPageName + ".class");

					int clusterKeyIndex = -1;
					for (int i = 0; i < colNames.size(); i++) {
						if (colNames.get(i).equals(toBeUpdatedIn.strClusteringKeyColumn)) {
							clusterKeyIndex = i;
						}
					}

					int startTupleIndex = getStartIndexStartUpdate(enteredKey, startPageIndex, toBeUpdatedIn);// bt2lesh
																												// binary
					// System.out.println("start updating at tuple index:" + startTupleIndex);
					int tupleIndex = startTupleIndex;
					boolean equalArea = false;
					while (!enough && (tupleIndex < startPage.vtrTuples.size())) {
						Tuple old = startPage.vtrTuples.get(tupleIndex);
						String parsed;
						if (keyType.equals("java.awt.Polygon")) {
							myPolygon p = new myPolygon((Polygon) old.vtrTupleObj.get(old.index));
							myPolygon entered = new myPolygon((Polygon) enteredKey);
							equalArea = (p.compareTo(entered) == 0);
							parsed = p.toString();

						} else
							parsed = old.vtrTupleObj.get(old.index) + ""; // polyyy
//						System.out.println(parsed);
//						System.out.println(parsed.equals(strClusteringKey + ""));
//						System.out.println(strClusteringKey + "");
						if ((enteredKey.getClass() + "").contains("java.util.Date")) {
							strClusteringKey = enteredKey + "";
						}
						if (parsed.equals(strClusteringKey + "") || equalArea) { // in case of polygon parsed and
																					// strClusteringKey will have same
																					// area not coordinates

//							 System.out.println("check: equalArea:"+equalArea);
							for (int i = 0; i < colToBeUpdated.size(); i++) {
								if (equalArea && !parsed.equals(strClusteringKey + "")) {
//									System.out.println("skip");
									continue;
								}
								String col = colToBeUpdated.get(i);
								for (int j = 0; j < colNames.size(); j++) {
									if (colNames.get(j).equals(col)) {

										if (isIndexed(strTableName, colNames.get(j))) {
											if (toBeUpdatedIn.usedIndicescols.contains(col)) {
												BTree bt = (BTree) getDeserlaized(
														"data//" + "BTree" + strTableName + colNames.get(j) + ".class");
												Object oldValue = old.vtrTupleObj.get(j);
												// System.out.println(valuesToBeUpdated.get(i));
												Comparable oldValueCom = (Comparable) oldValue;
												// System.out.println(oldValueCom);
												bt.delete(oldValueCom, startPage.pageName);
												Comparable newValueCom = (Comparable) valuesToBeUpdated.get(i);
												// System.out.println(newValueCom);
												bt.insert(newValueCom, startPage.pageName);
												bt.serializeTree();
											} else if (toBeUpdatedIn.usedRtreeCols.contains(col)) {
												RTree rt = (RTree) getDeserlaized(
														"data//" + "RTree" + strTableName + colNames.get(j) + ".class");
												Object oldValue = old.vtrTupleObj.get(j);
												Polygon pol = (Polygon) oldValue;
												// Comparable oldValueCom = (Comparable) oldValue;
												Polygon newPol = (Polygon) valuesToBeUpdated.get(i);
												rt.delete(pol, startPage.pageName);
												// Comparable newValueCom = (Comparable) valuesToBeUpdated.get(i);
												rt.insert(newPol, startPage.pageName);
												rt.serializeTree();
											}

										}

										indexToBeUpdated = j;

										// go change the value of this index with the value in index i in
										// valuesToBeUpdated

										old.vtrTupleObj.setElementAt(valuesToBeUpdated.get(i), j);
										SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
										Date date = new Date();
										old.vtrTupleObj.setElementAt(formatter.format(date), 0);

										// tupleIndex++;
										// System.out.println(old.vtrTupleObj.get(j));
										// done = true;
										// break;
									}

									// check if the next page has the same key
									// case2:
									// check next page if the key is the same update
									// serialize back
								}
							}
							tupleIndex++;

						} else {
							enough = true;
						}

					}
					serialize(startPage);
					if (enough != true) {
						// System.out.println("check53");

						startPageIndex++;
						boolean next = true;
						while (next) {
							while (startPageIndex < toBeUpdatedIn.usedPagesNames.size()) {
								Page nextPage = (Page) getDeserlaized(
										"data//" + toBeUpdatedIn.usedPagesNames.get(startPageIndex) + ".class");

								for (int z = 0; z < nextPage.vtrTuples.size(); z++) {
									Tuple nextTup = nextPage.vtrTuples.get(z);
									int indexKeyOfFirst = nextTup.index;
									String k;
									boolean nextEqualArea = false;
									if (keyType.equals("java.awt.Polygon")) {
										myPolygon p = new myPolygon((Polygon) nextTup.vtrTupleObj.get(indexKeyOfFirst));
										myPolygon entered = new myPolygon((Polygon) enteredKey);
										nextEqualArea = (p.compareTo(entered) == 0);
										k = p.toString();
//										System.out.println(k);
//										System.out.println(k.equals(strClusteringKey) +"  "+nextEqualArea +"  "+next);
									} else
										k = nextTup.vtrTupleObj.get(indexKeyOfFirst) + "";// polyyy
									if ((enteredKey.getClass() + "").contains("java.util.Date")) {
										strClusteringKey = enteredKey + "";
									}
									if (k.equals(strClusteringKey) || nextEqualArea) {
										for (int i = 0; i < colToBeUpdated.size(); i++) {
											if (nextEqualArea && !k.equals(strClusteringKey + "")) {
//												System.out.println("skip");
												continue;
											}
											String col = colToBeUpdated.get(i);
											for (int j = 0; j < colNames.size(); j++) {
												if (colNames.get(j).equals(col)) {
													if (isIndexed(strTableName, colNames.get(j))) {

														BTree bt = (BTree) getDeserlaized("data//" + "BTree"
																+ strTableName + colNames.get(j) + ".class");
														Object oldValue = nextTup.vtrTupleObj.get(j);
														Comparable oldValueCom = (Comparable) oldValue;
														// System.out.println(oldValueCom);
														bt.delete(oldValueCom, startPage.pageName);
														Comparable newValueCom = (Comparable) valuesToBeUpdated.get(i);
														// System.out.println("newValueCom");
														bt.insert(newValueCom, startPage.pageName);
														bt.serializeTree();
													} else if (toBeUpdatedIn.usedRtreeCols.contains(col)) {
														RTree rt = (RTree) getDeserlaized("data//" + "RTree"
																+ strTableName + colNames.get(j) + ".class");
														Object oldValue = nextTup.vtrTupleObj.get(j);
														Polygon pol = (Polygon) oldValue;
														Polygon newPol = (Polygon) valuesToBeUpdated.get(i);
														// Comparable oldValueCom = (Comparable) oldValue;
														// System.out.println(oldValueCom);
														rt.delete(pol, startPage.pageName);
														// Comparable newValueCom = (Comparable)
														// valuesToBeUpdated.get(i);
														rt.insert(newPol, startPage.pageName);
														rt.serializeTree();
													}
													indexToBeUpdated = j;
													nextTup.vtrTupleObj.setElementAt(valuesToBeUpdated.get(i), j);
													SimpleDateFormat formatter = new SimpleDateFormat(
															"dd/MM/yyyy HH:mm:ss");
													Date date = new Date();
													nextTup.vtrTupleObj.setElementAt(formatter.format(date), 0);
												}

											}
										}
									} else {

										// continue updating next tuples with the same clustering key until we hit a
										// wrong one
										next = false;
//										System.out.println("no more");
										break;
									}

								}
								startPageIndex++;
								serialize(nextPage);

							}
							if (startPageIndex == toBeUpdatedIn.usedPagesNames.size())
								next = false;
						}
					}

					ObjectOutputStream bin = new ObjectOutputStream(
							new FileOutputStream("data//" + toBeUpdatedIn.name + ".class"));
					bin.writeObject(toBeUpdatedIn);
					bin.flush();
					bin.close();

				}

				else {
					System.out.print("wrong data types");
				}

			}

		} catch (Exception e) {
			throw new DBAppException("error in updating");

		}
	}

	// serialize back everything
	public static int getStartIndexStartUpdate(Object key, int pageIndex, Table toBeInsertedIn) throws DBAppException {

		try {
			boolean flag = false;
			Page startPage = (Page) (getDeserlaized(
					"data//" + toBeInsertedIn.usedPagesNames.get(pageIndex) + ".class"));
			String keyType = clusteringKeyType(toBeInsertedIn);
//System.out.println("clusteringKeyType:"+keyType);
			int lowerBound = 0;
			int upperBound = startPage.vtrTuples.size() - 1;
			int curIn;
			int i = 0;
			while (true) {

				curIn = (lowerBound + upperBound) / 2;
				// System.out.println(curIn);
				Tuple testTuple = startPage.vtrTuples.get(curIn);
				Object comkey = testTuple.vtrTupleObj.get(testTuple.index);

				// System.out.println(comkey);
				// System.out.println(key.getClass());
				// System.out.println(Tuple.compareToHelper(comkey, key) );
				if (Tuple.compareToHelper(comkey, key) == 0) {
					// System.out.println("check5");
					serialize(startPage);
					flag = true;
					// to handle duplicates
					while (curIn > 0) {
						Tuple prevTuple = startPage.vtrTuples.get(curIn - 1);
						Object prevkey = prevTuple.vtrTupleObj.get(prevTuple.index);
						boolean equalArea = false;
						if (keyType.equals("java.awt.Polygon")) {
							myPolygon entered = new myPolygon((Polygon) key);
							myPolygon prevpoly = new myPolygon((Polygon) prevkey);
							equalArea = (prevpoly.compareTo(entered) == 0);
						}
						if (prevkey.equals(key) || equalArea) { // doesnt support polygon
							// System.out.println(prevkey);
							// System.out.println(curIn);
							curIn--;
						} else {
							break;
						}
					}
					return curIn;
				} else if (lowerBound > upperBound) {
					throw new DBAppException("key not found");
					// return -1; // can’t find it
				}
				if (Tuple.compareToHelper(comkey, key) < 0) {
					// this means that my key is greater search down
					lowerBound = curIn + 1;
				} else {
					if (Tuple.compareToHelper(comkey, key) > 0) {
						// this means that my key is smaller search up
						upperBound = curIn - 1;
					}
				}
			}

		} catch (Exception e) {

			throw new DBAppException("error in getting startIndexUpdate");
		}
		// return -1;

	}
//			if(a[curIn] == searchKey)
//			return curIn; // found it
//			else if(lowerBound > upperBound)
//			return nElems; // can’t find it
//			else // divide range
//			{
//			if(a[curIn] < searchKey)
//			lowerBound = curIn + 1; // it’s in upper half
//			else
//			upperBound = curIn - 1; // it’s in lower half
//			} // end else divide range
//			} // end while
//			} // end find()

//			for (int i = 0; i < startPage.vtrTuples.size(); i++) {
//
//				Tuple testTuple = startPage.vtrTuples.get(i);
//				String comkey;
//				if (keyType.equals("java.awt.Polygon")) {
//					myPolygon p = new myPolygon((Polygon) testTuple.vtrTupleObj.get(testTuple.index));
//					comkey = p.toString();
//				} else
//					comkey = testTuple.vtrTupleObj.get(testTuple.index) + ""; // polyyy
////System.out.println("comparing " + key +" with "+comkey+":"+comkey.equals(key));
//				if (comkey.equals(key)) {
//					serialize(startPage);
//					flag = true;
//					return i;
//				}
//			}

//		int upperIndex = 0;
//		int lowerIndex = middlePage.size() - 1;
//
//		while (upperIndex <= lowerIndex) {
//			int middleIndex = upperIndex + (lowerIndex - upperIndex) / 2;
//			int clusterIndex = middlePage.vtrTuples.get(middleIndex).index;
//			String middleKey = (String) middlePage.vtrTuples.get(middleIndex).get(clusterIndex);
//			if (Tuple.compareToHelper(middleKey, key) < 0) {
//				// this means that my key is greater search down
//				upperIndex = middleIndex + 1;
//			} else {
//				if (Tuple.compareToHelper(middleKey, key) > 0) {
//					// this means that my key is smaller search up
//					lowerIndex = middleIndex - 1;
//				} else {
//					return middleIndex;
//				}
//			}
//		}
//		throw new DBAppException("no clustering key with this value");
//	}

	// delete helper return an array for hashtable inserted

	public Object[] getArrayToDelete(Hashtable<String, Object> htblColNameValue, String strTableName)
			throws DBAppException {

		Table newTable = (Table) (getDeserlaized("data//" + strTableName + ".class"));

		String[] col = newTable.colNames;
		Object[] toBeReturned = new Object[col.length];

		Enumeration keys = htblColNameValue.keys();
		Enumeration values = htblColNameValue.elements();

		boolean checkType2 = checkType2(htblColNameValue, strTableName);

		if (checkType2) {

			while (keys.hasMoreElements()) {

				String key = (String) keys.nextElement();
				Object value = values.nextElement();
				// System.out.println(key);

				for (int i = 0; i < col.length; i++) {
					// System.out.println(col[i]);
					if (key.equals(col[i])) {

						// System.out.println("Print 3");
						toBeReturned[i] = value;
						// System.out.println(value +"");

					}
				}

			}
		}
		return toBeReturned;

	}

	public static void serialize(Object name) throws DBAppException {

		try {
			Page p = (Page) name;
			String n = p.pageName;

			ObjectOutputStream bin = new ObjectOutputStream(new FileOutputStream("data//" + n + ".class"));

			bin.writeObject(name);
			bin.flush();
			bin.close();

		} catch (Exception e) {
			throw new DBAppException("error in serialization");
		}
	}

	public static int getPageToBeInsertedIndexUsingClusteringKey(Table toBeInstertedIn, String clusteringKey)
			throws DBAppException {

		try {

			boolean found = false;
			int i = 0;
			String keyType = clusteringKeyType(toBeInstertedIn);
			// System.out.println(keyType);
			if (keyType.equals("java.util.Date")) {
				// String entered = obj + "";
				String[] enteredArr = clusteringKey.split("-");
				Date date = new Date(Integer.parseInt(enteredArr[0]), Integer.parseInt(enteredArr[1]),
						Integer.parseInt(enteredArr[2]));

				clusteringKey = date + "";
				// String pattern = "yyyy-MM-dd";
				// SimpleDateFormat simpleDateFormat = new SimpleDateFormat(pattern);
				// Date date = simpleDateFormat.parse("" + obj);
				// obj = date;
//				System.out.println(date.getClass());
			}
			for (i = 0; i < toBeInstertedIn.usedPagesNames.size(); i++) {

				Page testPage = (Page) (getDeserlaized("data//" + toBeInstertedIn.usedPagesNames.get(i) + ".class"));

				for (int j = 0; j < testPage.vtrTuples.size(); j++) {

					Tuple testTuple = testPage.vtrTuples.get(j);
					String key;
					if (keyType.equals("java.awt.Polygon")) {
						myPolygon p = new myPolygon((Polygon) testTuple.vtrTupleObj.get(testTuple.index));
						key = p.toString();
					} else
						key = testTuple.vtrTupleObj.get(testTuple.index) + ""; // polyyy
//System.out.println("comparing " + clusteringKey +" with "+key);
					// System.out.println(key);
					// System.out.println(clusteringKey);
					// System.out.println(key.equals(clusteringKey));
					if (key.equals(clusteringKey)) {

						found = true;
						break;
						// return i;
					}
				}
				if (found == true) {
					return i;

				}
			}

		} catch (Exception e) {
			throw new DBAppException("error in getting page to be inserted in");
		}
		return -1;
	}

//		int right = toBeInstertedIn.usedPagesNames.size() - 1;
//		// System.out.println(toBeInstertedIn.usedPagesNames.size());
//		int left = 0;
//
//		while (left <= right) {
//			int middle = left + (right - left) / 2;
//
//			Page middlePage = (Page) (getDeserlaized("data//" + toBeInstertedIn.usedPagesNames.get(middle) + ".class"));
//			int x = middlePage.vtrTuples.size();
//			//System.out.println(x);
//			Tuple leftTupleMiddlePage = middlePage.vtrTuples.get(0);
//
//			int keyIndex = leftTupleMiddlePage.index;
//			 //System.out.println(keyIndex);
//
//			int y = leftTupleMiddlePage.vtrTupleObj.size();
//			//System.out.println(y);
//			String leftKey = leftTupleMiddlePage.vtrTupleObj.get(keyIndex) + "";
//			
//			Tuple rightTupleMiddlePage = middlePage.vtrTuples.get(middlePage.vtrTuples.size() - 1);
//			
//			String rightKey = rightTupleMiddlePage.vtrTupleObj.get(keyIndex) + "";
//			
//			// System.out.println(leftTupleMiddlePage.vtrTupleObj.toString() + " this is
//			// leftTupleMiddlePage");
//			// System.out.println(rightTupleMiddlePage.vtrTupleObj.toString() + " this is
//			// rightTupleMiddlePage");
//
//			if (Tuple.compareToHelper(leftKey, clusteringKey) <= 0
//					&& Tuple.compareToHelper(rightKey, clusteringKey) >= 0) {
//				return middle;
//			} else if (Tuple.compareToHelper(rightKey, clusteringKey) < 0) {
//				
//				left = middle + 1;
//			} else if (Tuple.compareToHelper(leftKey, clusteringKey) > 0) {
//				System.out.println("koky2");
//				right = middle - 1;
//			}
//
//		}

//	public static int getPageToBeInsertedInIndex(Table toBeInstertedIn, Tuple nTuple) {
//		System.out.println(nTuple);
//		int right = toBeInstertedIn.usedPagesNames.size() - 1;
//		int left = 0;
//
//		while (left <= right) {
//			int middle = left + (right - left) / 2;
//
////			Page leftPage = (Page) (getDeserlaized("data//" + toBeInstertedIn.usedPagesNames.get(left) + ".class"));
////			Tuple rightTupleLeftPage = leftPage.vtrTuples.get(leftPage.vtrTuples.size() - 1);
////			System.out.println(rightTupleLeftPage.vtrTupleObj.toString() + " this is rightTupleLeftPage");
////
////			Page rightPage = (Page) (getDeserlaized("data//" + toBeInstertedIn.usedPagesNames.get(right) + ".class"));
////			if(rightPage==null) {
////				return left+1;
////			}
////			Tuple leftTupleRightPage = rightPage.vtrTuples.get(0);
////			System.out.println(leftTupleRightPage.vtrTupleObj.toString() + " this is leftTupleRightPage");
//
//			Page middlePage = (Page) (getDeserlaized("data//" + toBeInstertedIn.usedPagesNames.get(middle) + ".class"));
//
//			Tuple leftTupleMiddlePage = middlePage.vtrTuples.get(0);
//			Tuple rightTupleMiddlePage = middlePage.vtrTuples.get(middlePage.vtrTuples.size() - 1);
//			System.out.println(leftTupleMiddlePage.vtrTupleObj.toString() + " this is leftTupleMiddlePage");
//			System.out.println(rightTupleMiddlePage.vtrTupleObj.toString() + " this is rightTupleMiddlePage");
//
//			if (leftTupleMiddlePage.compareTo(nTuple) <= 0 && rightTupleMiddlePage.compareTo(nTuple) >= 0) {
//				return middle;
//			} else if (rightTupleMiddlePage.compareTo(nTuple) < 0) {
//				left = middle + 1;
//			} else {
//				right = middle - 1;
//			}
//
//		}
//		return -1;
//	}

	public static ArrayList<String> getColNames(String strTableName) throws DBAppException {
		String csvFile = "data/metadata.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		ArrayList<String> arrColumn = new ArrayList<String>();
		try {

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

				// use comma as separator
				String[] d = line.split(cvsSplitBy);
				if (d[0].equals(strTableName)) {
					// System.out.println(d[1]);

					arrColumn.add(d[1]);
				}
			}

			br.close();

		} catch (Exception e) {
			throw new DBAppException("error in getting col names");
		}
		return arrColumn;

	}

	public static String clusteringKeyType(Table t) {
		String keyName = t.strClusteringKeyColumn;
		Enumeration e = t.htblColNameType.keys();
		Enumeration n = t.htblColNameType.elements();
		while (e.hasMoreElements()) {

			String key = (String) e.nextElement();
			String value = (String) n.nextElement();
			if (keyName.equals(key))
				return value;
		}
		return "";
	}

	public static void displayTableContent(String tName) throws DBAppException {
		Table t = (Table) getDeserlaized("data//" + tName + ".class");
		System.out.println("Displaying table: " + tName);
		// column names:
		ArrayList<String> col = getColNames(tName);
		for (String c : col) {
			System.out.print(c + "  ");
		}
		System.out.println();

		ArrayList<String> p = new ArrayList<String>();
		// System.out.println(t.usedPagesNames.size());
		for (int i = 0; i < t.usedPagesNames.size(); i++) {

			p.add(t.usedPagesNames.get(i));
		}
		for (int i = 0; i < p.size(); i++) {
			Page pa = (Page) getDeserlaized("data//" + p.get(i) + ".class");
			System.out.println(pa.pageName);
			for (int j = 0; j < pa.vtrTuples.size(); j++) {
				System.out.println(pa.vtrTuples.get(j).toString());
			}
			serialize(pa);
		}
		try {
			ObjectOutputStream bin = new ObjectOutputStream(new FileOutputStream("data//" + tName + ".class"));
			bin.writeObject(t);
			bin.flush();
			bin.close();

		} catch (Exception e) {
			throw new DBAppException("error in displaying data in table");
		}
	}

	public static boolean isIndexed(String tableName, String colName) throws DBAppException {
		boolean flag = false;
		boolean colFound = false;
		String csvFile = "data/metadata.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		ArrayList<String> arrColumn = new ArrayList<String>();
		try {

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

				// use comma as separator
				String[] d = line.split(cvsSplitBy);
				if (d[0].equals(tableName)) {

					if (d[1].equals(colName)) {
						colFound = true;
						if (d[4].equals("true")) {
							flag = true;
						} else {
							flag = false;
						}
					}
				}
			}
			br.close();
			if (!colFound) {
				throw new DBAppException("column not found");
			}

		} catch (Exception e) {
			throw new DBAppException("error in checking if column is indexed");
		}
		return flag;
	}

	public static boolean isClusteringKey(String tableName, String colName) throws DBAppException {
		boolean flag = false;
		boolean colFound = false;
		String csvFile = "data/metadata.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		ArrayList<String> arrColumn = new ArrayList<String>();
		try {

			br = new BufferedReader(new FileReader(csvFile));
			while ((line = br.readLine()) != null) {

				// use comma as separator
				String[] d = line.split(cvsSplitBy);
				if (d[0].equals(tableName)) {

					if (d[1].equals(colName)) {
						colFound = true;
						if (d[3].equals("true")) {
							flag = true;
						} else {
							flag = false;
						}
					}
				}
			}
			br.close();
			if (!colFound) {
				throw new DBAppException("column not found");
			}

		} catch (Exception e) {
			throw new DBAppException("error in checking if column is indexed");
		}
		return flag;
	}

	public ArrayList<Tuple> equalOperator2(Table t, Object key, boolean indexed, boolean isClustering, String colName,
			boolean useArea) throws DBAppException {
		ArrayList<Tuple> result = new ArrayList<Tuple>();

		try {
			if (!indexed) {
				if ((key.getClass() + "").contains("java.awt.Polygon")) {
					myPolygon mpKey = new myPolygon((Polygon) key);
					key = mpKey;
				}
				if (isClustering) {
					// use binary search
					boolean nextPage = true;
					int startPageIndex = getPageToBeInsertedIndexUsingClusteringKey(t, key + "");
					// System.out.println(startPageIndex);
					if (startPageIndex >= 0) {
						String pageName = t.usedPagesNames.get(startPageIndex);
						Page p = (Page) getDeserlaized("data//" + pageName + ".class");
						int startTupleIndex = getStartIndexStartUpdate(key, startPageIndex, t);
						// System.out.println(startTupleIndex);

						for (int j = startTupleIndex; j < p.vtrTuples.size(); j++) {

							Tuple tup = p.vtrTuples.get(j);
							Object tupKey = tup.vtrTupleObj.get(tup.index);
							// System.out.println(tupKey);
							if ((tupKey.getClass() + "").contains("java.awt.Polygon")) {
								myPolygon mp = new myPolygon((Polygon) tupKey);
								tupKey = mp;
							}
							if (tupKey.equals(key) || useArea) {
								if (useArea) {
									if (Tuple.compareToHelper(tupKey, key) == 0) {
										result.add(tup);
									}
								} else {
									result.add(tup);
								}
							} else {
								nextPage = false;
							}
							serialize(p);
						}
						while (nextPage) {
							startPageIndex++;
							// System.out.println(startPageIndex);
							if (startPageIndex < t.usedPagesNames.size()) {
								String secondPage = t.usedPagesNames.get(startPageIndex);
								Page next = (Page) getDeserlaized("data//" + secondPage + ".class");
								for (int j = 0; j < next.vtrTuples.size(); j++) {
									Tuple tup = next.vtrTuples.get(j);
									Object tupKey = tup.vtrTupleObj.get(tup.index);
									if ((tupKey.getClass() + "").contains("java.awt.Polygon")) {
										myPolygon mp = new myPolygon((Polygon) tupKey);
										tupKey = mp;
									}
									if (tupKey.equals(key) || useArea) {
										if (useArea) {
											if (Tuple.compareToHelper(tupKey, key) == 0) {
												result.add(tup);
											}
										} else {
											result.add(tup);
										}
									} else {
										nextPage = false;
										break;
									}
								}
								serialize(next);
							} else {
								nextPage = false;
								break;
							}
						}
					}
				} else if (!isClustering) {
					// linear search
					String tableName = t.name;
					int colNumber = getColNumber(tableName, colName);
					for (int i = 0; i < t.usedPagesNames.size(); i++) {
						String pageName = t.usedPagesNames.get(i);
						Page p = (Page) getDeserlaized("data//" + pageName + ".class");
						for (int j = 0; j < p.vtrTuples.size(); j++) {
							Tuple tup = p.vtrTuples.get(j);
							Object value = tup.vtrTupleObj.get(colNumber);
							if ((value.getClass() + "").contains("java.awt.Polygon")) {
								myPolygon mp = new myPolygon((Polygon) value);
								value = mp;
							}
							if (Tuple.compareToHelper(value, key) == 0) {
								if (!useArea) {
									if (value.equals(key)) {
										result.add(tup);
										// System.out.println(tup);
										// System.out.println("check");
									}
								} else {

									result.add(tup);
								}
							}
						}
						serialize(p);
					}
				}
			} else if (indexed) {
				// use tree

				if (isClustering) {
					String pageName = "";
					if (t.usedIndicescols.contains(colName)) {
						BTree b = (BTree) getDeserlaized("data//" + "BTree" + t.name + colName + ".class");
						// System.out.println(b.toString());
						Comparable k = (Comparable) key;
						ReferenceValues ref = (ReferenceValues) b.search(k);
						if (ref != null) {
							if (!(ref.getOverflowNodes().isEmpty())) {

								boolean out = false;
								for (int i = 0; i < ref.getOverflowNodes().size(); i++) {
									OverflowNode n = ref.getOverflowNodes().get(i);
									for (int j = 0; j < n.referenceOfKeys.size(); j++) {
										if ((n.referenceOfKeys.get(j) + "").contains(t.name)) {
											out = true;
											pageName = n.referenceOfKeys.get(j) + "";
											break;
										}
									}
									if (out) {
										break;
									}
								}
							}
							b.serializeTree();
						}
					} else if (t.usedRtreeCols.contains(colName)) {
						RTree r = (RTree) getDeserlaized("data//" + "RTree" + t.name + colName + ".class");
						// System.out.println(b.toString());
						// Comparable k = (Comparable) key;
						Polygon k = (Polygon) key;
						RTreeReferenceValues ref = (RTreeReferenceValues) r.search(k);
						if (!(ref.getRTreeOverflowNodes().isEmpty())) {
							boolean out = false;
							for (int i = 0; i < ref.getRTreeOverflowNodes().size(); i++) {
								RTreeOverflowNode n = ref.getRTreeOverflowNodes().get(i);
								for (int j = 0; j < n.referenceOfKeys.size(); j++) {
									if ((n.referenceOfKeys.get(j) + "").contains(t.name)) {
										out = true;
										pageName = n.referenceOfKeys.get(j) + "";
										break;
									}
								}
								if (out) {
									break;
								}
							}
						}
						r.serializeTree();
					} else {
						throw new DBAppException("error in getting pageName in equal operator");
					}
					if (!(pageName.equals(""))) {
						Page p = (Page) getDeserlaized("data//" + pageName + ".class");
						int pageNumber = p.number;
						boolean flag = false;
						int lowerBound = 0;
						int upperBound = p.vtrTuples.size() - 1;
						int curIn = -1;
						int i = 0;
						if ((key.getClass() + "").contains("java.awt.Polygon")) {
							myPolygon mpKey = new myPolygon((Polygon) key);
							key = mpKey;
						}
						while (!flag) {
							curIn = (lowerBound + upperBound) / 2;
							Tuple testTuple = p.vtrTuples.get(curIn);
							Object comkey = testTuple.vtrTupleObj.get(testTuple.index);
							if ((comkey.getClass() + "").contains("java.awt.Polygon")) {
								myPolygon mp = new myPolygon((Polygon) comkey);
								comkey = mp;
							}
							if (Tuple.compareToHelper(comkey, key) == 0) {
								flag = true;
								// to handle duplicates
								while (curIn > 0) {
									Tuple prevTuple = p.vtrTuples.get(curIn - 1);
									Object prevkey = prevTuple.vtrTupleObj.get(prevTuple.index);
									if ((prevkey.getClass() + "").contains("java.awt.Polygon")) {
										myPolygon mp = new myPolygon((Polygon) prevkey);
										prevkey = mp;
									}
									boolean equalArea;
									equalArea = (Tuple.compareToHelper(prevkey, key) == 0);
									if (prevkey.equals(key) || equalArea) {
										curIn--;
									} else {
										break;
									}
								}
							} else if (lowerBound > upperBound) {
								throw new DBAppException("key not found"); // can't find it
							}
							if (Tuple.compareToHelper(comkey, key) < 0) {
								// this means that my key is greater search down
								lowerBound = curIn + 1;
							} else {
								if (Tuple.compareToHelper(comkey, key) > 0) {
									// this means that my key is smaller search up
									upperBound = curIn - 1;
								}
							}
						}
						boolean nextPage = true;
						for (int j = curIn; j < p.vtrTuples.size(); j++) {

							Tuple tup = p.vtrTuples.get(j);
							Object tupKey = tup.vtrTupleObj.get(tup.index);
							if ((tupKey.getClass() + "").contains("java.awt.Polygon")) {
								myPolygon mp = new myPolygon((Polygon) tupKey);
								tupKey = mp;
							}
							if (Tuple.compareToHelper(tupKey, key) == 0) {
								if (!useArea) {

									if (tupKey.equals(key)) {
										result.add(tup);
									}
								} else {
									result.add(tup);
								}
							} else {
								nextPage = false;
								break;
							}
						}
						serialize(p);
						while (nextPage) {
							pageNumber++;
							if (pageNumber < t.usedPagesNames.size()) {
								String secondPage = t.usedPagesNames.get(pageNumber);
								Page next = (Page) getDeserlaized("data//" + secondPage + ".class");
								for (int j = 0; j < next.vtrTuples.size(); j++) {
									Tuple tup = next.vtrTuples.get(j);
									Object tupKey = tup.vtrTupleObj.get(tup.index);
									if ((tupKey.getClass() + "").contains("java.awt.Polygon")) {
										myPolygon mp = new myPolygon((Polygon) tupKey);
										tupKey = mp;
									}
									if (Tuple.compareToHelper(tupKey, key) == 0) {
										if (!useArea) {
											if (tupKey.equals(key)) {
												result.add(tup);
											}
										} else {
											result.add(tup);
										}
									} else {
										nextPage = false;
										break;
									}
								}
								serialize(next);
							} else {
								nextPage = false;
							}
						}

					}
				} else if (!isClustering) {
					// retrieve from every occurrence found from tree;
					if (t.usedIndicescols.contains(colName)) {
						BTree b = (BTree) getDeserlaized("data//" + "BTree" + t.name + colName + ".class");
						Comparable k = (Comparable) key;
						// System.out.println(k);
						ReferenceValues ref = (ReferenceValues) b.search(k);
						ArrayList<String> midRes = new ArrayList<String>();
						// System.out.println(ref == null);
						if (ref != null) {
							for (int i = 0; i < ref.getOverflowNodes().size(); i++) {
								OverflowNode x = ref.getOverflowNodes().get(i);
								// System.out.println( x.referenceOfKeys.get(0));
								for (int j = 0; j < x.referenceOfKeys.size(); j++) {
									if ((x.referenceOfKeys.get(j) + " ").contains(t.name)) {
										midRes.add(x.referenceOfKeys.get(j) + " ");
										// System.out.println(x.referenceOfKeys.get(j) + " ");
									}
								}
							}
							result = getTuplesFromIndexSearch(midRes, t.name, colName, key, false);
						}
						// System.out.println("check");
						b.serializeTree();
					} else if (t.usedRtreeCols.contains(colName)) {
						RTree r = (RTree) getDeserlaized("data//" + "RTree" + t.name + colName + ".class");
						// Comparable k = (Comparable) key;
						// System.out.println(k);
						Polygon k = (Polygon) key;
						RTreeReferenceValues ref = (RTreeReferenceValues) r.search(k);
						ArrayList<String> midRes = new ArrayList<String>();
						// System.out.println(ref == null);
						if (ref != null) {
							for (int i = 0; i < ref.getRTreeOverflowNodes().size(); i++) {
								RTreeOverflowNode x = ref.getRTreeOverflowNodes().get(i);
								// System.out.println( x.referenceOfKeys.get(0));
								for (int j = 0; j < x.referenceOfKeys.size(); j++) {
									if ((x.referenceOfKeys.get(j) + " ").contains(t.name)) {
										midRes.add(x.referenceOfKeys.get(j) + " ");
										// System.out.println(x.referenceOfKeys.get(j) + " ");
									}
								}
							}
							result = getTuplesFromIndexSearch(midRes, t.name, colName, key, useArea);
						}
						// System.out.println("check");
						r.serializeTree();
					}

				}
			}
			// System.out.println(result);
		} catch (Exception e1) {
			throw new DBAppException("error in equal operation");
		}
		return result;
	}

	public ArrayList<Tuple> greaterThanOperator2(Table t, Object key, boolean indexed, boolean isClustering,
			String colName) throws DBAppException {
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		boolean foundStart = false;
		boolean start = false;
		try {
			if (!indexed) {
				if (isClustering) {
					// non-linear search
					int i = -1;
					int startTuple = -1;
					for (i = 0; i < t.usedPagesNames.size(); i++) {
						String pName = t.usedPagesNames.get(i);
						Page p = (Page) getDeserlaized("data//" + pName + ".class");
						if (!(p.vtrTuples.isEmpty())) {
							Tuple tup = p.vtrTuples.get(p.vtrTuples.size() - 1);
							Object tupKey = tup.vtrTupleObj.get(tup.index);
							if (Tuple.compareToHelper(tupKey, key) > 0) {
								// I am in the right page
								// search for tuple to begin with
								for (startTuple = 0; startTuple < p.vtrTuples.size(); startTuple++) {
									Tuple test = p.vtrTuples.get(startTuple);
									Object testKey = test.vtrTupleObj.get(test.index);
									if (Tuple.compareToHelper(testKey, key) > 0) {
										foundStart = true;
										break;
									}
								}
							}
						}
						serialize(p);
						if (foundStart) {
							start = true;
							break;
						}
					}
					if (start) {
						String pName = t.usedPagesNames.get(i);
						Page p = (Page) getDeserlaized("data//" + pName + ".class");
						for (int y = startTuple; y < p.vtrTuples.size(); y++) {
							result.add(p.vtrTuples.get(y));
							// System.out.println(p.vtrTuples.get(y) + "test");
						}
						i++;
						serialize(p);
						for (int z = i; z < t.usedPagesNames.size(); z++) {
							String nextPName = t.usedPagesNames.get(z);
							Page nextP = (Page) getDeserlaized("data//" + nextPName + ".class");
							for (int y = 0; y < nextP.vtrTuples.size(); y++) {
								result.add(nextP.vtrTuples.get(y));
								// System.out.println(p.vtrTuples.get(y) + "test");
							}
							serialize(nextP);
						}
					}
				} else if (!isClustering) {
					// linear search
					String tableName = t.name;
					int colNumber = getColNumber(tableName, colName);
					for (int i = 0; i < t.usedPagesNames.size(); i++) {
						String pageName = t.usedPagesNames.get(i);
						Page p = (Page) getDeserlaized("data//" + pageName + ".class");
						for (int j = 0; j < p.vtrTuples.size(); j++) {
							Tuple tup = p.vtrTuples.get(j);
							Object value = tup.vtrTupleObj.get(colNumber);
							if (Tuple.compareToHelper(value, key) > 0) {
								result.add(tup);
							}
						}
						serialize(p);
					}
				}
			} else if (indexed) {
				if (isClustering) {
					// use the tree to get only the first occurrence because the rest will be sorted
					// so no need to use the tree once more
					String pageName = "";
					if (t.usedIndicescols.contains(colName)) {
						BTree b = (BTree) getDeserlaized("data//" + "BTree" + t.name + colName + ".class");
						ArrayList<String> range = new ArrayList<String>();
						Comparable k = (Comparable) key;
						range = b.rangeMinSearch(k);
						if (!(range.isEmpty())) {
							for (int i = 0; i < range.size(); i++) {
								if (range.get(i).contains(t.name)) {
									pageName = range.get(i);
									break;
								}
							}
						}
						b.serializeTree();
					} else if (t.usedRtreeCols.contains(colName)) {
						RTree r = (RTree) getDeserlaized("data//" + "RTree" + t.name + colName + ".class");
						ArrayList<String> range = new ArrayList<String>();
						// Comparable k = (Comparable) key;
						Polygon k = (Polygon) key;
						range = r.rangeMinSearch(k);
						if (!(range.isEmpty())) {
							for (int i = 0; i < range.size(); i++) {
								if (range.get(i).contains(t.name)) {
									pageName = range.get(i);
									break;
								}
							}
						}
						r.serializeTree();
					}
					Page p = (Page) getDeserlaized("data//" + pageName + ".class");
					int i = p.number;
					int startTuple = -1;
					serialize(p);
					for (i = p.number; i < t.usedPagesNames.size(); i++) {
						String pName = t.usedPagesNames.get(i);
						Page up = (Page) getDeserlaized("data//" + pName + ".class");
						if (!(up.vtrTuples.isEmpty())) {
							Tuple tup = up.vtrTuples.get(up.vtrTuples.size() - 1);
							Object tupKey = tup.vtrTupleObj.get(tup.index);
							if (Tuple.compareToHelper(tupKey, key) > 0) {
								// I am in the right page
								// search for tuple to begin with
								for (startTuple = 0; startTuple < up.vtrTuples.size(); startTuple++) {
									Tuple test = up.vtrTuples.get(startTuple);
									Object testKey = test.vtrTupleObj.get(test.index);
									// to avoid getting values equal to our key as this is handled by equal operator
									if (Tuple.compareToHelper(testKey, key) > 0) {
										foundStart = true;
										break;
									}
								}
							}
						}
						serialize(up);
						if (foundStart) {
							start = true;
							break;
						}
					}
					if (start) {
						String pName = t.usedPagesNames.get(i);
						Page pp = (Page) getDeserlaized("data//" + pName + ".class");
						for (int y = startTuple; y < pp.vtrTuples.size(); y++) {
							result.add(pp.vtrTuples.get(y));
							// System.out.println(p.vtrTuples.get(y) + "test");
						}
						i++;
						serialize(pp);
						for (int z = i; z < t.usedPagesNames.size(); z++) {
							String nextPName = t.usedPagesNames.get(z);
							Page nextP = (Page) getDeserlaized("data//" + nextPName + ".class");
							for (int y = 0; y < nextP.vtrTuples.size(); y++) {
								result.add(nextP.vtrTuples.get(y));
								// System.out.println(p.vtrTuples.get(y) + "test");
							}
							serialize(nextP);
						}
					}

				} else if (!isClustering) {
					// retrieve from every occurrence found from tree
					if (t.usedIndicescols.contains(colName)) {
						BTree b = (BTree) getDeserlaized("data//" + "BTree" + t.name + colName + ".class");
						ArrayList<String> range = new ArrayList<String>();
						Comparable k = (Comparable) key;
						// System.out.println(k);
						range = b.rangeMinSearch(k);
						ArrayList<String> netRange = new ArrayList<String>();
						// System.out.println(range.get(0));
						if (!(range.isEmpty())) {
							for (int i = 0; i < range.size(); i++) {
								if (range.get(i).contains(t.name)) {
									netRange.add(range.get(i));
									// System.out.println(range.get(i));
								}
							}
							result = getTuplesFromIndexRangeSearch(netRange, t.name, colName, key, ">");

						}
						b.serializeTree();
					} else if (t.usedRtreeCols.contains(colName)) {
						RTree r = (RTree) getDeserlaized("data//" + "RTree" + t.name + colName + ".class");
						ArrayList<String> range = new ArrayList<String>();
						// Comparable k = (Comparable) key;
						// System.out.println(k);
						Polygon k = (Polygon) key;
						range = r.rangeMinSearch(k);
						ArrayList<String> netRange = new ArrayList<String>();
						// System.out.println(range.get(0));
						if (!(range.isEmpty())) {
							for (int i = 0; i < range.size(); i++) {
								if (range.get(i).contains(t.name)) {
									netRange.add(range.get(i));
									// System.out.println(range.get(i));
								}
							}
							result = getTuplesFromIndexRangeSearch(netRange, t.name, colName, key, ">");

						}
						r.serializeTree();
					} else {
						throw new DBAppException("error in greater than operation");
					}
				}

			}

		} catch (Exception e) {
			throw new DBAppException("error in greater than operator");
		}
		return result;
	}

	public ArrayList<Tuple> lessThanOperator2(Table t, Object key, boolean indexed, boolean isClustering,
			String colName) throws DBAppException {
		// in this operator linear search is more efficient
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		boolean stop = false;
		try {
			if (!indexed) {
				if (isClustering) {
					for (int i = 0; i < t.usedPagesNames.size(); i++) {
						String pName = t.usedPagesNames.get(i);
						Page p = (Page) getDeserlaized("data//" + pName + ".class");
						for (int j = 0; j < p.vtrTuples.size(); j++) {
							Tuple tup = p.vtrTuples.get(j);
							Object tupObj = tup.vtrTupleObj.get(tup.index);
							if (Tuple.compareToHelper(tupObj, key) < 0) {
								result.add(tup);
							} else {
								stop = true;
								break;
							}
						}
						serialize(p);
						if (stop) {
							break;
						}
					}
				} else if (!isClustering) {
					// linear search
					String tableName = t.name;
					int colNumber = getColNumber(tableName, colName);
					for (int i = 0; i < t.usedPagesNames.size(); i++) {
						String pageName = t.usedPagesNames.get(i);
						Page p = (Page) getDeserlaized("data//" + pageName + ".class");
						for (int j = 0; j < p.vtrTuples.size(); j++) {
							Tuple tup = p.vtrTuples.get(j);
							Object value = tup.vtrTupleObj.get(colNumber);
							if (Tuple.compareToHelper(value, key) < 0) {
								result.add(tup);
							}
						}
						serialize(p);
					}
				}
			} else if (indexed) {
				if (isClustering) {
					// there is no need to use the index as this can be retrieved from linear
					// traversing more efficiently
					for (int i = 0; i < t.usedPagesNames.size(); i++) {
						String pName = t.usedPagesNames.get(i);
						Page p = (Page) getDeserlaized("data//" + pName + ".class");
						for (int j = 0; j < p.vtrTuples.size(); j++) {
							Tuple tup = p.vtrTuples.get(j);
							Object tupObj = tup.vtrTupleObj.get(tup.index);
							if (Tuple.compareToHelper(tupObj, key) < 0) {
								result.add(tup);
							} else {
								stop = true;
								break;
							}
						}
						serialize(p);
						if (stop) {
							break;
						}
					}
				} else if (!isClustering) {
					// retrieve from every occurrence found from tree;
					if (t.usedIndicescols.contains(colName)) {
						BTree b = (BTree) getDeserlaized("data//" + "BTree" + t.name + colName + ".class");
						ArrayList<String> range = new ArrayList<String>();
						Comparable k = (Comparable) key;
						range = b.rangeMaxSearch(k);
						if (!(range.isEmpty())) {
							ArrayList<String> netRange = new ArrayList<String>();
							// System.out.println(range.get(0));
							if (!(range.isEmpty())) {
								for (int i = 0; i < range.size(); i++) {
									if (range.get(i).contains(t.name)) {
										netRange.add(range.get(i));
										// System.out.println(range.get(i));
									}
								}
								result = getTuplesFromIndexRangeSearch(range, t.name, colName, key, "<");
							}
						}
						b.serializeTree();
					} else if (t.usedRtreeCols.contains(colName)) {
						RTree r = (RTree) getDeserlaized("data//" + "RTree" + t.name + colName + ".class");
						ArrayList<String> range = new ArrayList<String>();
						// Comparable k = (Comparable) key;
						Polygon k = (Polygon) key;
						range = r.rangeMaxSearch(k);
						if (!(range.isEmpty())) {
							ArrayList<String> netRange = new ArrayList<String>();
							// System.out.println(range.get(0));
							if (!(range.isEmpty())) {
								for (int i = 0; i < range.size(); i++) {
									if (range.get(i).contains(t.name)) {
										netRange.add(range.get(i));
										// System.out.println(range.get(i));
									}
								}
								result = getTuplesFromIndexRangeSearch(range, t.name, colName, key, "<");
							}
						}
						r.serializeTree();
					} else {
						throw new DBAppException("error in less than operator");
					}
				}
			}
		} catch (Exception e) {
			throw new DBAppException("error in less than operator");
		}
		return result;
	}

	public ArrayList<Tuple> notEqualOperator2(Table t, Object key, boolean indexed, boolean isClustering,
			String colName) throws DBAppException {
		// for this operator linear search is more efficient in all cases
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		if (isClustering) {
			for (int i = 0; i < t.usedPagesNames.size(); i++) {
				String pName = t.usedPagesNames.get(i);
				Page p = (Page) getDeserlaized("data//" + pName + ".class");
				for (int j = 0; j < p.vtrTuples.size(); j++) {
					Tuple tup = p.vtrTuples.get(j);
					Object tupObj = tup.vtrTupleObj.get(tup.index);
					if (Tuple.compareToHelper(tupObj, key) != 0) {

						result.add(tup);
					}
				}
				serialize(p);
			}
		} else if (!isClustering) {
			// linear search
			String tableName = t.name;
			int colNumber = getColNumber(tableName, colName);
			for (int i = 0; i < t.usedPagesNames.size(); i++) {
				String pageName = t.usedPagesNames.get(i);
				Page p = (Page) getDeserlaized("data//" + pageName + ".class");
				for (int j = 0; j < p.vtrTuples.size(); j++) {
					Tuple tup = p.vtrTuples.get(j);
					Object value = tup.vtrTupleObj.get(colNumber);
					if (Tuple.compareToHelper(value, key) != 0) {
						result.add(tup);
					}
				}
				serialize(p);
			}
		}

		return result;
	}

	public static ArrayList<Tuple> andOperator2(ArrayList<Tuple> first, ArrayList<Tuple> second) throws DBAppException {
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		for (int i = 0; i < first.size(); i++) {
			Tuple f = first.get(i);
			String fs = f.toString();
			for (int j = 0; j < second.size(); j++) {
				Tuple s = second.get(j);
				String ss = s.toString();
				if (fs.equals(ss)) {
					result.add(f);
				}

			}
		}
		// System.out.println(result);
		return result;
	}

	public static ArrayList<Tuple> orOperator2(ArrayList<Tuple> first, ArrayList<Tuple> second) throws DBAppException {
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		// System.out.println(first.size());
		for (int i = 0; i < first.size(); i++) {
			Tuple f = first.get(i);
			String fs = f.toString();
			result.add(f);
			// System.out.println(fs);
			for (int j = 0; j < second.size(); j++) {
				Tuple s = second.get(j);
				String ss = s.toString();
				if (fs.equals(ss)) {
					second.remove(j);
				}

			}

		}
		// System.out.println(second.size());
		for (int i = 0; i < second.size(); i++) {
			// System.out.println(second.get(i));
			// System.out.println("check");
			result.add(second.get(i));
		}

//		for (int i = 0; i < first.size(); i++) {
//			result.add(first.get(i));
//		}

		return result;
	}

	public static ArrayList<Tuple> xorOperator2(ArrayList<Tuple> first, ArrayList<Tuple> second) throws DBAppException {
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		ArrayList<Tuple> andRes = new ArrayList<Tuple>();
		ArrayList<Tuple> orRes = new ArrayList<Tuple>();
		andRes = andOperator2(first, second);
		orRes = orOperator2(first, second);
		for (int i = 0; i < orRes.size(); i++) {
			Tuple f = orRes.get(i);
			String fs = f.toString();
			for (int j = 0; j < andRes.size(); j++) {
				Tuple s = andRes.get(j);
				String ss = s.toString();
				if (!(fs.equals(ss))) {
					result.add(f);
					break;
				}
			}
		}

		return result;
	}
	// boolean fCondition = false;
	// boolean sCondition = false;
//		for (int i = 0; i < first.size(); i++) {
//			Tuple f = first.get(i);
//			String fs = f.toString();
//			for (int j = 0; j < second.size(); j++) {
//				Tuple s = second.get(j);
//				String ss = s.toString();
//				if (fs.equals(ss)) {
//					first.remove(i);
//					second.remove(j);
//
////				}
////				if ((fCondition && !sCondition)) {
////					second.remove(j);
////					// System.out.println(fCondition);
////					// System.out.println(sCondition);
////					// System.out.println(first.get(i));
////					result.add(first.get(i));
////				} else if ((!fCondition && sCondition)) {
////					second.remove(j);
////					// System.out.println(fCondition);
////					// System.out.println(sCondition);
////					// System.out.println(first.get(i));
////					result.add(first.get(i));
////				}
//
//				}
//			}
//		}
//		for (int i = 0; i < first.size(); i++) {
//			result.add(first.get(i));
//		}
//		for (int i = 0; i < second.size(); i++) {
//			result.add(second.get(i));
//		}

	public static ArrayList<Tuple> handleOperators2(ArrayList<ArrayList<Tuple>> all, ArrayList<Integer> colNumbers,
			String[] strarrOperators) throws DBAppException {
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		int colRefer = -1;
		for (int i = 0; i < strarrOperators.length; i++) {
			String operator = strarrOperators[i];
			ArrayList<Tuple> first = new ArrayList<Tuple>();
			ArrayList<Tuple> second = new ArrayList<Tuple>();
			if (result.isEmpty()) {
				if (!all.isEmpty()) {
					first = all.remove(0);
					colRefer++;
					if (!all.isEmpty()) {
						second = all.remove(0);
						colRefer++;
					} else {
						throw new DBAppException("incorrect number of terms");
					}
				} else {
					throw new DBAppException("incorrect number of terms");
				}
			} else {
				first = result;
				if (!all.isEmpty()) {
					second = all.remove(0);
					colRefer++;
				}
			}
			switch (operator) {
			case ("AND"):
				result = andOperator2(first, second);
				break;
			case ("OR"):
				result = orOperator2(first, second);
				break;
			case ("XOR"):
				result = xorOperator2(first, second);
				break;
			default:
				throw new DBAppException("invalid operator");
			}
		}
		return result;
	}

	public Iterator selectFromTable(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {

		ArrayList<ArrayList<Tuple>> resList = new ArrayList<ArrayList<Tuple>>();
		ArrayList<Integer> colNumStore = new ArrayList<Integer>();
		boolean useArea = false;

		for (int i = 0; i < arrSQLTerms.length; i++) {

			String tableName = arrSQLTerms[i]._strTableName;
			// System.out.println(tableName);
			Table t = (Table) getDeserlaized("data/" + tableName + ".class");
			String colName = arrSQLTerms[i]._strColumnName;
			// System.out.println(colName);
			String operator = arrSQLTerms[i]._strOperator;
			// System.out.println(operator);
			Object obj = arrSQLTerms[i]._objValue;
			// System.out.println(obj);
			boolean indexed = isIndexed(tableName, colName);
			// System.out.println(indexed);
			boolean isClustering = isClusteringKey(tableName, colName);
			// System.out.println(isClustering);
			ArrayList<Tuple> midRes = new ArrayList<Tuple>();
			int colNum = getColNumber(tableName, colName);
			// System.out.println(colNum);
			colNumStore.add(colNum);

			switch (operator) {
			case ("="):
				midRes = equalOperator2(t, obj, indexed, isClustering, colName, false);
				// System.out.println(midRes);
				break;
			case ("!="):
				midRes = notEqualOperator2(t, obj, indexed, isClustering, colName);
				break;
			case (">"):
				midRes = greaterThanOperator2(t, obj, indexed, isClustering, colName);
				break;
			case ("<"):
				midRes = lessThanOperator2(t, obj, indexed, isClustering, colName);
				break;
			case (">="):
				ArrayList<Tuple> greater = new ArrayList<Tuple>();
				greater = greaterThanOperator2(t, obj, indexed, isClustering, colName);
				ArrayList<Tuple> equal = new ArrayList<Tuple>();
				equal = equalOperator2(t, obj, indexed, isClustering, colName, true);
				for (int j = 0; j < equal.size(); j++) {
					midRes.add(equal.get(j));
				}
				for (int j = 0; j < greater.size(); j++) {
					midRes.add(greater.get(j));
				}
				break;
			case ("<="):
				ArrayList<Tuple> less = new ArrayList<Tuple>();
				less = lessThanOperator2(t, obj, indexed, isClustering, colName);
				ArrayList<Tuple> equal2 = new ArrayList<Tuple>();
				equal2 = equalOperator2(t, obj, indexed, isClustering, colName, true);
				for (int j = 0; j < less.size(); j++) {
					midRes.add(less.get(j));
				}
				for (int j = 0; j < equal2.size(); j++) {
					midRes.add(equal2.get(j));
				}
				break;
			default:
				throw new DBAppException("invalid operator");

			}
			if (strarrOperators.length == 0 || i == arrSQLTerms.length - 1) {
				try {
					ObjectOutputStream bin = new ObjectOutputStream(new FileOutputStream("data//" + t.name + ".class"));
					bin.writeObject(t);
					bin.flush();
					bin.close();
				} catch (Exception e) {
					throw new DBAppException("error in serialization");
				}
			}
			resList.add(midRes);
		}

		// to be returned after applying operators2
		ArrayList<Tuple> almostLast = new ArrayList<Tuple>();
		if (strarrOperators.length == 0) {
			for (int i = 0; i < resList.size(); i++) {
				for (int j = 0; j < resList.get(i).size(); j++) {
					// System.out.println(resList.get(i).get(j));
					almostLast.add(resList.get(i).get(j));
					// System.out.println(resList.get(i).get(j));
				}
			}
		} else {
			almostLast = handleOperators2(resList, colNumStore, strarrOperators);

		}
		Iterator result = almostLast.iterator();
		return result;

	}

	public static ArrayList<Tuple> getTuplesFromIndexRangeSearch(ArrayList<String> midRes, String tableName,
			String colName, Object key, String operator) throws DBAppException {
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		int colNum = getColNumber(tableName, colName);
		String pageName = "";
		int count = -1;
		ArrayList<String> pageNames = new ArrayList<String>();
		ArrayList<String> copyNames = new ArrayList<String>();
		for (int i = 0; i < midRes.size(); i++) {
			copyNames.add(midRes.get(i));
		}
		// copyNames = midRes;
		for (int w = 0; w < midRes.size(); w++) {
			String newPageName = midRes.get(w);
			// System.out.println(newPageName);
			count = -1;
			for (int j = 0; j < copyNames.size(); j++) {
				if (newPageName.equals(copyNames.get(j))) {
					copyNames.remove(j);
					j--;
					count++;
				}
			}
			pageNames.add(newPageName + "_" + count);
			// System.out.println(newPageName + "_" + count);
		}
//			if (newPageName.equals(pageName)) {
//				count++;
//			} else if (count == -1) {
//				pageName = newPageName;
//				count = 0;
//			} else {
//				pageNames.add(pageName + "_" + count);
//				pageName = newPageName;
//				count = 0;
//
//			}
//			if (w == (midRes.size() - 1)) {
//				pageNames.add(pageName + "_" + count);
//				System.out.println(pageName + "_" + count);
//			}

		for (int u = 0; u < pageNames.size(); u++) {
			String[] x = pageNames.get(u).split("_");
			String[] pageNameSpace = x[0].split(" ");
			pageName = pageNameSpace[0];
			int count2 = Integer.parseInt(x[1]);
			Page p = (Page) getDeserlaized("data//" + pageName + ".class");
			for (int c = 0; c < p.vtrTuples.size(); c++) {
				Tuple toBeChecked = p.vtrTuples.get(c);
				Object checkKey = toBeChecked.vtrTupleObj.get(colNum);
				// System.out.println(checkKey);
				// long checKeyMod = modifyKey(checkKey);
				if (operator.equals(">")) {
					if (Tuple.compareToHelper(checkKey, key) > 0) {
						if (count2 > -1) {
							result.add(toBeChecked);
							count2--;
						}
						// System.out.println(toBeChecked.toString());
//						if (count2 == 0) {
//							result.add(toBeChecked);
//							break;
//						} else {
//							count2--;
//						}
					}
				} else if (operator.equals("<")) {
					if (Tuple.compareToHelper(checkKey, key) < 0) {
						if (count2 > -1) {
							result.add(toBeChecked);
							// System.out.println(toBeChecked);
							count2--;
						}
						// System.out.println(toBeChecked.toString());
//						if (count2 == 0) {
//							result.add(toBeChecked);
//							break;
//						} else {
//							count2--;
//						}
					}
				}
			}
			serialize(p);
		}
		return result;
	}

	public static ArrayList<Tuple> getTuplesFromIndexSearch(ArrayList<String> midRes, String tableName, String colName,
			Object key, boolean useArea) throws DBAppException {
		ArrayList<Tuple> result = new ArrayList<Tuple>();
		int colNum = getColNumber(tableName, colName);
		// System.out.println(colNum);
		String pageName = "";
		int count = -1;
		if ((key.getClass() + "").contains("java.awt.Polygon")) {
			myPolygon mpKey = new myPolygon((Polygon) key);
			key = mpKey;
		}
		ArrayList<String> pageNames = new ArrayList<String>();
		for (int w = 0; w < midRes.size(); w++) {
			String newPageName = midRes.get(w);
			// System.out.println(midRes.get(w));
			if (newPageName.equals(pageName)) {
				count++;
				// System.out.println(count);
			} else if (count == -1) {
				// System.out.println("check");
				pageName = newPageName;
				count = 0;
			} else {
				pageNames.add(pageName + "_" + count);
				pageName = newPageName;
				count = 0;
			}
			if (w == (midRes.size() - 1)) {
				pageNames.add(pageName + "_" + count);
				// System.out.println("check2");
			}
		}
		for (int u = 0; u < pageNames.size(); u++) {
			String[] x = pageNames.get(u).split("_");
			String[] pageNameSpace = x[0].split(" ");
			pageName = pageNameSpace[0];
			// System.out.println(pageName);
			int count2 = Integer.parseInt(x[1]);
			// System.out.println(count2);
			Page p = (Page) getDeserlaized("data//" + pageName + ".class");
			// System.out.println("check4");
			for (int c = 0; c < p.vtrTuples.size(); c++) {
				// System.out.println("check3");
				Tuple toBeChecked = p.vtrTuples.get(c);
				Object checkKey = toBeChecked.vtrTupleObj.get(colNum);
				if ((checkKey.getClass() + "").contains("java.awt.Polygon")) {
					myPolygon mp = new myPolygon((Polygon) checkKey);
					checkKey = mp;
//					myPolygon mpKey = new myPolygon((Polygon) key);
//					key = mpKey;
				}
				// System.out.println(checkKey);
				// long checKeyMod = modifyKey(checkKey);
				if (Tuple.compareToHelper(checkKey, key) == 0) {
					if (count2 > -1) {
						if (!useArea) {
							if (checkKey.equals(key)) {
								result.add(toBeChecked);
								count2--;
							} else {
								count2--;
							}
						} else {
							result.add(toBeChecked);
							count2--;
						}
					}
				}
			}
			serialize(p);
		}
		return result;
	}

	public static int getColNumber(String tableName, String colName) throws DBAppException {
		int result = -1;
		String csvFile = "data/metadata.csv";
		BufferedReader br = null;
		String line = "";
		String cvsSplitBy = ",";
		ArrayList<String> arrColumn = new ArrayList<String>();
		try {

			br = new BufferedReader(new FileReader(csvFile));
			int i = 0;
			boolean found = false;
			while ((line = br.readLine()) != null) {

				// use comma as separator
				String[] d = line.split(cvsSplitBy);
				if (d[0].equals(tableName)) {
					if (d[1].equals(colName)) {
						found = true;
						break;
					} else {
						i++;
					}
				}
			}
			if (found) {
				result = i;
			} else {
				throw new DBAppException("column name not found");
			}
			br.close();

		} catch (Exception e) {
			throw new DBAppException("error in finding column number");
		}
		return result;
	}

	public static boolean handleSelectionOperatorsLinearly(String tableName, Tuple tup, String[] strarrOperators,
			SQLTerm[] arrSQLTerms) throws DBAppException {
		boolean result = false;
		boolean[] selection = new boolean[arrSQLTerms.length];
		for (int i = 0; i < selection.length; i++) {
			selection[i] = false;
		}
		// boolean selection = false;
		for (int i = 0; i < arrSQLTerms.length; i++) {
			String colName = arrSQLTerms[i]._strColumnName;
			String operator = arrSQLTerms[i]._strOperator;
			Object key = arrSQLTerms[i]._objValue;
			int colNum = getColNumber(tableName, colName);
			Object tupKey = tup.vtrTupleObj.get(colNum);
			switch (operator) {
			case ("="):
				if (Tuple.compareToHelper(key, tupKey) == 0) {
					selection[i] = true;
				} else {
					selection[i] = false;
				}
				break;
			case (">"):
				if (Tuple.compareToHelper(tupKey, key) > 0) {
					selection[i] = true;
				} else {
					selection[i] = false;
				}
				break;
			case (">="):
				if (Tuple.compareToHelper(tupKey, key) >= 0) {
					selection[i] = true;
				} else {
					selection[i] = false;
				}
				break;
			case ("<"):
				if (Tuple.compareToHelper(tupKey, key) < 0) {
					selection[i] = true;
				} else {
					selection[i] = false;
				}
				break;
			case ("<="):
				if (Tuple.compareToHelper(tupKey, key) <= 0) {
					selection[i] = true;
				} else {
					selection[i] = false;
				}
				break;
			case ("!="):
				if (Tuple.compareToHelper(tupKey, key) != 0) {
					selection[i] = true;
				} else {
					selection[i] = false;
				}
				break;
			default:
				throw new DBAppException("invalid selection operator");
			}
		}

		boolean midRes = false;
		if (strarrOperators.length == 0 && selection.length != 0) {
			// System.out.println(midRes = selection[0]);
			midRes = selection[0];
			// System.out.println("check");
		}
		boolean firstIteration = true;
		boolean first = false;
		boolean second = false;
		int current = 0;
		for (int i = 0; i < strarrOperators.length; i++) {
			// System.out.println("check");
			if (current < selection.length - 1) {
				first = selection[current];
				second = false;
				if (firstIteration) {
					second = selection[current + 1];
				} else {
					second = midRes;
				}
			}
			String setOperator = strarrOperators[i];
			switch (setOperator) {
			case ("AND"):
				if (first && second) {
					midRes = true;
				} else {
					midRes = false;
				}
				break;
			case ("OR"):
				if (first || second) {
					midRes = true;
				} else {
					midRes = false;
				}
				break;
			case ("XOR"):
				if ((first && !second) || (!first && second)) {
					midRes = true;
				} else {
					midRes = false;
				}
				break;
			default:
				throw new DBAppException("invalid set operator");
			}
			current++;
		}
		// System.out.println(result = midRes);
		result = midRes;

		return result;

	}

//	public static ArrayList<Tuple> equalOperatorIndex(Table t, BPlusTree b, long modified, String colName, Object key)
//			throws DBAppException {
//		ArrayList<Tuple> result = new ArrayList<Tuple>();
//		try {
////			boolean nextPage = true;
////			String tab = b.treeName;
////			String[] tabb = tab.split("_");
//			// String tableName = tabb[0];
//			String tableName = t.name;
//
//			// binary search to find the tuple
//			// SearchResult s = b.searchKey(modified, false);
//			BTree btree = new BTree(); // TODO change this to deserialized tree
//			Comparable k = (Comparable) key;
//			ReferenceValues ref = (ReferenceValues) btree.search(k);
//			if (!(ref.getReferences().isEmpty())) {
//				if (isClusteringKey(tableName, colName)) {
//					// String fullIndex = s.getValues().getFirst();
//					OverflowNode n = ref.getReferences().get(0);
//					String pageName = n.referenceOfKeys.get(0) + "";
//					// String[] separated = fullIndex.split(",");
//					// String pageName = separated[0];
////					// int firstOcc = Integer.parseInt(separated[1]);
//					Page p = (Page) getDeserlaized("data//" + pageName + ".class");
//					int pageNumber = p.number;
//
//					boolean flag = false;
//					int lowerBound = 0;
//					int upperBound = p.vtrTuples.size() - 1;
//					int curIn = -1;
//					int i = 0;
//					while (!flag) {
//						curIn = (lowerBound + upperBound) / 2;
//						Tuple testTuple = p.vtrTuples.get(curIn);
//						Object comkey = testTuple.vtrTupleObj.get(testTuple.index);
//						if (Tuple.compareToHelper(comkey, key) == 0) {
//							flag = true;
//							// to handle duplicates
//							while (curIn > 0) {
//								Tuple prevTuple = p.vtrTuples.get(curIn - 1);
//								Object prevkey = prevTuple.vtrTupleObj.get(prevTuple.index);
//								if (prevkey.equals(key)) {
//									curIn--;
//								} else {
//									break;
//								}
//							}
//						} else if (lowerBound > upperBound) {
//							throw new DBAppException("key not found"); // can't find it
//						}
//						if (Tuple.compareToHelper(comkey, key) < 0) {
//							// this means that my key is greater search down
//							lowerBound = curIn + 1;
//						} else {
//							if (Tuple.compareToHelper(comkey, key) > 0) {
//								// this means that my key is smaller search up
//								upperBound = curIn - 1;
//							}
//						}
//					}
//					boolean nextPage = true;
//					for (int j = curIn; j < p.vtrTuples.size(); j++) {
//
//						Tuple tup = p.vtrTuples.get(j);
//						Object tupKey = tup.vtrTupleObj.get(tup.index);
//						if (Tuple.compareToHelper(tupKey, key) == 0) {
//							result.add(tup);
//						} else {
//							nextPage = false;
//							break;
//						}
//					}
//					serialize(p);
//					while (nextPage) {
//						pageNumber++;
//						if (pageNumber < t.usedPagesNames.size()) {
//							// System.out.println("check6");
//							String secondPage = t.usedPagesNames.get(pageNumber);
//							Page next = (Page) getDeserlaized("data//" + secondPage + ".class");
//							for (int j = 0; j < next.vtrTuples.size(); j++) {
//								Tuple tup = next.vtrTuples.get(j);
//								Object tupKey = tup.vtrTupleObj.get(tup.index);
//								if (tupKey.equals(key)) {
//									for (int z = 0; z < tup.vtrTupleObj.size(); z++) {
//										result.add(tup);
//									}
//								} else {
//									nextPage = false;
//									break;
//								}
//							}
//							serialize(next);
//						} else {
//							nextPage = false;
//						}
//					}
//				} else {
//					// retrieve from every occurrence found from tree;
//					// search linearly for the each occurrence
//					ArrayList<String> midRes = new ArrayList<String>();
//					for (int i = 0; i < ref.getReferences().size(); i++) {
//						OverflowNode x = ref.getReferences().get(i);
//						System.out.println("size =" + x.referenceOfKeys.size());
//						for (int j = 0; j < x.referenceOfKeys.size(); j++) {
//							midRes.add(x.referenceOfKeys.get(j) + " ");
//						}
//					}
//
//					result = getTuplesFromIndexSearch(midRes, tableName, colName, key);
//
////					int colNum = getColNumber(tableName, colName);
////					String pageName = "";
////					int count = -1;
////					ArrayList<String> pageNames = new ArrayList<String>();
////					for (int w = 0; w < s.getValues().size(); w++) {
////						String fullIndex = s.getValues().get(w);
////						String[] separated = fullIndex.split(",");
////						String newPageName = separated[0];
////						if (newPageName.equals(pageName)) {
////							count++;
////						} else if (count == -1) {
////							pageName = newPageName;
////							count = 0;
////						} else {
////							pageNames.add(pageName + "_" + count);
////							pageName = newPageName;
////							count = 0;
////
////						}
////						if (w == (s.getValues().size() - 1)) {
////							pageNames.add(pageName + "_" + count);
////						}
////					}
//
////					for (int u = 0; u < pageNames.size(); u++) {
////						String[] x = pageNames.get(u).split("_");
////						pageName = x[0];
////						int count2 = Integer.parseInt(x[1]);
////						Page p = (Page) getDeserlaized("data//" + pageName + ".class");
////						for (int c = 0; c < p.vtrTuples.size(); c++) {
////							Tuple toBeChecked = p.vtrTuples.get(c);
////							Object checkKey = toBeChecked.vtrTupleObj.get(colNum);
////							// System.out.println(checkKey);
////							// long checKeyMod = modifyKey(checkKey);
////							if (Tuple.compareToHelper(checkKey, key) == 0) {
////								System.out.println(toBeChecked.toString());
////								if (count2 == 0) {
////									result.add(toBeChecked);
////									break;
////								} else {
////									count2--;
////								}
////							}
////						}
////						serialize(p);
////					}
//				}
//			}
//		} catch (Exception e) {
//			throw new DBAppException("error in equal with index");
//		}
//		return result;
//	}

//	public static ArrayList<Tuple> handleIndexed(Table t, BPlusTree b, Object key, String operator, String colName)
//			throws DBAppException {
//		ArrayList<Tuple> result = new ArrayList<Tuple>();
//		long modified = modifyKey(key);
//		switch (operator) {
//		case ("="):
//			result = equalOperatorIndex(t, b, modified, colName, key);
//		}
////		// SearchResult s = b.searchKey(modified, false);
////		RangeResult r = b.rangeStopSearch(modified, false);
////		RangeResult s = new RangeResult();
////		for (int w = 0; w < r.getQueryResult().size(); w++) {
////			long k = r.getQueryResult().get(w).getKey();
////			if (k != modified) {
////				// to avoid getting values equal to our key as this is handled by equal operator
////				s.getQueryResult().add(r.getQueryResult().get(w));
////			}
////		}
////		for (int w = 0; w < s.getQueryResult().size(); w++) {
////			String fullIndex = s.getQueryResult().get(w).getValue();
////			String[] separated = fullIndex.split(",");
////			String pageName = separated[0];
////			String n = separated[1];
////			String[] removeSpaces = n.split(" ");
////			String m = removeSpaces[0];
////			// System.out.println(n.getClass());
////			int tupPosition = Integer.parseInt(m);
////			Page p = (Page) getDeserlaized("data//" + pageName + ".class");
////			result.add(p.vtrTuples.get(tupPosition));
////			serialize(p);
////
////		}
////		serializeTree(b);
////	}
//		return result;
//
//	}

//	public static ArrayList<Tuple> handleSetOperatorsIndex(ArrayList<ArrayList<Tuple>> midRes, String[] strarrOperators)
//			throws DBAppException {
//		ArrayList<Tuple> result = new ArrayList<Tuple>();
//		return result;
//
//	}

//	public Iterator selectFromTable2(SQLTerm[] arrSQLTerms, String[] strarrOperators) throws DBAppException {
//
//		ArrayList<Tuple> resultList = new ArrayList<Tuple>();
//
//		if (arrSQLTerms.length != 0) {
//			String tableName = arrSQLTerms[0]._strTableName;
//			Table t = (Table) getDeserlaized("data/" + tableName + ".class");
//			ArrayList<String> colNames = new ArrayList<String>();
//			for (int i = 0; i < arrSQLTerms.length; i++) {
//				String col = arrSQLTerms[i]._strColumnName;
//				colNames.add(col);
//				// this arrayList will be used to know which col has an index
//			}
//			// boolean noneIndexed = false;
//			boolean existsIndexed = false;
//			boolean existsClustering = false;
//			boolean allIndexed = true;
//
//			// case 3 one of them is clustering and not indexed => check if there is an or
//			// condition
//			// if yes then check if the col after or is indexed => if yes, then use index
//			// else => linear
//			// if no then check if other col have index => if yes , then use index. else =>
//			// use binary search for clustering and check conditions
//
//			for (int i = 0; i < colNames.size(); i++) {
//				String curCol = colNames.get(i);
//				if (isClusteringKey(tableName, curCol)) {
//					existsClustering = true;
//				}
//				if (isIndexed(tableName, curCol)) {
//					existsIndexed = true;
//				} else {
//					allIndexed = false;
//				}
//			}
//			if (!existsIndexed && !existsClustering) {
//
//				// case 1 none is indexed and none is clustering => linear
//				for (int i = 0; i < t.usedPagesNames.size(); i++) {
//					String pageName = t.usedPagesNames.get(i);
//					Page p = (Page) getDeserlaized("data/" + pageName + ".class");
//					for (int j = 0; j < p.vtrTuples.size(); j++) {
//						Tuple tup = p.vtrTuples.get(j);
//						boolean satisfied = false;
//						satisfied = handleSelectionOperatorsLinearly(tableName, tup, strarrOperators, arrSQLTerms);
//						if (satisfied) {
//							// System.out.println("check");
//							resultList.add(tup);
//						}
//					}
//					serialize(p);
//				}
//
//			} else if (allIndexed) {
//				// case 2 all are indexed => use index for each one
//				ArrayList<ArrayList<Tuple>> midRes = new ArrayList<ArrayList<Tuple>>();
//				for (int i = 0; i < arrSQLTerms.length; i++) {
//					String colName = arrSQLTerms[i]._strColumnName;
//					String operator = arrSQLTerms[i]._strOperator;
//					Object key = arrSQLTerms[i]._objValue;
//					BPlusTree b = (BPlusTree) deserializeTree("data//" + t.name + "_" + colName + ".class");
//					midRes.add(handleIndexed(t, b, key, operator, colName));
//					serializeTree(b);
//				}
//				resultList = handleSetOperatorsIndex(midRes, strarrOperators);
//
//			}
//
//			try {
//				ObjectOutputStream bin = new ObjectOutputStream(new FileOutputStream("data//" + t.name + ".class"));
//				bin.writeObject(t);
//				bin.flush();
//				bin.close();
//			} catch (Exception e) {
//				throw new DBAppException("error in serialization");
//			}
//		}
//		Iterator result = resultList.iterator();
//		return result;
//	}

	public static void makeIndexed(String tableName, String colName) throws DBAppException {

		try {
			// boolean flag = false;
			boolean colFound = false;
			String csvFile = "data/metadata.csv";
			BufferedReader br = null;
			String line = "";
			String cvsSplitBy = ",";
			ArrayList<String> arrColumn = new ArrayList<String>();
			String returnBack = "";

			try {

				br = new BufferedReader(new FileReader(csvFile));

				while ((line = br.readLine()) != null) {
//					System.out.println("line: " + line);

					// use comma as separator
					String[] d = line.split(cvsSplitBy);
//					System.out.println("d size: " + d.length);
					if (d[0].equals(tableName)) {
						// System.out.println(d[1]);
						if (d[1].equals(colName)) {
							colFound = true;
							d[4] = "true";
						}
					}

					for (int i = 0; i < d.length; i++) {
						returnBack = returnBack + d[i] + ",";
					}
					returnBack += "\n";
//					System.out.println(returnBack + "|||||||||");
				}
				br.close();
				File file = new File("data/metadata.csv");
				BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(file, true));
				PrintWriter fileWriter = new PrintWriter(bufferedWriter);
				FileWriter writer = new FileWriter(file);
//				System.out.println("check");
				writer.append(returnBack);
//				writer.append("\n");
				writer.flush();
				writer.close();
				if (!colFound) {
					throw new DBAppException("column not found");
				}

			} catch (Exception e) {
				throw new DBAppException("error in making a column indexed");
			}

		} catch (Exception e) {
			throw new DBAppException("error in changing index state");
		}
	}

	public void createBTreeIndex(String strTableName, String strColName)
			throws DBAppException, FileNotFoundException, IOException {
		// check table exists
		boolean found = checkIfTableFound(strTableName);

		if (!found) {
			throw new DBAppException("Table does not exist");
		} else {
			// check column exists
			ArrayList<String> columns = getColNames(strTableName);
			if (!columns.contains(strColName)) {
				throw new DBAppException("Column does not exist");
			} else {
				// check column is of type polygon
				ArrayList<String> nametype = getArrayOfColoumnDataTyoe(strTableName);

				if (nametype.contains(strColName + ",java.awt.Polygon")) {
					throw new DBAppException("Cannot create a B+Tree on a column of type Polygon");

				} else {

					// check column does not already have an index isindex
					if (isIndexed(strTableName, strColName)) {
						throw new DBAppException("Column already have an index");
					} else {

						// change indexed false to true in metadata
						makeIndexed(strTableName, strColName);

						// get column index in tuple
						int colIndex = columns.indexOf(strColName);

						// create a new BPlusTree
						// TODO restrict max keys in node (page size and key size)
						BTree bt = new BTree();
						bt.treeName = "BTree" + strTableName + strColName;

						/*
						 * Insert already existing records keys into tree loop on all tuples in table
						 * and insert each key (modify col content) and value(pointer: page name,tuple
						 * index)
						 */
						Table table = (Table) getDeserlaized("data//" + strTableName + ".class");
						Vector<String> usedPages = table.usedPagesNames;

						for (int i = 0; i < usedPages.size(); i++) {

							Page curPage = (Page) (getDeserlaized("data//" + table.usedPagesNames.get(i) + ".class"));
							Vector<Tuple> Tuples = curPage.vtrTuples;

							for (int j = 0; j < Tuples.size(); j++) {
								Tuple curTuple = Tuples.get(j);
								Object key = curTuple.vtrTupleObj.get(colIndex);
								bt.insert((Comparable) key, table.usedPagesNames.get(i));
							}
							serialize(curPage);
						}
						// add index name to table list of usedIndicesNames then serialize table
						table.usedIndicescols.add(strColName);
						table.usedIndicesNames.add(bt.treeName); // or should we just add column name??
						FileOutputStream f1 = new FileOutputStream("data//" + strTableName + ".class");
						ObjectOutputStream bin1 = new ObjectOutputStream(f1);
						bin1.writeObject(table);
						bin1.flush();
						bin1.close();

						// serialize tree

						bt.serializeTree();

					}
				}
			}
		}
	}

	public void createRTreeIndex(String strTableName, String strColName)
			throws DBAppException, FileNotFoundException, IOException {
		// check table exists
		boolean found = checkIfTableFound(strTableName);

		if (!found) {
			throw new DBAppException("Table does not exist");
		} else {
			// check column exists
			ArrayList<String> columns = getColNames(strTableName);
			if (!columns.contains(strColName)) {
				throw new DBAppException("Column does not exist");
			} else {
				// check column is of type polygon
				ArrayList<String> nametype = getArrayOfColoumnDataTyoe(strTableName);

				if (!nametype.contains(strColName + ",java.awt.Polygon")) {
					throw new DBAppException("You can only create a RTree on a column of type Polygon");
				} else {
					// check column does not already have an index
					if (isIndexed(strTableName, strColName)) {
						throw new DBAppException("Column already have an index");
					} else {

						// change indexed false to true in metadata
						makeIndexed(strTableName, strColName);

						// get column index in tuple
						int colIndex = columns.indexOf(strColName);

						// create a new RTree
						RTree rt = new RTree();
						rt.treeName = "RTree" + strTableName + strColName;

						/*
						 * Insert already existing records keys into tree loop on all tuples in table
						 * and insert each key (modify col content) and value(pointer: page name,tuple
						 * index)
						 */
						Table table = (Table) getDeserlaized("data//" + strTableName + ".class");
						Vector<String> usedPages = table.usedPagesNames;

						for (int i = 0; i < usedPages.size(); i++) {

							Page curPage = (Page) (getDeserlaized("data//" + table.usedPagesNames.get(i) + ".class"));
							Vector<Tuple> Tuples = curPage.vtrTuples;

							for (int j = 0; j < Tuples.size(); j++) {
								Tuple curTuple = Tuples.get(j);
								Object key = curTuple.vtrTupleObj.get(colIndex);
								rt.insert((Polygon) key, table.usedPagesNames.get(i));
							}
							serialize(curPage);
						}
						// add index name to table list of usedIndicesNames then serialize table

						table.usedRtreeCols.add(strColName);
						table.usedRtreeNames.add(rt.treeName); // or should we just add column name??
						FileOutputStream f1 = new FileOutputStream("data//" + strTableName + ".class");
						ObjectOutputStream bin1 = new ObjectOutputStream(f1);
						bin1.writeObject(table);
						bin1.flush();
						bin1.close();

						// serialize tree
						rt.serializeTree();

					}
				}
			}
		}
	}

	public void checkpolygon() throws DBAppException, IOException {
		String strTableName = "Student";
		Hashtable<String, String> htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("n", "java.lang.String");
//		htblColNameType.put("shape", "java.awt.Polygon");
////		htblColNameType.put("poly", "java.awt.Polygon");
//		createTable(strTableName, "id", htblColNameType);
//////
		for (int i = 0; i < 13; i++) {
			Hashtable htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(i));
			htblColNameValue.put("name", new String("Ab"));
//			htblColNameValue.put("n", new String("C"));
//			Polygon p = new Polygon();
//			p.addPoint(3,3);
//			p.addPoint(20,20);
//			htblColNameValue.put("shape", p);
//			Polygon p2 = new Polygon();
//			p2.addPoint(2,2);
//			p2.addPoint(i, i);
//			htblColNameValue.put("poly", p2);
//			insertIntoTable(strTableName, htblColNameValue);
		}

		Hashtable<String, Object> htblColNameValue = new Hashtable();
//		Polygon pol = new Polygon();
//		pol.addPoint(3,3);
//		pol.addPoint(20,20);
//		htblColNameValue.put("shape", pol);
//		htblColNameValue.put("id", 2);
		htblColNameValue.put("name", "Ab9");
//		deleteFromTable(strTableName, htblColNameValue);

//		Hashtable hash = new Hashtable();
//		hash.put("name", new String("wwwwwwwww"));		
//		updateTable(strTableName, "(3,3),(3,3)", hash);

//		createRTreeIndex(strTableName, "shape");
//		createRTreeIndex(strTableName, "poly");
//		createBTreeIndex(strTableName, "id");

//		Hashtable htblColNameValue = new Hashtable();
//		htblColNameValue.put("id", new Integer(6));
//		htblColNameValue.put("name", new String("Abbb"));
//		Polygon p1 = new Polygon();
//		p1.addPoint(2,2);
//		p1.addPoint(2,2);
//		htblColNameValue.put("shape", p1);
////		Polygon p2 = new Polygon();
////		p2.addPoint(2,2);
////		p2.addPoint(2,2);
////		htblColNameValue.put("poly", p2);
//		insertIntoTable(strTableName, htblColNameValue);
////	
		if (false) {

			RTree rt = (RTree) getDeserlaized("data//" + "RTree" + strTableName + "shape" + ".class");
			System.out.println(rt.treeName);
			System.out.println(rt.toString());

			Polygon p = new Polygon();
			p.addPoint(3, 3);
			p.addPoint(1, 1);
			RTreeReferenceValues ref = (RTreeReferenceValues) rt.search(p);
			for (int i = 0; i < ref.getRTreeOverflowNodes().size(); i++) {
				RTreeOverflowNode b = ref.getRTreeOverflowNodes().get(i);
				for (int j = 0; j < b.referenceOfKeys.size(); j++) {
					System.out.print(b.referenceOfKeys.get(j) + " ");
				}
				System.out.println();
			}
		}
//		RTree rt= (RTree) getDeserlaized("data//" + "RTree"+strTableName+"poly" + ".class");
//		System.out.println(rt.treeName);
//		System.out.println(rt.toString());
//		
//			Polygon p = new Polygon();
//			p.addPoint(2,2);
//			p.addPoint(2,2);
//		 RTreeReferenceValues ref = (RTreeReferenceValues) rt.search(p2);
//			for (int i = 0; i < ref.getRTreeOverflowNodes().size(); i++) {
//			RTreeOverflowNode b = ref.getRTreeOverflowNodes().get(i);
//			for (int j = 0; j < b.referenceOfKeys.size(); j++) {
//				System.out.print(b.referenceOfKeys.get(j) + " ");
//			}
//			System.out.println();
//		}

//		BTree bt= (BTree) getDeserlaized("data//" + "BTree"+strTableName+"id" + ".class");
//		System.out.println(bt.toString());
//		ReferenceValues ref = (ReferenceValues) bt.search(10);
//			for (int i = 0; i < ref.getOverflowNodes().size(); i++) {
//			OverflowNode b = ref.getOverflowNodes().get(i);
//			for (int j = 0; j < b.referenceOfKeys.size(); j++) {
//				System.out.print(b.referenceOfKeys.get(j) + " ");
//			}
//			System.out.println();
//		}
////		
		// displayTableContent(strTableName);
	}

	public static void main(String[] args)
			throws FileNotFoundException, DBAppException, IOException, InterruptedException {

		DBApp dbApp = new DBApp();
		dbApp.init();
//    System.out.println(dbApp.maxPageSize);
		String strTableName = "Student";

		dbApp.checkpolygon();
//*create table*
		Hashtable<String, String> htblColNameType = new Hashtable();
		htblColNameType.put("id", "java.lang.Integer");
		htblColNameType.put("name", "java.lang.String");
		htblColNameType.put("age", "java.lang.Integer");
//		htblColNameType.put("date", "java.util.Date");
//		htblColNameType.put("gpa", "java.lang.Double");
//		htblColNameType.put("shape", "java.awt.Polygon");
//		htblColNameType.put("grad", "java.lang.Boolean");

	//	dbApp.createTable(strTableName, "id", htblColNameType);

		//
	//
		//
	//
	//	dbApp.createBTreeIndex(strTableName, "id");
//		dbApp.createBTreeIndex(strTableName, "age");

//	dbApp.makeIndexed(strTableName, "name");

//	Table a=(Table)getDeserlaized("data//Student.class");
//	System.out.println(a.colNames[0]);
//	System.out.println(a.colNames[1]);
//	System.out.println(a.colNames[2]);

//* insert tuples*

//		for (int i = 0; i < 9; i++) {

			Hashtable htblColNameValue = new Hashtable();
			htblColNameValue.put("id", new Integer(2));
			htblColNameValue.put("name", new String("Ab"));
			htblColNameValue.put("age", 3 * 10);
			dbApp.insertIntoTable(strTableName, htblColNameValue);
//		}

//	htblColNameValue.put("age", new Integer(25));
//	htblColNameValue.put("date", new Date(2000, 11, 23));
////////		System.out.println((new Date(2020, 11, 11).getClass()));
////////		System.out.println((new Date(2020, 11, 11)).toString());
//	
//	htblColNameValue.put("gpa", new Double(2.0));
//		
//	if (4 % 2 == 0) {
//		htblColNameValue.put("grad", true);
//	} else
//		htblColNameValue.put("grad", false);
//	Polygon p = new Polygon();
//	p.addPoint(2, 2);
//	p.addPoint(5, 5);
//	
//////		 System.out.println("n:"+p.npoints);
//	htblColNameValue.put("shape", p);
//	

		// dbApp.deleteFromTable(strTableName, htblColNameValue);

//	 dbApp.createBTreeIndex(strTableName, "id");
//	 
//		RTree a = (RTree) (getDeserlaized("data//" + "RTree" + strTableName + "shape" + ".class"));
//		System.out.println(a.toString());
//		a.serializeTree();
//	 
//	 RTreeReferenceValues ref = (RTreeReferenceValues) a.search(p);
//		for (int i = 0; i < ref.getRTreeOverflowNodes().size(); i++) {
//		RTreeOverflowNode b = ref.getRTreeOverflowNodes().get(i);
//		for (int j = 0; j < b.referenceOfKeys.size(); j++) {
//			System.out.print(b.referenceOfKeys.get(j) + " ");
//		}
//		System.out.println();
//	}

//dbApp.checkpolygon();
//**create table**

//		Hashtable<String, String> htblColNameType = new Hashtable();
//
//		htblColNameType.put("id", "java.lang.Integer");
//		htblColNameType.put("name", "java.lang.String");
//		htblColNameType.put("age", "java.lang.Integer");
//		htblColNameType.put("date", "java.util.Date");
//		htblColNameType.put("gpa", "java.lang.Double");
//		htblColNameType.put("shape", "java.awt.Polygon");
//		htblColNameType.put("grad", "java.lang.Boolean");

		// dbApp.createTable(strTableName, "id", htblColNameType);
		// dbApp.createBTreeIndex(strTableName, "id");

//		dbApp.makeIndexed(strTableName, "name");

//		Table a=(Table)getDeserlaized("data//Student.class");
//		System.out.println(a.colNames[0]);
//		System.out.println(a.colNames[1]);
//		System.out.println(a.colNames[2]);

//	for (int i = 0; i < 210; i++) {

////		for (int i = 0; i < 210; i++) {

//		Hashtable htblColNameValue = new Hashtable();
//		htblColNameValue.put("id", new Integer(2));
//		htblColNameValue.put("name", new String("Ab"));
//		htblColNameValue.put("age", new Integer(25));
//		htblColNameValue.put("date", new Date(2000, 11, 23));
////////		System.out.println((new Date(2020, 11, 11).getClass()));
////////		System.out.println((new Date(2020, 11, 11)).toString());
//
//		htblColNameValue.put("gpa", new Double(2.0));
//
//		if (4 % 2 == 0) {
//			htblColNameValue.put("grad", true);
//		} else
//			htblColNameValue.put("grad", false);
//		Polygon p = new Polygon();
//		p.addPoint(1, 1);
//		p.addPoint(0, 0);

////		 System.out.println("n:"+p.npoints);
//		htblColNameValue.put("shape", p);
//
//		dbApp.insertIntoTable(strTableName, htblColNameValue);
//
//		BTree a = (BTree) (getDeserlaized("data//" + "BTree" + strTableName + "id" + ".class"));
//		System.out.println(a.toString());

//		ReferenceValues ref = (ReferenceValues) a.search(0);
//		for (int i = 0; i < ref.getOverflowNodes().size(); i++) {
//			OverflowNode b = ref.getOverflowNodes().get(i);
//			// System.out.println("size =" + b.referenceOfKeys.size());
//			for (int j = 0; j < b.referenceOfKeys.size(); j++) {
//				System.out.print(b.referenceOfKeys.get(j) + " ");
//			}
//			System.out.println();
//		}

//	Hashtable htblColNameValue = new Hashtable();
//	htblColNameValue.put("id", new Integer(50));
//	htblColNameValue.put("name", new String("c"));
//	htblColNameValue.put("age", new Integer("50"));
////////////	htblColNameValue.put("date", new Date(2000, 12, 23));
//////////	Polygon p = new Polygon();
//////////	p.addPoint(1,3);
//////////	p.addPoint(2,4);
////////////////////	System.out.println("n:"+p.npoints);
//////////	htblColNameValue.put("shape",  p);

//	dbApp.insertIntoTable(strTableName, htblColNameValue);
//	}
//	

//		for (int i = 0; i < 210; i++) {

//		Hashtable htblColNameValue = new Hashtable();
//		htblColNameValue.put("id", new Integer(1));
//		htblColNameValue.put("name", new String("Ab"));
//		htblColNameValue.put("age", new Integer(25));
//		htblColNameValue.put("date", new Date(2000, 11, 23));
////////			System.out.println((new Date(2020, 11, 11).getClass()));
////////			System.out.println((new Date(2020, 11, 11)).toString());
//		
//		htblColNameValue.put("gpa", new Double(2.0));
//			
//		if (4 % 2 == 0) {
//			htblColNameValue.put("grad", true);
//		} else
//			htblColNameValue.put("grad", false);
//		Polygon p = new Polygon();
//		p.addPoint(1, 1);
//		p.addPoint(0, 0);
//		
//////			 System.out.println("n:"+p.npoints);
//		htblColNameValue.put("shape", p);

//		 dbApp.insertIntoTable(strTableName, htblColNameValue);

//		 BTree a = (BTree)(getDeserlaized("data//" +"BTree"+strTableName+"id" + ".class"));
//		 System.out.println(a.toString());
//		 
//		 ReferenceValues ref = (ReferenceValues) a.search(0);
//		for (int i = 0; i < ref.getOverflowNodes().size(); i++) {
//			OverflowNode b = ref.getOverflowNodes().get(i);
//			//System.out.println("size =" + b.referenceOfKeys.size());
//			for (int j = 0; j < b.referenceOfKeys.size(); j++) {
//				System.out.print(b.referenceOfKeys.get(j) + " ");
//			}
//			System.out.println();
//		}

//		Hashtable htblColNameValue = new Hashtable();
//		htblColNameValue.put("id", new Integer(50));
//		htblColNameValue.put("name", new String("c"));
//		htblColNameValue.put("age", new Integer("50"));
////////////		htblColNameValue.put("date", new Date(2000, 12, 23));
//////////		Polygon p = new Polygon();
//////////		p.addPoint(1,3);
//////////		p.addPoint(2,4);
////////////////////		System.out.println("n:"+p.npoints);
//////////		htblColNameValue.put("shape",  p);

//		dbApp.insertIntoTable(strTableName, htblColNameValue);
//		}
//		

//
//	
//*delete tuples*
		Hashtable<String, Object> htblColNameValue1 = new Hashtable();

		htblColNameValue1.put("id", 8);

//	htblColNameValue1.put("name", "Ab");

//	htblColNameValue.put("gpa", 2.0);
//	htblColNameValue.put("date", new Date(2000, 11, 23));
//	Polygon p = new Polygon();
//	p.addPoint(1, 1);
//	p.addPoint(2, 2);
//	htblColNameValue.put("shape", p);
//	BTree b = (BTree) getDeserlaized("data//" + "BTreeStudentid" + ".class");
//	System.out.println(b.toString());

//	dbApp.insertIntoTable(strTableName, htblColNameValue);

//		dbApp.deleteFromTable(strTableName, htblColNameValue1);

		displayTableContent(strTableName);
//	BTree b1 = (BTree) getDeserlaized("data//" + "BTreeStudentid" + ".class");
//	System.out.println(b1.toString());

//	Page pageToBeDeleteFrom = (Page) (getDeserlaized(
//			"data//Student0.class"));
//	System.out.println(pageToBeDeleteFrom.vtrTuples.toString());
//	Page pageToBeDeleteFrom1 = (Page) (getDeserlaized(
//			"data//Student1.class"));
//	System.out.println(pageToBeDeleteFrom1.vtrTuples.toString());

////*update table*
//		Hashtable hash = new Hashtable();
//	
//	Polygon p = new Polygon();
//	p.addPoint(1,3); 
//	p.addPoint(2,4);
//	hash.put("shape", p);

//	hash.put("age", new Integer(50));
//		hash.put("name", new String("a"));
//////	hash.put("gpa", new Double(0.6));
//////	hash.put("date", new Date(2000-05-23));

//		dbApp.updateTable(strTableName, "(1,0),(5,2)", hash);
////////

//* testing SELECT*
//		displayTableContent(strTableName);
//		Polygon p = new Polygon();
//		p.addPoint(2, 2);
//		p.addPoint(5, 5);
////		hash.put("shape", p);
//
//		SQLTerm[] arrSQLTerms;
//		arrSQLTerms = new SQLTerm[1];
//		for (int i = 0; i < arrSQLTerms.length; i++) {
//			arrSQLTerms[i] = new SQLTerm();
//		}
//		arrSQLTerms[0]._strTableName = "Student";
//		arrSQLTerms[0]._strColumnName = "shape";
//		arrSQLTerms[0]._strOperator = ">";
//		arrSQLTerms[0]._objValue = p;

//////
//	arrSQLTerms[1]._strTableName = "Student";
//	arrSQLTerms[1]._strColumnName = "name";
//	arrSQLTerms[1]._strOperator = "=";
//	arrSQLTerms[1]._objValue = "c";
//
//	arrSQLTerms[2]._strTableName = "Student";
//	arrSQLTerms[2]._strColumnName = "id";
//	arrSQLTerms[2]._strOperator = "=";
//	arrSQLTerms[2]._objValue = new Integer(30);
//
//////	 System.out.println(arrSQLTerms[0]._strTableName);
//////	
//////	

		String[] strarrOperators = new String[0];
//	strarrOperators[0] = "AND";
//////	strarrOperators[1] = "AND";
//////////////////	// select * from Student where name = “John Noor” or gpa = 1.5; 

//		Iterator resultSet = dbApp.selectFromTable(arrSQLTerms, strarrOperators);
//		while (resultSet.hasNext()) {
//			System.out.print(resultSet.next() + " ");
//			System.out.println();
//		}
//////  

//***testing B+ tree
//	Table a= (Table) getDeserlaized("data//Student.class");
//	displayTableContent(a.name);
//	try {
//		ObjectOutputStream bin = new ObjectOutputStream(new FileOutputStream("data//Student.class"));
//		bin.writeObject(a);
//		bin.flush();
//		bin.close();
//
//	} catch (Exception e) {
//		throw new DBAppException("error in displaying data in table");
//	}
//	dbApp.createBTreeIndex(strTableName, "age");

//	dbApp.checkTree();

//		displayTableContent(strTableName);

//	long modified = dbApp.modifyKey(new Integer(30));
//	BTree b = (BTree) getDeserlaized("data//" + "BTreeStudentage" + ".class");
//	ReferenceValues a= (ReferenceValues) b.search(50);
//	for(int i=0;i<a.getOverflowNodes().size();i++) {
//		OverflowNode c=a.getOverflowNodes().get(i);
//		for(int j=0;j<c.referenceOfKeys.size();j++) {
//			System.out.println(c.referenceOfKeys.get(j));
//		}
//	}
//	RangeResult r = b.rangeStopSearch(modified, false);
//	System.out.println(r.getQueryResult().size());
//	System.out.println(r.getQueryResult().get(0).getValue());
//	System.out.println(r.getQueryResult().get(1).getValue());
//	System.out.println(r.getQueryResult().get(2).getValue());
//	System.out.println(r.getQueryResult().get(3).getValue());
//	System.out.println(r.getQueryResult().get(4).getValue());
//    SearchResult s = b.searchStartKey(modified, false);
//	System.out.println(s.getValues().get(0));
//	serializeTree(b);
//	dbApp.makeIndexed(strTableName, "age");
//	displayTableContent("Student");

//Object [] a=dbApp.getArrayToDelete(htblColNameType, strTableName);
//	for (int i = 0; i < a.length; i++)
//		System.out.println(a[i]);

		// System.out.println(isIndexed("Student", "id"));

	}
}// throw DBAexception
