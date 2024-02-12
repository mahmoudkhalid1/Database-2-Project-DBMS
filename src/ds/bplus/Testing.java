package ds.bplus;

import java.util.ArrayList;

import eminem.DBAppException;

public class Testing {
	public static void main(String[] args) throws DBAppException {
		BTree btree = new BTree();

		btree.insert(-1,"Student0");
		btree.insert(0,"Student0");
		btree.insert(1,"Student0");
		btree.insert(3,"Student0");
		btree.insert(3,"Student0");
		
		btree.insert(2,"Student1");
		btree.insert(4,"Student0");
		btree.insert(4,"Student1");
		btree.insert(5,"Student0");
		btree.insert(6,"Student0");
		btree.insert(7,"Student0");
		
		btree.insert(8,"Student1");
		
ArrayList<String> a = 	btree.rangeMinSearchKeys(3);
//for(int i = 0 ; i<a.size();i++)
//{
//	System.out.print(a.get(i));
//}

	System.out.println(btree.toString());



		// System.out.println(btree.toString());

//		 ReferenceValues ref = (ReferenceValues) btree.search(2);
//		for (int i = 0; i < ref.getOverflowNodes().size(); i++) {
//			OverflowNode b = ref.getOverflowNodes().get(i);
//			//System.out.println("size =" + b.referenceOfKeys.size());
//			for (int j = 0; j < b.referenceOfKeys.size(); j++) {
//				System.out.print(b.referenceOfKeys.get(j) + " ");
//			}
//			System.out.println();
//		}

//		ArrayList<String> ref = new ArrayList<String>();
//		ref = btree.rangeMaxSearch(1);
//		for (int i = 0; i < ref.size(); i++) {
//			System.out.println(ref.get(i));
//		}

//		for (int i = 0; i < ref.getOverflowNodes().size(); i++) {
//			OverflowNode b = ref.getOverflowNodes().get(i);
//			System.out.println("size =" + b.referenceOfKeys.size());
//			for (int j = 0; j < b.referenceOfKeys.size(); j++) {
//				System.out.print(b.referenceOfKeys.get(j) + " ");
//			}
//			System.out.println();
//		}
	}

//		ArrayList<String> ref = new ArrayList<String>();
//		ref = btree.rangeMinSearch(4);
//		for (int i = 0; i < ref.size(); i++) {
//			System.out.println(ref.get(i));
//		}

//		ArrayList a = new ArrayList<>();
//		a.add(10);
//		a.add(20);
//		System.out.println(a.size());

}