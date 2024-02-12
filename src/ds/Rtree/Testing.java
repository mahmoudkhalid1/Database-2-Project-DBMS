package ds.Rtree;

import java.awt.Polygon;
import java.util.ArrayList;

import eminem.DBAppException;

public class Testing {
	public static void main(String[] args) throws DBAppException {
		RTree tree = new RTree();
		
		Polygon p = new Polygon();
		p.addPoint(1,1);
		p.addPoint(2,2);
		tree.insert(p, "shape1");
		Polygon p2 = new Polygon();
		p2.addPoint(1,1);
		p2.addPoint(2,2);
		tree.insert(p2, "shape2");
		Polygon p3 = new Polygon();
		p3.addPoint(1,1);
		p3.addPoint(2,2);
		tree.insert(p3, "shape3");
		Polygon p4 = new Polygon();
		p4.addPoint(1,1);
		p4.addPoint(2,2);
		tree.insert(p4, "shape4");
		Polygon p5 = new Polygon();
		p5.addPoint(2,2);
		p5.addPoint(3,3);
		tree.insert(p5, "shape5");

		
		ArrayList<String> a = 	tree.rangeMaxSearchKeys(p);
		System.out.println(tree.toString());
		 
//		 RTreeReferenceValues ref = (RTreeReferenceValues) tree.search(p6);
//			for (int i = 0; i < ref.getRTreeOverflowNodes().size(); i++) {
//			RTreeOverflowNode b = ref.getRTreeOverflowNodes().get(i);
//			for (int j = 0; j < b.referenceOfKeys.size(); j++) {
//				System.out.print(b.referenceOfKeys.get(j) + " ");
//			}
//			System.out.println();
//		}
	}}