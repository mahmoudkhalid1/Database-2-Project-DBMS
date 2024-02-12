package ds.Rtree;

import java.io.Serializable;
import java.util.ArrayList;

import eminem.DBAppException;

public class RTreeReferenceValues implements Serializable {
	ArrayList<RTreeOverflowNode> overFlowNodes;

	public RTreeReferenceValues() {
		overFlowNodes = new ArrayList<RTreeOverflowNode>();
	}

	public int getSize() {
		int count = 0;
		for (int i = 0; i < overFlowNodes.size(); i++) {
			count = count + overFlowNodes.get(i).referenceOfKeys.size();
		}
		return count;
	}

	public void setReference(Object strReference) throws DBAppException {

		if (overFlowNodes.size() == 0) {
			RTreeOverflowNode ofn = new RTreeOverflowNode();
			ofn.referenceOfKeys.add(strReference);
			overFlowNodes.add(ofn);

		} else {

			
			for (int i = 0; i < overFlowNodes.size(); i++) {
				RTreeOverflowNode ofn = overFlowNodes.get(i);

//				System.out.println("order"+ ofn.nodeOrder+ "sizeOFN"+ofn.referenceOfKeys.size());

				if (ofn.nodeOrder > ofn.referenceOfKeys.size()) {
					ofn.referenceOfKeys.add(strReference);
					break;
				} else if (i + 1 == overFlowNodes.size()) {

					RTreeOverflowNode ofnew = new RTreeOverflowNode();
					ofnew.referenceOfKeys.add(strReference);
					overFlowNodes.add(ofnew);
					break;

				}
			}
		}

	}

	public ArrayList<RTreeOverflowNode> getRTreeOverflowNodes() {
		return overFlowNodes;
	}

	public void removeReference(Object strReference) {
		for (int i = 0; i < overFlowNodes.size(); i++) {
			RTreeOverflowNode ofn = overFlowNodes.get(i);
			ArrayList<Object> referenceOfKeys = ofn.referenceOfKeys;
			if (referenceOfKeys.contains(strReference)) {
				referenceOfKeys.remove(strReference);
				if (referenceOfKeys.size() == 0) {
					overFlowNodes.remove(i);
				}
				break;
			}
		}
	}

	public void replaceRef(Object oldRef, Object newRef) {
		
		for (int i = 0; i < overFlowNodes.size(); i++) {
			RTreeOverflowNode ofn = overFlowNodes.get(i);
			ArrayList<Object> referenceOfKeys = ofn.referenceOfKeys;
			
			if (referenceOfKeys.contains(oldRef)) {
				int indexOfOld = referenceOfKeys.indexOf(oldRef);
				
				referenceOfKeys.set(indexOfOld, newRef);
				break;
				
			}
		}
	}

}
