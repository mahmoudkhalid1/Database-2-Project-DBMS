package ds.bplus;

import java.io.Serializable;
import java.util.ArrayList;

import eminem.DBAppException;

public class ReferenceValues implements Serializable {
	ArrayList<OverflowNode> overFlowNodes;

	public ReferenceValues() {
		overFlowNodes = new ArrayList<OverflowNode>();
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
			OverflowNode ofn = new OverflowNode();
			ofn.referenceOfKeys.add(strReference);
			overFlowNodes.add(ofn);

		} else {

			
			for (int i = 0; i < overFlowNodes.size(); i++) {
				OverflowNode ofn = overFlowNodes.get(i);

//				System.out.println("order"+ ofn.nodeOrder+ "sizeOFN"+ofn.referenceOfKeys.size());

				if (ofn.nodeOrder > ofn.referenceOfKeys.size()) {
					ofn.referenceOfKeys.add(strReference);
					break;
				} else if (i + 1 == overFlowNodes.size()) {

					OverflowNode ofnew = new OverflowNode();
					ofnew.referenceOfKeys.add(strReference);
					overFlowNodes.add(ofnew);
					break;

				}
			}
		}

	}

	public ArrayList<OverflowNode> getOverflowNodes() {
		return overFlowNodes;
	}

	public void removeReference(Object strReference) {
		for (int i = 0; i < overFlowNodes.size(); i++) {
			OverflowNode ofn = overFlowNodes.get(i);
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
			OverflowNode ofn = overFlowNodes.get(i);
			ArrayList<Object> referenceOfKeys = ofn.referenceOfKeys;
			
			if (referenceOfKeys.contains(oldRef)) {
				int indexOfOld = referenceOfKeys.indexOf(oldRef);
				
				referenceOfKeys.set(indexOfOld, newRef);
				break;
				
			}
		}
	}

}
