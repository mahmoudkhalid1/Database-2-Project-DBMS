package ds.Rtree;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import eminem.DBAppException;

class RTreeInnerNode<TKey extends Comparable<TKey>> extends RTreeNode<TKey> implements Serializable {
	protected static int INNERORDER;
	protected Object[] children; 
	
	public RTreeInnerNode() throws DBAppException {
		int num;

		try {
			FileReader reader = new FileReader("config\\DBApp.properties");

			Properties p = new Properties();
			p.load(reader);

			INNERORDER = Integer.parseInt(p.getProperty("NodeSize"));
			// System.out.println(p.getProperty("password")); }
		} catch (IOException e) {
			throw new DBAppException("error in finding config file");
		}
		this.keys = new Object[INNERORDER + 1];
		this.children = new Object[INNERORDER + 2];
	}
	public String toString() {
		String s = "[";
		for ( int i=0 ; i<super.keys.length && keys[i]!=null;i++)
		{
			s = s + super.keys[i].toString() + "| ";
		}
		s = s + "] ";
		int i=0;
			while(children[i]!=null) {
				s=s+children[i].toString();
				i++;
			}
			
		return s;
		
	}
	
	@SuppressWarnings("unchecked")
	public RTreeNode<TKey> getChild(int index) {
		return (RTreeNode<TKey>)this.children[index];
	}

	public void setChild(int index, RTreeNode<TKey> child) {
		this.children[index] = child;
		if (child != null)
			child.setParent(this);
	}
	
	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.InnerNode;
	}
	
	@Override
	public int search(TKey key) {
		int index = 0;
		for (index = 0; index < this.getKeyCount(); ++index) {
			int cmp = this.getKey(index).compareTo(key);
			if (cmp == 0) {
				return index + 1;
			}
			else if (cmp > 0) {
				return index;
			}
		}
		
		return index;
	}
	
//	public int searchMax(TKey key) {
//		int index = 0;
//		for (index = 0; index < this.getKeyCount(); ++index) {
//			int cmp = this.getKey(index).compareTo(key);
//			if (cmp <= 0) {
//				return index + 1;
//			}
//			else if (cmp > 0) {
//				return index;
//			}
//		}
//		
//		return index;
//	}
	
	/* The codes below are used to support insertion operation */
	
	private void insertAt(int index, TKey key, RTreeNode<TKey> leftChild, RTreeNode<TKey> rightChild) {
		// move space for the new key
		for (int i = this.getKeyCount() + 1; i > index; --i) {
			this.setChild(i, this.getChild(i - 1));
		}
		for (int i = this.getKeyCount(); i > index; --i) {
			this.setKey(i, this.getKey(i - 1));
		}
		
		// insert the new key
		this.setKey(index, key);
		this.setChild(index, leftChild);
		this.setChild(index + 1, rightChild);
		this.keyCount += 1;
	}
	
	/**
	 * When splits a internal node, the middle key is kicked out and be pushed to parent node.
	 * @throws DBAppException 
	 */
	@Override
	protected RTreeNode<TKey> split() throws DBAppException {
		int midIndex = this.getKeyCount() / 2;
		
		RTreeInnerNode<TKey> newRNode = new RTreeInnerNode<TKey>();
		for (int i = midIndex + 1; i < this.getKeyCount(); ++i) {
			newRNode.setKey(i - midIndex - 1, this.getKey(i));
			this.setKey(i, null);
		}
		for (int i = midIndex + 1; i <= this.getKeyCount(); ++i) {
			newRNode.setChild(i - midIndex - 1, this.getChild(i));
			newRNode.getChild(i - midIndex - 1).setParent(newRNode);
			this.setChild(i, null);
		}
		this.setKey(midIndex, null);
		newRNode.keyCount = this.getKeyCount() - midIndex - 1;
		this.keyCount = midIndex;
		
		return newRNode;
	}
	
	@Override
	protected RTreeNode<TKey> pushUpKey(TKey key, RTreeNode<TKey> leftChild, RTreeNode<TKey> rightNode) throws DBAppException {
		// find the target position of the new key
		int index = this.search(key);
		
		// insert the new key
		this.insertAt(index, key, leftChild, rightNode);

		// check whether current node need to be split
		if (this.isOverflow()) {
			return this.dealOverflow();
		}
		else {
			return this.getParent() == null ? this : null;
		}
	}
	
	
	
	
	/* The codes below are used to support delete operation */
	
	private void deleteAt(int index) {
		int i = 0;
		for (i = index; i < this.getKeyCount() - 1; ++i) {
			this.setKey(i, this.getKey(i + 1));
			this.setChild(i + 1, this.getChild(i + 2));
		}
		this.setKey(i, null);
		this.setChild(i + 1, null);
		--this.keyCount;
	}
	
	
	@Override
	protected void processChildrenTransfer(RTreeNode<TKey> borrower, RTreeNode<TKey> lender, int borrowIndex) throws DBAppException {
		int borrowerChildIndex = 0;
		while (borrowerChildIndex < this.getKeyCount() + 1 && this.getChild(borrowerChildIndex) != borrower)
			++borrowerChildIndex;
		
		if (borrowIndex == 0) {
			// borrow a key from right sibling
			TKey upKey = borrower.transferFromSibling(this.getKey(borrowerChildIndex), lender, borrowIndex);
			this.setKey(borrowerChildIndex, upKey);
		}
		else {
			// borrow a key from left sibling
			TKey upKey = borrower.transferFromSibling(this.getKey(borrowerChildIndex - 1), lender, borrowIndex);
			this.setKey(borrowerChildIndex - 1, upKey);
		}
	}
	
	@Override
	protected RTreeNode<TKey> processChildrenFusion(RTreeNode<TKey> leftChild, RTreeNode<TKey> rightChild) throws DBAppException {
		int index = 0;
		while (index < this.getKeyCount() && this.getChild(index) != leftChild)
			++index;
		TKey sinkKey = this.getKey(index);
		
		// merge two children and the sink key into the left child node
		leftChild.fusionWithSibling(sinkKey, rightChild);
		
		// remove the sink key, keep the left child and abandon the right child
		this.deleteAt(index);
		
		// check whether need to propagate borrow or fusion to parent
		if (this.isUnderflow()) {
			if (this.getParent() == null) {
				// current node is root, only remove keys or delete the whole root node
				if (this.getKeyCount() == 0) {
					leftChild.setParent(null);
					return leftChild;
				}
				else {
					return null;
				}
			}
			
			return this.dealUnderflow();
		}
		
		return null;
	}
	
	
	@Override
	protected void fusionWithSibling(TKey sinkKey, RTreeNode<TKey> rightSibling) {
		RTreeInnerNode<TKey> rightSiblingNode = (RTreeInnerNode<TKey>)rightSibling;
		
		int j = this.getKeyCount();
		this.setKey(j++, sinkKey);
		
		for (int i = 0; i < rightSiblingNode.getKeyCount(); ++i) {
			this.setKey(j + i, rightSiblingNode.getKey(i));
		}
		for (int i = 0; i < rightSiblingNode.getKeyCount() + 1; ++i) {
			this.setChild(j + i, rightSiblingNode.getChild(i));
		}
		this.keyCount += 1 + rightSiblingNode.getKeyCount();
		
		this.setRightSibling(rightSiblingNode.rightSibling);
		if (rightSiblingNode.rightSibling != null)
			rightSiblingNode.rightSibling.setLeftSibling(this);
	}
	
	@Override
	protected TKey transferFromSibling(TKey sinkKey, RTreeNode<TKey> sibling, int borrowIndex) {
		RTreeInnerNode<TKey> siblingNode = (RTreeInnerNode<TKey>)sibling;
		
		TKey upKey = null;
		if (borrowIndex == 0) {
			// borrow the first key from right sibling, append it to tail
			int index = this.getKeyCount();
			this.setKey(index, sinkKey);
			this.setChild(index + 1, siblingNode.getChild(borrowIndex));			
			this.keyCount += 1;
			
			upKey = siblingNode.getKey(0);
			siblingNode.deleteAt(borrowIndex);
		}
		else {
			// borrow the last key from left sibling, insert it to head
			this.insertAt(0, sinkKey, siblingNode.getChild(borrowIndex + 1), this.getChild(0));
			upKey = siblingNode.getKey(borrowIndex);
			siblingNode.deleteAt(borrowIndex);
		}
		
		return upKey;
	}
}