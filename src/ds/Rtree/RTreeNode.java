package ds.Rtree;

import java.awt.Polygon;
import java.io.Serializable;

import eminem.DBAppException;

enum TreeNodeType {
	InnerNode, LeafNode
}

abstract class RTreeNode<TKey extends Comparable<TKey>> implements Serializable {
	protected Object[] keys;
	protected int keyCount;
	protected RTreeNode<TKey> parentNode;
	protected RTreeNode<TKey> leftSibling;
	protected RTreeNode<TKey> rightSibling;

	protected RTreeNode() {
		this.keyCount = 0;
		this.parentNode = null;
		this.leftSibling = null;
		this.rightSibling = null;

	}

	public int getKeyCount() {
		return this.keyCount;
	}

	@SuppressWarnings("unchecked")
	public TKey getKey(int index) {
		return (TKey) this.keys[index];
	}
	
	
	public void setKey(int index, TKey key) {
		this.keys[index] = key;
	}

	public RTreeNode<TKey> getParent() {
		return this.parentNode;
	}

	public void setParent(RTreeNode<TKey> parent) {
		this.parentNode = parent;
	}

	public abstract TreeNodeType getNodeType();

	/**
	 * Search a key on current node, if found the key then return its position,
	 * otherwise return -1 for a leaf node, return the child node index which should
	 * contain the key for a internal node.
	 */
	public abstract int search(TKey key);

//	public abstract int searchMax(TKey key);

	/* The codes below are used to support insertion operation */

	public boolean isOverflow() {
		return this.getKeyCount() == this.keys.length;
	}

	public RTreeNode<TKey> dealOverflow() throws DBAppException {
		int midIndex = this.getKeyCount() / 2;
		TKey upKey = this.getKey(midIndex);
		

		RTreeNode<TKey> newRNode = this.split();

		if (this.getParent() == null) {
			this.setParent(new RTreeInnerNode<TKey>());
		}
		newRNode.setParent(this.getParent());

		// maintain links of sibling nodes
		newRNode.setLeftSibling(this);
		newRNode.setRightSibling(this.rightSibling);
		if (this.getRightSibling() != null)
			this.getRightSibling().setLeftSibling(newRNode);
		this.setRightSibling(newRNode);

		// push up a key to parent internal node
		return this.getParent().pushUpKey(upKey, this, newRNode);
	}

	protected abstract RTreeNode<TKey> split() throws DBAppException;

	protected abstract RTreeNode<TKey> pushUpKey(TKey key, RTreeNode<TKey> leftChild, RTreeNode<TKey> rightNode)
			throws DBAppException;

	/* The codes below are used to support deletion operation */

	public boolean isUnderflow() {
		return this.getKeyCount() < (this.keys.length / 2);
	}

	public boolean canLendAKey() {
		return this.getKeyCount() > (this.keys.length / 2);
	}

	public RTreeNode<TKey> getLeftSibling() {
		if (this.leftSibling != null && this.leftSibling.getParent() == this.getParent())
			return this.leftSibling;
		return null;
	}

	public void setLeftSibling(RTreeNode<TKey> sibling) {
		this.leftSibling = sibling;
	}

	public RTreeNode<TKey> getRightSibling() {
		if (this.rightSibling != null && this.rightSibling.getParent() == this.getParent())
			return this.rightSibling;
		return null;
	}

	public void setRightSibling(RTreeNode<TKey> silbling) {
		this.rightSibling = silbling;
	}

	public RTreeNode<TKey> dealUnderflow() throws DBAppException {
		
		if (this.getParent() == null)
			return null;

		// try to borrow a key from sibling
		RTreeNode<TKey> leftSibling = this.getLeftSibling();
		if (leftSibling != null && leftSibling.canLendAKey()) {
			this.getParent().processChildrenTransfer(this, leftSibling, leftSibling.getKeyCount() - 1);
			return null;
		}

		RTreeNode<TKey> rightSibling = this.getRightSibling();
		if (rightSibling != null && rightSibling.canLendAKey()) {
			this.getParent().processChildrenTransfer(this, rightSibling, 0);
			return null;
		}
		

		// Can not borrow a key from any sibling, then do fusion with sibling
		if (leftSibling != null) {
			return this.getParent().processChildrenFusion(leftSibling, this);
		} else {
			return this.getParent().processChildrenFusion(this, rightSibling);
		}
	}

	protected abstract void processChildrenTransfer(RTreeNode<TKey> borrower, RTreeNode<TKey> lender, int borrowIndex)
			throws DBAppException;

	protected abstract RTreeNode<TKey> processChildrenFusion(RTreeNode<TKey> leftChild, RTreeNode<TKey> rightChild)
			throws DBAppException;

	protected abstract void fusionWithSibling(TKey sinkKey, RTreeNode<TKey> rightSibling) throws DBAppException;

	protected abstract TKey transferFromSibling(TKey sinkKey, RTreeNode<TKey> sibling, int borrowIndex)
			throws DBAppException;
}