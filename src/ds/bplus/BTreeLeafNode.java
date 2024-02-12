package ds.bplus;

import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.Properties;

import eminem.DBAppException;

class BTreeLeafNode<TKey extends Comparable<TKey>, TValue> extends BTreeNode<TKey> implements Serializable {
	protected static int LEAFORDER;

	private ReferenceValues[] values;

	

	@Override
	public String toString() {
		String s = "[";
		for (int i = 0; i < super.keys.length && keys[i] != null; i++) {
			s = s + super.keys[i].toString() + ", ";
		}
		s = s + "]";

//		for(int i=0;i<values.length;i++) {
//			System.out.println(values[i].getReferences().size());
//		}
		return s;

	}

	public BTreeLeafNode() throws DBAppException {

		try {
			FileReader reader = new FileReader("config\\DBApp.properties");

			Properties p = new Properties();
			p.load(reader);

			LEAFORDER = Integer.parseInt(p.getProperty("NodeSize"));

		} catch (IOException e) {
			throw new DBAppException("error in finding config file");
		}
		this.keys = new Object[LEAFORDER + 1];
		this.values = new ReferenceValues[LEAFORDER + 1];

		for (int i = 0; i < this.values.length; i++) {
			this.values[i] = new ReferenceValues();
		}
	}

	@SuppressWarnings("unchecked")
	public TValue getValue(int index) {
		return (TValue) this.values[index];
	}

	public void setValue(int index, TValue value) throws DBAppException {
	
		if(values[index]==null) {
			values[index]= new ReferenceValues();
		}
		this.values[index].setReference(value);
		
	}

	@Override
	public TreeNodeType getNodeType() {
		return TreeNodeType.LeafNode;
	}

	@Override
	public int search(TKey key) {
		for (int i = 0; i < this.getKeyCount(); ++i) {
			int cmp = this.getKey(i).compareTo(key);
			if (cmp == 0) {
				return i;
			} else if (cmp > 0) {
				return -1;
			}
		}

		return -1;
	}

	public int searchMin(TKey key) {
		for (int i = 0; i < this.getKeyCount(); ++i) {
			int cmp = this.getKey(i).compareTo(key);
			if (cmp >= 0) {
				return i;
			}
//			} else if (cmp > 0) {
//				return -1;
//			}
		}

		return -1;
	}
	
	public int searchMax(TKey key) {
		for (int i = 0; i < this.getKeyCount(); ++i) {
			int cmp = this.getKey(i).compareTo(key);
			//System.out.println("key" + this.getKey(i));
			if (cmp <= 0) {
				return i;
			}
//			} else if (cmp > 0) {
//				return -1;
//			}
		}

		return -1;
	}

	/* The codes below are used to support insertion operation */

	public void insertKey(TKey key, TValue value) throws DBAppException {
		//System.out.println(value);
		Object[] superKeys = super.keys;
		boolean containsKey = false;
		int i;
		for (i = 0; i < superKeys.length; i++) {

			if (superKeys[i] != null && key.compareTo((TKey) superKeys[i]) == 0) {
				containsKey = true;
				break;
			}
		}
		if (containsKey) {
			
			values[i].setReference(value);

		} else {
			
			int index = 0;
			while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0) {
				
				++index;}
			this.insertAt(index, key, value);

		//	System.out.println("key: "+key +" value: "+value);
		}
//		else {
//		int index = 0;
//		while (index < this.getKeyCount() && this.getKey(index).compareTo(key) < 0)
//			++index;
//		this.insertAt(index, key, value);
//>>>>>>> 6d27b0eaecd0ad568ca94fda01a12dc6fe0ae450
//		}
	}

	private void insertAt(int index, TKey key, TValue value) throws DBAppException {
		// move space for the new key
		
		for (int i = this.getKeyCount() - 1; i >= index; --i) {
			this.setKey(i + 1, this.getKey(i));
			this.setValueShift(i + 1, this.getValue(i));
		}

		// insert new key and value
		if(value instanceof ReferenceValues) {
			this.setKey(index, key);
			this.setValueShift(index, value);
		}
		else {
		this.setKey(index, key);
		this.setValueShift(index, null);
		this.setValue(index, value);
		}

		++this.keyCount;
	}

	/**
	 * When splits a leaf node, the middle key is kept on new node and be pushed to
	 * parent node.
	 * 
	 * @throws DBAppException
	 */
	@Override
	protected BTreeNode<TKey> split() throws DBAppException {
		int midIndex = this.getKeyCount() / 2;

		BTreeLeafNode<TKey, TValue> newRNode = new BTreeLeafNode<TKey, TValue>();
		for (int i = midIndex; i < this.getKeyCount(); ++i) {
			newRNode.setKey(i - midIndex, this.getKey(i));
			newRNode.setValueShift(i - midIndex, this.getValue(i));
			this.setKey(i, null);
			this.setValueShift(i, null);
		}
		
		newRNode.keyCount = this.getKeyCount() - midIndex;
		this.keyCount = midIndex;

		return newRNode;
	}

	@Override
	protected BTreeNode<TKey> pushUpKey(TKey key, BTreeNode<TKey> leftChild, BTreeNode<TKey> rightNode) {
		throw new UnsupportedOperationException();
	}

	/* The codes below are used to support deletion operation */

	public boolean delete(TKey key, TValue value) {

		int index = this.search(key);
		if (index == -1)
			return false;

		// check how many references do I have
		int countRef = values[index].getSize();
		// System.out.println(countRef);

		if (countRef != 1) {
			values[index].removeReference(value);
			return true;
		} else {
			this.deleteAt(index);
			return true;
		}

	}

	private void setValueShift(int i, TValue tValue) {
		this.values[i] = (ReferenceValues) tValue;

	}

	private void deleteAt(int index) {
		int i = index;
		
		for (i = index; i < this.getKeyCount() - 1; ++i) {
			this.setKey(i, this.getKey(i + 1));

			this.setValueShift(i, this.getValue(i + 1));
		}
		this.setKey(i, null);
		this.setValueShift(i, null);
		--this.keyCount;
	}

	@Override
	protected void processChildrenTransfer(BTreeNode<TKey> borrower, BTreeNode<TKey> lender, int borrowIndex) {
		throw new UnsupportedOperationException();
	}

	@Override
	protected BTreeNode<TKey> processChildrenFusion(BTreeNode<TKey> leftChild, BTreeNode<TKey> rightChild) {
		throw new UnsupportedOperationException();
	}

	/**
	 * Notice that the key sunk from parent is be abandoned.
	 * 
	 * @throws DBAppException
	 */
	@Override
	@SuppressWarnings("unchecked")
	protected void fusionWithSibling(TKey sinkKey, BTreeNode<TKey> rightSibling) throws DBAppException {
		BTreeLeafNode<TKey, TValue> siblingLeaf = (BTreeLeafNode<TKey, TValue>) rightSibling;

		int j = this.getKeyCount();
		for (int i = 0; i < siblingLeaf.getKeyCount(); ++i) {
			this.setKey(j + i, siblingLeaf.getKey(i));
			this.setValueShift(j + i, siblingLeaf.getValue(i));
		}
		this.keyCount += siblingLeaf.getKeyCount();

		this.setRightSibling(siblingLeaf.rightSibling);
		if (siblingLeaf.rightSibling != null)
			siblingLeaf.rightSibling.setLeftSibling(this);
	}

	@Override
	@SuppressWarnings("unchecked")
	protected TKey transferFromSibling(TKey sinkKey, BTreeNode<TKey> sibling, int borrowIndex) throws DBAppException {
		BTreeLeafNode<TKey, TValue> siblingNode = (BTreeLeafNode<TKey, TValue>) sibling;

		this.insertKey(siblingNode.getKey(borrowIndex), siblingNode.getValue(borrowIndex));
		siblingNode.deleteAt(borrowIndex);

		return borrowIndex == 0 ? sibling.getKey(0) : this.getKey(0);
	}

	public void update(TKey key, TValue oldRef, TValue newRef) {
		int index = this.search(key);
		if (index == -1)
			return;
		else {
			values[index].replaceRef(oldRef, newRef);
		}

	}
}
