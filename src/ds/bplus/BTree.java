package ds.bplus;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Vector;

import eminem.DBAppException;

/**
 * A B+ tree Since the structures and behaviors between internal node and
 * external node are different, so there are two different classes for each kind
 * of node.
 * 
 * @param <TKey> the data type of the key
 * @param <TValue> the data type of the value
 */
public class BTree<TKey extends Comparable<TKey>, TValue> implements Serializable {
	private BTreeNode<TKey> root;
	public String treeName;

	private ArrayList<BTreeLeafNode<TKey, TValue>> findLeafNodeStartKey(TKey key) {
		BTreeNode<TKey> node = this.root;
		ArrayList<BTreeLeafNode<TKey, TValue>> result = new ArrayList<BTreeLeafNode<TKey, TValue>>();
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			// System.out.println("check");
			node = ((BTreeInnerNode<TKey>) node).getChild(node.search(key));
		}
		// System.out.println("check");
		result.add((BTreeLeafNode<TKey, TValue>) node);
		while ((BTreeLeafNode<TKey, TValue>) node.rightSibling != null) {
			result.add((BTreeLeafNode<TKey, TValue>) node.rightSibling);
			node = (BTreeLeafNode<TKey, TValue>) node.rightSibling;
		}

		return result;
		// return (BTreeLeafNode<TKey, TValue>) node;
	}

	public ArrayList<String> rangeMinSearch(TKey key) { // returns ReferenceValues that contains a list of overflow
		// nodes
// BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		ArrayList<BTreeLeafNode<TKey, TValue>> leaves = new ArrayList<BTreeLeafNode<TKey, TValue>>();
		leaves = this.findLeafNodeStartKey(key);
// System.out.println(leaf.keys[1]);
		ArrayList<String> result = new ArrayList<String>();
//System.out.println(leaves.size());
		for (int w = 0; w < leaves.size(); w++) {
			BTreeLeafNode<TKey, TValue> leaf = leaves.get(w);
			for (int i = 0; i < leaf.keys.length; i++) {
				if (leaf.getKey(i) != null) {
					if (leaf.getKey(i).compareTo(key) >= 0) {
//System.out.println(leaf.keys[i]);
						int index = leaf.searchMin(leaf.getKey(i));
// System.out.println(index + "s");
						if (index != -1) {
							ReferenceValues ref = (ReferenceValues) leaf.getValue(index);
//System.out.println(ref.getReferences().size());
							for (int z = 0; z < ref.getOverflowNodes().size(); z++) {
								OverflowNode f = ref.getOverflowNodes().get(z);
								for (int j = 0; j < f.referenceOfKeys.size(); j++) {
//System.out.println(f.referenceOfKeys.get(j) + "");
									result.add(f.referenceOfKeys.get(j) + "");
								}
							}
						}
					}
				}
			}
		}
		return result;
//int index = leaf.searchMin(key);
//return (index == -1) ? null : leaf.getValue(index);
	}

	public ArrayList<String> rangeMaxSearch(TKey key) { // returns ReferenceValues that contains a list of overflow
		// nodes
		// BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		ArrayList<BTreeLeafNode<TKey, TValue>> leaves = new ArrayList<BTreeLeafNode<TKey, TValue>>();
		leaves = this.findLeafNodeStopKey(key);
		// System.out.println(leaf.keys[1]);
		ArrayList<String> result = new ArrayList<String>();
		// System.out.println(leaves.size());
		for (int w = 0; w < leaves.size(); w++) {
			BTreeLeafNode<TKey, TValue> leaf = leaves.get(w);
			// System.out.println(leaf.keys[1]);
			for (int i = 0; i < leaf.keys.length; i++) {
				if (leaf.getKey(i) != null) {
					if (leaf.getKey(i).compareTo(key) <= 0) {
						// System.out.println(leaf.keys[i]);
						int index = leaf.searchMin(leaf.getKey(i));
						// System.out.println(index + "s" + leaf.keys[i]);
						if (index != -1) {
							ReferenceValues ref = (ReferenceValues) leaf.getValue(index);
							// System.out.println(ref.getReferences().size());
							for (int z = 0; z < ref.getOverflowNodes().size(); z++) {
								OverflowNode f = ref.getOverflowNodes().get(z);
								for (int j = 0; j < f.referenceOfKeys.size(); j++) {
									// System.out.println(f.referenceOfKeys.get(j) + "");
									result.add(f.referenceOfKeys.get(j) + "");
								}
							}
						}
					}
				}
			}
		}
		return result;
//int index = leaf.searchMin(key);
//return (index == -1) ? null : leaf.getValue(index);
	}

	private ArrayList<BTreeLeafNode<TKey, TValue>> findLeafNodeStopKey(TKey key) {
		BTreeNode<TKey> node = this.root;
		ArrayList<BTreeLeafNode<TKey, TValue>> result = new ArrayList<BTreeLeafNode<TKey, TValue>>();
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			// System.out.println("check");
			node = ((BTreeInnerNode<TKey>) node).getChild(node.search(key));
		}
		// System.out.println("check");
		result.add((BTreeLeafNode<TKey, TValue>) node);
		while ((BTreeLeafNode<TKey, TValue>) node.leftSibling != null) {
			result.add((BTreeLeafNode<TKey, TValue>) node.leftSibling);
			node = (BTreeLeafNode<TKey, TValue>) node.leftSibling;
		}

		return result;
		// return (BTreeLeafNode<TKey, TValue>) node;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return root.toString();
	}

	public BTree() throws DBAppException {
		this.root = new BTreeLeafNode<TKey, TValue>();
	}

	/**
	 * Insert a new key and its associated value into the B+ tree.
	 * 
	 * @throws DBAppException
	 */
	public void insert(TKey key, TValue value) throws DBAppException {
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);

		leaf.insertKey(key, value);

		if (leaf.isOverflow()) {

			BTreeNode<TKey> n = leaf.dealOverflow();
			if (n != null)
				this.root = n;
		}
	}

	/**
	 * Search a key value on the tree and return its associated value.
	 */

	public TValue search(TKey key) { // returns ReferenceValues that contains a list of overflow nodes
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		// System.out.println(leaf.keyCount);

		int index = leaf.search(key);
		return (index == -1) ? null : leaf.getValue(index);
	}

	/**
	 * Delete a key and its associated value from the tree.
	 * 
	 * @throws DBAppException
	 */
	public void delete(TKey key, TValue value) throws DBAppException { // key=fady tvalue =page 1 deletes only one
																		// instance of key fady and page 1
		BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		if (leaf.delete(key, value) && leaf.isUnderflow()) {
			BTreeNode<TKey> n = leaf.dealUnderflow();
			if (n != null)
				this.root = n;
		}

	}

	public void update(TKey key, TValue oldRef, TValue newRef) { // fady page 1 wadeto page 2 (fady,page1,page2) only
																	// one instance
		BTreeLeafNode node = findLeafNodeShouldContainKey(key);
		node.update(key, oldRef, newRef);
	}

	/**
	 * Search the leaf node which should contain the specified key
	 */
	@SuppressWarnings("unchecked")
	public BTreeLeafNode<TKey, TValue> findLeafNodeShouldContainKey(TKey key) {
		BTreeNode<TKey> node = this.root;
		while (node.getNodeType() == TreeNodeType.InnerNode) {
			node = ((BTreeInnerNode<TKey>) node).getChild(node.search(key));
		}

		return (BTreeLeafNode<TKey, TValue>) node;
	}

	public ArrayList<String> rangeMaxSearchKeys(TKey key) { // returns ReferenceValues that contains a list of overflow
		// nodes
		// BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		ArrayList<String> ref = new ArrayList<String>();
		Vector<TKey> result = new Vector<TKey>();
		ArrayList<BTreeLeafNode<TKey, TValue>> leaves = new ArrayList<BTreeLeafNode<TKey, TValue>>();
		leaves = this.findLeafNodeStopKey(key);
		// System.out.println(leaf.keys[1]);
		// System.out.println(leaves.size());
		for (int w = 0; w < leaves.size(); w++) {
			BTreeLeafNode<TKey, TValue> leaf = leaves.get(w);
			// System.out.println(leaf.keys[1]);
			for (int i = 0; i < leaf.keys.length; i++) {
				if (leaf.getKey(i) != null) {
					if (leaf.getKey(i).compareTo(key) <= 0) {
						// System.out.println(leaf.keys[i]);
						int index = leaf.searchMin(leaf.getKey(i));
						// System.out.println(index + "s" + leaf.keys[i]);
						Object keyObj = (Object) leaf.getKey(i);
						if (index != -1) {
							result.add(leaf.getKey(i));

						}
					}
				}
			}
		}
		if (result.size() != 0) {

			result.sort(null);
			ReferenceValues ref1 = (ReferenceValues) this.search(result.lastElement());
			for (int i = 0; i < ref1.getOverflowNodes().size(); i++) {
				OverflowNode b = ref1.getOverflowNodes().get(i);
				// System.out.println("size =" + b.referenceOfKeys.size());
				for (int j = 0; j < b.referenceOfKeys.size(); j++) {
					ref.add(b.referenceOfKeys.get(j) + " ");
				}
			}
		}
		ref.sort(null);
		if (ref.size() != 0) {
			String temp = ref.get(0);
			ref.clear();
			ref.add(temp);
		}
		return ref;
	}

	public ArrayList<String> rangeMinSearchKeys(TKey key) { // returns ReferenceValues that contains a list of overflow
		// nodes
		// BTreeLeafNode<TKey, TValue> leaf = this.findLeafNodeShouldContainKey(key);
		ArrayList<String> ref = new ArrayList<String>();
		Vector<TKey> result = new Vector<TKey>();
		ArrayList<BTreeLeafNode<TKey, TValue>> leaves = new ArrayList<BTreeLeafNode<TKey, TValue>>();
		leaves = this.findLeafNodeStartKey(key);
		// System.out.println(leaf.keys[1]);
		// System.out.println(leaves.size());
		for (int w = 0; w < leaves.size(); w++) {
			BTreeLeafNode<TKey, TValue> leaf = leaves.get(w);
			for (int i = 0; i < leaf.keys.length; i++) {
				if (leaf.getKey(i) != null) {
					if (leaf.getKey(i).compareTo(key) >= 0) {
						// System.out.println(leaf.keys[i]);
						int index = leaf.searchMin(leaf.getKey(i));
						// System.out.println(index + "s");
						if (index != -1) {
							result.add(leaf.getKey(i));
						}
					}
				}
			}
		}
		if (result.size() != 0) {
			String a = "";
			result.sort(null);
			for (int i = 0; i < result.size(); i++) {
				a += result.get(i).toString() + ",";
			}
			System.out.println(a);
			ReferenceValues ref1 = (ReferenceValues) this.search(result.get(0));
			for (int i = 0; i < ref1.getOverflowNodes().size(); i++) {
				OverflowNode b = ref1.getOverflowNodes().get(i);
				// System.out.println("size =" + b.referenceOfKeys.size());
				for (int j = 0; j < b.referenceOfKeys.size(); j++) {
					ref.add(b.referenceOfKeys.get(j) + " ");
				}
			}
		}
		ref.sort(null);
		if (ref.size() != 0) {
			String temp = ref.get(0);
			ref.clear();
			ref.add(temp);
		}
		return ref;
		// int index = leaf.searchMin(key);
		// return (index == -1) ? null : leaf.getValue(index);
	}

	public void serializeTree() throws DBAppException, IOException {

		try {
			String n = this.treeName;
			ObjectOutputStream bin = new ObjectOutputStream(new FileOutputStream("data//" + n + ".class"));

			bin.writeObject(this);
			bin.flush();
			bin.close();
		} catch (Exception e) {
			throw new DBAppException("error in serialization");
		}
	}

}
