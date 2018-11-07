package com.pmaps.pmap.index;


class BTreeNodePosition<K, V> {

	BTreeNodeSwitcher switcher;
	BTreeNode<K, V> bTreeNode;

	public BTreeNodePosition(BTreeNodeSwitcher switcher, BTreeNode<K, V> bTreeNode) {
		super();
		this.switcher = switcher;
		this.bTreeNode = bTreeNode;
	}

	public BTreeNodePosition(BTreeNode<K, V> bTreeNode) {
		this(BTreeNodeSwitcher.BEFORE, bTreeNode);
	}
}
