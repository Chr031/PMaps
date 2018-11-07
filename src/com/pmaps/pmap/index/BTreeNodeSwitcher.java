package com.pmaps.pmap.index;



public enum BTreeNodeSwitcher {
	BEFORE {
		@Override
		BTreeNodeSwitcher next() {
			return HASH1;
		}
	},
	HASH1 {
		@Override
		BTreeNodeSwitcher next() {
			return CENTER;
		}
	},
	CENTER {
		@Override
		BTreeNodeSwitcher next() {
			return HASH2;
		}
	},
	HASH2 {
		@Override
		BTreeNodeSwitcher next() {
			return AFTER;
		}
	},
	AFTER {
		@Override
		BTreeNodeSwitcher next() {
			return BEFORE;
		}
	};
	abstract BTreeNodeSwitcher next();

}