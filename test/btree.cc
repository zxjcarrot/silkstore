
#include"btree.h"

template<class T>
Btree<T>::Btree(){
	root = make_shared<TreeNode>();
}

template<class T>
bool Btree<T>::del( T const& value ){
	if (root == nullptr){
		cout<<"The tree is empty !\n";
		return false;
	}
	//如果root->keyNum，需要做一些特判和处理。安照《算法导论》中的描述：BTREE的高度降低只会反生在root
	if (root->keyNum == 1){
		if (root->isLeaf){
			if (root->key[0] == value){
				root = nullptr;
				return true;
			}else{
				return false;
			}
		}
		auto leftChild = root->child[0];
		auto rightChild = root->child[1];
		if (rightChild == nullptr){
			cout << "rightChild == nullptr \n";
		}
		// 如果root左右孩子上的keyNum都小于MinChild-1，则将左右孩子合并树的高度减一
		if( leftChild->keyNum <= MinChild-1 && rightChild->keyNum <= MinChild-1){
			mergeChild(root,0,leftChild,rightChild);
			root = leftChild;
			return remove(root,value);
		// 如果存在一个节点的keyNum > MinChild-1,则可以直接执行remove(root,value)。
		// 原因在于,在删除过程中即使节点keyNum <= MinChild-1也可以从兄弟节点借一个值。
		// 而不会发生从root节点借一个值，合并子树使得root节点的keyNum==0不满足条件的情况
		}else{
			return remove(root,value);
		} 
	}else{
		return remove(root,value);
	}

}

template<class T>
bool Btree<T>::removeAtNode(shared_ptr<TreeNode> node, int position){
	shared_ptr<TreeNode>  leftChild = node->child[position];
	shared_ptr<TreeNode>  rightChild = node->child[position+1];
	if (leftChild->keyNum >= MinChild){
		T newValue = precursor(leftChild);
		node->key[position] = newValue;
		return true;
	} else if (rightChild->keyNum >= MinChild){
		T newValue = successor(rightChild);
		node->key[position] = newValue;
		return true;
	} else{
		mergeChild(node,position,leftChild,rightChild);
		return true;
	}
	return false;
}


template<class T>
bool Btree<T>::removeMergeOrBorrow(shared_ptr<TreeNode> node, int position, int value){
	shared_ptr<TreeNode> nextNode = node->child[position];
	//如果待删除的key在最左边的子树上，则只存在向右子树借key和合并子树的可能
	if(position == 0){
		shared_ptr<TreeNode> righNode = node->child[position+1];
		if (righNode->keyNum > MinChild - 1){
			borrowFromRight(node,position,nextNode,righNode);
			return remove(nextNode,value);
		}else{
			mergeChild(node,position,nextNode,righNode);
			return remove(nextNode,value);
		}
	//如果待删除的key在最右边边的子树上，则只存在向左子树借key和合并子树的可能
	}else if(position == node->keyNum) {
		shared_ptr<TreeNode> leftNode = node->child[position-1];
		if(leftNode->keyNum > MinChild - 1){
			borrowFromLeft(node,position-1,nextNode,leftNode);
			return remove(nextNode,value);
		}else{
			mergeChild(node,position-1,leftNode,nextNode);
			return remove(leftNode,value);
		}
	// 否则同时存在向左右子树和合并子树的可能
	} else{
		shared_ptr<TreeNode>  righNode = node->child[position+1];
		shared_ptr<TreeNode>  leftNode = node->child[position-1];
		if (righNode->keyNum > MinChild - 1){
			borrowFromRight(node,position,nextNode,righNode);
			return remove(nextNode,value);
		}else if(leftNode->keyNum > MinChild - 1){
			borrowFromLeft(node,position-1,nextNode,leftNode);
			return remove(nextNode,value);
		}else{
			mergeChild(node,position,nextNode,righNode);
			return remove(nextNode,value);
		}
	}
}

template<class T>
bool Btree<T>::remove(shared_ptr<TreeNode> node, T const& value){

	//找到正确的position的值
	int position = 0;
	while(position < node->keyNum && value > node->key[position]){
		position++;
	}
	// 如果待删除的key在叶子节点上找到，因为在上一步的递归删除过程中保证了
	// next->node的keynum > minChild-1所以可以直接直接删除，无需回溯检查
	if (node->isLeaf){
		if (value == node->key[position]){
			for (int i = position; i < node->keyNum-1; ++i){
				node->key[i] = node->key[i+1];
			}
			node->keyNum--;
			return true;
		}
		return false;
	}else{
		// 如果待删除的key在中间节点上找到，且左右孩子存在一个keyNum大于MinChild - 1的节点，
		// 则找该key的前驱或者后继（newvalue),并用newvalue的值覆盖当前的key以后， 继续向下递归删除newvalue。
		//（不直接删除newvalue的原因是，无法保证删除以后的树仍合法，如leaf节点的keyNum<MinChild - 1）
		// 如果key值附近的左右孩子都为MinChild - 1,则将key和与其相邻的左右child节点合并，再继续向下删除。
		// 执行这一步的目的是保证向下删除过成中delete递归所经过的路径上，所有节点的keynum数量>MinChild - 1.
		// 这样在后面的节点合并时不会出现父节点的keynum < MinChild - 1导致回溯的情况。(MergeChild候需要向父节点的一个key)
		if (value == node->key[position]){
			return removeAtNode(node,position);
		// 如果key在内部节点中没有找到，则需要继续向下递归删除
		}else{
			shared_ptr<TreeNode> nextNode = node->child[position];
			// 如果nextNode的keynum大于MinChild - 1，可以直接继续删除操作
			if(nextNode->keyNum > MinChild - 1){
				return remove(nextNode,value);
			// 否则需要借一个节点或者合并两个子节点来保证nextNode的keyNum > MinChild-1	
			}else{
				return removeMergeOrBorrow(node,position,value);
			}
		}
	}
	return false;
}

template<class T>
bool Btree<T>::borrowFromLeft(shared_ptr<TreeNode>  father, int position,
	shared_ptr<TreeNode> node, shared_ptr<TreeNode> leftChild){
	node->child[node->keyNum + 1] =  node->child[node->keyNum]; 
	for (int i = node->keyNum; i > 0; --i){
		node->key[i] = node->key[i-1];
		node->child[i] = node->child[i-1];
	}
	node->key[0] = father->key[position];
	node->child[0] = leftChild->child[leftChild->keyNum];
	node->keyNum++;
	father->key[position] = leftChild->key[leftChild->keyNum-1];
	leftChild->child[leftChild->keyNum] = nullptr;
	leftChild->keyNum--;
    return true;
}

template<class T>
bool Btree<T>::borrowFromRight(shared_ptr<TreeNode>  father, int position,
	shared_ptr<TreeNode> node, shared_ptr<TreeNode> rightChild){
	node->key[node->keyNum] = father->key[position];
	node->child[node->keyNum+1] = rightChild->child[0];
	node->keyNum++;
	father->key[position] = rightChild->key[0];
	for (int i = 0; i < rightChild->keyNum - 1; ++i){
		rightChild->key[i] = rightChild->key[i+1];
		rightChild->child[i] = rightChild->child[i+1];
	}
	rightChild->child[rightChild->keyNum-1] = rightChild->child[rightChild->keyNum];
	rightChild->child[rightChild->keyNum] = nullptr;
	rightChild->keyNum--;
    return true;
}

template<class T>
bool Btree<T>::mergeChild(shared_ptr<TreeNode>  father, int position,
	shared_ptr<TreeNode> leftChild, shared_ptr<TreeNode> rightChild){
	leftChild->key[MinChild-1]= father->key[position];
	for (int i = 0; i < MinChild - 1; ++i){
		leftChild->key[MinChild+i] = rightChild->key[i];
	}
	if (!leftChild->isLeaf){
		for (int i = 0; i < MinChild; ++i){
			leftChild->child[MinChild+i] = rightChild->child[i];
			rightChild->child[i] = nullptr;
		}
	}
	leftChild->keyNum = MinChild * 2 - 1; 
	for (int i = position; i < father->keyNum - 1; ++i){
		father->key[i] = father->key[i+1];
		father->child[i+1] = father->child[i+2];
	}
	father->child[father->keyNum] = nullptr;
	father->keyNum--;
    return true;
}

template<class T>
T Btree<T>::precursor(shared_ptr<TreeNode> node){
	if (node->isLeaf)
		return node->key[node->keyNum - 1];
	else
		return precursor(node->child[node->keyNum]);
}

template<class T>
T Btree<T>::successor(shared_ptr<TreeNode> node){
	if (node->isLeaf)
		return node->key[0];
	else
		return successor(node->child[0]);
}

template<class T>
bool Btree<T>::find( T const& key){

	int position = 0;
	while(position < root->keyNum && key > root->key[position]){
		position++;
	}
	if (key == root->key[position]){
		//cout<<"find value  "<< key << "\n";
		return true;
	}
	if (root->child[position] == nullptr){
		//cout<<"can't Find value \n";
		return false;
	}
	return find(root->child[position],key);
}


template<class T>
bool Btree<T>::find(shared_ptr<TreeNode> node , T const& key){
	int position = 0;
	while(position < node->keyNum && key > node->key[position]){
		position++;
	}
	if (key == node->key[position]){
		//cout<<"find key "<< key << "\n";
		return true;
	}

	if (node->child[position] == nullptr){
		//cout<<"can't Find value \n";
		return false;
	}
	return find(node->child[position],key);
}

template<class T>
bool Btree<T>::splitChildNode(shared_ptr<TreeNode> parent, int position){

    cout << "splitChildNode \n";


	shared_ptr<TreeNode> newChild = make_shared<TreeNode>()/*(MinChild)*/;
	shared_ptr<TreeNode> child = parent->child[position];
	newChild->isLeaf = child->isLeaf; 
	// Copy the bigest value in oldchild to the new child
	// 从原有的孩子拷贝最大到新的节点，不拷贝最小的值是因为，当前实现的Btree中的数据都是从小到大左对齐
	for (int i = 0; i < MinChild - 1; ++i){
		newChild->key[i] = child->key[MinChild + i];
	}
	if (!child->isLeaf){
		for (int i = 0; i < MinChild ; ++i){
			newChild->child[i] = child->child[MinChild + i];
		}
	}
	newChild->keyNum = MinChild - 1;
	child->keyNum = MinChild - 1;
	// 把位于MinChild - 1的值复制到父节点
	for (int i = parent->keyNum; i > position ; --i){
		parent->key[i] = parent->key[i-1];
		parent->child[i+1] = parent->child[i];
	}
	parent->key[position] = child->key[MinChild-1];
	parent->child[position+1] = newChild;
	parent->keyNum++;
    return true;
}

template<class T>
bool Btree<T>::insert( T const& value){
	if (root == nullptr){
		return false;
	}
	// 按照《算法导论》的描述，当root->keyNum == MinChild*2-1需要特殊处理，生成新的root节点，使得BTree的高度加一
	if (root->keyNum == MinChild * 2 - 1){
		shared_ptr<TreeNode> newNode = make_shared<TreeNode>();
		newNode->child[0] = root;
		newNode->isLeaf = false;
		root = newNode;
		splitChildNode(root,0);
		insertNotFull(root,value);
	}else{
		insertNotFull(root,value);
	}
	return true;
}

template<class T>
bool Btree<T>::insertNotFull(shared_ptr<TreeNode> node ,T const& value){
	if (node == nullptr){
		return false;
	}
	int position = 0;
    cout << "compator: "<< value.ToString() << " " <<  node->key[position].ToString() << " \n";
	while(position < node->keyNum && value > node->key[position] ){
		position++;
	}
	// 如果是叶节，由于之前的递归判断，保证了node->keyNum<MinChild * 2 - 1,因此可以直接插入
	if (node->isLeaf){	
		for (int i = node->keyNum; i > position; --i){
            cout<< "position: " << position  << " move "<<  node->key[i-1].ToString() 
                 << " -> " << node->key[i].ToString() << " ";
            node->key[i] = node->key[i-1];
                 
		}
		node->key[position] = value;
        cout <<"insert: "<< value.ToString()<< " keyNum: " << node->keyNum << "\n";
        
		node->keyNum++;
	}else{
	// 否则需要判断递归插入路径上的节点的 keyNum是否等于MinChild * 2 - 1，如果相等，为了防止后面插入过程中
	// splitChildNode使得父节点出现keyNum == MinChild * 2的情况发生向上的递归检验，这里提前将节点拆分
		if (node->child[position]->keyNum == MinChild * 2 - 1){
			splitChildNode(node,position);
			if (value > node->key[position]){
				position++;
			}		
		}
		insertNotFull(node->child[position],value);
	}
	return true;
}


template<class T>
void Btree<T>::printTree(){
	
	if (root == nullptr){
		cout<<"Empty Tree \n";
		return;
	}

	queue< pair<shared_ptr<TreeNode>,int> > q;
	q.push( make_pair(root,1) );
	int depth = 0;
	while(!q.empty()){
		shared_ptr<TreeNode> node = q.front().first;
		if (depth < q.front().second){
			cout<< " \n depth : "<< depth << " --------";
			depth = q.front().second;
		}
		q.pop();
		
		for (int i = 0; i < node->keyNum; ++i){
			cout << node->key[i].ToString() << " ";
		}
		cout<< "# ";
		//if (!node->isLeaf){
			for (int i = 0; i <= node->keyNum; ++i){
				if (node->child[i] == nullptr){
					break;
				}
				q.push(make_pair(node->child[i],depth+1));
			}
		//}
		
	}
}





#include "leveldb/slice.h"


int SliceTest(){

    leveldb::Slice a[6];
    
    a[0] = leveldb::Slice("a0");
    a[1] = leveldb::Slice("a1");
    a[2] = leveldb::Slice("a2");
    a[3] = leveldb::Slice("a3");
    a[4] = leveldb::Slice("a4");
    a[5] = leveldb::Slice("a5");
  

    Btree<leveldb::Slice> btree;
    for (int i = 0; i < 5; ++i){
        string str = std::to_string(i);
        leveldb::Slice slice =  leveldb::Slice(str);
		bool rt  = btree.insert(a[i]);
		if (!rt){
			cout << "insert "<< i << "ERR\n";
			return 0;
		}
	}
    btree.printTree();
    for (int i = 0; i < 150; ++i){
		bool rt = btree.find(leveldb::Slice(std::to_string(i))); 
		if (!rt){
			cout<<"find "<< i <<" ERR\n"; 
			return 0;
		}
	}
    return 0;
}


int main(int argc, char const *argv[]){

	
	//randomInsAndDel();

    SliceTest();
	//sequenceInsAndDel();

	return 0;
}