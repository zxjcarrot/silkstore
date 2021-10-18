#ifndef BTREE_H
#define BTREE_H


#include <memory>
#include <vector>
#include <queue>
#include <utility>
#include <iostream>
#include <cstdlib>
#include <ctime>
#include <map>
#define MinChild 3 

//#define Node shared_ptr<TreeNode>


using namespace std;


template <class T> 
class Btree{


	public:
	struct TreeNode{
		TreeNode(){
			keyNum = 0;
			isLeaf = true;
		}

		T key[MinChild*2-1];
		shared_ptr<TreeNode> child[MinChild*2];
		bool isLeaf;
		int keyNum; 
	};	

	Btree();
	//~Btree();		
	
	bool insert(T const& value);
	bool find(shared_ptr<TreeNode> node , T const& key);
	bool find(T const& key);

	bool del(T const& value);
	bool remove(T const& value);
	bool removeAtNode(shared_ptr<TreeNode> node, int position); 
	bool removeMergeOrBorrow(shared_ptr<TreeNode> node, int position,int value); 
	//bool remove(shared_ptr<TreeNode> node, T const& value);
	bool remove(shared_ptr<TreeNode> node, T const& value);
	bool mergeChild(shared_ptr<TreeNode>  father, int position,
		shared_ptr<TreeNode> leftChild, shared_ptr<TreeNode> rightChild);
	T precursor(shared_ptr<TreeNode> node);
	
	T successor(shared_ptr<TreeNode> node);
	bool borrowFromRight(shared_ptr<TreeNode>  father, int position,
		shared_ptr<TreeNode> node, shared_ptr<TreeNode> rightChild);
	bool borrowFromLeft(shared_ptr<TreeNode>  father, int position,
		shared_ptr<TreeNode> node, shared_ptr<TreeNode> leftChild);

	bool splitChildNode(shared_ptr<TreeNode>, int );	
	bool insertNotFull(shared_ptr<TreeNode> node ,T const& value);
	void printTree();
	private:
	shared_ptr<TreeNode> root;

};

#endif