/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"
#include "exceptions/page_not_pinned_exception.h"

using namespace std;
//#define DEBUG

namespace badgerdb
{

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------

BTreeIndex::BTreeIndex(const std::string &relationName,
					   std::string &outIndexName,
					   BufMgr *bufMgrIn,
					   const int attrByteOffset,
					   const Datatype attrType)
{
	// Buffer Manager Instance
	bufMgr = bufMgrIn;
	// if index scan has been started
	scanExecuting = false;
	// # leaf slots
	leafOccupancy = INTARRAYLEAFSIZE;
	// # non-leaf slots
	nodeOccupancy = INTARRAYNONLEAFSIZE;

	// constructing index name
	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str();

	try
	{
		// open file
		file = new BlobFile(outIndexName, false);
		// Page number of meta page
		headerPageNum = file->getFirstPageNo();
		Page *metaPage;
		bufMgr->readPage(file, headerPageNum, metaPage);
		IndexMetaInfo *metaInfo = (IndexMetaInfo *)metaPage;
		if ((relationName != metaInfo->relationName) || (attrType != metaInfo->attrType) || (attrByteOffset != metaInfo->attrByteOffset))
		{
			throw BadIndexInfoException(outIndexName);
		}
		rootPageNum = metaInfo->rootPageNo;
		bufMgr->unPinPage(file, headerPageNum, false);
	}
	// create new index file
	catch (FileNotFoundException e)
	{
		file = new BlobFile(outIndexName, true);
		Page *metaPage, *rootPage;
		bufMgr->allocPage(file, headerPageNum, metaPage);
		bufMgr->allocPage(file, rootPageNum, rootPage);
		IndexMetaInfo *metaInfo = (IndexMetaInfo *)metaPage;
		metaInfo->attrByteOffset = attrByteOffset;
		metaInfo->attrType = attrType;
		metaInfo->rootPageNo = rootPageNum;
		strcpy(metaInfo->relationName, relationName.c_str());

		// root
		NonLeafNodeInt *root = (NonLeafNodeInt *)rootPage;
		root->level = 0;
		root->isLeaf = 0;
		root->key_count = 0;
		root->parent = 0;

		bufMgr->unPinPage(file, headerPageNum, true);
		bufMgr->unPinPage(file, rootPageNum, true);

		FileScan fileScan(relationName, bufMgr);
		RecordId rid;
		try
		{
			while (1)
			{
				fileScan.scanNext(rid);
				std::string record = fileScan.getRecord();
				insertEntry(record.c_str() + attrByteOffset, rid);
			}
		}
		catch (EndOfFileException e)
		{
			bufMgr->flushFile(file);
		}
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex()
{
	if (scanExecuting) endScan();

	bufMgr->flushFile(file);
	delete file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

const void BTreeIndex::insertEntry(const void *key, const RecordId rid)
{
	insert(key, rootPageNum, rid);
}

void BTreeIndex::insert(const void *key, const PageId pid, const RecordId rid)
{
	// is leaf?
	int isLeaf;
	// the key value
	int keyValue = *((int *)key);
	// page pointer
	Page *page;
	bufMgr->readPage(file, pid, page);
	isLeaf = *((int *)page);
	if (isLeaf == 0)
	{
		NonLeafNodeInt *node = (NonLeafNodeInt *)page;
		// empty at the begining
		if (node->key_count == 0)
		{
			node->keyArray[0] = keyValue + 1;

			// the left leaf
			Page *left;
			PageId leftId;
			bufMgr->allocPage(file, leftId, left);
			LeafNodeInt *leftLeaf = (LeafNodeInt *)left;

			// the right leaf
			Page *right;
			PageId rightId;
			bufMgr->allocPage(file, rightId, right);
			LeafNodeInt *rightLeaf = (LeafNodeInt *)right;

			leftLeaf->isLeaf = 1;
			leftLeaf->keyArray[0] = keyValue;
			leftLeaf->ridArray[0] = rid;
			leftLeaf->key_count = 1;
			leftLeaf->parent = pid;

			rightLeaf->isLeaf = 1;
			rightLeaf->key_count = 0;
			rightLeaf->parent = pid;
			rightLeaf->rightSibPageNo = 0;

			node->pageNoArray[0] = leftId;
			node->pageNoArray[1] = rightId;
			node->level = 1;
			node->key_count++;
			leftLeaf->rightSibPageNo = rightId;

			bufMgr->unPinPage(file, leftId, true);
			bufMgr->unPinPage(file, rightId, true);
		}
		else
		{
			// location found within the range of existing keys
			bool placeFound = false;
			int i;
			// location
			int index;
			for (i = 0; i < node->key_count; i++)
			{
				if (keyValue < node->keyArray[i])
				{
					placeFound = true;
					index = i;
					break;
				}
			}
			
			if (placeFound == true)
			{
				insert(key, node->pageNoArray[index], rid);
			}
			else
			{
				insert(key, node->pageNoArray[node->key_count], rid);
			}
		} 
	}

	else if (isLeaf == 1)
	{		
		LeafNodeInt *node = (LeafNodeInt *)page;
		if (node->key_count < INTARRAYLEAFSIZE)
		{
			// whether insertion is done
			bool insertDone = false;
			int i;
			for (i = 0; i < node->key_count; i++)
			{
				if (keyValue < node->keyArray[i])
				{
					memmove(&node->keyArray[i + 1], &node->keyArray[i], (node->key_count - i) * sizeof(int));
					memmove(&node->ridArray[i + 1], &node->ridArray[i], (node->key_count - i) * sizeof(RecordId));
					node->keyArray[i] = keyValue;
					node->ridArray[i] = rid;
					node->key_count++;
					insertDone = true;
					break;
				}
			}
			if (insertDone == false)
			{
				node->keyArray[node->key_count] = keyValue;
				node->ridArray[node->key_count] = rid;
				node->key_count++;
			}
		}
		else
		{
			leafSplitInsert(key, pid, rid);
		}
	}
	bufMgr->unPinPage(file, pid, true);
}

void BTreeIndex::leafSplitInsert(const void *key, const PageId pid, const RecordId rid)
{
	// the key value
	int keyValue = *((int *)key);
	// page pointer
	Page *page;
	bufMgr->readPage(file, pid, page);
	LeafNodeInt *node = (LeafNodeInt *)page;
	// the middle index
	int middle = INTARRAYLEAFSIZE / 2;
	// new leaf
	Page *newPage;
	PageId newPageId;
	bufMgr->allocPage(file, newPageId, newPage);
	LeafNodeInt *newLeaf = (LeafNodeInt *)newPage;
	newLeaf->isLeaf = 1;
	int i;
	for (i = middle; i < INTARRAYLEAFSIZE; i++)
	{
		newLeaf->keyArray[i - middle] = node->keyArray[i];
		newLeaf->ridArray[i - middle] = node->ridArray[i];
	}
	node->key_count = middle;
	newLeaf->key_count = INTARRAYLEAFSIZE - middle;
	newLeaf->rightSibPageNo = node->rightSibPageNo;
	node->rightSibPageNo = newPageId;

	// new non-leaf node
	Page *newNonleaf;
	PageId newNonleafID;
	bufMgr->allocPage(file, newNonleafID, newNonleaf);
	NonLeafNodeInt *nonleaf = (NonLeafNodeInt *)newNonleaf;
	// the node to combine with the new non-leaf node
	PageId combineNode = node->parent;
	nonleaf->isLeaf = 0;
	nonleaf->level = 1;
	nonleaf->key_count = 1;
	nonleaf->keyArray[0] = newLeaf->keyArray[0];
	nonleaf->pageNoArray[0] = pid;
	nonleaf->pageNoArray[1] = newPageId;
	node->parent = newNonleafID;
	newLeaf->parent = newNonleafID;

	// leaf to store keyValue
	LeafNodeInt *addLeaf;
	if (keyValue < nonleaf->keyArray[0])
		addLeaf = node;
	else
		addLeaf = newLeaf;

	// insert done
	bool insertDone = false;
	for (i = 0; i < addLeaf->key_count; i++)
	{
		if (keyValue < addLeaf->keyArray[i])
		{
			memmove(&addLeaf->keyArray[i + 1], &addLeaf->keyArray[i], (addLeaf->key_count - i) * sizeof(int));
			memmove(&addLeaf->ridArray[i + 1], &addLeaf->ridArray[i], (addLeaf->key_count - i) * sizeof(RecordId));
			addLeaf->keyArray[i] = keyValue;
			addLeaf->ridArray[i] = rid;
			addLeaf->key_count++;
			insertDone = true;
			break;
		}
	}
	if (insertDone == false)
	{
		addLeaf->keyArray[addLeaf->key_count] = keyValue;
		addLeaf->ridArray[addLeaf->key_count] = rid;
		addLeaf->key_count++;
	}

	combineNonleaf(newNonleafID, combineNode);

	bufMgr->unPinPage(file, pid, true);
	bufMgr->unPinPage(file, newPageId, true);
	bufMgr->unPinPage(file, newNonleafID, true);
}

// pid1 has 1 key. combine it with pid2
void BTreeIndex::combineNonleaf(const PageId pid1, const PageId pid2)
{
	// page1 pointer
	Page *page1;
	bufMgr->readPage(file, pid1, page1);
	NonLeafNodeInt *node1 = (NonLeafNodeInt *)page1;
	// page2 pointer
	Page *page2;
	bufMgr->readPage(file, pid2, page2);
	NonLeafNodeInt *node2 = (NonLeafNodeInt *)page2;

	if (node2->key_count < INTARRAYNONLEAFSIZE)
	{
		// found place before the last available key
		bool foundDone = false;
		int i;
		for (i = 0; i < node2->key_count; i++)
		{
			if (node2->keyArray[i] > node1->keyArray[0])
			{
				foundDone = true;
				memmove(&node2->keyArray[i + 1], &node2->keyArray[i], (node2->key_count - i) * sizeof(int));
				memmove(&node2->pageNoArray[i + 2], &node2->pageNoArray[i + 1], (node2->key_count - i) * sizeof(PageId));
				node2->keyArray[i] = node1->keyArray[0];
				node2->pageNoArray[i] = node1->pageNoArray[0];
				node2->pageNoArray[i + 1] = node1->pageNoArray[1];
				node2->key_count++;
				break;
			}
		}
		if (foundDone == false)
		{
			node2->keyArray[node2->key_count] = node1->keyArray[0];
			node2->pageNoArray[node2->key_count] = node1->pageNoArray[0];
			node2->pageNoArray[node2->key_count + 1] = node1->pageNoArray[1];
			node2->key_count++;
		}
		if (node1->level == 1)
		{
			// child1 of node1
			Page *child1;
			bufMgr->readPage(file, node1->pageNoArray[0], child1);
			LeafNodeInt *leftChild = (LeafNodeInt *)child1;
			// child2 of node1
			Page *child2;
			bufMgr->readPage(file, node1->pageNoArray[1], child2);
			LeafNodeInt *rightChild = (LeafNodeInt *)child2;
			leftChild->parent = pid2;
			rightChild->parent = pid2;
			bufMgr->unPinPage(file, node1->pageNoArray[0], true);
			bufMgr->unPinPage(file, node1->pageNoArray[1], true);
		}
		else
		{
			// child1 of node1
			Page *child1;
			bufMgr->readPage(file, node1->pageNoArray[0], child1);
			NonLeafNodeInt *leftChild = (NonLeafNodeInt *)child1;
			// child2 of node1
			Page *child2;
			bufMgr->readPage(file, node1->pageNoArray[1], child2);
			NonLeafNodeInt *rightChild = (NonLeafNodeInt *)child2;
			leftChild->parent = pid2;
			rightChild->parent = pid2;
			bufMgr->unPinPage(file, node1->pageNoArray[0], true);
			bufMgr->unPinPage(file, node1->pageNoArray[1], true);
		}
	}

	else
	{
		// create a new node
		Page *newPage1;
		PageId newNodeID;
		bufMgr->allocPage(file, newNodeID, newPage1);
		NonLeafNodeInt *newNode = (NonLeafNodeInt *)newPage1;
		// create a new parent
		Page *newPage2;
		PageId newParentID;
		bufMgr->allocPage(file, newParentID, newPage2);
		NonLeafNodeInt *newParent = (NonLeafNodeInt *)newPage2;

		newNode->isLeaf = 0;
		newNode->level = node2->level;
		newParent->isLeaf = 0;
		newParent->level = 0;
		// split index
		int splitIndex = INTARRAYNONLEAFSIZE / 2;
		memmove(&newNode->keyArray[0], &node2->keyArray[splitIndex + 1], (node2->key_count - splitIndex - 1) * sizeof(int));
		memmove(&newNode->pageNoArray[0], &node2->pageNoArray[splitIndex + 1], (node2->key_count - splitIndex) * sizeof(PageId));
		newParent->keyArray[0] = node2->keyArray[splitIndex];
		newParent->pageNoArray[0] = pid2;
		newParent->pageNoArray[1] = newNodeID;
		newParent->key_count = 1;
		node2->key_count = splitIndex;
		newNode->key_count = INTARRAYNONLEAFSIZE - splitIndex - 1;

		int keyValue = node1->keyArray[0];
		// the node to include node1
		NonLeafNodeInt *addNode;
		if (keyValue < newNode->keyArray[0])
			addNode = node2;
		else
			addNode = newNode;

		int i;
		// insert done
		bool insertDone = false;
		for (i = 0; i < addNode->key_count; i++)
		{
			if (keyValue < addNode->keyArray[i])
			{
				memmove(&addNode->keyArray[i + 1], &addNode->keyArray[i], (addNode->key_count - i) * sizeof(int));
				memmove(&addNode->pageNoArray[i + 2], &addNode->pageNoArray[i + 1], (addNode->key_count - i) * sizeof(PageId));
				addNode->keyArray[i] = keyValue;
				addNode->pageNoArray[i] = node1->pageNoArray[0];
				addNode->pageNoArray[i + 1] = node1->pageNoArray[1];
				addNode->key_count++;
				insertDone = true;
				break;
			}
		}
		if (insertDone == false)
		{
			addNode->keyArray[addNode->key_count] = keyValue;
			addNode->pageNoArray[addNode->key_count] = node1->pageNoArray[0];
			addNode->pageNoArray[addNode->key_count + 1] = node1->pageNoArray[1];
			addNode->key_count++;
		}

		// recognizing parents (big drama show)
		for (i = 0; i < (node2->key_count + 1); i++)
		{
			// child page
			Page *childPage;
			bufMgr->readPage(file, node2->pageNoArray[i], childPage);
			if (node2->level == 1)
			{
				LeafNodeInt *child = (LeafNodeInt *)childPage;
				child->parent = pid2;
			}
			else
			{
				NonLeafNodeInt *child = (NonLeafNodeInt *)childPage;
				child->parent = pid2;
			}
			bufMgr->unPinPage(file, node2->pageNoArray[i], true);
		}
		for (i = 0; i < (newNode->key_count + 1); i++)
		{
			// child page
			Page *childPage;
			bufMgr->readPage(file, newNode->pageNoArray[i], childPage);
			if (newNode->level == 1)
			{
				LeafNodeInt *child = (LeafNodeInt *)childPage;
				child->parent = newNodeID;
			}
			else
			{
				NonLeafNodeInt *child = (NonLeafNodeInt *)childPage;
				child->parent = newNodeID;
			}
			bufMgr->unPinPage(file, newNode->pageNoArray[i], true);
		}
		// who's the parent of node2? let's see the test result:
		PageId n2Parent = node2->parent;
		// new parent!
		node2->parent = newParentID;
		newNode->parent = newParentID;
		if (n2Parent == 0)
		{
			newParent->parent = 0;
			Page *metaPage;
			bufMgr->readPage(file, headerPageNum, metaPage);
			IndexMetaInfo *metaInfo = (IndexMetaInfo *)metaPage;
			rootPageNum = newParentID;
			metaInfo->rootPageNo = newParentID;
			bufMgr->unPinPage(file, headerPageNum, true);
		}
		else
			combineNonleaf(newParentID, n2Parent);

		bufMgr->unPinPage(file, newNodeID, true);
		bufMgr->unPinPage(file, newParentID, true);
	}
	bufMgr->unPinPage(file, pid1, true);
	bufMgr->unPinPage(file, pid2, true);
}

// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
// -------------------- SCAN ---------------------------------------------------
// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------
// -----------------------------------------------------------------------------

// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

const void BTreeIndex::startScan(const void *lowValParm,
								 const Operator lowOpParm,
								 const void *highValParm,
								 const Operator highOpParm)
{
	if (lowOpParm != GT && lowOpParm != GTE)
		throw BadOpcodesException();
	if (highOpParm != LT && highOpParm != LTE)
		throw BadOpcodesException();

	lowValInt = *((int *)lowValParm);
	highValInt = *((int *)highValParm);
	if (lowValInt > highValInt)
		throw BadScanrangeException();

	lowOp = lowOpParm;
	highOp = highOpParm;

	scanExecuting = true;
	Page *metaPage;
	bufMgr->readPage(file, headerPageNum, metaPage);
	IndexMetaInfo *metaInfo = (IndexMetaInfo *)metaPage;
	currentPageNum = metaInfo->rootPageNo;
	bufMgr->unPinPage(file, headerPageNum, false);
	setPageIdForScan();
	setEntryIndexForScan();

	LeafNodeInt *node = (LeafNodeInt *)currentPageData;
	RecordId outRid = node->ridArray[nextEntry];
	if ((outRid.page_number == 0 && outRid.slot_number == 0) ||
		node->keyArray[nextEntry] > highValInt ||
		(node->keyArray[nextEntry] == highValInt && highOp == LT))
	{
		endScan();
		throw NoSuchKeyFoundException();
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::isLeaf
// -----------------------------------------------------------------------------

bool BTreeIndex::isLeaf(Page *page)
{
	return *((int *)page) == 1;
}

// -----------------------------------------------------------------------------
// BTreeIndex::setPageIdForScan
// -----------------------------------------------------------------------------

void BTreeIndex::setPageIdForScan()
{
	bufMgr->readPage(file, currentPageNum, currentPageData);
	if (isLeaf(currentPageData))
		return;
	NonLeafNodeInt *node = (NonLeafNodeInt *)currentPageData;
	bufMgr->unPinPage(file, currentPageNum, false);
	currentPageNum = node->pageNoArray[findIndexNonLeaf(node, lowValInt)];
	setPageIdForScan();
}

// -----------------------------------------------------------------------------
// BTreeIndex::setNextEntry
// -----------------------------------------------------------------------------

void BTreeIndex::setNextEntry()
{
	nextEntry++;
	LeafNodeInt *node = (LeafNodeInt *)currentPageData;
	if (nextEntry >= node->key_count ||
		node->ridArray[nextEntry].page_number == 0)
	{
		moveToNextPage(node);
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::findIndexNonLeaf
// -----------------------------------------------------------------------------

int BTreeIndex::findIndexNonLeaf(NonLeafNodeInt *node, int key)
{
/*	static auto comp = [](const PageId &p1, const PageId &p2) { return p1 > p2; };
	PageId *start = node->pageNoArray;
	PageId *end = &node->pageNoArray[INTARRAYNONLEAFSIZE + 1];
	int len = lower_bound(start, end, 0, comp) - start;
	int len = node->key_count;
	int result = findArrayIndex(node->keyArray, len - 1, key, false);
	return result == -1 ? len - 1 : result;
*/
	int i;
	// the value to be returned
	int retVal;
	// whether there is a key greater than the given key
	bool found = false;
	for (i = 0; i < node->key_count; i++)
	{
		if (node->keyArray[i] > key)
		{
			retVal = i;
			found = true;
			break;
		}
	}
	if (found == false)
	{
		retVal = node->key_count;
	}
	return retVal;
}
// -----------------------------------------------------------------------------
// BTreeIndex::findArrayIndex
// -----------------------------------------------------------------------------

/*
int BTreeIndex::findArrayIndex(const int *arr, int len, int key,
									 bool includeKey)
{
	if (!includeKey)
		key++;
	int result = lower_bound(arr, &arr[len], key) - arr;
	return result >= len ? -1 : result;
}
*/

// -----------------------------------------------------------------------------
// BTreeIndex::findScanIndexLeaf
// -----------------------------------------------------------------------------
/*
int BTreeIndex::findScanIndexLeaf(LeafNodeInt *node, int key, bool includeKey)
{
	static auto comp = [](const RecordId &r1, const RecordId &r2) {
		return r1.page_number > r2.page_number;
	};
	static RecordId emptyRecord{};

	RecordId *start = node->ridArray;
	RecordId *end = &node->ridArray[INTARRAYLEAFSIZE];
	int temp = lower_bound(start, end, emptyRecord, comp) - start;
	return findArrayIndex(node->keyArray, temp, key, includeKey);
}
*/
// -----------------------------------------------------------------------------
// BTreeIndex::setEntryIndexForScan
// -----------------------------------------------------------------------------

void BTreeIndex::setEntryIndexForScan()
{
	LeafNodeInt *node = (LeafNodeInt *)currentPageData;
//	int entryIndex = findScanIndexLeaf(node, lowValInt, lowOp == GTE);
	int entryIndex;
	int i;
	// found?
	bool found = false;
	for (i = 0; i < node->key_count; i++)
	{
		if ((lowOp == GTE) && (node->keyArray[i] >= lowValInt))
		{
			entryIndex = i;
			found = true;
			break;
		}
		if ((lowOp != GTE) && (node->keyArray[i] > lowValInt))
		{
			entryIndex = i;
			found = true;
			break;
		}
	}
	if (found == false)
	{
		entryIndex = -1;
	}
	
	if (entryIndex == -1)
	{
		moveToNextPage(node);
	}
	else
	{
		nextEntry = entryIndex;
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::moveToNextPage
// -----------------------------------------------------------------------------

void BTreeIndex::moveToNextPage(LeafNodeInt *node)
{
	bufMgr->unPinPage(file, currentPageNum, false);
	currentPageNum = node->rightSibPageNo;
	bufMgr->readPage(file, currentPageNum, currentPageData);
	nextEntry = 0;
}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

const void BTreeIndex::scanNext(RecordId &outRid)
{
	if (!scanExecuting)
		throw ScanNotInitializedException();

	LeafNodeInt *node = (LeafNodeInt *)currentPageData;
	
	outRid = node->ridArray[nextEntry];
	int val = node->keyArray[nextEntry];

	if ((outRid.page_number == 0 &&
		 outRid.slot_number == 0) ||
		val > highValInt ||
		(val == highValInt && highOp == LT))
	{
		throw IndexScanCompletedException();
	}
	setNextEntry();
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
const void BTreeIndex::endScan()
{
	if (!scanExecuting)
		throw ScanNotInitializedException();
	scanExecuting = false;
	bufMgr->unPinPage(file, currentPageNum, false);
}

} // namespace badgerdb
