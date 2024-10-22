
#ifndef BPLUS_C
#define BPLUS_C

#include "MyDB_INRecord.h"
#include "MyDB_BPlusTreeReaderWriter.h"
#include "MyDB_PageReaderWriter.h"
#include "MyDB_PageListIteratorSelfSortingAlt.h"
#include "RecordComparator.h"

MyDB_BPlusTreeReaderWriter :: MyDB_BPlusTreeReaderWriter (string orderOnAttName, MyDB_TablePtr forMe, 
	MyDB_BufferManagerPtr myBuffer) : MyDB_TableReaderWriter (forMe, myBuffer) {

	// find the ordering attribute
	auto res = forMe->getSchema ()->getAttByName (orderOnAttName);

	// remember information about the ordering attribute
	orderingAttType = res.second;
	whichAttIsOrdering = res.first;

	// and the root location
	rootLocation = getTable ()->getRootLocation ();
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getSortedRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high) 
{
	// get pages
	vector<MyDB_PageReaderWriter> pages;
	discoverPages(this->rootLocation, pages, low, high);

	// get ListIterator
	bool sortOrNotIn = true;
	auto lhs = getEmptyRecord();
	auto rhs = getEmptyRecord();
	auto myRecIn = getEmptyRecord();

	auto lhsIn = getINRecord();
	auto rhsIn = getINRecord();
	lhsIn->setKey(low);
	rhsIn->setKey(high);

	auto comp = buildComparator(lhs, rhs);
	auto lowComp = buildComparator(myRecIn, lhsIn);
	auto highComp = buildComparator(rhsIn, myRecIn);

	auto myRecordIter = make_shared<MyDB_PageListIteratorSelfSortingAlt>(pages, lhs, rhs, comp, myRecIn, lowComp, highComp, sortOrNotIn);
	return myRecordIter;
}

MyDB_RecordIteratorAltPtr MyDB_BPlusTreeReaderWriter :: getRangeIteratorAlt (MyDB_AttValPtr low, MyDB_AttValPtr high)
{
	// get pages
	vector<MyDB_PageReaderWriter> pages;
	discoverPages(this->rootLocation, pages, low, high);

	// get ListIterator
	bool sortOrNotIn = false;
	auto lhs = getEmptyRecord();
	auto rhs = getEmptyRecord();
	auto myRecIn = getEmptyRecord();

	auto lhsIn = getINRecord();
	auto rhsIn = getINRecord();
	lhsIn->setKey(low);
	rhsIn->setKey(high);

	auto comp = buildComparator(lhs, rhs);
	auto lowComp = buildComparator(myRecIn, lhsIn);
	auto highComp = buildComparator(rhsIn, myRecIn);

	auto myRecordIter = make_shared<MyDB_PageListIteratorSelfSortingAlt>(pages, lhs, rhs, comp, myRecIn, lowComp, highComp, sortOrNotIn);
	return myRecordIter;
}

void MyDB_BPlusTreeReaderWriter :: append (MyDB_RecordPtr appendMe)
{
	// insert in B+ Tree
	// if there is no root
	if (rootLocation == -1) {
		// get an internal root page and set rootlocation
		this->rootLocation = 0;
		auto rootPage = (*this)[0];
		this->forMe->setRootLocation(this->rootLocation);
		rootPage.clear();
		rootPage.setType(MyDB_PageType::DirectoryPage);
		// get an internal node
		auto myRecIn = getINRecord();
		myRecIn->setPtr(1);
		rootPage.append(myRecIn);

		// get an leaf page
		forMe->setLastPage(1);
		auto leafPage = (*this)[1];
		leafPage.clear();
		leafPage.setType(MyDB_PageType::RegularPage);
		leafPage.append(appendMe);

		// cout << endl;
		// cout << "After Insert Root page" << endl;
		// printTree();
		return;
	}
	// append a new node to the tree
	auto myRec = append(this->rootLocation, appendMe);
	// if it get split in the next level
	if (myRec != nullptr) {
		// get a new root page
		int newRoot = forMe->lastPage() + 1;
		forMe->setLastPage(newRoot);
		auto newRootPage = (*this)[newRoot];
		newRootPage.clear();
		newRootPage.setType(MyDB_PageType::DirectoryPage);

		// get a new iternal nodes
		auto oldRootRecIn = getINRecord();
		oldRootRecIn->setPtr(this->rootLocation);

		// insert/append two iternal nodes to new root
		// first is new; second is old
		newRootPage.append(myRec);
		newRootPage.append(oldRootRecIn);

		// update rootlocation
		this->rootLocation = newRoot;
		forMe->setRootLocation(newRoot);

	}
}

void MyDB_BPlusTreeReaderWriter :: printTree ()
{
	printTree(this->rootLocation, 0);
}

// HELPER METHODS
bool MyDB_BPlusTreeReaderWriter :: discoverPages (int whichPage, vector <MyDB_PageReaderWriter> &list, MyDB_AttValPtr low, MyDB_AttValPtr high)
{
	// get page
	auto myPage = (*this)[whichPage];

	if (myPage.getType() == MyDB_PageType :: RegularPage) {
		// if current page is a regular page
		// put it in the list and return true
		list.push_back(myPage);
		return true;
	} else {
		// if current page is not a regular page
		// check if the key is in the range
		auto myRec = myPage.getIteratorAlt();

		auto nextRec = getINRecord();

		auto lhsIn = getINRecord();
		auto rhsIn = getINRecord();
		lhsIn->setKey(low);
		rhsIn->setKey(high);
		auto lowComp = buildComparator(nextRec, lhsIn);
		auto highComp = buildComparator(rhsIn, nextRec);

		// assume keys in file are sorted ?
		// isLower: true if key < low
		// isHigher: true if key > high
		// only !isLower && !isHigher is the key we want
		bool isLower = true;
		bool isHigher = false;
		bool isPage = false;
		while (myRec->advance() == true) {
			// get the key in myRecIn and compare
			myRec->getCurrent(nextRec);

			if (!lowComp() == true) {
				int current = nextRec->getPtr();
				if(isPage){
					list.push_back((*this)[current]);
				}
				else{
					isPage = discoverPages(current, list, low, high);
				}
			}
			if (highComp() == true)	{
				return false;
			}
		}
		return false;
	}
	// return false;
}

// leaving all of the records with big key values in place
// creating a new page with all of the small key values at the end of the file
// return internal node record
MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: split (MyDB_PageReaderWriter splitMe, MyDB_RecordPtr andMe)
{
	// cout << "Split is called" << endl;
	// create a new page
	// printTree();
	int newPage = forMe->lastPage() + 1;
	forMe->setLastPage(newPage);
	auto myPage = (*this)[newPage];
	myPage.clear();

	// create a support page
	int supPage = forMe->lastPage() + 1;
	// forMe->setLastPage(supPage);
	auto tempPage = (*this)[supPage];
	tempPage.clear();

	// return newptr
	auto newPtr = getINRecord();
	newPtr->setPtr(newPage);

	// get page type
	auto myType = splitMe.getType();
	myPage.setType(myType);
	tempPage.setType(myType);

	MyDB_RecordPtr lhs, rhs;
	MyDB_RecordPtr temp;
	if (myType == MyDB_PageType::RegularPage) {
		lhs = getEmptyRecord();
		rhs = getEmptyRecord();
		temp = getEmptyRecord();
	} else {
		lhs = getINRecord();
		rhs = getINRecord();
		temp = getINRecord();
	}
	auto comp = buildComparator(lhs, rhs);
	splitMe.sortInPlace(comp, lhs, rhs);
	int recordCounts = 0;
	int mid = 0;
	auto recIter = splitMe.getIteratorAlt();

	// first iterate, get the number of records
	while (recIter->advance()) {
		recIter->getCurrent(temp);
		++recordCounts;
	}
	int copyCount = 0;
	mid = recordCounts / 2;

	// second iterate, copy from old page
	recIter	= splitMe.getIteratorAlt();

	if (myType == MyDB_PageType::RegularPage) {
		temp = getEmptyRecord();
	} else {
		temp = getINRecord();
	}

	while (recIter->advance()) {
		recIter->getCurrent(temp);
		if (copyCount == mid) {
			newPtr->setKey(getKey(temp));
			myPage.append(temp);
		} else if (copyCount < mid) {
			myPage.append(temp);
		} else {
			tempPage.append(temp);
		}
		++copyCount;
	}

	// decide where should andMe go
	auto compAdd = buildComparator(andMe, newPtr);
	if (compAdd() == true) {
		// andMe < newPtr
		myPage.append(andMe);
		myPage.sortInPlace(comp, lhs, rhs);
	} else {
		tempPage.append(andMe);
		tempPage.sortInPlace(comp, lhs, rhs);
	}

	// third iterate, copy from tempPage to old page
	splitMe.clear();
	if (myType == MyDB_PageType::RegularPage) {
		temp = getEmptyRecord();
	} else {
		temp = getINRecord();
		splitMe.setType(MyDB_PageType::DirectoryPage);
	}
	recIter = tempPage.getIteratorAlt();
	while (recIter->advance()) {
		recIter->getCurrent(temp);
		splitMe.append(temp);
	}

	// sayonara, tempPage
	tempPage.clear();

	return newPtr;
	// return nullptr;
}

MyDB_RecordPtr MyDB_BPlusTreeReaderWriter :: append (int whichPage, MyDB_RecordPtr appendMe)
{
	auto myPage = (*this)[whichPage];
	// myPage is a leaf page
	if (myPage.getType() == MyDB_PageType::RegularPage)	{
		// can't insert in, split
		if (myPage.append(appendMe) == false) {
			return split(myPage, appendMe);
		} else {
			return nullptr;
		}
	}
	// myPage is a internal page
	auto nextRec = getINRecord();
	auto myRec = myPage.getIteratorAlt();
	auto comp = buildComparator(appendMe, nextRec);
	while (myRec->advance() == true) {
		myRec->getCurrent(nextRec);

		// appendMe < nextRec
		if (comp() == true) {
			auto nextPage = nextRec->getPtr();
			auto retRec = append(nextPage, appendMe);

			if (retRec == nullptr) {
				// append without split
				return nullptr;
			} else {
				// append with a split in next level
				if (myPage.append(retRec) == false) {
					return split(myPage, retRec);
				} else {
					auto myRecIn = getINRecord();
					auto sortComp = buildComparator(retRec, myRecIn);
					myPage.sortInPlace(sortComp, retRec, myRecIn);
					return nullptr;
				}
			}
		}
	}
	// I hope will never use this return
	// return nullptr;
}

MyDB_INRecordPtr MyDB_BPlusTreeReaderWriter :: getINRecord () {
	return make_shared <MyDB_INRecord> (orderingAttType->createAttMax ());
}


MyDB_AttValPtr MyDB_BPlusTreeReaderWriter :: getKey (MyDB_RecordPtr fromMe) {

	// in this case, got an IN record
	if (fromMe->getSchema () == nullptr)
		return fromMe->getAtt (0)->getCopy ();

	// in this case, got a data record
	else
		return fromMe->getAtt (whichAttIsOrdering)->getCopy ();
}

function <bool ()>  MyDB_BPlusTreeReaderWriter :: buildComparator (MyDB_RecordPtr lhs, MyDB_RecordPtr rhs) {

	MyDB_AttValPtr lhAtt, rhAtt;

	// in this case, the LHS is an IN record
	if (lhs->getSchema () == nullptr) {
		lhAtt = lhs->getAtt (0);

	// here, it is a regular data record
	} else {
		lhAtt = lhs->getAtt (whichAttIsOrdering);
	}

	// in this case, the RHS is an IN record
	if (rhs->getSchema () == nullptr) {
		rhAtt = rhs->getAtt (0);

	// here, it is a regular data record
	} else {
		rhAtt = rhs->getAtt (whichAttIsOrdering);
	}

	// now, build the comparison lambda and return
	if (orderingAttType->promotableToInt ()) {
		return [lhAtt, rhAtt] {return lhAtt->toInt () < rhAtt->toInt ();};
	} else if (orderingAttType->promotableToDouble ()) {
		return [lhAtt, rhAtt] {return lhAtt->toDouble () < rhAtt->toDouble ();};
	} else if (orderingAttType->promotableToString ()) {
		return [lhAtt, rhAtt] {return lhAtt->toString () < rhAtt->toString ();};
	} else {
		cout << "This is bad... cannot do anything with the >.\n";
		exit (1);
	}
}

// MY HELPER METHODS

void MyDB_BPlusTreeReaderWriter :: printTree (int whichPage, int depth) {

	MyDB_PageReaderWriter pageToPrint = (*this)[whichPage];

	//  a leaf page
	if (pageToPrint.getType () == MyDB_PageType :: RegularPage) {
		MyDB_RecordPtr myRec = getEmptyRecord ();
		MyDB_RecordIteratorAltPtr temp = pageToPrint.getIteratorAlt ();
		while (temp->advance ()) {

			temp->getCurrent (myRec);
			for (int i = 0; i < depth; i++)
				cout << "\t";
			cout << myRec << "\n";
		}

	//  a directory page
	} else {

		MyDB_INRecordPtr myRec = getINRecord ();
		MyDB_RecordIteratorAltPtr temp = pageToPrint.getIteratorAlt ();
		while (temp->advance ()) {

			temp->getCurrent (myRec);
			printTree (myRec->getPtr (), depth + 1);
			for (int i = 0; i < depth; i++)
				cout << "\t";
			cout << (MyDB_RecordPtr) myRec << "\n";
		}
	}
}

#endif
