BINARIES=TestChunk TestRangedListView TestChunkedList
X10CPP=x10c++
X10OPTS=-sourcepath ../../ -x10rt mpi


all : $(BINARIES)

clean :
	rm -rf $(BINARIES)

test :
	./TestChunk validate testchunk.log testchunk-list.log
	./TestRangedListView validate testrangedlistview.log testrangedlistview-list.log
	./TestChunkedList validate testchunkedlist.log testchunkedlist-list.log

trace :
	./TestChunk trace testchunk.log testchunk-list.log
	./TestRangedListView trace testrangedlistview.log testrangedlistview-list.log
	./TestChunkedList trace testchunkedlist.log testchunkedlist-list.log

TestChunk : TestChunk.x10
	$(X10CPP) $(X10OPTS) -o $@ $^

TestRangedListView : TestRangedListView.x10
	$(X10CPP) $(X10OPTS) -o $@ $^

TestChunkedList : TestChunkedList.x10
	$(X10CPP) $(X10OPTS) -o $@ $^
