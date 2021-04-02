// Expected output: 70

fun f(z : Date ) -> Date { return z  }

fun main(exec : *ExecutionContext) -> int32 {
    var y = 11
    var lam = lambda [y] (z: Integer ) -> nil {
                    y = y + z
                }
    lam(10)


    var d = @dateToSql(1999, 2, 11)
    //f(lam, d)
    var k : Date
    //var h = &k
    //*h = d
    //k = f(d)
    lam(d)
    if(@datePart(y, @intToSql(21)) == @intToSql(1999)){
        // good
        return 1
    }
    return 0
}

fun pipeline1(QueryState *q) {
    TableIterator tvi;
    for(@tableIteratorAdvance(&tvi)){
        @hashTableInsert(q.join_ht, @getTupleValue(&tvi, 3)))
    }
}

fun pipeline2(QueryState *q) {
    TableIterator tvi;
    for(@tableIteratorAdvance(&tvi)){
        var o_custkey = @getTupleValue(&tvi, 1)
        if(@hashTableKeyExists(q.join_ht, o_custkey)){
            var out = @outputBufferAlloc(q.output_buff)
            out.col1 = o_custkey + 1
        }
    }
}