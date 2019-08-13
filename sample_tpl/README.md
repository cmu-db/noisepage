# TPL Tests
This folder contains TPL tests. The `tpl_tests.txt` automates these tests and is used by the `make check-tpl` directive.
To add a test, insert an entry with the format `filename,is_sql,expected_return_value` to the `tpl_tests.txt` file.  
The `is_sql` flag indicates whether an execution context is needed to run the file. For example:
* `simple.tpl,false,44` indicates that the `simple.tpl` file does not need an execution context and should return `44`.
* `scan-table.tpl,true,500` indicates that the `scan-table.tpl` file needs an execution context and should return `500`.