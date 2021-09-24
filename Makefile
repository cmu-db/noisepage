# Makefile
# Some scripting shortcuts.

# Run all regression checks in all execution modes
check-regress: check-regress-interpreted check-regress-compiled

# Run all regression tests in interpreted mode
check-regress-interpreted:
	cd build && PYTHONPATH=.. python -m script.testing.junit --build-type=debug --query-mode=simple

# Run all regression tests in compiled mode
check-regress-compiled:
	PYTHONPATH=.. python -m script.testing.junit --build-type=debug --query-mode=simple -a 'compiled_query_execution=True' -a 'bytecode_handlers_path=./bytecode_handlers_ir.bc'

check-regress-udf:
	cd build && PYTHONPATH=.. python -m script.testing.junit --build-type=debug --query-mode=simple --tracefile-test=udf.test

# Re-generate the trace file for UDF regression tests
generate-regress-udf:
	cd script/testing/junit/ && ant generate-trace -Dpath=sql/udf.sql -Ddb-url=jdbc:postgresql://localhost/test -Ddb-user=postgres -Ddb-password=password -Doutput-name=udf.test
