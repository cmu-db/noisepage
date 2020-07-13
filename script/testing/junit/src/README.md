Instruction to use GenerateTrace:

First, establish a local postgresql database

Second, start the database server with "pg_ctl -D /usr/local/var/postgres start"

Third, in the command line, run ant compile

Finally, run ant generate-trace with 4 arguments: path, db-url, db-user and db-password

Command format: ant generate-trace -Dpath=PATH_TO_YOUR_FILE
 -Ddb-url=YOUR_JDBC_URL -Ddb-user=YOUR_DB_USERNAME -Ddb=password=YOUR_DB_PASSWORD

Format of your input file: sql statements, one per line
 
An output file should be produced called output.txt which is of tracefile format