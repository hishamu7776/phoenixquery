# phoenixquery
This code is developed for who intent to create phoenix tables/views from large hbase tables. This will help them with very less effort. This will reduce you data entry for query and you just have to provide hbase table information and hbase configuration.<br/><br/>

Step 1: Create a jar file from this code or download jar file from our repository<br/><br/>
        NOTE: Try to make jar with dependencies, otherwise you have to mention you dependencies in your command.<br/><br/>
Step 2: Then copy jar file to a place where you have permission to access hbase.<br/><br/>
Step 3: Run below command<br/><br/>
        java -cp "HbaseToPhoenixQueryGenerator.jar:<--mention the jar lib-->" com.hbase.querygenerator.HbaseToPhoenixQueryGenerator
