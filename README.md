# HeniTest
Test space for Heni

## The following steps have been performed to set up the virtual env 
1. Created a new project HeniTest in PyCharm and created a virtual env called venv.
2. From the PyCharm terminal installed pyspark, pip install pyspark
3. Wrote the HeniTest.py and tested it 
4. Outputs are uploaded here: Output/Agg_Output1.parquet and Output/Agg_Output2.parquet

In a typical Databricks environment, would install databrick-connect, through pip install and proceed with the development.
That way, we will be able to test the changes on the databricks dev cluster itself, if there is one. 
Also, that gives us the flexibility to work with abfss:// or  s3:// from the PyCharm IDE itself.

And, we will also be able to work with delta format files within Databricks.

