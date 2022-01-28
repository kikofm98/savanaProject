# savanaProject
Exercise: Clinical service mapping ETL

This test has the problem of loading the data correctly, changing the format of the dates, mapping the data in the "original_service" and "original_gender" columns to get the new ones specified in the other csv files and changing the column types to those specified in the test statement. 

To map the data I have loaded the csv's indicating the change of service and generated them as an rdd, which I have subsequently converted into Map objects. To facilitate the mapping of the entire columns I have converted these Maps into Column Map objects to facilitate this by simply using the "withColumn" method on the Dataframe loaded from the csv with the specified incoming data to the desired columns. I have chosen this way because, although it might be more efficient to treat everything as dataframes and using the "join" method to map the data as if it was a relational database, loading to RDDs is faster and transforming to map objects data with two columns is also a good way to do it.
To format the date columns I have created a method that uses the "withColumn" method and inside it I make a conversion to Date type for entries in different formats.

Finally, I have changed the type of the rest of the columns and renamed all of them to the ones specified by the statement.

It should also be mentioned that I have created two additional csv to the one provided by the statement to perform three different tests with different data and check that the output is as expected.

To run the tests:

- Have an IDE (recommended IntelliJ IDEA 2021.3.1) configured to run Scala projects (download Scala extension and preferably with jdk 1.8).
- Import the project into the IDE and run the Build of the project.
- Execute the file "/src/test/scala/ClinicalServiceTest.scala".
