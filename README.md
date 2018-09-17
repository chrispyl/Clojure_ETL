## Usage

To begin the operation call the ```update-db``` function in ```core.clj```. During the opearation messages describing the stage of the operation will be printed in the console.

## Project structure

```db_handling.clj``` : Database operations.

```xml_handling.clj``` : Update file operations.

```load_statistics.clj``` : CPU and memory usage monitoring.

```utilities.clj``` : Helper functions.

```core.clj``` : Orchestration of the above.

## Implementation strategy

The most expensive operation in this exercise is querying the db and as a result we have to find a way to limit the queries. On the other hand, we are memory limited and maybe we can't fit the whole db with a single query into memory. The middle ground is to process batches of records each time. 

In addtion, parsing the update file is also an expensive operation because all the characters are in the same line. Apart from being a time consuming operation, its content is big and maybe it can't fit into memory. So, the solution again is to process batches of update file records each time.

Eventually, the general mechanism of operation is that we fetch a batch from the update file and then we fetch batches from the db until we have compared the update file batch with the whole db. This means that for every update file batch a whole db traversal is required. During such a traveresal we store the update file records that must be inserted to the db or update it. At the end of each db traversal we insert to the db and update it with the aforementioned records. The picture below depicts these operations.

![Data flow](https://github.com/chrispyl/lambdawerks_test/blob/master/images/Data%20flow.jpg)


According to the above the total number of queries is ![Equation](https://github.com/chrispyl/lambdawerks_test/blob/master/images/equation.jpg)

The size of the record batch of the db and the update file can be changed, and if we have enough memory we can fit everything in it and complete the whole operation very fast.

## Database operations

To create the queries the library Korma was used. It was selected over clojure.java.jdbc because it allows their creation without concatenating strings, although for some queries strings where used to add further functionality. Also, it manages internally the connection pooling which is a plus. 

```SELECT```: In order to retrieve different records from the db every time, an extra ```id``` collumn was inserted for the duration of the whole operation. 

```INSERT``` : Regarding the insertions, they happen fast and only problem was that they had to be sent in batches of 5000, otherwise the connection to the db was being lost.

```UPDATE``` :To the updates now, updates were very if we tried to do them with the update query. The solution was to create a temporary table, insert them there and from there update the ```person``` table.



