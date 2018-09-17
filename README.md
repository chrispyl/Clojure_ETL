## Usage

To begin the operation call the ```update-db``` function in ```core.clj```. During the operation, messages describing the its stage will be printed in the console.

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

```INSERT``` : Regarding the insertions, they happen fast and the only problem was that they had to be sent in batches of 5000, otherwise the connection to the db was being lost.

```UPDATE``` :To the updates now, updates were very if we tried to do them with the update query. The solution was to create a temporary table, insert them there and from there update the ```person``` table.

## CPU and memory utilization

The CPU and memory utilization are logged during the operation and saved to a file named ```load statistics.txt``` at the end.
But we can get a more clear picture by looking at the plots produced by the VisualVM profiler.

In the picture below we can observe the heap utilization after some db traversals. At the beggining of the application, the size of the heap is around 300MB. Suddenly, a spike expands it to 1000MB. This is the moment when we extract records from the update file which can't fit in the current heap and so it has to expand. Right after, the size of the heap goes to 1750 MB and that is the moment when we retrieve records from the db. Sudden drops to around 250MB (red color) happen when th GC kicks in to collect retrieved records from the db or file update records which we don't need anymore. As time passes by, we can observe that these blue spikes reach higher and make the heap grow little by little (black lines). This may be happening because there were references that needed a 'stop the world' garbage colletion which was not needed at that point.

![heap](https://github.com/chrispyl/lambdawerks_test/blob/master/images/heap.jpg)

Regarding the CPU utilization we can see that the spikes happen when the GC kicks in and also when we query the database.

![CPU](https://github.com/chrispyl/lambdawerks_test/blob/master/images/cpu.jpg)

## Results

The results after completing the whole operation.

|Inserts|Updates|
|-------|-------|
|500.035|499.982|

## Misc

By default, the maximum heap is 1/4 of the available RAM. If we wanted to get a significant reduction in the execution time of the operation we could set the flags for the JVM to use more heap and specifically a fixed heap from the beggining in order to avoid the heap allocation time ```Xmx12G Xms12G```.

