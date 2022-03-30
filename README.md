# UW-CSE-544

# About this course

The relational data model and the SQL query language. Conceptual modeling: entity/relationships, normal forms. XML, XPath, and XQuery. Transactions: recovery and concurrency control. Implementation of a database system. A medium sized project using a rational database backend.

# About the project

## overview description

We start off by building up the most basic element of a database system ---- the representation
of the data. In SimpleDB, we represent data in tuples and a self-defined class called tupleDesc
to represent schema. Now, we need an class to keep track of the all available tables in the
database and their associated schema and that will be our catalog. After building up the place
for the tables, we have to build the tables themselves and the way to read and write data to the
table. For building the tables, we first divide a table into pages, which is populated using the
class heapPage and heapPageID. This allows the user to load only a small portion of the table if
they want to read only some specific data in the table. To make the search of the page in the
table easier, we developed a class called recordID, which is a reference to a specific tuple on a
specific page of a specific table. For the way of reading and writing to the tables, we use the
class heapFile and bufferPool as a way to send information between the disk and the memory.
The heapFile not only stores a collection of the pages which stores tuples with no particular
orders, but it also contains functions to read and write data between the disk and the memory.
After heapFile reads data from the disk to the memory, it has to pass all the data it got to
bufferPool, and the bufferPool will be responsible for caching the data and deciding which
transaction is able to touch the data. We used LRU cache as our way of caching the data and
evicting pages since it is the most widely used method in the industry. We check the availability
of the data to a specific transaction by check whether our implementation of the lockManager is
holding a lock on the page this transaction is trying to access or not. The algorithm that we used
for granting and managing locks is 2PL where in every transaction, all lock requests must
precede all unlock requests. This method does not prevent potential deadlocks, so we
implemented a timeouts algorithm as a way to detect a deadlock. If a deadlock is detected, our
SimpleDB will terminate the transaction that causes this deadlock. This method is a NOSTEAL/
FORCE policy which has a very low flexibility. Therefore, we improved our system
with a STEAL/NO-FORCE recovery policy where we implemented the ARIES algorithm for
dealing with the potential crash of our system. Our SimpleDB allows the pages of uncommitted
transaction to be written into the disk and giving the flexibility of not forcing the pages of a
committed transaction to be written to disk. We always force the logs of the pages that are either
belong to an uncommitted transaction but is written to the disk or the pages that belong to a
committed transaction to the disk. In this way, we will have the states of all the pages that we
lost during a shutdown. This gives the system the freedom of deciding when is the best time to
write the pages to disk. As for recovering tour database, we first redo all the transactions
starting from the last check point or from the start of the log if no check point is found. Then,
undo all the loser transactions. With all these mechanisms backing up our database system, we
can develop the method of processing the query without worrying any potential loss of the data.
We implemented an integerAggregator and a stringAggregator to aggregate the needed data for
the query since there are only integer type and string type data. A hashjoin algorithm is
developed for dealing with the join predicates since it is the fastest algorithm in most cases.
Now, we have to have an operators that are responsible for the actual execution of the query
plan. SequenceScan is developed as an access method that reads each tuple of a table in no
particular order, and insert and delete operators are responsible for adding and deleting tuples
from a table.

## diagram

![SimpleDB](https://github.com/Jack-Chuang/UW-CSE-544/tree/main/image/SimpleDB%20diagram.png)
