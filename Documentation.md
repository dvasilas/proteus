This document describes the basic design of the Proteus framework, and provides the reader with usefull information for setting up and using the frameowrk, including:
- the interface between the system's components
- the distributed query execution semantics
- how the framework can be installed and configured

#### Document status: work-in-progress, incomplete
----
### Overview

With Proteus, a query processing system is deployed as a modular distributed architecture, composed of components that perform primitive query processing tasks.

The architecture's building block, termed **Q**uery **P**rocessing **U**nit, works as a service that receives and processes queries.

There are multiple types of QPUs; They all expose common API, and each type implements a different search algorithm.

Query processing systems are built by deploying QPUs in different nodes of the system, and interconnecting them in a directed acyclic graph.

----

### Data model
Proteus can in principle support multiple data models (object storage, NoSQL column storage, text document). The current implementation supports the object storage model.

#### Object storage
The dataset consists of a set of data items called *objects*.

Each object data object is uniquely identified by a *key* (a sequence of Unicode characters) and is composed of an uninterpreted blob of *data* accompanied by a set of *secondary (metadata) attributes*. 

A secondary attribute is a *key-value pair*, where the key has the same form as an object key, and the value can be have the following data types:
- integer
- float
- a sequence of Unicode characters

In this model, queries describe a set of secondary attributes, and the system responds with the keys of all objects that match the given attributes.

----

#### QPU Types
**Index QPU**: QPUs of this type maintain index structures, and process queries by performing index lookups. For more details see <a href="#iqpu"><code><b>Index QPU</b></code></a>.

**Cache QPU**: QPUs of this type maintain a cache of query results which use to process queries. For more details see <a href="#cqpu"><code><b>Cache QPU</b></code></a>.

=Filter QPU: Filters objects that match given query from an object stream; provides search by scanning the data store

Dispatch QPU: Works as query router for the QPU network


<a name="iqpu"></a>
#### Index QPU
- [ ] configuration
- [ ] index maintenance
- [ ] index-data consistency

<a name="cqpu"></a>
#### Cache QPU
- [ ] Cache type
- [ ] Invalication policy


Data store QPU: Data store abstraction; exposes common API for any data store; creates streams of objects/updates

----

#### QPU Interface


----

#### Stream processing

----

....

....

----


### Decentralized query processing protocol
1. Given a query, a QPU first tries to process it according the search algorithm it implements (performing index/cache lookup, data store scan).

2. If not possible, breaks down the given query to sub-queries; forwards sub-queries to neighbouring QPUs.
Query break down based on set of rules & QPU knowledge about neighbour capabilities.

4. Each neighbour recursively runs the same protocol; until sub-queries simple enough that can be processed in step1; results then incrementally combined.
