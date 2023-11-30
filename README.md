# ETL Transforming Script (Local Demo)

This is a script made for the transforming part of my ETL made for my NoSQL lecture.

## Overview

This code answers questions that I have about an actual economy:

- How to calculate the PIB of a day?

- How is the PIB doing inside the economy?

- How many transactions are being made of an specific items list a day? 

- How to divide in buys and sells this transactions?

- How can I compare the demand of these items with respect of the entire demand of objects?

- How can be done the Consumer Price Index of that set of items?
    - I calculate the CPI using [this](https://www.indeed.com/career-advice/career-development/how-to-calculate-cpi).

- How does the demand of this items look

- There is also a part of this project that uses Neo4J to make better analysis, this is not included here. The reason is explained below.

## Installation?

You cannot make a full installation of the system. This is because the publication of this data can be harmful in many ways. Because of that I chose not to add it here.
What you can do is open the visualizations notebook and see the preloaded data.

## How does it work?

Essentially this system calculates the transactions in a market. It just calculates the differences in stock between lookups to the market. It uses Spark
because is made thinking in a cloud based environment. 

It also uses ScyllaDB instead of a Cassandra. The main reason is that I want to learn more about Scylla and make a test of it. 
Obviously I want to do that because is a implementation of Cassandra made with C++ and, as they stated in their website,
has a better performance that Cassandra. If you want to learn more about it go and check this [link](https://www.scylladb.com/scylladb-vs-cassandra/).
There is more of it than this repo shows. But for the reasons above I decide to leave it like this. Was a lot of fun, I really enjoy the journey of this project.
Big shout out to my professor Miguel Escalante, I learn a lot from him and this project. Thank you for being my partner!

