# Local Clone of Database

We want to be able to clone the production database for rapid testing 
on our local machines. There are two simple steps:

1) Install postgres on local machine

```brew install postgresql```
2) Dump Database: Clone the prod db to local repo 
2) Spin up dockerfile 
3) Call `LocalDatabase` rather than `ProductionDatabase`