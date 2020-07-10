# IMDB-dataset-postgres-mongodb
This project has two components loading the IMDB dataset to Postgres relational database and migrating the relational database to mongoDB with appropriate types.
 
 
 ## 1) Loading Dataset<br/>
The IMBD data is populated in IMDB Postgres database. The relationship of the schema has been designed considering various logical relation. The details are given below:<br/>
● Movie ( id , type, title, originalTitle, startYear, endYear, runtime, avgRating, numVotes)<br/>
● Genre ( id , genre)<br/>
● Movie_Genre ( genre , movie )<br/>
● Member ( id , name, birthYear, deathYear)<br/>
● Movie_Actor ( actor , movie )<br/>
● Movie_Writer ( writer , movie )<br/>
● Movie_Director ( director , movie )<br/>
● Movie_Producer ( producer , movie )<br/>
● Role ( id , role)<br/>
● Actor_Movie_Role ( actor , movie , role )<br/>

## 2) Migrating to MongoDB<br/>
The imdb data is migrated to MongoDB and populated to two collections Movies and Members.


Movies:
{ _id, 
type, 
title, 
originalTitle, 
startYear, 
endYear, 
runtime, 
avgRating, 
numVotes, 
genres, 
actors, 
roles, 
directors,
writers,
producers
}

Members:
{ _id,
name,
birthYear,
deathYear,
}
