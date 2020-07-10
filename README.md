# IMDB-dataset-postgres-mongodb
This project has two components loading the IMDB dataset to Postgres relational database and migrating the relational database to mongoDB with appropriate types.
 
 
 ## 1) Loading Dataset<br/>
The IMBD data is populated in IMDB Postgres database. The relationship of the schema has been designed considering various logical relation. The details are given below 
● Movie ( id , type, title, originalTitle, startYear, endYear, runtime, avgRating, numVotes)
● Genre ( id , genre)
● Movie_Genre ( genre , movie )
● Member ( id , name, birthYear, deathYear)
● Movie_Actor ( actor , movie )
● Movie_Writer ( writer , movie )
● Movie_Director ( director , movie )
● Movie_Producer ( producer , movie )
● Role ( id , role)
● Actor_Movie_Role ( actor , movie , role )

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
