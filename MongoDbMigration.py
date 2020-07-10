"""
This Programs migrates the data from postgres and load it to mongoDb


Author: Pranjal Pandey
"""
import psycopg2
import pymongo
import itertools
from itertools import chain



def memberTable():
   cursor = conn.cursor()
   jsonList= []
   query = """select json_strip_nulls(json_build_object
              ('_id', m.id, 'name', m.name, 'birthyear', m.birthyear, 'deathyear', m.deathyear))
              FROM Member m"""
   cursor.execute(query)
   fetchedRecords = cursor.fetchall()

   for row in fetchedRecords:
      for jobject in row:
          jsonList.append(jobject)

   memberCollection.insert_many(jsonList)



def movieCollection():
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS tempActor")
    cursor.execute("CREATE TABLE tempActor(movie_id Integer, actors jsonb)")

    cursor.execute("""INSERT INTO tempActor(movie_id, actors)
    select mx.movie, (jsonb_strip_nulls(jsonb_build_object('actor', mx.actor, 'roles', mx.roles))) from
    (select amr.actor as actor, amr.movie as movie,
    array_to_json(NULLIF(array_agg(Distinct r.name), '{null}')) as roles
    from movie m left join actor_movie_role amr on
    m.id = amr.movie left join role r
    on amr.role = r.id Group by (amr.actor, amr.movie) ) mx""")


    query = """select json_strip_nulls(json_build_object
    ('_id', n._id, 'type', n.type, 'title', n.title, 'originalTitle', n.originalTitle,
    'startYear', n.startYear, 'endYear', n.endYear, 'runtime', n.runtime, 'avgRating', n.avgRating,
    'numVotes', n.numVotes, 'genres', n.genres, 'actors', n.actors, 'directors', n.directors,
    'producers', n.producers, 'writers', n.writers))
    From
    (Select m.id as _id, m.type as type, m.title as title, m.originaltitle as originalTitle,
    m.startyear as startYear, m.endyear as endYear, m.runtime as runtime, m.avgrating as avgRating,
    m.numvotes as numVotes, array_to_json(NULLIF(array_agg(Distinct g.name), '{null}')) as genres,
    array_to_json(NULLIF(array_agg(Distinct ta.actors), '{null}')) as actors,
    array_to_json(NULLIF(array_agg(Distinct md.director), '{null}')) as directors,
    array_to_json(NULLIF(array_agg(Distinct mw.writer), '{null}')) as writers,
    array_to_json(NULLIF(array_agg(Distinct mp.producer), '{null}')) as producers
    from movie m left join movie_genre mg
    ON m.id = mg.movie left join movie_producer mp
    ON m.id = mp.movie left join movie_director md
    on m.id = md.movie left join movie_writer mw
    on m.id = mw.movie left join genre g
    on mg.genre = g.id left join tempactor ta
    on m.id = ta.movie_id
    group by (m.id) )n"""

    cursor.execute(query)
    jsonList = []
    fetchedRecords = cursor.fetchall()

    for row in fetchedRecords:
        for jobject in row:
            jsonList.append(jobject)

    movCollection.insert_many(jsonList)
    cursor.execute("DROP TABLE IF EXISTS tempActor")



if __name__ == '__main__':
    print("to connect with postgres")
    user = input("Enter user")
    password = input("Enter password")
    host = input("enter host ip")
    port = input("enter port")
    database = input("Enter database to connect with")

    try:
        conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)
        print(conn.get_dsn_parameters(), "\n")
    except(Exception, psycopg2.Error) as msg:
        print("Connection error", msg)

    print("to connect with Mongodb")
    client = input("Enter the client info")
    db = input("Enter the db name")
    myclient = pymongo.MongoClient(client)
    mydb = myclient[db]
    memberCollection = mydb["Members"]
    movCollection = mydb["Movies"]
    memberTable()
    movieCollection()