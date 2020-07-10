import psycopg2
import csv
import timeit

"""
This Programs loads the IMDB data set into the database to the tables according
to the proper entity relation.

Author: Pranjal Pandey
"""
# start time of execution
start = timeit.default_timer()


def createSchema():
    """
    This function create the required Schema According  to the requirements
    and print out the connection parameters
    It creates the following table
    Movie, Genre, Movie_Genre, Member, Movie_Actor, Movie_Writer, Movie_Director, Movie_producer,
    Role, Actor_Movie_Role
    :return: None
    """
    cursor = conn.cursor()

    # drop the table if already exists

    cursor.execute("DROP TABLE IF EXISTS MOVIE_GENRE")
    cursor.execute("DROP TABLE IF EXISTS GENRE")
    cursor.execute("DROP TABLE IF EXISTS ACTOR_MOVIE_ROLE")
    cursor.execute("DROP TABLE IF EXISTS MOVIE_ACTOR")
    cursor.execute("DROP TABLE IF EXISTS MOVIE_WRITER")
    cursor.execute("DROP TABLE IF EXISTS MOVIE_DIRECTOR")
    cursor.execute("DROP TABLE IF EXISTS MOVIE_PRODUCER")
    cursor.execute("DROP TABLE IF EXISTS ROLE")
    cursor.execute("DROP TABLE IF EXISTS MEMBER")
    cursor.execute("DROP TABLE IF EXISTS MOVIE")

    cursor.execute("""CREATE TABLE MOVIE
                    (
                        id INTEGER PRIMARY KEY,
                        type varchar(512),
                        title text,
                        originalTitle text,
                        startYear INTEGER,
                        endYear INTEGER,
                        runtime INTEGER,
                        avgRating FlOAT,
                        numVotes INTEGER
                    )"""
                   )
    cursor.execute("""CREATE TABLE Genre 
                ( 
                     id SERIAL PRIMARY KEY,
                     genre varchar(150) 
                )  """
                   )

    cursor.execute(
        '''CREATE TABLE MOVIE_GENRE
        (
            genre INTEGER REFERENCES Genre(id) ON DELETE CASCADE,
            movie INTEGER REFERENCES MOVIE(id) ON DELETE CASCADE
        )'''
    )
    cursor.execute(
        """CREATE TABLE MEMBER
        (
            id INTEGER PRIMARY KEY,
            name text NOT NULL,
            birthYear integer,
            deathYear integer
        )"""
    )

    cursor.execute(
        '''CREATE TABLE Movie_Actor
        (
            actor INTEGER REFERENCES MEMBER(id) ON DELETE CASCADE,
            movie INTEGER REFERENCES MOVIE(id) ON DELETE CASCADE
        )'''
    )

    cursor.execute(
        '''CREATE TABLE Movie_Writer
        (
            writer INTEGER REFERENCES MEMBER(id) ON DELETE CASCADE,
            movie INTEGER REFERENCES MOVIE(id) ON DELETE CASCADE
        )'''
    )

    cursor.execute(
        '''CREATE TABLE Movie_Director
        (
            director INTEGER REFERENCES MEMBER(id) ON DELETE CASCADE,
            movie INTEGER REFERENCES MOVIE(id) ON DELETE CASCADE
        )'''
    )

    cursor.execute(
        '''CREATE TABLE Movie_Producer
        (
            producer INTEGER REFERENCES MEMBER(id) ON DELETE CASCADE,
            movie INTEGER REFERENCES MOVIE(id) ON DELETE CASCADE
        )'''
    )
    cursor.execute('''CREATE TABLE ROLE
        (
            id SERIAL PRIMARY KEY,
            role text
        )'''
                   )
    cursor.execute("""CREATE TABLE ACTOR_MOVIE_ROLE
            (
              movie INTEGER,
              actor INTEGER,
              role INTEGER REFERENCES ROLE(id) on DELETE CASCADE
            )"""
                   )
    conn.commit()


def createTableMovieAndGenre(filePath1, filePath2):
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS title_basics_temp")

    # creating temporary table
    cursor.execute(
        '''CREATE TABLE title_basics_temp
            (
                tconst text PRIMARY KEY,
                title_type text,
                primary_title text,
                original_title text,
                is_adult text,
                start_year text,
                end_year text,
                runtime_mins text,
                genres text
            )''')

    copyCommand = """COPY title_basics_temp
                (tconst, title_type, primary_title, original_title, is_adult, start_year, end_year, runtime_mins, genres)
                FROM """ + repr(filePath1) + """
                WITH DELIMITER E'\t' NUll '\\N'  """

    # copying from tsv file
    cursor.execute(copyCommand)
    cursor.execute("""DELETE FROM title_basics_temp where tconst = 'tconst' """)

    conn.commit()

    cursor.execute("DROP TABLE IF EXISTS title_ratings_temp")
    cursor.execute(
        '''CREATE TABLE title_ratings_temp
        (
            tconst text,
            average_rating float,
            num_votes integer
        )'''

    )
    copyCommand = """COPY title_ratings_temp (tconst, average_rating, num_votes)
    FROM""" + repr(filePath2) + """ WITH DELIMITER E'\t' NUll '\\N' CSV HEADER  """

    cursor.execute(copyCommand)
    conn.commit()
    cursor.execute("DELETE FROM MOVIE")
    cursor.execute("""INSERT INTO Movie(id, type, title, originalTitle, startYear, endYear, runtime, avgRating,
                    numVotes )
                SELECT  replace(M.tconst,'tt','')::INTEGER,
                        M.title_type,
                        M.primary_title,
                        M.original_title,
                        M.start_year::INTEGER,
                        M.end_year::INTEGER,
                        M.runtime_mins::INTEGER,
                        R.average_rating::FLOAT,
                        R.num_votes::INTEGER
                FROM title_basics_temp M
                LEFT join title_ratings_temp R on
                M.tconst = R.tconst ORDER by M.tconst
                """)
    conn.commit()

    cursor.execute(
        """INSERT INTO GENRE(genre)
        SELECT DISTINCT regexp_split_to_table(genres, E',')
        FROM title_basics_temp"""
    )
    conn.commit()
    cursor.execute("DROP TABLE IF EXISTS tempMG")
    cursor.execute(
        '''CREATE TABLE tempMG
        (
            genre_text text,
            id INTEGER
        )'''
    )
    cursor.execute(
        """INSERT INTO tempMG(genre_text, id)
        SELECT regexp_split_to_table(genres, E','), replace(tconst,'tt','')::INTEGER
        FROM title_basics_temp """
    )
    cursor.execute("""INSERT INTO MOVIE_GENRE 
                      select G.id, t.id 
                      from tempMG t
                      inner join Genre G on
                      t.genre_text = G.genre
                       """)

    conn.commit()


def createTableMember(filePath):
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS name_basics_temp")

    # creating temp table
    cursor.execute(
        '''CREATE TABLE name_basics_temp
            (
             nconst text PRIMARY KEY,
             primary_name text,
             birth_year text,
             death_year text,
             primary_profession text,
             known_for_titles text
            )''')

    # using copycomand
    copyCommand = """COPY name_basics_temp
           (nconst, primary_name, birth_year, death_year, primary_profession,known_for_titles)
              FROM""" + repr(filePath) + """
              WITH DELIMITER E'\t' NUll '\\N'
            """
    # copying the content of file to temp table
    cursor.execute(copyCommand)
    cursor.execute("""DELETE FROM name_basics_temp where nconst = 'nconst' """)

    # copying from temp table to database table
    cursor.execute("""INSERT INTO MEMBER(id, name, birthYear, deathYear)
           SELECT replace(j.nconst,'nm','')::INTEGER,
           j.primary_name,
           j.birth_year::INTEGER,
           j.death_year::INTEGER
           FROM name_basics_temp j
           """)

    cursor.execute("DROP TABLE IF EXISTS name_basics_temp")
    conn.commit()


def createTableCast(filePath):
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS title_principals_temp")
    # creating the temp table
    cursor.execute(
        '''CREATE TABLE title_principals_temp
        (
            tconst text,
            ordering text,
            nconst text,
            category text,
            job text,
            characters text
        )'''
    )
    
    # copy command from file to table temp
    cursor.execute(
        """COPY title_principals_temp
        (tconst, ordering, nconst, category, job, characters)
           FROM """ + repr(filePath) + """
           WITH DELIMITER E'\t' NUll '\\N'
         """
    )
    conn.commit()
    # deleting the first row contains header info
    cursor.execute("""DELETE FROM title_principals_temp where nconst = 'nconst' """)
    
    cursor.execute("DROP TABLE IF EXISTS temp_table_principals")
    cursor.execute(
        '''CREATE TABLE temp_table_principals
        (
            tconst Integer,
            nconst Integer,
            category text,
            characters text
        )'''
    )
    cursor.execute("""INSERT INTO temp_table_principals(tconst, nconst, category, characters)
                select
                replace(tconst,'tt','')::INTEGER,
                replace(nconst,'nm','')::INTEGER,
                category,
                trim(both '[]' from characters)
                from title_principals_temp where
                category = 'director' OR category = 'actor' OR
                category ='producer' OR category = 'writer' OR
                category = 'actress'
                """)
    
    
    conn.commit()

    createTableActor()
    createTableWriter()
    createTableProducer()
    createTableDirector()
    createTableRole()
    createTableMovieRoleActor()



def createTableActor():
    cursor = conn.cursor()
    cursor.execute("""INSERT INTO MOVIE_ACTOR(actor, movie)
                select
                Distinct
                ttp.nconst,
                ttp.tconst
                from temp_table_principals ttp inner join Movie M on
                ttp.tconst = M.id inner join Member Me on 
                ttp.nconst = Me.id where
                ttp.category = 'actor' OR ttp.category = 'actress' 
                """)
    conn.commit()

def createTableWriter():

    cursor = conn.cursor()
    cursor.execute("""INSERT INTO MOVIE_Writer(writer, movie)
                select
                Distinct
                ttp.nconst,
                ttp.tconst
                from temp_table_principals ttp inner join Movie M on
                ttp.tconst = M.id  inner join Member Me on 
                ttp.nconst = Me.id where
                ttp.category = 'writer'
                """)
    conn.commit()

def createTableProducer():
    cursor = conn.cursor()
    cursor.execute("""INSERT INTO MOVIE_Producer(producer, movie)
                    select
                    Distinct
                    ttp.nconst,
                    ttp.tconst
                    from temp_table_principals ttp inner join Movie M on
                    ttp.tconst = M.id inner join Member Me on 
                    ttp.nconst = Me.id where
                    ttp.category = 'producer'
                    """)
    conn.commit()

def createTableDirector():
    cursor = conn.cursor()
    cursor.execute("""INSERT INTO MOVIE_Director(director, movie)
                    select
                    Distinct
                    ttp.nconst,
                    ttp.tconst
                    from temp_table_principals ttp inner join Movie M on
                    ttp.tconst = M.id inner join Member Me on 
                    ttp.nconst = Me.id where
                    ttp.category = 'director'
                    """)
    conn.commit()

def createTableRole():
    cursor = conn.cursor()
    cursor.execute(
        """INSERT INTO Role(role)
        SELECT DISTINCT characters
        FROM temp_table_principals"""
    )
    conn.commit()

def createTableMovieRoleActor():
    cursor = conn.cursor()

    cursor.execute("""INSERT INTO Actor_Movie_Role
                      select
                      distinct
                      MA.Movie,
                      MA.Actor,
                      R.id
                      from Movie_Actor MA
                      inner join temp_table_principals ttp on
                      ttp.tconst = MA.movie inner join Role R on
                      ttp.characters = R.role
                       """)
    cursor.execute("ALTER TABLE Movie_actor ADD PRIMARY KEY (Actor, Movie)")
    cursor.execute("""ALTER TABLE Actor_Movie_role
                      ADD CONSTRAINT fk
                      FOREIGN KEY (movie, actor) REFERENCES Movie_actor(movie, actor)""")
    cursor.execute("ALTER TABLE Movie_writer ADD PRIMARY KEY (writer, Movie)")
    cursor.execute("ALTER TABLE Movie_director ADD PRIMARY KEY (director, Movie)")
    cursor.execute("ALTER TABLE Movie_producer ADD PRIMARY KEY (producer, Movie)")
    conn.commit()




if __name__ == '__main__':
    To connect to the database
    user = input("Enter the user")
    password = input("Enter the password")
    host = input("Enter the host info")
    port = input("Enter the post info")
    database = input("Enter the database info")
    try:
        conn = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)
    except(Exception, psycopg2.Error) as msg:
        print("Connection error", msg)

    createSchema()
    filePathTitleBasics = input("Enter the filePath for title_basis")
    filePathTitleRatings = input("Enter the filePath for titleRatings")
    createTableMovieAndGenre(filePathTitleBasics, filePathTitleRatings)
    filePathNamesBasics = input("Enter the filePath for name-basics")
    createTableMember(filePathNamesBasics)
    filePathTitlePrincipal = input("Enter the filePath for title-principal")
    createTableCast(filePathTitlePrincipal)
