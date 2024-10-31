# IMDb Data Loader

IMDb Data Loader is a Java application that extracts, processes, and loads movie data from IMDb into an Oracle database. This application is designed to facilitate movie data analysis by storing information about movies, actors, directors, writers, and genres in a structured format, making it easier to perform queries, data visualizations, and analyses.

## Table of Contents
- [Features](#features)
- [Technologies Used](#technologies-used)
- [Database Setup](#database-setup)
- [Usage](#usage)
- [Configuration](#configuration)
- [License](#license)

---

## Features
- Fetches movie data from IMDb API, including title, runtime, release date, languages, aspect ratio, and budget.
- Retrieves and stores cast information (actors, directors, and writers) and their roles for each movie.
- Automatically creates relationships between movies and related entities (e.g., actors in movies, director of movies, writer of movies).
- Supports data storage for genres associated with each movie.
- Formats and inserts actor attributes like height, gender, and salary, ensuring clean and organized database storage.

## Technologies Used
- **Java**: Core application language for data handling and interaction with IMDb API.
- **JDBC (Java Database Connectivity)**: For database connectivity and SQL executions.
- **JSON**: To handle IMDb API responses and parse movie and person data.
- **Oracle Database**: Target database for storing movie and related information.

## Database Setup
1. Ensure Oracle Database is installed and accessible from your machine.
2. Create a new database with tables for `Movie`, `Actor`, `Director`, `Writer`, `Genre`, and related association tables like `Acts_In`, `Directs`, and `Writes`.
3. Modify `jdbcUrl`, `username`, and `password` variables in the Java code to match your Oracle Database settings.
4. Modify the API key to use a new API key. This is a feature that you must buy from IMDB.

### Additional Notes:
- Ensure Oracle Database is correctly configured and accessible.
- Use your unique IMDb API key and check API usage limits, as frequent requests might exhaust the quota.
- For testing, modify the range of IMDb IDs or reduce loop iterations to avoid long processing times.

### Sample Table Schema
Below are example schemas for the database tables:

```sql
CREATE TABLE Movie (
    Title VARCHAR2(255),
    Runtime NUMBER,
    Release_Date DATE,
    Aspect_Ratio VARCHAR2(10),
    Language VARCHAR2(50),
    Budget VARCHAR2(50)
);

-- ----------------- Entities -----------------

CREATE TABLE Movie(
Title		VARCHAR2(50) CONSTRAINT Movie_Title_PK PRIMARY KEY,
Runtime		NUMBER(3),
Release_Date	DATE,
Aspect_Ratio	VARCHAR(5),
Language	VARCHAR2(27),
Budget  	VARCHAR2(10)
);

CREATE TABLE Review(
Movie_Title	VARCHAR2(50),
RUser		VARCHAR2(25),
RTitle		VARCHAR2(50),
RDate		DATE,
Description	VARCHAR2(500),
Star		NUMBER(1),
CONSTRAINT review_pks PRIMARY KEY(Movie_Title, RUser)
);

CREATE TABLE Actor(
Fname				VARCHAR2(30),
Lname				VARCHAR2(30),
Salary				NUMBER(7),
Height				VARCHAR2(10),
Bdate				DATE,
Gender				CHAR(1),
Role 				VARCHAR2(40),
CONSTRAINT actor_pks PRIMARY KEY(Fname, Lname)
);

CREATE TABLE Director(
Fname			VARCHAR2(30),
Lname			VARCHAR2(30),
Salary			NUMBER(7),
Bdate			DATE,
Height			VARCHAR2(15),
Gender			CHAR(1),
CONSTRAINT director_pks PRIMARY KEY(Fname, Lname)
);

CREATE TABLE Writer(
Fname		VARCHAR2(30),
Lname		VARCHAR2(30),
Birth_date		DATE,
Height		VARCHAR2(15),
Salary		NUMBER(7),
Gender		CHAR(1),
CONSTRAINT writer_pks PRIMARY KEY(Fname, Lname)
);

CREATE TABLE Oscar(
Award_name 	VARCHAR2(50),
Year_awarded	NUMBER(4),
A_Fname		VARCHAR2(30),
A_Lname		VARCHAR2(30),
W_Fname		VARCHAR2(30),
W_Lname		VARCHAR2(30),
D_Fname		VARCHAR2(30),
D_Lname		VARCHAR2(30),
CONSTRAINT oscar_pks PRIMARY KEY(Award_name, Year_awarded)
);

CREATE TABLE Nominations(
Award_name	VARCHAR2(50),
Year_Awarded	NUMBER(4),
Nominee		VARCHAR2(60),
CONSTRAINT nominations_pks PRIMARY KEY(Award_name, Year_Awarded, Nominee)
);

-- ----------------- M to N Relationship Tables -----------------

-- Actor : Movie Relationship
CREATE TABLE Acts_in(
Actor_Fname 	VARCHAR2(30),
Actor_Lname 	VARCHAR2(30),
Movie_Title	VARCHAR2(50),
CONSTRAINT acts_in_pks PRIMARY KEY(Actor_Fname, Actor_Lname, Movie_Title)
);

-- Director : Movie Relationship
CREATE TABLE Directs(
Director_Fname 	VARCHAR2(30),
Director_Lname 	VARCHAR2(30),
Movie_Title	VARCHAR2(50),
CONSTRAINT directs_pks PRIMARY KEY(Director_Fname, Director_Lname, Movie_Title)
);

-- Writer : Movie Relationship
CREATE TABLE Writes(
Writer_Fname 	VARCHAR2(30),
Writer_Lname 	VARCHAR2(30),
Movie_Title	VARCHAR2(50),
CONSTRAINT writes_pks PRIMARY KEY(Writer_Fname, Writer_Lname, Movie_Title)
);

--------------------- Multi-Value Attribute Tables ------------------------

CREATE TABLE Soundtrack(
Movie_Name	VARCHAR2(50) CONSTRAINT Soundtrack_Movie_Name_PK PRIMARY KEY,
Song		VARCHAR2(50)
);

CREATE TABLE Genre(
Movie_Name	VARCHAR2(50),
Genre		VARCHAR2(30)
);

-- ----------------- Foreign Key Mapping --------------------

ALTER TABLE Soundtrack
ADD CONSTRAINT soundtrack_movie_name_fk FOREIGN KEY(Movie_Name)
REFERENCES Movie(Title);

ALTER TABLE Genre
ADD CONSTRAINT genre_movie_name_fk FOREIGN KEY(Movie_Name)
REFERENCES Movie(Title);

ALTER TABLE Review
ADD CONSTRAINT review_movie_title_fk FOREIGN KEY(Movie_Title)
REFERENCES Movie(Title);

ALTER TABLE Oscar
ADD CONSTRAINT oscar_a_fname_fk FOREIGN KEY(A_Fname, A_Lname)
REFERENCES Actor(Fname, Lname);


ALTER TABLE Oscar
ADD CONSTRAINT oscar_w_fname_fk FOREIGN KEY(W_Fname, W_Lname)
REFERENCES Writer(Fname, Lname);


ALTER TABLE Oscar
ADD CONSTRAINT oscar_d_fname_fk FOREIGN KEY(D_Fname, D_Lname)
REFERENCES Director(Fname, Lname);


ALTER TABLE Nominations
ADD CONSTRAINT nominations_year_awarded_fk FOREIGN KEY(Award_name, Year_Awarded)
REFERENCES Oscar(Award_name, Year_Awarded);

ALTER TABLE Acts_in
ADD CONSTRAINT acts_in_actor_fname_fk FOREIGN KEY(Actor_Fname, Actor_Lname)
REFERENCES Actor(Fname, Lname);


ALTER TABLE Acts_in
ADD CONSTRAINT acts_in_movie_title_fk FOREIGN KEY(Movie_Title)
REFERENCES Movie(Title);

ALTER TABLE Directs
ADD CONSTRAINT directs_director_fname_fk FOREIGN KEY(Director_Fname, Director_Lname)
REFERENCES Director(Fname, Lname);


ALTER TABLE Directs
ADD CONSTRAINT directs_movie_title_fk FOREIGN KEY(Movie_Title)
REFERENCES Movie(Title);

ALTER TABLE Writes
ADD CONSTRAINT writes_writer_fname_fk FOREIGN KEY(Writer_Fname, Writer_Lname)
REFERENCES Writer(Fname, Lname);


ALTER TABLE Writes
ADD CONSTRAINT writes_movie_title_fk FOREIGN KEY(Movie_Title)
REFERENCES Movie(Title);
