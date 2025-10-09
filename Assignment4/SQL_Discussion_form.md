# SQL Discussion form

## Administration
* Your name: Zahra Taheri

* Your discussion partner's name:Sanne

* Commit hash of the code you discussed:  
8720d6671986a01f4145874f8150f9d835fedddc
* Your code's pylint score: 9.59

## Discussion
### Pre-set discussion points:
- Start by discussing your DB design choices (tables, data types)
- Then go on to your implementation;

DB design choices (tables, data types):
I used 3 tables — species, genes, and proteins.

species stores general info about the organism (name, accession, genome size, taxid).

genes stores gene info linked to the species by a foreign key.

proteins stores protein info linked to both species and genes.
Data types like VARCHAR, INT, and TEXT were used to keep it simple and compatible with MariaDB.

1. How did you split up the work (functions/classes)?
I used small helper functions for each task — reading the config file, connecting to the database, creating tables, inserting species/genes/proteins, and parsing the GenBank file.
No classes were needed.

2. How long (about) does it take, where is time spent?
It depends on the file size. For one Archaea file, it takes a few minutes. (6 min)
Most time is spent reading and parsing the .gbff file and inserting data into the database.

3. Did you use parallel processing, if so how? If not, why not?
No. I used batch inserts instead to improve speed.
I might try adding it later for larger files.

4. What was your difference in pylint scores? Can you see why?
My pylint score was 9.59.
I got a high score because the code is clean, well-structured, has docstrings, and follows PEP8 style.

5. How did the normalization level (table number) influence the code?
Using 3 tables made the code simpler and faster to write.
A more normalized (5-table) version would reduce redundancy but increase complexity.

6. Did your choice in database table design make it more or less hard to do the assignment?
For this assignment, 3 tables were fine and easier to implement.
Some queries may be harder, so I plan to expand it to 5 tables in the future.

Extra points that came up during discussion of your code:

None.

Extra comments about the assignment:

I had a small commit error on GitHub related to the 5-table version.
I plan to improve and extend the design to 5 tables later if I have enough time.