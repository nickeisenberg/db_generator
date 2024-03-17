# About
The goal of this repo is to help people get practice with SQL. There are plenty
of online resources for this however, this repo will allow you practice on your
own machine, %100 locally. The generation of a database will give the user the
opportunity to become familiar with

* Python and SQL integration,
* creation of a SQL server, ie mysql, postgresql, sqlite, etc.,
* SQLAlchemy,
* connecting the SQL server to SQLAlchemy through engines,
* SQL syntax.

Often, online tutorial-sites have a built-in SQL interpreter as well
as built in databases and so only the last bullet point above is 
addressed. Below are the listed subdirectories that correspond to a 
database generator.

# Current database generators
1. parents_and_children
2. investor_returns

# Instructions
In each of the subdirectories, there will be a `create.py` file that contains
a `Create` class. This class will be used to generate the database. More 
details about this `Create` class are given in each subdirectory. Moreover,
there are some questions listed in each subdirectory that you can choose to do 
for practice.

# Installation
```
python3 -m venv db_generator
source db_generator/bin/activate
git clone https://github.com/nickeisenberg/DatabaseGenerator.git
pip install .
pip install -r dependencies.txt
```
