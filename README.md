*******************************************************************************
* pgcheck.py is a program to report various PostgreSQL database info for DBA
*
*
* some python packages required:
*
*       pip install psycopg2-binary
*       pip install config
*       pip install configparser
*
* tested with python 2.7.5
*
* Author:  Yu (Denis) Sun
*
*******************************************************************************

== program files:

pgcheck.py
config.py
database.ini  ( containing password,  readonly by owner : 600)

== to try out in this server:

   copy above *.py files to your directory. edit a database.ini file
   pgcheck.py is currently being updated frequently, check often to get lastest version


== content of database.ini - example
    put your own username and password !

        [username@123-456-7-89 pycheck]$ cat database.ini
        [postgresql]
        host=abcdeg.us-east-1.rds.amazonaws.com
        port=5432
        database=vvoprdtp
        user=username
        password=xxxx


      note: if password not in the int file, you will be prompted to enter password

=== usage
        Specify  -h to get help message


        [username@123-456-7-89 pycheck]$ python pgcheck.py -h
        usage: pgcheck.py [-h] [-c] [-a] [-ps] [-v] [-we] [DBConfigFile]

        PGCHECK - Report Various Info Of PostgreSQL Database

        positional arguments:
          DBConfigFile          Specify a PostgreSQL instance connection configuration
                                file. Default: database.ini

        optional arguments:
          -h, --help            show this help message and exit
          -c, --conn_info       Display current connecting session related info
          -a, --active_session  Display active session
          -ps, --pg_settings    Display PG settings
          -v, --version         Display PG version
          -we, --wait_event     Display Session wait event count


        -- use a DBconfigFile called db.ini
        pgcheck.py -c  db.ini


        -- In the following case the default DBconfigFile called database.ini is used
        pgcheck.py -c


