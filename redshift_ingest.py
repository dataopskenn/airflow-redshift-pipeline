import  psycopg2
import configparser

# Loading cluster configurations from cluster.config
config = configparser.ConfigParser()
config.read_file(open('cluster.config'))

def test_connection(host):

    dbname = config.get('DWH','DWH_DB')
    port = config.get('DWH','DWH_PORT')
    user = config.get('DWH','DWH_DB_USER')
    password = config.get('DWH','DWH_DB_PASSWORD')

    connect=psycopg2.connect(dbname= dbname, host=host, port= port, user= user, password= password)
    cur = connect.cursor()

    cur.execute("CREATE TABLE (id int);")
    cur.execute("INSERT INTO VALUES (10);")
    print(cur.execute('SELECT * FROM'))

    connect.close()