# Importing the main library used to connect Denodo via JDBC
import jaydebeapi as dbdriver

# Importing the gethostname function from socket to put the hostname in the useragent variable
from socket import gethostname

denodoserver_name = "serer_name"

# This is the standard port for jdbc connections
denodoserver_jdbc_port="9999"

denodoserver_database="denodo_database_name"
denodoserver_uid="uid_of_server"
denodoserver_pwd="pwd_of_server"
denododriver_path="need to install denodo driver campatible with source server version(denodo-vdp-jdbcdriver.jar"

# create the useragent as the concatenation of the client hostname and the python library used
client_hostname=gethostname()
useragent="%s-%s" % (dbdriver.__name__,client_hostname)

connctn_url="jdbc:vdb://%s:%s/%s?userAgent=%s" % (denodoserver_name,denodoserver_jdbc_port,denodoserver_database,useragent)
cnxn=dbdriver.connect("com.denodo.vdp.jdbc.Driver",connctn_url,
driver_args={"user":denodoserver_uid,"password":denodoserver_pwd},
jars=denododriver_path)

# Query to be sent to the Denoddo VDP server
query="select * from db_name limit 10"

# Define a cursor and execute the results
cur=cnxn.cursor()
cur.execute(query)

results=cur.fetchall()
print(results)