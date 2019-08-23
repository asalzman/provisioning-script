import psycopg2
import unicodecsv as csv
from tqdm import tqdm

print("Starting work...")

databasename = "ira_tweets"
username = "tweet_user"
password = "tL,dSW?P8fhyP=#B"
host = "130.211.229.132"

conn = psycopg2.connect(host = host, database = databasename, user = username, password = password)
cursor = conn.cursor()

print("Connected to database...")

userinput = "/home/miles/ira_tweets_csv_hashed.csv"
f = open(userinput, "rb")

print("Loading data...")

df = csv.reader(f, encoding = 'utf-8')
df2 = csv.DictReader(f, encoding = 'utf-8')

labels = next(df)
secondrow = next(df)

typelist = ["bigint PRIMARY KEY", "text", "text", "text", "text",  "text","text", "bigint", "bigint","text", "text","text", "text", "text", "text", "bigint", "text", "bigint", "text", "text", "bigint", "text", "text", "numeric", "numeric",  "numeric", "numeric", "text",  "text", "text", "text"]

print("Dropping old table...")

cursor.execute("DROP TABLE tweets;")

print("Creating new table...")

create_table_string = "CREATE TABLE IF NOT EXISTS tweets ("
for i in range(len(typelist)):
    create_table_string += labels[i] + " " + typelist[i] + ", "
create_table_string = create_table_string[:-2]

create_table_string+= ");"

cursor.execute(create_table_string)
conn.commit()

print("Generating initial values...")

list_of_names = labels
comma_separated_list = ""

for i in list_of_names:
    comma_separated_list = comma_separated_list + i + ", "

comma_separated_list = comma_separated_list[:-2]

insert_statement = ""

for i in range(len(labels)):
    insert_statement = insert_statement + "%s , "
insert_statement = insert_statement[:-2]
insertion_string = "INSERT INTO tweets VALUES (" + insert_statement + ");"

secondrow = [x if x != "" else None for x in secondrow]
cursor.execute(insertion_string, secondrow)

print("Inserting data...")

for z in tqdm(df):
    z = [x if x != "" else None for x in z]
    cursor.execute(insertion_string, z)

print("Creating index...")

cursor.execute("CREATE MATERIALIZED VIEW search_index AS SELECT tweetid, tweet_text, user_screen_name, user_reported_location, follower_count, tweet_language, like_count, retweet_count, setweight(to_tsvector(tweets.tweet_text), 'A') as document from tweets;")
cursor.execute("CREATE INDEX active_search_idx ON search_index USING gin(document);")  

print("Finishing up...")

conn.commit()

print("Finished!")
