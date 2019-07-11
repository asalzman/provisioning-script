import psycopg2
import unicodecsv as csv

databasename = "test3"
username = "postgres"
password = "1234"

conn = psycopg2.connect(database = databasename, user = username, password = password)
cursor = conn.cursor()

userinput = "/Users/michelleglewis/Downloads/ira_tweets_csv_hashed.csv"
f = open(userinput, "rb")

df = csv.reader(f, encoding = 'utf-8')
df2 = csv.DictReader(f, encoding = 'utf-8')

labels = next(df)
secondrow = next(df)

typelist = ["bigint", "text", "text", "text", "text",  "text","text", "bigint", "bigint","bigint", "text","text", "text", "text", "text", "bigint", "text", "bigint", "text", "text", "bigint", "text", "text", "bigint", "bigint",  "bigint", "bigint", "text",  "text", "text", "text"]
    
create_table_string = "CREATE TABLE IF NOT EXISTS tweettable("
for i in range(len(typelist)):
    create_table_string += labels[i] + " " + typelist[i] + ", "
create_table_string = create_table_string[:-2]

create_table_string+= ");"

cursor.execute(create_table_string)
conn.commit()

list_of_names = labels
comma_separated_list = ""

for i in list_of_names:
    comma_separated_list = comma_separated_list + i + ", "

comma_separated_list = comma_separated_list[:-2]

insert_statement = ""

for i in range(len(labels)):
    insert_statement = insert_statement + "%s , "
insert_statement = insert_statement[:-2]
insertion_string = "INSERT INTO tweettable VALUES (" + insert_statement + ")"

secondrow = [x if x != "" else None for x in secondrow]
cursor.execute(insertion_string, secondrow)

for z in df:
    z = [x if x != "" else None for x in z]
    cursor.execute(insertion_string, z)

cursor.execute("CREATE MATERIALIZED VIEW search_index2 AS SELECT tweet_text, user_screen_name, user_reported_location, follower_count, tweet_language, like_count, retweet_count, setweight(to_tsvector(tweettable.tweet_text), 'A') as document from tweettable;")
cursor.execute("CREATE INDEX idx_search2 ON search_index2 USING gin(document);")  

conn.commit()
