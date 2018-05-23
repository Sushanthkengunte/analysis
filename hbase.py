from starbase import Connection

c = Connection("127.0.0.1","8000")
ratings = c.table('ratings')
if(ratings.exists()):
    print("Dropping existing table")
    ratings.drop()
ratings.create('rating')

print("parsing u.data")

ratingFile = open(r'E:\Studies\udemy_hadoop\dataset\ml-100k\u.data','r')

batch = ratings.batch()

for line in ratingFile:
    (userID,movieID,rating,timestamp) = line.split()
    batch.update(userID,{'rating': {movieID:rating}})
ratingFile.close()

print("Commiting ratings data into HBase")
batch.commit(finalize=True)

print('Get back ratings from users')
print('User id 1')
print(ratings.fetch('1'))
print('User id 33')
print(ratings.fetch('33'))