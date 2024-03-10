from pymongo import MongoClient

MONGODB_HOST = 'mongo'
MONGODB_PORT = 27017
DB_NAME = 'student_db'
COLLECTION_NAME = 'student'

client = MongoClient(MONGODB_HOST, MONGODB_PORT)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

if db.counters.find_one({'_id': COLLECTION_NAME}) is None:
    db.counters.insert_one({'_id': COLLECTION_NAME, 'seq': 0})

# Returns next id in a ACID way
def get_next_student_id():
    sequence_document = db.counters.find_one_and_update(
        {'_id': COLLECTION_NAME},
        {'$inc': {'seq': 1}},
        return_document=True
    )
    next_seq = sequence_document['seq']
    return next_seq

def add(student=None):
    query = {'first_name': student.first_name, 'last_name': student.last_name}

    if collection.find_one(query):
        return 'already exists', 409

    encoded_data = student.to_dict()
    student_data = {
        'first_name': encoded_data['first_name'],
        'last_name': encoded_data['last_name'],
        'grade_records': encoded_data['grade_records'],
    }
    student_data['_id'] = get_next_student_id()
    res = collection.insert_one(student_data)
    student.student_id = student_data['_id']
    return student.student_id, 200


def get_by_id(student_id=None, subject=None):
    query = {'_id': int(student_id)}
    doc = collection.find_one(query)
    if not doc:
        return 'not found', 404
    return {
        'student_id': int(student_id),
        'first_name': doc['first_name'],
        'last_name': doc['last_name'],
        'grade_records': doc['grade_records'],
    }


def delete(student_id=None):
    query = {'_id': int(student_id)}
    doc = collection.find_one(query)
    if not doc:
        return 'not found', 404
    collection.delete_one({'_id': int(student_id)})
    return int(student_id)
