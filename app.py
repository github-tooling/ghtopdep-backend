import os
from urllib.parse import urlparse

from chalice import Chalice, Response
import pymongo
from pymongo.errors import OperationFailure
import datetime
from bson.json_util import dumps

app = Chalice(app_name='ghtopdep-slessback')
app.debug = True
mongo_con = pymongo.MongoClient(os.environ.get("DB_URL", "localhost"), 27017,
                                retryWrites=False)
mongo_db = mongo_con.ghtopdep
mongo_col = mongo_db.my_TTL_collection
try:
    ONE_WEEK = 604800
    mongo_col.create_index("date", expireAfterSeconds=ONE_WEEK)
    mongo_col.create_index("url", unique=True)
except OperationFailure:
    pass


@app.route('/repos', methods=['POST'], cors=True)
def repos_post():
    repo_deps = app.current_request.json_body
    utc_timestamp = datetime.datetime.utcnow()
    result = mongo_col.find_one({"url": repo_deps['url']})
    if result:
        mongo_col.update_one({'_id': result.get('_id')},
                             {"$set": {"date": utc_timestamp,
                                       "deps": repo_deps['deps']}
                              }, upsert=False)
        return "updated"
    else:
        mongo_col.insert_one({
            "date": utc_timestamp,
            "url": repo_deps['url'],
            'text': urlparse(repo_deps['url']).path[1:],
            "deps": repo_deps['deps']
        })
        return "OK"


@app.route('/repos', methods=['GET'], cors=True)
def repos_get():
    url = app.current_request.query_params.get('url')
    result = mongo_col.find_one({"url": url})
    if result:
        return dumps(result)
    return Response(body='repo not found',
                    status_code=404,
                    headers={'Content-Type': 'text/plain'})


@app.route('/all', methods=['GET'], cors=True)
def repos_all():
    result = mongo_col.find({})
    return [{'id': str(i.get('_id')), 'url': i['url'], 'text': urlparse(i['url']).path[1:]} for i in result]
