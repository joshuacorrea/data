
from google.cloud import secretmanager
from elasticsearch import Elasticsearch, helpers
from elasticsearch import NotFoundError
from datetime import datetime

secrets = secretmanager.SecretManagerServiceClient()
elastic_host = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_host/versions/1"}).payload.data.decode()
elastic_username_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_username_data/versions/1"}).payload.data.decode()
elastic_password_data = secrets.access_secret_version(request={"name": "projects/952416783871/secrets/elastic_password_data/versions/1"}).payload.data.decode()
es = Elasticsearch(elastic_host, http_auth=(elastic_username_data, elastic_password_data), scheme="https", port=443)

def get_author(id):
    try:
        raw = es.get(index='twitter_users_new', id=id)
        if 'obj' not in raw['_source']:
            print('dehydrated user', id)
            return {}
        return raw['_source']['obj']
    except NotFoundError as e:
        print('user not found', id)
        return {}

def get_retweet(raw):

    tweet = raw['_source']
    try:
        retweeted = get_tweet(tweet['target'])
    except NotFoundError as e:
        print('retweeted tweet not found', tweet['target'])
        return

    author = get_author(tweet['source'])
    username = ''
    if 'obj' in author:
        username = author['obj']['username']

    tweet_new = {
            "referenced_tweets":
                [
                    {
                    "id": tweet['target'],
                    "type": "retweeted"
                    }
                ],
            "created_at": tweet['created_at'],
            "entities": retweeted['obj']['tweet']['entities'],
            "attachments": retweeted['obj']['tweet']['attachments'],
            "text": "RT @" + username + ": " + retweeted['obj']['tweet']['text'],
            "id": tweet['retweet_id'],
            "lang": retweeted['obj']['tweet']['lang'],
            "author_id": tweet['source']
            }

    item = {
            "obj":
                {
                "tweet": tweet_new,
                "author": author
                },
            "context":
                {
                "last_updated": tweet['last_updated'],
                "api_version": 1
                }
            }

    store(item)

    return item


def get_tweet(id):
    raw = es.get(index='twitter_tweets', id=id)

    tweet = raw['_source']

    entities = tweet['obj']['entities']

    hashtags_new = []
    for h in entities['hashtags']:
        hashtags_new.append({
            "start": h['indices'][0],
            "end":   h['indices'][1],
            "tag":   h['text']
            })
    entities['hashtags'] = hashtags_new

    mentions_new = []
    for u in entities['user_mentions']:
        mentions_new.append({
            "start": u['indices'][0],
            "end":   u['indices'][1],
            "id":    u['id'],
            "username": u['screen_name']
            })
    entities['mentions'] = mentions_new
    del entities['user_mentions']

    urls_new = []
    for u in entities['urls']:
        urls_new.append({
            "start": u['indices'][0],
            "end":   u['indices'][1],
            "url": u['url'],
            "display_url": u['display_url'],
            "expanded_url": u['expanded_url']
            })
    entities['urls'] = urls_new

    tweet_new = {
            "referenced_tweets": [],
            "public_metrics": {
                "retweet_count": tweet['obj']['retweet_count'],
                "like_count": tweet['obj']['favorite_count']
                },
            "created_at": tweet['obj']['created_at'],
            "source": tweet['obj']['source'],
            "entities": tweet['obj']['entities'],
            "text": tweet['obj']['text'],
            "id": tweet['obj']['id'],
            "lang": tweet['obj']['lang'],
            "author_id": tweet['user_id']
            }
    if 'media' in tweet_new['entities']:
        tweet_new['attachments'] = {
                "media_keys": tweet_new['entities']['media']
                }
        del tweet_new['entities']['media']
    else:
        tweet_new['attachments'] = {}

    item = {
            "obj":
                {
                "tweet": tweet_new,
                "author": get_author(tweet['user_id'])
                },
            "context":
                {
                "last_updated": tweet['last_updated'],
                "api_version": 1
                }
            }

    if tweet['is_quote']:
        tweet_new['referenced_tweets'].append({'id': tweet['quote']['id'], 'type': 'quoted'})
        try:
            quoted_tweet = get_tweet(tweet['quote']['id'])
            item['obj']['quoted'] = {
                    "tweet": quoted_tweet['obj']['tweet'],
                    "author": quoted_tweet['obj']['author']
                    }
        except NotFoundError as e:
            print ('quoted tweet not found', tweet['quote']['id'])

    if tweet['is_reply']:
        tweet_new['referenced_tweets'].append({'id': tweet['reply']['id'], 'type': 'replied'})
        try:
            replied_tweet = get_tweet(tweet['reply']['id'])
            item['obj']['replied'] = {
                    "tweet": replied_tweet['obj']['tweet'],
                    "author": replied_tweet['obj']['author']
                    }
        except NotFoundError as e:
            print ('replied tweet not found', tweet['reply']['id'])
        item['obj']['tweet']['in_reply_to_user_id'] = tweet['reply']['user_id']


    store(item)

    return item


def store(item):
    global actions
    action = {
        "_op_type": "index",
        "_index": "twitter_tweets_new_backfill_test",
        "_id": item['obj']['tweet']['id'],
        "_source": item
    }
    actions.append(action)
    l = len(actions)
    if l % 10 == 0:
        print(datetime.now(), l)
    if l >= 1000:
        helpers.bulk(es, actions)
        actions = []

actions = []
store({"obj":{"tweet":{"id":"asdf"}}})
for hit_user in helpers.scan(es, index='twitter_users_new', _source="false", query={"query": {"match": {"context.primary": True}}}):
    userid = hit_user['_id']

    for hit in helpers.scan(es, index='twitter_tweets', _source="false", query={"query": {"match": {"user_id": userid}}}):
        get_tweet(hit['_id'])

    for hit in helpers.scan(es, index='twitter_retweets2', query={"query": {"match": {"source": userid}}}):
        get_retweet(hit)

helpers.bulk(es, actions)
