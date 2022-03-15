import tweepy
# # For tweepy documentation go to https://www.tweepy.org/
import pandas as pd
# # For pandas documentation go to https://pandas.pydata.org/docs/
from datetime import datetime, timezone
import time
from collections import Counter

# # Authentification
client = tweepy.Client(bearer_token="AAAAAAAAAAAAAAAAAAAAANegTgEAAAAA%2FNe3uH1%2B76X1bXQdCYFV6VvjxQo%3DD27egWluZcQKyZcgnDJ68Ff7q3It25SswU43V7t4ISoHkojywB",
                       consumer_key="BzjYwEDLCdGI0D45tQiemLPp1",
                       consumer_secret="CgZtcbadsI0wKJSxIIG5LSYKOBULD0o2U9J55dz9zuA6UlxZsW",
                       access_token="419746241-CvoL86Pw2r047uMJGkwf0bE9Y27AeSFy0jcnhMmX",
                       access_token_secret="BFNvCbJc5A2j4ly8KrkrukaB9Mtqg6uBoGFXo21i4RlkH",
                       wait_on_rate_limit=True)
auth = tweepy.OAuthHandler("BzjYwEDLCdGI0D45tQiemLPp1", "CgZtcbadsI0wKJSxIIG5LSYKOBULD0o2U9J55dz9zuA6UlxZsW")
auth.set_access_token("419746241-CvoL86Pw2r047uMJGkwf0bE9Y27AeSFy0jcnhMmX", "BFNvCbJc5A2j4ly8KrkrukaB9Mtqg6uBoGFXo21i4RlkH")
api = tweepy.API(auth, wait_on_rate_limit=True)

tweet_id_list = []
tweets_list = []
retweeters_list = []
tell_tweet_dict = {'tweet_id': [], 'amount_retweets': [], 'retweeters': []}
look_up_list = []
tweet_obj_list = []


def scrape_tweets(user_id):
    for tweet in tweepy.Paginator(client.get_users_tweets, id=user_id, start_time='2021-08-26T00:00:01Z', end_time='2021-09-26T23:59:59Z',
                            max_results=100, exclude="retweets").flatten(250):
        tweet_id_list.append(tweet.id)
        tweet_obj_list.append(tweet)
    print(len(tweet_id_list))
    return tweet_id_list


def get_retweeters(tweet_id):
    tell_tweet_dict['tweet_id'].append(tweet_id)
    tell_retweeters_list = []
    for retweeter in tweepy.Paginator(client.get_retweeters, id=tweet_id).flatten(1000):
        tell_retweeters_list.append(retweeter.id)
        tell_amount_retweets = len(tell_retweeters_list)
        if retweeter.id not in retweeters_list:
            retweeters_list.append(retweeter.id)
    tell_tweet_dict['amount_retweets'].append(tell_amount_retweets)
    tell_tweet_dict['retweeters'].append(tell_retweeters_list)
    return tell_tweet_dict, retweeters_list


def clean_alt_list(list_):
    list_ = list_.replace(', ', '","')
    list_ = list_.replace('[', '["')
    list_ = list_.replace(']', '"]')
    return list_


def to_1d(series):
    return pd.Series([x for _list in series for x in _list])


def user_lookup(retweeter_id):
    user = api.get_user(user_id=retweeter_id, include_entities=True)
    return user


def lookup_user_list(user_id_list, api):
    full_users = []
    users_count = len(user_id_list)
    try:
        for i in range(int((users_count / 100) + 1)):
            full_users.extend(api.lookup_users(user_id=user_id_list[i*100:min((i+1)*100, users_count)]))
            # print 'getting users batch:', i
    except tweepy.TweepyException as e:
        print('Something went wrong, quitting...', e)
        time.sleep(15 * 60)
    return full_users


def add_to_lookup(lookup_item):
    if lookup_item not in look_up_list:
        look_up_list.append(lookup_item)


def tweets_per_day(user_id):
    now = datetime.now(timezone.utc)
    created_date = info.created_at
    delta = now - created_date
    # print(delta.days)
    tweet_count = info.statuses_count
    # print(tweet_count)
    # print("Tweets per day: ")
    tweets_per_day = round((tweet_count / delta.days), 2)
    # print(tweets_per_day)
    return tweets_per_day


def years_since_created_at(user_id):
    now = datetime.now(timezone.utc)
    created_date = info.created_at
    delta = now - created_date
    years = round((delta.days/365), 2)
    return years


# # Defining User with ID
user_id = 844081278

# # Scrape Tweets of defined User
print(scrape_tweets(user_id))

# # Get Retweeters and save to csv
for tweet_id in tweet_id_list:
    get_retweeters(tweet_id)
df_checklist = pd.DataFrame(retweeters_list)
df_checklist.to_csv('retweeters_list.csv', index=False, header=True)
print(tweet_obj_list)
df_tell_tweet = pd.DataFrame(tell_tweet_dict)
print(df_tell_tweet)
df_tell_tweet.to_csv('tell_tweet.csv', index=False, header=True)
print("retweeters scraped and dataframe saved")

# # Get Content of Tweets
data_list = []
for info in tweet_obj_list:
    tweet_dict = {
        'data_id': str(info.id),
        'date_time_posted': str(info.created_at),
        'tweet': info.text,
    }
    tweets_list.append(tweet_dict)

print(tweets_list)
df_tell_tweet = pd.DataFrame(tweets_list)
print(df_tell_tweet)
df_tell_tweet.to_csv('tweets.csv')

# # Possibility to implement a sample, not used
# # Sample retweeters and add to lookup list, write to csv
# sample_retweeters = random.sample(retweeters_list, 5)
# for sample_item in retweeters_list:
#     add_to_lookup(sample_item)
# print("Retweeters sampled")


# # Load CSV File
df = pd.read_csv('tell_tweet.csv')

# # Count Retweets by User_id
df["retweeters"] = df["retweeters"].apply(clean_alt_list)
df["retweeters"] = df["retweeters"].apply(eval)
retweeters_counted = to_1d(df["retweeters"]).value_counts()
retweeters_counted = retweeters_counted.to_frame().reset_index()
retweeters_counted = retweeters_counted.rename(columns={0: 'retweets'})
retweeters_counted = retweeters_counted.rename(columns={'index': 'user_id'})

# # Look up Retweeters Info and add to list
retweeters_info_list = []
for retweeter in retweeters_counted.user_id:
    print(retweeter)
    try:
        info = user_lookup(retweeter)
        user_info_dict = {
            'user_id': info.id_str,
            'screen_name': info.screen_name,
            'verified': info.verified,
            'followers_count': info.followers_count,
            'friends_count': info.friends_count,
            'tweets': info.statuses_count,
            'age_in_years': years_since_created_at(info),
            'tweets_per_day': tweets_per_day(info)
        }
    except tweepy.TweepyException:
        user_info_dict = {
            'user_id': retweeter,
            'screen_name': "private user",
            'verified': "private user",
            'followers_count': "private user",
            'friends_count': "private user",
            'tweets': "private user",
            'age_in_years': "private user",
            'tweets_per_day': "private user"
        }
    retweeters_info_list.append(user_info_dict)
df_retweeter_info = pd.DataFrame(retweeters_info_list)
print(df_retweeter_info)

# # Combine both dataframes and sage to csv
df_combined = pd.merge(retweeters_counted, df_retweeter_info, on='user_id')
print(df_combined)
df_combined.to_csv("retweets_afd.csv")

# # Scrape retweeters friends, add to dict_ids, write to retweetersfriendscount.csv
dict_ids = {'id': [], 'amount': []}
print("Start get_friends_ids")
retweeter_friends_object_list = []
retweeter_friends = []
retweeter_friends_objects = {'id': [], 'username': [], 'verified': [], 'followers_count': []}
for retweeter_id in retweeters_list:
    x = retweeter_id
    for rf in tweepy.Paginator(client.get_users_following, id=x, user_fields=['username','verified','public_metrics'], max_results=1000).flatten(limit=178000):
        # print(rf.data['id'])
        retweeter_friends.append(rf.data['id'])
        if rf.data['id'] not in retweeter_friends_objects['id']:
            try:
                retweeter_friends_objects['id'].append(rf.data['id'])
                retweeter_friends_objects['username'].append(rf.data['username'])
                retweeter_friends_objects['verified'].append(rf.data['verified'])
                retweeter_friends_objects['followers_count'].append(rf.public_metrics['followers_count'])
            except tweepy.TweepyException:
                retweeter_friends_objects['id'].append(rf.data['id'])
                retweeter_friends_objects['username'].append("private user")
                retweeter_friends_objects['verified'].append("private user")
                retweeter_friends_objects['followers_count'].append("private user")
    print("get one user: ", (x), "current length retweeter_friends: ", (len(retweeter_friends)))
df_friend_info = pd.DataFrame(retweeter_friends_objects)
amount_retweeter_friends = len(retweeter_friends)
print("amount_retweeter_friends =")
print(amount_retweeter_friends)
u = Counter(retweeter_friends)
for u, count in u.items():
    dict_ids['id'].append(u)
    dict_ids['amount'].append(count)
print("get_friends_ids complete")

# # change dictionary to DataFrame and print
df_dict_ids = pd.DataFrame.from_dict(dict_ids)
print("print(df_dict_ids)")
print(df_dict_ids)
df_dict_ids.to_csv('user_dict_info.csv', index=False, header=True)

# # choose how many counts at least as relevant
# relevant_friend = df_dict_ids.loc[df_dict_ids['amount'] > 0]  # change number if necessary
# print("relevant items:")
# print(relevant_friend)
# relevant_for_lookup_list = relevant_friend['id']
# for relevant_item in relevant_for_lookup_list:
#     add_to_lookup(relevant_item)
# print(look_up_list)

# # combining userinfo and relevant users
combined = pd.merge(df_dict_ids, df_friend_info, on='id')
print(combined)

# # save as csv
df_friend_info.to_csv('user_info.csv', index=False, header=True)
# relevant_friend.to_csv('data.csv', index=False, header=True)
combined.to_csv('datacombined.csv', index=False, header=True)

print("finished")

