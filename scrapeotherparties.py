import time
import tweepy
# # For tweepy documentation go to https://www.tweepy.org/
import pandas as pd
# # For pandas documentation go to https://pandas.pydata.org/docs/
from datetime import datetime, timezone

auth = tweepy.OAuthHandler("BzjYwEDLCdGI0D45tQiemLPp1", "CgZtcbadsI0wKJSxIIG5LSYKOBULD0o2U9J55dz9zuA6UlxZsW")
auth.set_access_token("419746241-CvoL86Pw2r047uMJGkwf0bE9Y27AeSFy0jcnhMmX",
                      "BFNvCbJc5A2j4ly8KrkrukaB9Mtqg6uBoGFXo21i4RlkH")

api = tweepy.API(auth, wait_on_rate_limit=True)

tweet_ids = []
retweeters_list = []
followers_retweeter_scraped = []
look_up_list = []


def scrape_tweets(user):
    tweets = api.user_timeline(screen_name=user, since_id='1430851165537902592', max_id='1442503074384207875',
                               count=200, trim_user=True, include_rts=False)
    for info in tweets:
        tweet_ids.append(info.id)
    print(tweet_ids)
    return tweet_ids


def get_retweeters(tweet_id):
    tell_tweet_dict['tweet_id'].append(tweet_id)
    tell_retweeters_list = []
    tell_amount_retweets = []
    for retweeter in tweepy.Cursor(api.get_retweeter_ids, id=tweet_id).items():
        tell_retweeters_list.append(retweeter)
        tell_amount_retweets = len(tell_retweeters_list)
        if retweeter not in retweeters_list:
            retweeters_list.append(retweeter)
    tell_tweet_dict['retweeters'].append(tell_retweeters_list)
    tell_tweet_dict['amount_retweets'].append(tell_amount_retweets)
    return retweeters_list


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


# # Define User with Username
user = "CSU"

# # scrape tweets
scrape_tweets(user)
print("Tweets scraped")
amount_tweets = len(tweet_ids)
print("amount tweets:")
print(amount_tweets)
tell_tweet_dict = {'tweet_id': [], 'amount_retweets': [], 'retweeters': []}

for tw in tweet_ids:
    get_retweeters(tw)

df_tell_tweet = pd.DataFrame(tell_tweet_dict)
df_tell_tweet.to_csv('csu_tell_tweet.csv', index=False, header=True)
# # check filename above

# # Load CSV File
df = pd.read_csv('csu_tell_tweet.csv')
# # check filename above

# # Count Retweets by User_id
df["retweeters"] = df["retweeters"].apply(clean_alt_list)
df["retweeters"] = df["retweeters"].apply(eval)
retweeters_counted = to_1d(df["retweeters"]).value_counts()
retweeters_counted = retweeters_counted.to_frame().reset_index()
retweeters_counted = retweeters_counted.rename(columns={0: 'retweets'})
retweeters_counted = retweeters_counted.rename(columns={'index': 'user_id'})

retweeters_list = []

# # Get info of retweeter
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
    retweeters_list.append(user_info_dict)

# print(user_info_dict)
df_user_info = pd.DataFrame(retweeters_list)
print(df_user_info)
# # Combine both dataframes and sage to csv
df_combined = pd.merge(retweeters_counted, df_user_info, on='user_id')
print(df_combined)
df_combined.to_csv("retweeter_csu.csv")
# # check filename above
