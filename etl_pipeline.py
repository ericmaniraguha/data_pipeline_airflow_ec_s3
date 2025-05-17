import tweepy
import pandas as pd
import json
from datetime import datetime
import s3fs
import os
from dotenv import load_dotenv

# Load .env file variables
load_dotenv()

def run_twitter_etl():
    print("Starting Twitter ETL process...")
    
    # Get API credentials
    consumer_key = os.getenv("TWITTER_CONSUMER_KEY")
    # consumer_secret = os.getenv("TWITTER_CONSUMER_SECRET")
    # access_token = os.getenv("TWITTER_ACCESS_TOKEN")
    # access_token_secret = os.getenv("TWITTER_ACCESS_TOKEN_SECRET")
    bearer_token = os.getenv("TWITTER_BEARER_TOKEN")
    
    # Debug output for credentials (don't include secrets in production)
    print(f"Consumer key: {consumer_key[:4] if consumer_key else 'None'}...")
    print(f"Bearer token: {bearer_token[:4] if bearer_token else 'None'}...")
    
    if not bearer_token:
        print("ERROR: Missing Twitter API Bearer Token in .env file")
        return
    
    try:
        # Twitter v2 API authentication (using Bearer Token)
        print("Authenticating with Twitter API v2...")
        client = tweepy.Client(bearer_token=bearer_token)
        
        # Get user ID for @paulkagame
        print("Looking up user ID for @paulkagame...")
        user_response = client.get_user(username="paulkagame")
        
        if not user_response.data:
            print("User not found!")
            return
        
        user_id = user_response.data.id
        print(f"Found user ID: {user_id}")
        
        # Get tweets for the user
        print(f"Retrieving tweets for user ID: {user_id}...")
        tweets_response = client.get_users_tweets(
            id=user_id,
            max_results=100,
            tweet_fields=['created_at', 'public_metrics'],
            exclude=['retweets', 'replies']
        )
        
        if not tweets_response.data:
            print("No tweets found!")
            return
        
        tweets = tweets_response.data
        print(f"Retrieved {len(tweets)} tweets")
        
        # Show the first tweet's content
        print(f"First tweet: {tweets[0].text[:50]}...")
        
        # Process the tweets and create a dataframe
        tweet_list = []
        for tweet in tweets:
            tweet_data = {
                'id': tweet.id,
                'text': tweet.text,
                'created_at': tweet.created_at,
                'like_count': tweet.public_metrics['like_count'],
                'retweet_count': tweet.public_metrics['retweet_count'],
                'reply_count': tweet.public_metrics['reply_count'],
                'quote_count': tweet.public_metrics['quote_count']
            }
            tweet_list.append(tweet_data)
        
        # Create dataframe
        df = pd.DataFrame(tweet_list)
        print(f"Created dataframe with shape: {df.shape}")
        
        # Save as CSV
        csv_path = 'kagame_tweets.csv'
        df.to_csv(csv_path, index=False)
        print(f"Saved tweets to {csv_path}")
        
        return df
    
    except Exception as e:
        print(f"ERROR: {str(e)}")
        raise

# Execute the function when script is run directly
if __name__ == "__main__":
    print("Script started")
    result = run_twitter_etl()
    print("Script completed")