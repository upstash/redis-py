import datetime
import json
from uuid import uuid4

from upstash_redis import Redis

# A simple twitter clone in redis

# A user has a username and a password
# A token is generated for the user after login, it expires in 24 hours

# A user can have tweets
# A tweet has an author, timestamp and text
# A tweet can have people who liked it

# A user can follow other users
# A user can have followers

# tweet:id:data -> json array of [timestamp, user_id, text]
# tweet:id:likes -> set of user ids

# user:id:password -> password
# user:id:tweets -> list of tweet ids
# user:id:followers -> set of user ids
# user:id:following -> set of user ids


class UserError(Exception):
    pass


def timestamp():
    return datetime.datetime.now().isoformat()


class TwitterModel:
    def __init__(self, redis: Redis):
        self.redis = redis

    def create_user(self, username: str, password: str):
        # Check if the username is taken
        if self.redis.exists(f"user:{username}"):
            raise UserError("Username already taken")

        self.redis.set(f"user:{username}", timestamp())
        self.redis.set(f"user:{username}:password", password)

    def sign_in(self, username: str, password: str):
        correct_password = self.redis.get(f"user:{username}:password")

        # User does not exist
        if correct_password is None:
            raise UserError("User not found")

        if correct_password != password:
            raise UserError("Incorrect password")

        # Generate a random token
        token = str(uuid4())

        # Token expires in 24 hours
        self.redis.set(f"tokens:{token}", ex=24 * 60 * 60, value=username)

        return token

    def check_token(self, token: str):
        "Returns the username"

        user = self.redis.get(f"tokens:{token}")
        if user is None:
            raise UserError("Invalid token")

        return user

    def create_tweet(self, user: str, text: str):
        tweet_id = str(uuid4())

        # Add the tweet to the user's tweet list
        self.redis.rpush(f"user:{user}:tweets", tweet_id)

        # Tweet is an array of [timestamp, username, text]
        data = json.dumps([timestamp(), user, text])

        self.redis.set(f"tweet:{tweet_id}:data", data)

        return tweet_id

    def get_tweet(self, tweet_id: str):
        data = self.redis.get(f"tweet:{tweet_id}:data")

        if data is None:
            raise UserError("Tweet not found")

        [stamp, username, text] = json.loads(data)

        return (stamp, username, text)

    def get_user_tweet_ids(self, username: str):
        return self.redis.lrange(f"user:{username}:tweets", 0, -1)

    def like_tweet(self, username: str, tweet_id: str):
        # Add the username to the tweet's like list
        self.redis.sadd(f"tweet:{tweet_id}:likes", username)

    def unlike_tweet(self, username: str, tweet_id: str):
        # Remove the username from the tweet's like list
        self.redis.srem(f"tweet:{tweet_id}:likes", username)

    def get_tweet_likes(self, tweet_id: str):
        return self.redis.smembers(f"tweet:{tweet_id}:likes")

    def follow_user(self, user: str, user_to_follow: str):
        # Add the user to the following list
        self.redis.sadd(f"user:{user}:following", user_to_follow)

        # Add the user to the followed list
        self.redis.sadd(f"user:{user_to_follow}:followers", user)

    def unfollow_user(self, user: str, user_to_unfollow: str):
        # Remove the user from the following list
        self.redis.srem(f"user:{user}:following", user_to_unfollow)

        # Remove the user from the followed list
        self.redis.srem(f"user:{user_to_unfollow}:followers", user)

    def get_followers(self, user: str):
        return list(self.redis.smembers(f"user:{user}:followers"))

    def get_followed(self, user: str):
        return list(self.redis.smembers(f"user:{user}:following"))

    def get_is_following(self, user: str, user_to_check: str):
        return self.redis.sismember(f"user:{user}:following", user_to_check)

    def get_is_followed(self, user: str, user_to_check: str):
        return self.redis.sismember(f"user:{user}:followers", user_to_check)


def test_model():
    # Initialize the TwitterModel with a Redis instance
    redis = Redis.from_env()

    # You might want to flush the database before running tests
    # redis.flushall()

    tw = TwitterModel(redis)

    # Create users
    tw.create_user("user1", "password1")
    tw.create_user("user2", "password2")

    # Sign in and get tokens
    token_user1 = tw.sign_in("user1", "password1")
    token_user2 = tw.sign_in("user2", "password2")

    # Check tokens
    assert tw.check_token(token_user1) == "user1"
    assert tw.check_token(token_user2) == "user2"

    # Create tweets
    tweet_id_user1 = tw.create_tweet(token_user1, "Hello from user1!")
    tweet_id_user2 = tw.create_tweet(token_user2, "Greetings from user2!")

    # Get tweet details
    tweet_user1 = tw.get_tweet(tweet_id_user1)
    tweet_user2 = tw.get_tweet(tweet_id_user2)

    assert tweet_user1[2] == "Hello from user1!"
    assert tweet_user2[2] == "Greetings from user2!"

    # Get user tweet IDs
    user1_tweets = tw.get_user_tweet_ids(token_user1)
    user2_tweets = tw.get_user_tweet_ids(token_user2)

    assert tweet_id_user1 in user1_tweets
    assert tweet_id_user2 in user2_tweets

    # Like and unlike tweets
    tw.like_tweet(token_user1, tweet_id_user2)
    likes_user2 = tw.get_tweet_likes(tweet_id_user2)
    assert token_user1 in likes_user2

    tw.unlike_tweet(token_user1, tweet_id_user2)
    likes_user2_after_unlike = tw.get_tweet_likes(tweet_id_user2)
    assert token_user1 not in likes_user2_after_unlike

    # Follow and unfollow users
    tw.follow_user(token_user1, token_user2)
    assert tw.get_followers(token_user2) == [token_user1]

    assert tw.get_is_following(token_user1, token_user2) is True
    assert tw.get_is_followed(token_user2, token_user1) is True

    tw.unfollow_user(token_user1, token_user2)
    assert tw.get_followers(token_user2) == []

    print("All tests passed!")


if __name__ == "__main__":
    test_model()
