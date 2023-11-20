from upstash_redis import Redis

# An example chat history model for storing
# messages of different users with the chatbot

# A user can have multiple chats
# A chat can have multiple messages

# user:123:chats -> set of chat ids

# chat:123 -> list of messages

# chat:next_id -> 1 (incrementing id for chats)

class ChatModel:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    def get_user_chats(self, user_id: str) -> list[str]:
        # A set of chat ids for a user, stores which chats belong to the user

        return list(self.redis.smembers(f"user:{user_id}:chats"))

    def get_chat_messages(self, chat_id: str) -> list[str]:
        return list(self.redis.lrange(f"chat:{chat_id}", 0, -1))

    def add_message(self, chat_id: str, message: str):
        # Push the message to the end of the list

        self.redis.rpush(f"chat:{chat_id}", message)
    
    def create_chat(self, user_id: str):
        # A unique incrementing id for the chat
        # Since increment is atomic and returns the new value
        chat_id = str(self.redis.incr("chat:next_id"))

        # Add the chat to the user's chat list
        self.redis.sadd(f"user:{user_id}:chats", chat_id)

        return chat_id
    
    def delete_chat(self, chat_id: str, user_id: str):
        # Remove the chat from the user's chat list
        self.redis.srem(f"user:{user_id}:chats", str(chat_id))

        # Delete the chat
        self.redis.delete(f"chat:{chat_id}")
    
    def delete_user(self, user_id: str):
        # Delete all chats of the user
        for chat_id in self.get_user_chats(user_id):
            self.delete_chat(chat_id, user_id)
        
        # Delete the user's chat list
        self.redis.delete(f"user:{user_id}:chats")

chat = ChatModel(Redis.from_env())

chat.redis.flushall()

# You can acquire the userid from an authentication service
# or from the session cookie
userid = "myid"

chatid_1 = chat.create_chat(userid)

chat.add_message(chatid_1, "user:Hello")

chat.add_message(chatid_1, "bot:Hello")

chat.add_message(chatid_1, "user:How are you?")

chatid_2 = chat.create_chat(userid)

chat.add_message(chatid_2, "user:This is chat2")

chat_ids = chat.get_user_chats(userid)

# Print all the data
print(f"chatid_1: {chatid_1}")
print(f"chatid_2: {chatid_2}")
print("chat_ids:", chat_ids)

print("chat 1 messages:", chat.get_chat_messages(chatid_1))
print("chat 2 messages:", chat.get_chat_messages(chatid_2))

# Delete the first chat
chat.delete_user(userid)

print(f"chatids after deletion: {chat.get_user_chats(userid)}")

# Output
# chatid_1: 1
# chatid_2: 2
# chat_ids: ['2', '1']
# chat 1 messages: ['user:Hello', 'bot:Hello', 'user:How are you?']
# chat 2 messages: ['user:This is chat2']
# chatids after deletion: []
