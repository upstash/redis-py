from upstash_redis import Redis

# A simple task queue for scanning websites

# Multiple workers can run the same code and
# pop tasks from the queue

# Scan the website and store the results in list

# scan:waiting -> list of tasks with 

# scan:running -> a set of running tasks,
# since a task must be popped from the waiting list by its id,
# we use a set

# scan:completed:123 -> a list of completed tasks for the client

redis = Redis.from_env()

class TaskQueue:
    def __init__(self, redis: Redis) -> None:
        self.redis = redis

    def add_scan_task(self, clientid: str, website: str):
        # Add the task to the waiting list
        self.redis.rpush("scan:waiting", f"{clientid}:{website}")

    def consume_completed_tasks(self, clientid: str) -> list[str]:
        # Consume all completed tasks for the client
        # and return them

        # Redis doesn't have a popall command but you can atomically
        # pop all elements from a list by specifying a large count

        # Ignore type since if count is specified, this method returns a list
        return self.redis.rpop(f"scan:completed:{clientid}", 99999) # type: ignore

    def start_scan(self):
        # Pop task from the waiting list and add it to the running list

        task: str | None = self.redis.rpop("scan:waiting") # type: ignore

        if task is None:
            # No task to run
            return None

        self.redis.sadd("scan:running", task)

        [client, website] = task.split(":", 1)

        return (client, website)

    def complete_scan(self, clientid: str, website: str, result: str):
        # Add the result to the client's completed list
        self.redis.rpush(f"scan:completed:{clientid}", result)

        # Remove the task from the running list
        self.redis.srem("scan:running", f"{clientid}:{website}")

queue = TaskQueue(redis)

queue.add_scan_task("client1", "https://example.com")

queue.add_scan_task("client1", "https://google.com")

# This code will be run by multiple worker
def work():
    task = queue.start_scan()

    if task is None:
        return
    
    [client, website] = task

    # scan the website and store the result in a list
    queue.complete_scan(client, website, f"results for {website}")

work()
work()

completed = queue.consume_completed_tasks("client1")

print("completed: ", completed)



