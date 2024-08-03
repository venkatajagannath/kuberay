import ray
import time

@ray.remote
def hello_world():
    return "hello world"

def main():
    ray.init()
    start_time = time.time()
    duration = 15 * 60  # 15 minutes in seconds

    while time.time() - start_time < duration:
        result = ray.get(hello_world.remote())
        print(f"Result: {result}")
        print(f"Time elapsed: {time.time() - start_time:.2f} seconds")
        time.sleep(1)  # Wait for 1 second before the next iteration

    ray.shutdown()

if __name__ == "__main__":
    main()