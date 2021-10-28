import time
import requests

if __name__ == "__main__":
    start = time.time()
    response = requests.post("http://127.0.0.1:8000/compute", json={"x": 2, "y": 1})
    print("compute consume time: %.2fs, get result: %s" % (time.time() - start, response.json()))
