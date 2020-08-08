import concurrent.futures
import code

def thread_function(name):
    print("starting", name)
    code.source(name)
    print("finishing", name)


format = "%(asctime)s: %(message)s"


with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
    executor.map(thread_function, ['AUDCHF','USDINR','AUDINR','EURUSD'])