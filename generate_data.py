import random
import time
import os

DOMAINS = [
    "reuters.com", "apnews.com", "bbc.co.uk", "cnn.com", 
    "aljazeera.com", "bloomberg.com", "ft.com", "nytimes.com", 
    "theguardian.com", "wsj.com", "npr.org", "cnbc.com"
]

filename = "news_logs.psv"

if not os.path.exists(filename):
    open(filename, 'w').close()

print(f"Streaming live log data to {filename}... (Press Ctrl+C to stop)")

article_counter = 100000

with open(filename, 'a') as f:
    try:
        while True:
            chunk = []
            for _ in range(5000):
                timestamp = random.randint(1670000000, 1700000000)
                domain = random.choice(DOMAINS)
                byte_size = random.randint(1000, 15000)
                
                line = f"{timestamp}|ART-{article_counter}|{domain}|{byte_size}\n"
                chunk.append(line)
                article_counter += 1
                
            text_chunk = "".join(chunk)
            f.write(text_chunk)
            
            f.flush() 
            
            time.sleep(0.5) 
            
    except KeyboardInterrupt:
        print("\nStopped streaming.")
