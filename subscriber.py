import os
from google.cloud import pubsub_v1
import math
import collections

# Write a heavy hitter app for incoming streaming data

class Subscriber:
    def __init__(self, project_id, topic, subscription):
        self.subscription = f'projects/{project_id}/subscriptions/{subscription}'
        self.topic_name = f'projects/{project_id}/topics/{topic}'
        self.limit = math.ceil(1/0.5) - 1
        self.count = collections.Counter()

    def callback(self, message):
        result = message.data.decode('utf-8')
        print(result)
        address = result['event']['activity'][0]['contractAdress']
        self.update(address)
        message.ack()

    def update(self, nft):
        if nft in self.count:
            self.count[nft] += 1
        elif len(self.count) < self.limit:
            self.count[nft] = 1
        else:
            for current in self.count:
                self.count[current] -= 1
            self.count += collections.Counter()
    
    def get_most_common(self):
        return self.count.most_common()
    
    def main(self):
        with pubsub_v1.SubscriberClient() as subscriber:
            future = subscriber.subscribe(self.subscription, self.callback)
            try:
                future.result()
            except KeyboardInterrupt:
                future.cancel()

if __name__ == '__main__':
    project_id='charming-well-384209'
    sub='nftstreaming-sub'
    topic='nftstreaming'
    subscriber = Subscriber(project_id, topic, sub)
    subscriber.main()