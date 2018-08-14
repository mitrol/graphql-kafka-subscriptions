import * as kafka from 'kafka-node';
import { PubSubEngine } from 'graphql-subscriptions';
import * as Logger from 'bunyan';
import { createChildLogger } from './child-logger';
import { PubSubAsyncIterator } from './pubsub-async-iterator';

export interface IKafkaOptions {
  topics: string[];
  host: string;
  logger?: Logger;
  groupId?: any;
}

const defaultLogger = Logger.createLogger({
  name: 'pubsub',
  stream: process.stdout,
  level: 'info'
});

export class KafkaPubSub implements PubSubEngine {
  private client: any;
  private producer: any;
  private subscriptionIndex: number;
  private options: any;
  private subscriptionMap: {
    [subId: number]: {
      topic: string;
      onMessageCb: (message: any) => any;
    };
  };
  private subscriptionsByTopic: { [topic: string]: Array<number> };
  private logger: Logger;

  constructor(options: IKafkaOptions) {
    this.client = new kafka.KafkaClient({ kafkaHost: options.host });
    this.options = options;
    this.subscriptionMap = {};
    this.subscriptionsByTopic = {};
    this.logger = createChildLogger(
      this.options.logger || defaultLogger,
      'KafkaPubSub'
    );
    this.subscriptionIndex = 0;

    this.createConsumer(options.host, this.options.topics);
    this.producer = this.createProducer(this.client);
  }

  public asyncIterator<T>(triggers: string | string[]): AsyncIterator<T> {
    return new PubSubAsyncIterator<T>(this, triggers);
  }

  public subscribe(
    topic: string,
    onMessageCb: (message: any) => any
  ): Promise<number> {
    const index = this.subscriptionIndex;
    this.subscriptionIndex++;

    this.subscriptionMap[index] = { topic, onMessageCb };

    this.subscriptionsByTopic[topic] = [
      ...(this.subscriptionsByTopic[topic] || []),
      index
    ];

    return Promise.resolve(index);
  }

  public unsubscribe(index: number) {
    const { topic } = this.subscriptionMap[index];
    this.subscriptionsByTopic[topic] = this.subscriptionsByTopic[topic].filter(
      current => current !== index
    );
  }

  public publish(topic: string, message: any) {
    const request = {
      topic,
      messages: JSON.stringify(message)
    };

    this.producer.send([request], (err /*data*/) => {
      if (err) {
        this.logger.error(err, 'Error while publishing message');
      }
    });

    return true;
  }

  private onMessage(topic: string, message) {
    const subscriptions = this.subscriptionsByTopic[topic];
    if (!subscriptions) {
      return;
    }

    for (const index of subscriptions) {
      const { onMessageCb } = this.subscriptionMap[index];
      onMessageCb(message);
    }
  }

  private createProducer(client) {
    const producer = new kafka.Producer(client);
    producer.on('error', err => {
      this.logger.error(err, 'Error in our kafka stream');
    });
    return producer;
  }

  private createConsumer = (host, topics: [string]) => {
    const groupId = this.options.groupId || Math.ceil(Math.random() * 9999);

    var options = {
      kafkaHost: host, // connect directly to kafka broker (instantiates a KafkaClient)
      groupId: String(groupId),
      // Offsets to use for new groups other options could be 'earliest' or 'none' (none will emit an error if no offsets were saved)
      // equivalent to Java client's auto.offset.reset
      fromOffset: 'latest', // default
      commitOffsetsOnFirstJoin: true, // on the very first time this consumer group subscribes to a topic, record the offset returned in fromOffset (latest/earliest)
      // how to recover from OutOfRangeOffset error (where save offset is past server retention) accepts same value as fromOffset
      outOfRangeOffset: 'earliest' // default
    };

    var consumer = new kafka.ConsumerGroup(options, topics);

    consumer.on('message', message => {
      let parsedMessage = JSON.parse(message.value.toString());
      this.onMessage(message.topic, parsedMessage);
    });

    consumer.on('error', err => {
      this.logger.error(err, 'Error in our kafka stream');
    });

    consumer.on('offsetOutOfRange', err => {
      this.logger.error(err, 'Error in our kafka stream');
    });
  };
}
