# @time-loop/unique-queue: A simple queue which lowers the chances of scheduling messages with the same key twice

A simple queue which lowers the chances of scheduling messages with the same key
twice. It still does NOT guarantee that the message processing function will be
a singleton though: it may happen that it's started at 2 places for the same key
if this key is scheduled twice after it's been picked up. To guarantee a single
running instance, another library must be used from inside the subscription
callback.

There is also no support for retries of failed jobs recovery. This is because in
practice a recovery needs to be done at a higher level often times, otherwise
"dead letter queue" would accumulate forever.

## Example

```ts
// You can also use Redis Cluster here.
export const redis = new Redis({
  host: process.env.REDIS_WORKER_HOST,
  port: process.env.REDIS_WORKER_PORT,
  password: process.env.REDIS_WORKER_PASS,
  keyPrefix: "queue:",
});

const queue = new Queue<
  | {
      type: "type1";
      key: string;
      some: string;
    }
  | {
      type: "type2";
      key: string;
      other: string;
    }
>({
  redis,
  namespace: "my-queue",
});

// ...

await queue.push({ type: "type1", key: "some", some: "some" }, 10);

// ...

queue.subscribe("type1", (message) => console.log(message));

// ...

const promise = queue.run();
// ...
promise.abort();
```
