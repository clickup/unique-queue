import delay from "delay";
import type { RedisOptions } from "ioredis";
import Redis from "ioredis";
import pDefer from "p-defer";
import waitForExpect from "wait-for-expect";
import Queue from "../Queue";

const redisOptions: RedisOptions = {
  host:
    process.env["REDISCLI_HOST"] ||
    process.env["REDIS_WORKER_HOST"] ||
    "127.0.0.1",
  port:
    parseInt(
      process.env["REDISCLI_PORT"] || process.env["REDIS_WORKER_PORT"] || "0"
    ) || undefined,
  password:
    process.env["REDISCLI_AUTH"] ||
    process.env["REDIS_WORKER_PASS"] ||
    undefined,
  keyPrefix: "test:",
};

let queue: Queue<
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
>;

beforeEach(async () => {
  queue = new Queue({
    redis: redisOptions.password?.startsWith("cluster")
      ? new Redis.Cluster(
          [{ host: redisOptions.host, port: redisOptions.port }],
          { redisOptions, slotsRefreshTimeout: 10000 }
        )
      : new Redis(redisOptions),
    namespace: "queue" + process.hrtime.bigint(),
  });
});

it("picks up messages with highest priority", async () => {
  expect(
    await queue.push({ type: "type1", key: "some10", some: "some10" }, 10)
  ).toEqual("added");
  expect(
    await queue.push({ type: "type1", key: "some20", some: "some20" }, 20)
  ).toEqual("added");
  expect(
    await queue.push({ type: "type2", key: "other", other: "other" }, 20)
  ).toEqual("added");
  queue.subscribe("type1", async () => {});
  queue.subscribe("type2", async () => {});
  const gen = queue.listen({ longPollSec: 2 });
  expect(await gen.next()).toMatchObject({ value: { key: "some20" } });
  expect(await gen.next()).toMatchObject({ value: { key: "some10" } });
  expect(await gen.next()).toMatchObject({ value: { key: "other" } });
}, 15000);

it("works when run() and subscribe() are used", async () => {
  const defer = pDefer();

  const messages1: unknown[] = [];
  const messages2: unknown[] = [];
  queue.subscribe("type1", async (message) => {
    await defer.promise;
    messages1.push(message);
  });
  queue.subscribe("type2", async (message) => {
    await defer.promise;
    messages2.push(message);
  });
  queue.run().catch(() => {});

  await queue.push({ type: "type1", key: "some", some: "some" }, 10);
  await queue.push({ type: "type2", key: "other", other: "other" }, 20);

  // Subscribe callbacks started, but are doing some work now.
  await waitForExpect(() => expect(queue.jobs().size).toEqual(2));

  defer.resolve();

  await waitForExpect(async () => {
    expect(messages1).toMatchObject([{ type: "type1", key: "some" }]);
    expect(messages2).toMatchObject([{ type: "type2", key: "other" }]);
  });
}, 15000);

it("resumes and aborts when abort() is called on a paused processing", async () => {
  await queue.push({ type: "type1", key: "some", some: "some" }, 10);
  queue.subscribe("type1", async () => {});
  const res = queue.run();
  res.pause();
  await delay(10);
  res.abort();
  await res;
}, 15000);
