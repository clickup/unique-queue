import crypto from "crypto";
import delay from "delay";
import type { Cluster } from "ioredis";
import type Redis from "ioredis";
import type { DeferredPromise } from "p-defer";
import pDefer from "p-defer";

const DEFAULT_LONG_POLL_SEC = 10;
const DEFAULT_RETRY_ON_ERROR_MS = 1000;

export interface QueueMessage<TType extends string = string> {
  type: TType;
  key?: string | undefined;
}

export interface QueueOptions {
  /** A Redis instance to operate on. Notice that internally, it will be cloned
   * (duplicated) to support blpop() operation. */
  redis: Redis | Cluster;
  /** Logical namespace within the passed Redis instance: common prefix of all
   * queue related keys. */
  namespace: string;
  /** If passed, it's called when we don't want to throw an error through and/or
   * crash Node process. */
  swallowedErrorLogger?: (error: unknown) => void;
}

export interface QueueRunOptions {
  longPollSec?: number;
  /** The contract of run() is that it will never exit on an error; instead, its
   * internals will be retried (e.g. in case of a connection blip). */
  retryOnErrorMs?: number;
}

export interface QueueJob<TMessage extends QueueMessage> {
  type: string;
  callback: (message: TMessage) => Promise<unknown>;
  promise: Promise<unknown>;
}

export default class Queue<TMessage extends QueueMessage> {
  private _redis;
  private _namespace: string;
  private _swallowedErrorLogger: null | ((error: unknown) => void) = null;
  /** By-message-type subscribers. */
  private _callbacks = new Map<
    string, // type
    Set<(message: TMessage) => Promise<unknown>>
  >();
  /** Running jobs (callbacks which started and did not finish yet). */
  private _jobs = new Set<QueueJob<TMessage>>();

  constructor(public readonly options: QueueOptions) {
    this._redis = options.redis as QueueOptions["redis"] & {
      addIfNotExists: (
        hashKey: string,
        zsetKey: string,
        listKey: string,
        score: number,
        type: string,
        key: string,
        message: string
      ) => Promise<"added" | "duplicate">;
    };
    this._redis.defineCommand("addIfNotExists", {
      numberOfKeys: 3,
      lua: `
        local hashKey, zsetKey, listKey, score, type, key, message = KEYS[1], KEYS[2], KEYS[3], tonumber(ARGV[1]), ARGV[2], ARGV[3], ARGV[4]
        local numAdded = redis.call("ZADD", zsetKey, "NX", score, key)
        redis.call("LPUSH", listKey, type)
        if numAdded == 0 then
          return "duplicate"
        end
        redis.call("HSET", hashKey, key, message)
        return "added"
      `,
    });
    this._namespace = options.namespace;
    this._swallowedErrorLogger = options.swallowedErrorLogger ?? null;
  }

  /**
   * Pushes a message with priority to the queue. The larger is the value in
   * priority, the sooner the message will be picked up in case of congestion
   * (we use ZPOPMAX to extract the next messages).
   */
  async push(
    message: TMessage,
    priority?: number
  ): Promise<"added" | "duplicate"> {
    return this._redis.addIfNotExists(
      this._hashKey(message.type),
      this._zsetKey(message.type),
      this._listKey(message.type),
      priority ?? 0,
      message.type,
      message.key
        ? `${message.type}:${message.key}`
        : this.messageHash(message),
      JSON.stringify(message)
    );
  }

  /**
   * Adds a callback which will be called when a message of the particular type
   * is received.
   */
  subscribe<TType extends TMessage["type"]>(
    type: TType,
    callback: (message: Extract<TMessage, { type: TType }>) => Promise<unknown>
  ): () => void {
    let set = this._callbacks.get(type);
    if (!set) {
      set = new Set();
      this._callbacks.set(type, set);
    }

    set.add(callback as any);
    return () => this._callbacks.get(type)?.delete(callback as any);
  }

  /**
   * Runs the processing loop in background. Returns a Promise with several
   * additional methods, e.g. abort() to abort the execution. The promise never
   * rejects (in case of connection problems, it retries internally).
   */
  run(options?: QueueRunOptions): Promise<void> & {
    abort: () => void;
    pause: () => void;
    resume: () => void;
  } {
    let aborted = false;
    let paused: DeferredPromise<void> | undefined;
    const promise = (async () => {
      while (true) {
        try {
          for await (const _dummy of this.listen(options)) {
            if (paused) {
              await paused.promise;
            }

            if (aborted) {
              return;
            }
          }
        } catch (error: unknown) {
          // Here we catch errors from Redis, NOT from the callbacks (the
          // callbacks are always run in background).
          //
          // IORedis internally retries errors and reconnects/resubscribes, but
          // it gives up by default after some number of attempts and throws. We
          // could've turned this behavior off and let it retry forever, but it
          // damages the visibility (plus, we can't control it here: an already
          // configured Redis instance is injected by the caller), so better
          // retry errors at the application level too.
          if (this._redis.listenerCount("error") > 0) {
            this._redis.emit("error", error);
          }

          await delay(options?.retryOnErrorMs ?? DEFAULT_RETRY_ON_ERROR_MS);
        }
      }
    })();
    return Object.assign(promise, {
      abort: () => {
        paused?.resolve();
        paused = undefined;
        aborted = true;
      },
      pause: () => {
        paused ??= pDefer<void>();
      },
      resume: () => {
        paused?.resolve();
        paused = undefined;
      },
    });
  }

  /**
   * Returns all the jobs (running callbacks) currently running in the current
   * Node process.
   */
  jobs(): ReadonlySet<QueueJob<TMessage>> {
    return this._jobs;
  }

  /**
   * Listens to the messages in the queue and calls the provided callbacks.
   * - Any exceptions thrown from the callbacks are ignored, so they must take
   *   care about them internally.
   * - Any connection related errors will be thrown through.
   */
  async *listen({
    longPollSec = DEFAULT_LONG_POLL_SEC,
  }: QueueRunOptions = {}): AsyncGenerator<
    TMessage | "timeout" | "taken-over"
  > {
    // We can't use this._redis and need a new connection, because BLPOP is a
    // connection-blocking command.
    const redis = this._redis.duplicate() as typeof this._redis & {
      popIfExists: (hashKey: string, zsetKey: string) => Promise<string | null>;
    };

    // Forward errors from the cloned Redis to the original.
    redis.on("error", (...args) => this._redis.emit("error", ...args));

    try {
      redis.defineCommand("popIfExists", {
        numberOfKeys: 2,
        lua: `
          local hashKey, zsetKey = KEYS[1], KEYS[2]
          local key = unpack(redis.call("ZPOPMAX", zsetKey))
          if key == nil or not key then
            return nil
          end
          local message = redis.call("HGET", hashKey, key)
          redis.call("HDEL", hashKey, key)
          return message
        `,
      });

      while (true) {
        if (this._callbacks.size === 0) {
          await delay(longPollSec * 1000);
          yield "timeout";
          continue;
        }

        // Unfreeze when there is a good chance that there are messages
        // available (we can't decide here, WHICH message is available, since we
        // must rely on its priority in the ordered set below).
        const res = await redis.blpop(
          ...[...this._callbacks.keys()].map((type) => this._listKey(type)),
          longPollSec
        );
        if (!res) {
          yield "timeout";
          continue;
        }

        const type = res[1];
        const value = await redis.popIfExists(
          this._hashKey(type),
          this._zsetKey(type)
        );
        if (!value) {
          // Some other worker unfroze and picked up all the other messages.
          yield "taken-over";
          continue;
        }

        // Spawn all callbacks for this message in the background.
        const message: TMessage = JSON.parse(value);
        for (const callback of this._callbacks.get(message.type) ?? []) {
          const job: QueueJob<TMessage> = {
            type: message.type,
            callback,
            promise: callback(message)
              .catch((error) => this._swallowedErrorLogger?.(error))
              .finally(() => this._jobs.delete(job)),
          };
          this._jobs.add(job);
        }

        yield message;
      }
    } finally {
      redis.disconnect();
    }
  }

  /**
   * Returns an unique hash code for a message (debug friendly).
   */
  messageHash(message: object): string {
    return (
      Object.values(message)
        .filter(
          (v) =>
            v === null ||
            typeof v === "number" ||
            typeof v === "boolean" ||
            (typeof v === "string" && v.length < 32)
        )
        .map((v) => v)
        .join(":") +
      ":" +
      crypto.createHash("md5").update(JSON.stringify(message)).digest("hex")
    );
  }

  /**
   * Returns a store key for the actual message data.
   * - { [message1.key]: JSON(message1), [message2.key]: JSON(message2), ... }
   */
  private _hashKey(type: string): string {
    return `{${this._namespace}}:${type}`;
  }

  /**
   * Returns a ZSet key to track first-level uniqueness and to pick the message
   * with highest priority (in sync with the hash above).
   * - [ message1.key(score1), message2.key(score2), ... ]
   */
  private _zsetKey(type: string): string {
    return `{${this._namespace}}:${type}:zset`;
  }

  /**
   * Returns a signal list to make BLPOP unfreeze in one of the workers for the
   * particular message types.
   * - [ message1.type, message2.type, ... ]
   */
  private _listKey(type: string): string {
    return `{${this._namespace}}:${type}:list`;
  }
}
