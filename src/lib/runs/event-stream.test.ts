import { describe, expect, it, vi, beforeEach, afterEach } from "vitest";
import type { RunEvent } from "./event-stream";
import { ReconnectingEventStream } from "./event-stream";

describe("ReconnectingEventStream", () => {
  let mockEventSource: {
    onmessage: ((event: MessageEvent) => void) | null;
    onopen: (() => void) | null;
    onerror: (() => void) | null;
    close: () => void;
  };
  let mockEventSourceConstructor: (url: string) => typeof mockEventSource;

  beforeEach(() => {
    mockEventSource = {
      onmessage: null,
      onopen: null,
      onerror: null,
      close: vi.fn(),
    };
    mockEventSourceConstructor = vi.fn(() => mockEventSource);
  });

  describe("event handling", () => {
    it("parses and sends valid events to the callback", () => {
      const onEvent = vi.fn();
      const stream = new ReconnectingEventStream(
        "http://example.com/events",
        onEvent,
        mockEventSourceConstructor,
      );

      stream.start();

      const event: RunEvent = {
        id: "evt_1",
        type: "queued",
        at: "2024-01-01T00:00:00Z",
        message: "Run queued",
      };

      mockEventSource.onmessage?.(
        new MessageEvent("message", { data: JSON.stringify(event) }),
      );

      expect(onEvent).toHaveBeenCalledWith(event);
    });

    it("ignores malformed events and keeps the stream alive", () => {
      const onEvent = vi.fn();
      const stream = new ReconnectingEventStream(
        "http://example.com/events",
        onEvent,
        mockEventSourceConstructor,
      );

      stream.start();

      mockEventSource.onmessage?.(
        new MessageEvent("message", { data: "not-json" }),
      );
      mockEventSource.onmessage?.(
        new MessageEvent("message", { data: "" }),
      );

      expect(onEvent).not.toHaveBeenCalled();
      expect(mockEventSource.close).not.toHaveBeenCalled();
    });

    it("handles multiple events in sequence", () => {
      const onEvent = vi.fn();
      const stream = new ReconnectingEventStream(
        "http://example.com/events",
        onEvent,
        mockEventSourceConstructor,
      );

      stream.start();

      const events: RunEvent[] = [
        {
          id: "evt_1",
          type: "queued",
          at: "2024-01-01T00:00:00Z",
          message: "Run queued",
        },
        {
          id: "evt_2",
          type: "started",
          at: "2024-01-01T00:00:01Z",
          message: "Run started",
        },
      ];

      events.forEach((event) => {
        mockEventSource.onmessage?.(
          new MessageEvent("message", { data: JSON.stringify(event) }),
        );
      });

      expect(onEvent).toHaveBeenCalledTimes(2);
      expect(onEvent).toHaveBeenNthCalledWith(1, events[0]);
      expect(onEvent).toHaveBeenNthCalledWith(2, events[1]);
    });
  });

  describe("reconnection", () => {
    beforeEach(() => {
      vi.useFakeTimers();
    });

    afterEach(() => {
      vi.useRealTimers();
    });

    it("closes and reconnects on connection error", () => {
      const onEvent = vi.fn();
      const stream = new ReconnectingEventStream(
        "http://example.com/events",
        onEvent,
        mockEventSourceConstructor,
      );

      stream.start();

      expect(mockEventSourceConstructor).toHaveBeenCalledTimes(1);

      mockEventSource.onerror?.();

      expect(mockEventSource.close).toHaveBeenCalled();

      vi.advanceTimersByTime(1000);

      expect(mockEventSourceConstructor).toHaveBeenCalledTimes(2);
    });

    it("resets attempt counter on successful connection", () => {
      const onEvent = vi.fn();
      const stream = new ReconnectingEventStream(
        "http://example.com/events",
        onEvent,
        mockEventSourceConstructor,
      );

      stream.start();
      mockEventSource.onopen?.();

      mockEventSource.onerror?.();
      vi.advanceTimersByTime(1000);

      mockEventSource.onopen?.();

      mockEventSource.onerror?.();
      vi.advanceTimersByTime(1000);

      expect(mockEventSourceConstructor).toHaveBeenCalledTimes(3);
    });

    it("exponentially backs off reconnection attempts", () => {
      const onEvent = vi.fn();
      const stream = new ReconnectingEventStream(
        "http://example.com/events",
        onEvent,
        mockEventSourceConstructor,
      );

      stream.start();

      const delays: number[] = [];
      let nextEventSource = mockEventSource;

      for (let i = 0; i < 5; i++) {
        nextEventSource.onerror?.();
        const startTime = vi.now();
        vi.runAllTimers();
        const delay = vi.now() - startTime;
        delays.push(delay);

        nextEventSource = mockEventSource;
      }

      expect(delays).toEqual([1000, 2000, 4000, 8000, 15000]);
    });

    it("caps reconnection delay at 15 seconds", () => {
      const onEvent = vi.fn();
      const stream = new ReconnectingEventStream(
        "http://example.com/events",
        onEvent,
        mockEventSourceConstructor,
      );

      stream.start();

      for (let i = 0; i < 10; i++) {
        mockEventSource.onerror?.();
        vi.runAllTimers();
      }

      expect(mockEventSourceConstructor).toHaveBeenCalled();
    });

    it("does not reconnect after stop() is called", () => {
      const onEvent = vi.fn();
      const stream = new ReconnectingEventStream(
        "http://example.com/events",
        onEvent,
        mockEventSourceConstructor,
      );

      stream.start();
      expect(mockEventSourceConstructor).toHaveBeenCalledTimes(1);

      stream.stop();
      mockEventSource.onerror?.();

      vi.advanceTimersByTime(2000);

      expect(mockEventSourceConstructor).toHaveBeenCalledTimes(1);
    });
  });

  describe("lifecycle", () => {
    it("creates EventSource with the provided URL", () => {
      const onEvent = vi.fn();
      const stream = new ReconnectingEventStream(
        "http://example.com/events",
        onEvent,
        mockEventSourceConstructor,
      );

      stream.start();

      expect(mockEventSourceConstructor).toHaveBeenCalledWith(
        "http://example.com/events",
      );
    });

    it("can be stopped", () => {
      const onEvent = vi.fn();
      const stream = new ReconnectingEventStream(
        "http://example.com/events",
        onEvent,
        mockEventSourceConstructor,
      );

      stream.start();
      stream.stop();

      expect(mockEventSource.close).toHaveBeenCalled();
    });
  });
});
