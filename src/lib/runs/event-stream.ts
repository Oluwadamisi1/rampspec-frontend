export type RunEvent = {
  id: string;
  type: "queued" | "started" | "check" | "completed" | "failed";
  at: string;
  message: string;
};

/**
 * ReconnectingEventStream safely streams events from a Server-Sent Events endpoint.
 *
 * Handles:
 * - Automatic reconnection with exponential backoff (1s to 15s)
 * - Malformed and invalid events (ignored, stream continues)
 * - Graceful shutdown with stop()
 * - Connection success resets backoff counter
 */
export class ReconnectingEventStream {
  private stopped = false;
  private attempt = 0;
  private seenIds = new Set<string>();

  constructor(
    private readonly url: string,
    private readonly onEvent: (event: RunEvent) => void,
    private readonly connect: (url: string) => EventSource = (value) =>
      new EventSource(value),
  ) {}

  start() {
    this.open();
  }

  stop() {
    this.stopped = true;
  }

  private open() {
    if (this.stopped) return;

    const source = this.connect(this.url);

    source.onmessage = (message) => {
      try {
        const event = JSON.parse(message.data) as RunEvent;

        // Validate event structure
        if (!event.id || !event.type || !event.at || !event.message) {
          return;
        }

        // Skip duplicate events
        if (this.seenIds.has(event.id)) {
          return;
        }

        this.seenIds.add(event.id);
        this.onEvent(event);
      } catch {
        /* Ignore malformed events and keep the stream alive. */
      }
    };

    source.onopen = () => {
      this.attempt = 0;
    };

    source.onerror = () => {
      source.close();

      if (this.stopped) return;

      const delay = Math.min(1000 * 2 ** this.attempt++, 15000);
      window.setTimeout(() => this.open(), delay);
    };
  }
}

