export type StreamHandlers<T> = {
  onMessage: (event: T) => void;
  onError?: (error: Event) => void;
  onOpen?: () => void;
};

export class EventStreamClient<T> {
  private source: EventSource | null = null;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  private reconnectAttempt = 0;

  connect(endpoint: string, handlers: StreamHandlers<T>): void {
    this.disconnect();

    this.source = new EventSource(endpoint, { withCredentials: true });
    this.source.onopen = () => {
      this.reconnectAttempt = 0;
      handlers.onOpen?.();
    };

    this.source.onerror = (event) => {
      handlers.onError?.(event);
      this.scheduleReconnect(endpoint, handlers);
    };

    this.source.onmessage = (event) => {
      const parsed = JSON.parse(event.data) as T;
      handlers.onMessage(parsed);
    };
  }

  private scheduleReconnect(endpoint: string, handlers: StreamHandlers<T>): void {
    this.source?.close();
    this.source = null;

    const timeoutMs = Math.min(1000 * 2 ** this.reconnectAttempt, 15_000);
    this.reconnectAttempt += 1;

    this.reconnectTimer = setTimeout(() => {
      this.connect(endpoint, handlers);
    }, timeoutMs);
  }

  disconnect(): void {
    if (this.reconnectTimer) {
      clearTimeout(this.reconnectTimer);
      this.reconnectTimer = null;
    }

    this.source?.close();
    this.source = null;
    this.reconnectAttempt = 0;
  }
}
