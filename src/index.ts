export interface OsEvent<TData extends Record<string, unknown> = Record<string, unknown>> {
  id: string;
  tenantId: string;
  event: string;
  source: string;
  actorId?: string;
  role?: string;
  correlationId?: string;
  data: TData;
  createdAt: string;
  updatedAt: string;
}

export interface EventActor {
  tenantId: string;
  userId?: string;
  role?: string;
}

export type EventHandler<TData extends Record<string, unknown> = Record<string, unknown>> = (event: OsEvent<TData>) => void | Promise<void>;

export interface PublishEventInput<TData extends Record<string, unknown> = Record<string, unknown>> {
  event: string;
  source: string;
  tenantId: string;
  data: TData;
  actorId?: string;
  role?: string;
  correlationId?: string;
}

export class EventBus {
  private readonly handlers = new Map<string, EventHandler[]>();
  private readonly log: OsEvent[] = [];

  subscribe(eventName: string, handler: EventHandler): () => void {
    const handlers = this.handlers.get(eventName) ?? [];
    handlers.push(handler);
    this.handlers.set(eventName, handlers);
    return () => this.unsubscribe(eventName, handler);
  }

  unsubscribe(eventName: string, handler: EventHandler): void {
    const handlers = this.handlers.get(eventName) ?? [];
    this.handlers.set(eventName, handlers.filter((candidate) => candidate !== handler));
  }

  async publish<TData extends Record<string, unknown>>(input: PublishEventInput<TData>): Promise<OsEvent<TData>> {
    const envelope = publishOsEvent(input);
    this.log.unshift(envelope);
    const handlers = [...(this.handlers.get(input.event) ?? []), ...(this.handlers.get("*") ?? [])];
    for (const handler of handlers) await handler(envelope);
    return envelope;
  }

  events(): OsEvent[] {
    return this.log.map((event) => ({ ...event, data: { ...event.data } }));
  }
}

export function publishOsEvent<TData extends Record<string, unknown>>(input: PublishEventInput<TData>): OsEvent<TData> {
  const now = new Date().toISOString();
  return {
    id: createEventId(),
    tenantId: input.tenantId,
    event: input.event,
    source: input.source,
    actorId: input.actorId,
    role: input.role,
    correlationId: input.correlationId,
    data: input.data,
    createdAt: now,
    updatedAt: now
  };
}

function createEventId(): string {
  return `evt_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}
