import type { MatrixClient } from "../client.ts";
import type { Statistics } from "./types.ts";
import { type Logger } from "../logger.ts";
import { KeyTransportEvents, type KeyTransportEventsHandlerMap, type IKeyTransport } from "./IKeyTransport.ts";
import { type MatrixEvent } from "../models/event.ts";
import { type CallMembership } from "./CallMembership.ts";
import { TypedEventEmitter } from "../models/typed-event-emitter.ts";
import { type Room } from "../models/room.ts";
export declare class RoomKeyTransport extends TypedEventEmitter<KeyTransportEvents, KeyTransportEventsHandlerMap> implements IKeyTransport {
    private room;
    private client;
    private statistics;
    private e2eeLogger;
    constructor(room: Pick<Room, "on" | "off" | "roomId">, client: Pick<MatrixClient, "sendEvent" | "getDeviceId" | "getUserId" | "cancelPendingEvent" | "decryptEventIfNeeded">, statistics: Statistics, parentLogger?: Logger);
    private get logContext();
    start(): void;
    stop(): void;
    private consumeCallEncryptionEvent;
    /** implements {@link IKeyTransport#sendKey} */
    sendKey(keyBase64Encoded: string, index: number, members: CallMembership[]): Promise<void>;
    onEncryptionEvent(event: MatrixEvent): void;
}
//# sourceMappingURL=RoomKeyTransport.d.ts.map