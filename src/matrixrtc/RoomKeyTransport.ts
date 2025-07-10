/*
Copyright 2025 The Matrix.org Foundation C.I.C.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

import type { MatrixClient } from "../client.ts";
import type { EncryptionKeysEventContent, Statistics } from "./types.ts";
import { EventType } from "../@types/event.ts";
import { type MatrixError } from "../http-api/errors.ts";
import { logger as rootLogger, type Logger } from "../logger.ts";
import { KeyTransportEvents, type KeyTransportEventsHandlerMap, type IKeyTransport } from "./IKeyTransport.ts";
import { type MatrixEvent } from "../models/event.ts";
import { type CallMembership } from "./CallMembership.ts";
import { TypedEventEmitter } from "../models/typed-event-emitter.ts";
import { type Room, RoomEvent } from "../models/room.ts";
import { logSessionId } from "./MatrixRTCSession.ts";

export class RoomKeyTransport
    extends TypedEventEmitter<KeyTransportEvents, KeyTransportEventsHandlerMap>
    implements IKeyTransport
{
    private e2eeLogger: Logger;

    public constructor(
        private room: Pick<Room, "on" | "off" | "roomId">,
        private client: Pick<
            MatrixClient,
            "sendEvent" | "getDeviceId" | "getUserId" | "cancelPendingEvent" | "decryptEventIfNeeded"
        >,
        private statistics: Statistics,
        parentLogger?: Logger,
    ) {
        super();
        this.e2eeLogger = (parentLogger ?? rootLogger).getChild(`[E2EE_FLOW_MX][ROOM_KEY_TRANSPORT]`);
    }

    private get logContext(): { logSessionId: string | null; matrixUserId: string | null } {
        return {
            logSessionId,
            matrixUserId: this.client.getUserId(),
        };
    }

    public start(): void {
        this.room.on(RoomEvent.Timeline, (ev) => void this.consumeCallEncryptionEvent(ev));
    }
    public stop(): void {
        this.room.off(RoomEvent.Timeline, (ev) => void this.consumeCallEncryptionEvent(ev));
    }

    private async consumeCallEncryptionEvent(event: MatrixEvent, isRetry = false): Promise<void> {
        await this.client.decryptEventIfNeeded(event);

        if (event.isDecryptionFailure()) {
            if (!isRetry) {
                this.e2eeLogger.warn(
                    `Decryption failed for ${event.getType()} event: ${event.decryptionFailureReason} will retry once only`,
                    { ...this.logContext, sender: event.sender?.userId, eventId: event.getId() },
                );
                // retry after 1 second. After this we give up.
                setTimeout(() => void this.consumeCallEncryptionEvent(event, true), 1000);
            } else {
                this.e2eeLogger.error(
                    `Decryption failed for ${event.getType()} event: ${event.decryptionFailureReason}`,
                    {
                        ...this.logContext,
                        sender: event.sender?.userId,
                        eventId: event.getId(),
                    },
                );
            }
            return;
        } else if (isRetry) {
            this.e2eeLogger.info(`Decryption succeeded for ${event.getType()} event ${event.getId()} after retry`, {
                ...this.logContext,
                sender: event.sender?.userId,
                eventId: event.getId(),
            });
        }

        if (event.getType() !== EventType.CallEncryptionKeysPrefix) return Promise.resolve();

        this.e2eeLogger.info("Received io.element.call.encryption_keys event", {
            ...this.logContext,
            sender: event.sender?.userId,
            eventId: event.getId(),
        });

        if (!this.room) {
            this.e2eeLogger.error(`Got room state event for unknown room ${event.getRoomId()}!`, {
                ...this.logContext,
                sender: event.sender?.userId,
                eventId: event.getId(),
            });

            return Promise.resolve();
        }

        this.onEncryptionEvent(event);
    }

    /** implements {@link IKeyTransport#sendKey} */
    public async sendKey(keyBase64Encoded: string, index: number, members: CallMembership[]): Promise<void> {
        // members not used in room transports as the keys are sent to all room members

        this.e2eeLogger.info("Sending encryption key", { ...this.logContext, index });

        const content: EncryptionKeysEventContent = {
            keys: [
                {
                    index: index,
                    key: keyBase64Encoded,
                },
            ],
            device_id: this.client.getDeviceId()!,
            call_id: "",
            sent_ts: Date.now(),
        };

        try {
            await this.client.sendEvent(this.room.roomId, EventType.CallEncryptionKeysPrefix, content);
        } catch (error) {
            this.e2eeLogger.error("Failed to send call encryption key", { ...this.logContext, index, error });
            const matrixError = error as MatrixError;
            if (matrixError.event) {
                // cancel the pending event: we'll just generate a new one with our latest
                // keys when we resend
                this.client.cancelPendingEvent(matrixError.event);
            }
            throw error;
        }
    }

    public onEncryptionEvent(event: MatrixEvent): void {
        const userId = event.getSender();
        const content = event.getContent<EncryptionKeysEventContent>();

        const deviceId = content["device_id"];
        const callId = content["call_id"];
        const logData = { ...this.logContext, eventId: event.getId(), sender: event.sender?.userId };

        if (!userId) {
            this.e2eeLogger.warn(`Received m.call.encryption_keys with no userId: callId=${callId}`, logData);
            return;
        }

        // We currently only handle callId = "" (which is the default for room scoped calls)
        if (callId !== "") {
            this.e2eeLogger.warn(
                `Received m.call.encryption_keys with unsupported callId: userId=${userId}, deviceId=${deviceId}, callId=${callId}`,
                logData,
            );
            return;
        }

        if (!Array.isArray(content.keys)) {
            this.e2eeLogger.warn(
                `Received m.call.encryption_keys where keys wasn't an array: callId=${callId}`,
                logData,
            );
            return;
        }

        if (userId === this.client.getUserId() && deviceId === this.client.getDeviceId()) {
            // We store our own sender key in the same set along with keys from others, so it's
            // important we don't allow our own keys to be set by one of these events (apart from
            // the fact that we don't need it anyway because we already know our own keys).
            this.e2eeLogger.info("Ignoring our own keys event", logData);
            return;
        }

        this.statistics.counters.roomEventEncryptionKeysReceived += 1;
        const age = Date.now() - (typeof content.sent_ts === "number" ? content.sent_ts : event.getTs());
        this.statistics.totals.roomEventEncryptionKeysReceivedTotalAge += age;

        for (const key of content.keys) {
            if (!key) {
                this.e2eeLogger.info("Ignoring false-y key in keys event", logData);
                continue;
            }

            const encryptionKey = key.key;
            const encryptionKeyIndex = key.index;

            if (
                !encryptionKey ||
                encryptionKeyIndex === undefined ||
                encryptionKeyIndex === null ||
                callId === undefined ||
                callId === null ||
                typeof deviceId !== "string" ||
                typeof callId !== "string" ||
                typeof encryptionKey !== "string" ||
                typeof encryptionKeyIndex !== "number"
            ) {
                this.e2eeLogger.warn(
                    `Malformed call encryption_key: userId=${userId}, deviceId=${deviceId}, encryptionKeyIndex=${encryptionKeyIndex} callId=${callId}`,
                    logData,
                );
            } else {
                this.e2eeLogger.info(
                    `onCallEncryption userId=${userId}:${deviceId} encryptionKeyIndex=${encryptionKeyIndex} age=${age}ms`,
                    logData,
                );
                this.emit(
                    KeyTransportEvents.ReceivedKeys,
                    userId,
                    deviceId,
                    encryptionKey,
                    encryptionKeyIndex,
                    event.getTs(),
                );
            }
        }
    }
}
