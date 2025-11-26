import _asyncToGenerator from "@babel/runtime/helpers/asyncToGenerator";
import _defineProperty from "@babel/runtime/helpers/defineProperty";
function ownKeys(e, r) { var t = Object.keys(e); if (Object.getOwnPropertySymbols) { var o = Object.getOwnPropertySymbols(e); r && (o = o.filter(function (r) { return Object.getOwnPropertyDescriptor(e, r).enumerable; })), t.push.apply(t, o); } return t; }
function _objectSpread(e) { for (var r = 1; r < arguments.length; r++) { var t = null != arguments[r] ? arguments[r] : {}; r % 2 ? ownKeys(Object(t), !0).forEach(function (r) { _defineProperty(e, r, t[r]); }) : Object.getOwnPropertyDescriptors ? Object.defineProperties(e, Object.getOwnPropertyDescriptors(t)) : ownKeys(Object(t)).forEach(function (r) { Object.defineProperty(e, r, Object.getOwnPropertyDescriptor(t, r)); }); } return e; }
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

import { EventType } from "../@types/event.js";
import { logger as rootLogger } from "../logger.js";
import { KeyTransportEvents } from "./IKeyTransport.js";
import { TypedEventEmitter } from "../models/typed-event-emitter.js";
import { RoomEvent } from "../models/room.js";
import { logSessionId } from "./MatrixRTCSession.js";
export class RoomKeyTransport extends TypedEventEmitter {
  constructor(room, client, statistics, parentLogger) {
    super();
    this.room = room;
    this.client = client;
    this.statistics = statistics;
    _defineProperty(this, "e2eeLogger", void 0);
    this.e2eeLogger = (parentLogger !== null && parentLogger !== void 0 ? parentLogger : rootLogger).getChild("[E2EE_FLOW_MX][ROOM_KEY_TRANSPORT]");
  }
  get logContext() {
    return {
      logSessionId,
      matrixUserId: this.client.getUserId()
    };
  }
  start() {
    this.room.on(RoomEvent.Timeline, ev => void this.consumeCallEncryptionEvent(ev));
  }
  stop() {
    this.room.off(RoomEvent.Timeline, ev => void this.consumeCallEncryptionEvent(ev));
  }
  consumeCallEncryptionEvent(event) {
    var _arguments = arguments,
      _this = this;
    return _asyncToGenerator(function* () {
      var _event$sender4;
      var isRetry = _arguments.length > 1 && _arguments[1] !== undefined ? _arguments[1] : false;
      yield _this.client.decryptEventIfNeeded(event);
      if (event.isDecryptionFailure()) {
        if (!isRetry) {
          var _event$sender;
          _this.e2eeLogger.warn("Decryption failed for ".concat(event.getType(), " event: ").concat(event.decryptionFailureReason, " will retry once only"), _objectSpread(_objectSpread({}, _this.logContext), {}, {
            sender: (_event$sender = event.sender) === null || _event$sender === void 0 ? void 0 : _event$sender.userId,
            eventId: event.getId()
          }));
          // retry after 1 second. After this we give up.
          setTimeout(() => void _this.consumeCallEncryptionEvent(event, true), 1000);
        } else {
          var _event$sender2;
          _this.e2eeLogger.error("Decryption failed for ".concat(event.getType(), " event: ").concat(event.decryptionFailureReason), _objectSpread(_objectSpread({}, _this.logContext), {}, {
            sender: (_event$sender2 = event.sender) === null || _event$sender2 === void 0 ? void 0 : _event$sender2.userId,
            eventId: event.getId()
          }));
        }
        return;
      } else if (isRetry) {
        var _event$sender3;
        _this.e2eeLogger.info("Decryption succeeded for ".concat(event.getType(), " event ").concat(event.getId(), " after retry"), _objectSpread(_objectSpread({}, _this.logContext), {}, {
          sender: (_event$sender3 = event.sender) === null || _event$sender3 === void 0 ? void 0 : _event$sender3.userId,
          eventId: event.getId()
        }));
      }
      if (event.getType() !== EventType.CallEncryptionKeysPrefix) return Promise.resolve();
      _this.e2eeLogger.info("Received io.element.call.encryption_keys event", _objectSpread(_objectSpread({}, _this.logContext), {}, {
        sender: (_event$sender4 = event.sender) === null || _event$sender4 === void 0 ? void 0 : _event$sender4.userId,
        eventId: event.getId()
      }));
      if (!_this.room) {
        var _event$sender5;
        _this.e2eeLogger.error("Got room state event for unknown room ".concat(event.getRoomId(), "!"), _objectSpread(_objectSpread({}, _this.logContext), {}, {
          sender: (_event$sender5 = event.sender) === null || _event$sender5 === void 0 ? void 0 : _event$sender5.userId,
          eventId: event.getId()
        }));
        return Promise.resolve();
      }
      _this.onEncryptionEvent(event);
    })();
  }

  /** implements {@link IKeyTransport#sendKey} */
  sendKey(keyBase64Encoded, index, members) {
    var _this2 = this;
    return _asyncToGenerator(function* () {
      // members not used in room transports as the keys are sent to all room members

      _this2.e2eeLogger.info("Sending encryption key", _objectSpread(_objectSpread({}, _this2.logContext), {}, {
        index
      }));
      var content = {
        keys: [{
          index: index,
          key: keyBase64Encoded
        }],
        device_id: _this2.client.getDeviceId(),
        call_id: "",
        sent_ts: Date.now()
      };
      try {
        yield _this2.client.sendEvent(_this2.room.roomId, EventType.CallEncryptionKeysPrefix, content);
      } catch (error) {
        _this2.e2eeLogger.error("Failed to send call encryption key", _objectSpread(_objectSpread({}, _this2.logContext), {}, {
          index,
          error
        }));
        var matrixError = error;
        if (matrixError.event) {
          // cancel the pending event: we'll just generate a new one with our latest
          // keys when we resend
          _this2.client.cancelPendingEvent(matrixError.event);
        }
        throw error;
      }
    })();
  }
  onEncryptionEvent(event) {
    var _event$sender6;
    var userId = event.getSender();
    var content = event.getContent();
    var deviceId = content["device_id"];
    var callId = content["call_id"];
    var logData = _objectSpread(_objectSpread({}, this.logContext), {}, {
      eventId: event.getId(),
      sender: (_event$sender6 = event.sender) === null || _event$sender6 === void 0 ? void 0 : _event$sender6.userId
    });
    if (!userId) {
      this.e2eeLogger.warn("Received m.call.encryption_keys with no userId: callId=".concat(callId), logData);
      return;
    }

    // We currently only handle callId = "" (which is the default for room scoped calls)
    if (callId !== "") {
      this.e2eeLogger.warn("Received m.call.encryption_keys with unsupported callId: userId=".concat(userId, ", deviceId=").concat(deviceId, ", callId=").concat(callId), logData);
      return;
    }
    if (!Array.isArray(content.keys)) {
      this.e2eeLogger.warn("Received m.call.encryption_keys where keys wasn't an array: callId=".concat(callId), logData);
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
    var age = Date.now() - (typeof content.sent_ts === "number" ? content.sent_ts : event.getTs());
    this.statistics.totals.roomEventEncryptionKeysReceivedTotalAge += age;
    for (var key of content.keys) {
      if (!key) {
        this.e2eeLogger.info("Ignoring false-y key in keys event", logData);
        continue;
      }
      var encryptionKey = key.key;
      var encryptionKeyIndex = key.index;
      if (!encryptionKey || encryptionKeyIndex === undefined || encryptionKeyIndex === null || callId === undefined || callId === null || typeof deviceId !== "string" || typeof callId !== "string" || typeof encryptionKey !== "string" || typeof encryptionKeyIndex !== "number") {
        this.e2eeLogger.warn("Malformed call encryption_key: userId=".concat(userId, ", deviceId=").concat(deviceId, ", encryptionKeyIndex=").concat(encryptionKeyIndex, " callId=").concat(callId), logData);
      } else {
        this.e2eeLogger.info("onCallEncryption userId=".concat(userId, ":").concat(deviceId, " encryptionKeyIndex=").concat(encryptionKeyIndex, " age=").concat(age, "ms"), logData);
        this.emit(KeyTransportEvents.ReceivedKeys, userId, deviceId, encryptionKey, encryptionKeyIndex, event.getTs());
      }
    }
  }
}
//# sourceMappingURL=RoomKeyTransport.js.map