#ifndef EM_MSYNC_CODEC_H
#define EM_MSYNC_CODEC_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define EM_CODEC_ABI_VERSION 1u
#define EM_CODEC_MAX_INPUT_BYTES (16u * 1024u * 1024u)
#define EM_CODEC_MAX_OUTPUT_BYTES (16u * 1024u * 1024u)

typedef struct EMCodec EMCodec;
typedef struct EMCodecFrame EMCodecFrame;

typedef enum EMCodecError {
    EM_CODEC_OK = 0,
    EM_CODEC_INVALID_ARGUMENT = 1,
    EM_CODEC_LIMIT_EXCEEDED = 2,
    EM_CODEC_MALFORMED_FRAME = 3,
    EM_CODEC_SERIALIZE_FAILED = 4,
    EM_CODEC_INTERNAL_ERROR = 5
} EMCodecError;

typedef struct EMCodecBuffer {
    uint32_t struct_size;
    uint8_t *data;
    size_t size;
} EMCodecBuffer;

/*
 * Ownership and lifetime:
 * - Input pointers are borrowed for the duration of the synchronous call and
 *   are never retained by the codec. Strings must be NUL-terminated.
 * - Successful encode calls transfer output.data ownership to the caller;
 *   release it exactly once with em_codec_buffer_free before reusing the
 *   EMCodecBuffer. The buffer must be zero-initialized (data == NULL, size == 0)
 *   when passed to an encode function.
 * - Decode frame getter pointers are borrowed and remain valid only until
 *   em_codec_frame_free(frame); callers must copy them before freeing frame.
 */

typedef struct EMCodecJID {
    uint32_t struct_size;
    const char *app_key;
    const char *name;
    const char *domain;
    const char *resource;
} EMCodecJID;

typedef struct EMCodecKeyValue {
    uint32_t struct_size;
    const char *key;
    uint32_t type;
    int64_t integer_value;
    double number_value;
    const char *string_value;
} EMCodecKeyValue;

typedef struct EMCodecMessageContent {
    uint32_t struct_size;
    uint32_t type;
    const char *text;
    const char *action;
    const char *custom_event;
    const EMCodecKeyValue *values;
    size_t value_count;
} EMCodecMessageContent;

typedef struct EMCodecSendRequest {
    uint32_t struct_size;
    uint64_t client_message_id;
    EMCodecJID from;
    EMCodecJID to;
    uint32_t message_type;
    uint32_t route_type;
    const char *const *directed_users;
    size_t directed_user_count;
    const EMCodecMessageContent *contents;
    size_t content_count;
    const EMCodecKeyValue *extensions;
    size_t extension_count;
} EMCodecSendRequest;

typedef enum EMCodecDecodedKind {
    EM_CODEC_FRAME_RAW = 0,
    EM_CODEC_FRAME_PROVISION = 1,
    EM_CODEC_FRAME_UNREAD = 2,
    EM_CODEC_FRAME_NOTICE = 3,
    EM_CODEC_FRAME_SYNC_ACK = 4,
    EM_CODEC_FRAME_SYNC_BATCH = 5,
    EM_CODEC_FRAME_LOGOUT = 6
} EMCodecDecodedKind;

EMCodec *em_codec_create(uint32_t abi_version, EMCodecError *error);
void em_codec_destroy(EMCodec *codec);
uint32_t em_codec_abi_version(void);
uint64_t em_codec_features(void);
void em_codec_buffer_free(EMCodecBuffer *buffer);

/* Encode a raw MSync envelope. payload may be NULL when payload_size is zero. */
EMCodecError em_codec_encode_frame(EMCodec *codec, uint32_t command,
                                   const char *guid_app_key, const char *guid_name,
                                   const char *guid_domain, const char *guid_resource,
                                   const uint8_t *payload, size_t payload_size,
                                   EMCodecBuffer *output);
EMCodecError em_codec_encode_provision(EMCodec *codec, const EMCodecJID *guid,
                                       const char *sdk_version, const char *resource,
                                       const uint8_t *auth_token, size_t auth_token_size,
                                       EMCodecBuffer *output);
EMCodecError em_codec_encode_unread(EMCodec *codec, EMCodecBuffer *output);
EMCodecError em_codec_encode_sync_send(EMCodec *codec, const EMCodecSendRequest *request,
                                       EMCodecBuffer *output);
EMCodecError em_codec_encode_message_body(EMCodec *codec, const EMCodecSendRequest *request,
                                          EMCodecBuffer *output);
EMCodecError em_codec_encode_sync_meta(EMCodec *codec, uint64_t id,
                                       const EMCodecJID *from, const EMCodecJID *to,
                                       uint64_t timestamp, uint32_t meta_namespace,
                                       uint32_t route_type, const uint8_t *payload,
                                       size_t payload_size, const char *const *directed_users,
                                       size_t directed_user_count, EMCodecBuffer *output);
EMCodecError em_codec_encode_sync_queue(EMCodec *codec, const EMCodecJID *queue,
                                        uint64_t key, EMCodecBuffer *output);
EMCodecError em_codec_encode_logout(EMCodec *codec, const char *session_id,
                                    const char *reason, EMCodecBuffer *output);

EMCodecError em_codec_decode_frame(EMCodec *codec, const uint8_t *data, size_t size,
                                   EMCodecFrame **output);
EMCodecError em_codec_decode_message_body(EMCodec *codec, const uint8_t *data, size_t size,
                                          EMCodecFrame **output);
EMCodecError em_codec_decode_statistic(EMCodec *codec, const uint8_t *data, size_t size,
                                       EMCodecFrame **output);
void em_codec_frame_free(EMCodecFrame *frame);
uint32_t em_codec_frame_command(const EMCodecFrame *frame);
uint64_t em_codec_frame_trace_id(const EMCodecFrame *frame);
const uint8_t *em_codec_frame_payload(const EMCodecFrame *frame, size_t *size);
const char *em_codec_frame_guid_app_key(const EMCodecFrame *frame);
const char *em_codec_frame_guid_name(const EMCodecFrame *frame);
const char *em_codec_frame_guid_domain(const EMCodecFrame *frame);
const char *em_codec_frame_guid_resource(const EMCodecFrame *frame);
uint32_t em_codec_frame_kind(const EMCodecFrame *frame);
int32_t em_codec_frame_status_code(const EMCodecFrame *frame);
const char *em_codec_frame_status_reason(const EMCodecFrame *frame);
size_t em_codec_frame_redirect_count(const EMCodecFrame *frame);
const char *em_codec_frame_redirect_host(const EMCodecFrame *frame, size_t index);
uint32_t em_codec_frame_redirect_port(const EMCodecFrame *frame, size_t index);
const char *em_codec_frame_session_id(const EMCodecFrame *frame);
const uint8_t *em_codec_frame_auth_token(const EMCodecFrame *frame, size_t *size);
uint64_t em_codec_frame_ack_client_id(const EMCodecFrame *frame);
uint64_t em_codec_frame_ack_server_id(const EMCodecFrame *frame);
uint64_t em_codec_frame_timestamp(const EMCodecFrame *frame);
size_t em_codec_frame_unread_queue_count(const EMCodecFrame *frame);
int em_codec_frame_unread_queue(const EMCodecFrame *frame, size_t index,
                                EMCodecJID *queue, uint32_t *unread_count);
uint64_t em_codec_frame_next_key(const EMCodecFrame *frame);
int em_codec_frame_is_last(const EMCodecFrame *frame);
int em_codec_frame_queue(const EMCodecFrame *frame, EMCodecJID *queue);
size_t em_codec_frame_meta_count(const EMCodecFrame *frame);
uint64_t em_codec_meta_id(const EMCodecFrame *frame, size_t index);
uint64_t em_codec_meta_timestamp(const EMCodecFrame *frame, size_t index);
uint32_t em_codec_meta_namespace(const EMCodecFrame *frame, size_t index);
uint32_t em_codec_meta_route_type(const EMCodecFrame *frame, size_t index);
const uint8_t *em_codec_meta_payload(const EMCodecFrame *frame, size_t index, size_t *size);
/*
 * Server-populated attribute blob (msync Meta field 9). When present it is a
 * JSON object carrying delivery metadata such as "is_online". The field is
 * optional and only emitted when the server enables it, so callers must treat
 * an empty result as "unknown" rather than as a default value.
 */
const uint8_t *em_codec_meta_attributes(const EMCodecFrame *frame, size_t index, size_t *size);
int em_codec_meta_from(const EMCodecFrame *frame, size_t index, EMCodecJID *jid);
int em_codec_meta_to(const EMCodecFrame *frame, size_t index, EMCodecJID *jid);
size_t em_codec_meta_directed_user_count(const EMCodecFrame *frame, size_t index);
const char *em_codec_meta_directed_user(const EMCodecFrame *frame, size_t index, size_t user_index);
uint32_t em_codec_meta_message_type(const EMCodecFrame *frame, size_t index);
int em_codec_message_from(const EMCodecFrame *frame, size_t index, EMCodecJID *jid);
int em_codec_message_to(const EMCodecFrame *frame, size_t index, EMCodecJID *jid);
size_t em_codec_meta_content_count(const EMCodecFrame *frame, size_t index);
uint32_t em_codec_content_type(const EMCodecFrame *frame, size_t meta_index, size_t content_index);
const char *em_codec_content_text(const EMCodecFrame *frame, size_t meta_index, size_t content_index);
const char *em_codec_content_action(const EMCodecFrame *frame, size_t meta_index, size_t content_index);
const char *em_codec_content_custom_event(const EMCodecFrame *frame, size_t meta_index, size_t content_index);
const uint8_t *em_codec_content_raw(const EMCodecFrame *frame, size_t meta_index, size_t content_index, size_t *size);
size_t em_codec_content_key_value_count(const EMCodecFrame *frame, size_t meta_index, size_t content_index);
size_t em_codec_meta_key_value_count(const EMCodecFrame *frame, size_t meta_index);
int em_codec_content_key_value(const EMCodecFrame *frame, size_t meta_index, size_t content_index,
                               size_t value_index, EMCodecKeyValue *value);
int em_codec_meta_key_value(const EMCodecFrame *frame, size_t meta_index, size_t value_index,
                            EMCodecKeyValue *value);
int32_t em_codec_meta_statistic_operation(const EMCodecFrame *frame, size_t index);
const char *em_codec_meta_statistic_device(const EMCodecFrame *frame, size_t index);
const char *em_codec_meta_statistic_reason(const EMCodecFrame *frame, size_t index);
const char *em_codec_meta_statistic_session_id(const EMCodecFrame *frame, size_t index);

#ifdef __cplusplus
}
#endif
#endif
