#include "em_msync_codec.h"
#include "messagebody.pb.h"
#include "msync.pb.h"
#include <cassert>
#include <cstring>

int main() {
    EMCodecError error = EM_CODEC_OK;
    EMCodec *codec = em_codec_create(EM_CODEC_ABI_VERSION, &error);
    assert(codec && error == EM_CODEC_OK);

    EMCodecJID guid = {sizeof(EMCodecJID), "org#app", "server", "easemob.com", "go"};
    const uint8_t payload[] = {'{', '"', 't', 'o', 'k', 'e', 'n', '"', ':', '"', 'x', '"', '}'};
    EMCodecBuffer encoded = {sizeof(EMCodecBuffer), NULL, 0};
    assert(em_codec_encode_provision(codec, &guid, "4.0.0-go", "go", payload, sizeof(payload), &encoded) == EM_CODEC_OK);
    assert(encoded.data && encoded.size);

    EMCodecFrame *frame = NULL;
    assert(em_codec_decode_frame(codec, encoded.data, encoded.size, &frame) == EM_CODEC_OK);
    assert(frame && em_codec_frame_command(frame) == 3);
    assert(em_codec_frame_kind(frame) == EM_CODEC_FRAME_PROVISION);
    assert(std::strcmp(em_codec_frame_guid_name(frame), "server") == 0);
    em_codec_frame_free(frame);
    em_codec_buffer_free(&encoded);

    EMCodecKeyValue send_kv = {sizeof(EMCodecKeyValue), "flag", 1, 1, 0, NULL};
    EMCodecMessageContent send_content = {sizeof(EMCodecMessageContent), 6, NULL, "action", NULL, &send_kv, 1};
    EMCodecJID recipient = {sizeof(EMCodecJID), "org#app", "bob", "easemob.com", NULL};
    const char *directed[] = {"bob"};
    EMCodecSendRequest send = {sizeof(EMCodecSendRequest), 77, guid, recipient, 2, 2,
                               directed, 1, &send_content, 1, NULL, 0};

    // Public uint32 enum inputs must return an error, never reach protobuf's
    // assert-based generated setters and abort the hosting process.
    EMCodecSendRequest invalid = send;
    invalid.message_type = 999;
    assert(em_codec_encode_sync_send(codec, &invalid, &encoded) == EM_CODEC_INVALID_ARGUMENT);
    invalid = send; invalid.route_type = 999;
    assert(em_codec_encode_sync_send(codec, &invalid, &encoded) == EM_CODEC_INVALID_ARGUMENT);
    EMCodecMessageContent invalid_content = send_content;
    invalid_content.type = 999; invalid.contents = &invalid_content;
    assert(em_codec_encode_sync_send(codec, &invalid, &encoded) == EM_CODEC_INVALID_ARGUMENT);
    assert(em_codec_encode_sync_meta(codec, 1, &guid, &recipient, 0, 999, 0,
                                     NULL, 0, NULL, 0, &encoded) == EM_CODEC_INVALID_ARGUMENT);
    assert(em_codec_encode_frame(codec, 999, NULL, NULL, NULL, NULL,
                                 NULL, 0, &encoded) == EM_CODEC_INVALID_ARGUMENT);
    assert(em_codec_encode_sync_send(codec, &send, &encoded) == EM_CODEC_OK);
    easemob::pb::MSync sent_envelope;
    assert(sent_envelope.ParseFromArray(encoded.data, (int)encoded.size));
    easemob::pb::CommSyncUL sent_sync;
    assert(sent_sync.ParseFromString(sent_envelope.payload()));
    assert(sent_sync.meta().id() == 77 && sent_sync.meta().directed_users_size() == 1);
    easemob::pb::MessageBody sent_body;
    assert(sent_body.ParseFromString(sent_sync.meta().payload()));
    assert(sent_body.contents(0).action() == "action");
    em_codec_buffer_free(&encoded);

    assert(em_codec_encode_sync_queue(codec, &recipient, 123, &encoded) == EM_CODEC_OK);
    assert(sent_envelope.ParseFromArray(encoded.data, (int)encoded.size));
    assert(sent_sync.ParseFromString(sent_envelope.payload()) && sent_sync.key() == 123);
    em_codec_buffer_free(&encoded);

    assert(em_codec_encode_unread(codec, &encoded) == EM_CODEC_OK);
    em_codec_buffer_free(&encoded);
    assert(em_codec_encode_logout(codec, "session", "shutdown", &encoded) == EM_CODEC_OK);
    em_codec_buffer_free(&encoded);

    // Downlink semantic batch: CHAT/custom + all eight KeyValue wire types.
    easemob::pb::MessageBody body;
    body.set_type(easemob::pb::MessageBody_Type_CHAT);
    body.mutable_from()->set_name("alice"); body.mutable_to()->set_name("server");
    easemob::pb::MessageBody_Content *content = body.add_contents();
    content->set_type(easemob::pb::MessageBody_Content_Type_CUSTOM);
    content->set_customevent("event");
    for (int type = 1; type <= 8; ++type) {
        easemob::pb::KeyValue *kv = content->add_customexts();
        kv->set_key("k"); kv->set_type((easemob::pb::KeyValue_ValueType)type);
        if (type == 5) kv->set_float_value(1.5f);
        else if (type == 6) kv->set_double_value(2.5);
        else if (type >= 7) kv->set_string_value("value");
        else kv->set_varint_value(type);
    }
    std::string body_bytes; assert(body.SerializeToString(&body_bytes));
    easemob::pb::CommSyncDL dl; dl.mutable_status()->set_error_code(easemob::pb::Status_ErrorCode_OK);
    dl.set_next_key(42); dl.set_is_last(true); dl.mutable_queue()->set_name("queue");
    easemob::pb::Meta *meta = dl.add_metas(); meta->set_id(99); meta->set_ns(easemob::pb::Meta_NameSpace_CHAT); meta->set_payload(body_bytes);
    std::string dl_bytes; assert(dl.SerializeToString(&dl_bytes));
    easemob::pb::MSync envelope; envelope.set_command(easemob::pb::MSync_Command_SYNC); envelope.set_payload(dl_bytes);
    std::string wire; assert(envelope.SerializeToString(&wire));
    assert(em_codec_decode_frame(codec, (const uint8_t *)wire.data(), wire.size(), &frame) == EM_CODEC_OK);
    assert(em_codec_frame_kind(frame) == EM_CODEC_FRAME_SYNC_BATCH);
    assert(em_codec_frame_meta_count(frame) == 1 && em_codec_frame_next_key(frame) == 42);
    assert(em_codec_meta_content_count(frame, 0) == 1);
    assert(em_codec_content_key_value_count(frame, 0, 0) == 8);
    EMCodecKeyValue value = {sizeof(EMCodecKeyValue), NULL, 0, 0, 0, NULL};
    for (size_t i = 0; i < 8; ++i) assert(em_codec_content_key_value(frame, 0, 0, i, &value));
    size_t raw_size = 0; assert(em_codec_content_raw(frame, 0, 0, &raw_size) && raw_size > 0);
    em_codec_frame_free(frame);
    em_codec_destroy(codec);
    return 0;
}
