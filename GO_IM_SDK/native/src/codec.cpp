#include "em_msync_codec.h"
#include "messagebody.pb.h"
#include "msync.pb.h"
#include "statisticsbody.pb.h"
#include <climits>
#include <cstring>
#include <new>
#include <string>
#include <vector>
#include <chrono>
#include <atomic>

using namespace easemob::pb;

struct EMCodec { uint32_t abi; };
struct ParsedMeta {
    bool has_message;
    bool has_statistic;
    MessageBody message;
    StatisticsBody statistic;
    std::vector<std::string> raw_contents;
    ParsedMeta() : has_message(false), has_statistic(false) {}
};
struct EMCodecFrame {
    MSync envelope;
    Provision provision;
    CommUnreadDL unread;
    CommNotice notice;
    CommSyncDL sync;
    Logout logout;
    uint32_t kind;
    std::vector<ParsedMeta> parsed;
    EMCodecFrame() : kind(EM_CODEC_FRAME_RAW) {}
};

static bool bounded(size_t n, size_t limit) { return n <= limit; }
static std::atomic<uint64_t> trace_seq(0);
static uint64_t next_trace_id() {
    uint64_t ms = static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::now().time_since_epoch()).count());
    return (ms << 22) | (trace_seq.fetch_add(1, std::memory_order_relaxed) & ((1ull << 22) - 1));
}
static bool valid_text(const char *s) { return !s || std::strlen(s) <= 4096u; }
static bool valid_count(size_t n) { return n <= 4096u; }
static bool valid_request_budget(const EMCodecSendRequest *r) {
    if (!r || !valid_count(r->content_count) || !valid_count(r->extension_count) ||
        !valid_count(r->directed_user_count)) return false;
    size_t nodes = r->extension_count + r->directed_user_count + r->content_count;
    if (nodes > 4096u) return false;
    for (size_t i = 0; i < r->content_count; ++i) {
        if (!valid_count(r->contents[i].value_count) || nodes > 4096u - r->contents[i].value_count) return false;
        nodes += r->contents[i].value_count;
    }
    return true;
}
static bool valid_message_budget(const MessageBody &m) {
    size_t nodes = static_cast<size_t>(m.contents_size()) + static_cast<size_t>(m.ext_size());
    if (nodes > 4096u) return false;
    for (int i = 0; i < m.contents_size(); ++i) {
        const MessageBody_Content &c = m.contents(i);
        size_t values = static_cast<size_t>(c.params_size()) + static_cast<size_t>(c.customexts_size());
        if (values > 4096u || nodes > 4096u - values) return false;
        nodes += values;
    }
    return true;
}
static bool valid_command(uint32_t v) { return v <= static_cast<uint32_t>(INT_MAX) && MSync_Command_IsValid(static_cast<int>(v)); }
static bool valid_namespace(uint32_t v) { return v <= static_cast<uint32_t>(INT_MAX) && Meta_NameSpace_IsValid(static_cast<int>(v)); }
static bool valid_route(uint32_t v) { return v <= static_cast<uint32_t>(INT_MAX) && Meta_RouteType_IsValid(static_cast<int>(v)); }
static bool valid_message_type(uint32_t v) { return v <= static_cast<uint32_t>(INT_MAX) && MessageBody_Type_IsValid(static_cast<int>(v)); }
static bool valid_content_type(uint32_t v) { return v <= static_cast<uint32_t>(INT_MAX) && MessageBody_Content_Type_IsValid(static_cast<int>(v)); }
static void clear_buffer(EMCodecBuffer *b) { if (b) { b->data = NULL; b->size = 0; } }
static void set_jid(JID *out, const EMCodecJID *in) {
    if (in->app_key) out->set_app_key(in->app_key);
    if (in->name) out->set_name(in->name);
    if (in->domain) out->set_domain(in->domain);
    if (in->resource) out->set_client_resource(in->resource);
}
static void set_bare_jid(JID *out, const EMCodecJID *in) {
    if (in && in->name) out->set_name(in->name);
}
static bool valid_jid(const EMCodecJID *j) {
    return j && j->struct_size == sizeof(*j) && valid_text(j->app_key) && valid_text(j->name) &&
           valid_text(j->domain) && valid_text(j->resource);
}
static int copy_jid(const JID *j, EMCodecJID *out) {
    if (!j || !out || out->struct_size != sizeof(*out)) return 0;
    out->app_key=j->app_key().c_str(); out->name=j->name().c_str();
    out->domain=j->domain().c_str(); out->resource=j->client_resource().c_str(); return 1;
}
static EMCodecError serialize_envelope(uint32_t command, const JID *guid, const std::string &payload,
                                       const char *user_agent, EMCodecBuffer *output) {
    if (!valid_command(command)) return EM_CODEC_INVALID_ARGUMENT;
    MSync m; m.set_version(MSync_Version_MSYNC_V1); m.set_command(static_cast<MSync_Command>(command));
    m.set_compress_algorimth(0); m.add_encrypt_type(ENCRYPT_NONE); m.set_trace_id(next_trace_id());
    if (guid) m.mutable_guid()->CopyFrom(*guid);
    if (user_agent) m.set_user_agent(user_agent);
    m.set_payload(payload);
    std::string encoded;
    if (!m.SerializeToString(&encoded)) return EM_CODEC_SERIALIZE_FAILED;
    if (!bounded(encoded.size(), EM_CODEC_MAX_OUTPUT_BYTES)) return EM_CODEC_LIMIT_EXCEEDED;
    output->data = new (std::nothrow) uint8_t[encoded.size()];
    if (!output->data && !encoded.empty()) return EM_CODEC_INTERNAL_ERROR;
    output->size=encoded.size(); if (!encoded.empty()) std::memcpy(output->data, encoded.data(), encoded.size());
    return EM_CODEC_OK;
}
static EMCodecError start_output(EMCodec *c, EMCodecBuffer *o) {
    if (!c || !o || o->struct_size != sizeof(*o)) return EM_CODEC_INVALID_ARGUMENT;
    // Reuse is allowed only after em_codec_buffer_free. Silently clearing a
    // live pointer here would lose ownership and leak the previous buffer.
    if (o->data || o->size) return EM_CODEC_INVALID_ARGUMENT;
    return EM_CODEC_OK;
}
static bool set_kv(KeyValue *out, const EMCodecKeyValue &in) {
    if (in.struct_size != sizeof(in) || !valid_text(in.key) || !valid_text(in.string_value) ||
        in.type < KeyValue_ValueType_BOOL || in.type > KeyValue_ValueType_JSON_STRING) return false;
    if (in.key) out->set_key(in.key);
    out->set_type(static_cast<KeyValue_ValueType>(in.type));
    switch (in.type) {
    case KeyValue_ValueType_FLOAT: out->set_float_value(static_cast<float>(in.number_value)); break;
    case KeyValue_ValueType_DOUBLE: out->set_double_value(in.number_value); break;
    case KeyValue_ValueType_STRING: case KeyValue_ValueType_JSON_STRING:
        out->set_string_value(in.string_value ? in.string_value : ""); break;
    default: out->set_varint_value(in.integer_value); break;
    }
    return true;
}
static const Status *frame_status(const EMCodecFrame *f) {
    if (!f) return NULL;
    switch (f->kind) {
    case EM_CODEC_FRAME_PROVISION: return f->provision.has_status()?&f->provision.status():NULL;
    case EM_CODEC_FRAME_UNREAD: return f->unread.has_status()?&f->unread.status():NULL;
    case EM_CODEC_FRAME_SYNC_ACK: case EM_CODEC_FRAME_SYNC_BATCH: return f->sync.has_status()?&f->sync.status():NULL;
    case EM_CODEC_FRAME_LOGOUT: return f->logout.has_status()?&f->logout.status():NULL;
    default: return NULL;
    }
}
static const Meta *meta_at(const EMCodecFrame *f,size_t i) { return f && i<(size_t)f->sync.metas_size()?&f->sync.metas((int)i):NULL; }
static const ParsedMeta *parsed_at(const EMCodecFrame *f,size_t i) { return f&&i<f->parsed.size()?&f->parsed[i]:NULL; }
static const MessageBody_Content *content_at(const EMCodecFrame *f,size_t mi,size_t ci) {
    const ParsedMeta *p=parsed_at(f,mi); return p&&p->has_message&&ci<(size_t)p->message.contents_size()?&p->message.contents((int)ci):NULL;
}
static const KeyValue *content_kv(const MessageBody_Content *c,size_t i) {
    if (!c) return NULL;
    if (c->type()==MessageBody_Content_Type_COMMAND) return i<(size_t)c->params_size()?&c->params((int)i):NULL;
    if (c->type()==MessageBody_Content_Type_CUSTOM) return i<(size_t)c->customexts_size()?&c->customexts((int)i):NULL;
    return NULL;
}
static int fill_kv(const KeyValue *v, EMCodecKeyValue *out) {
    if (!v||!out||out->struct_size!=sizeof(*out)) return 0;
    out->key=v->key().c_str(); out->type=(uint32_t)v->type(); out->integer_value=v->varint_value();
    out->number_value=v->type()==KeyValue_ValueType_FLOAT?v->float_value():v->double_value();
    out->string_value=v->string_value().c_str(); return 1;
}

extern "C" {
uint32_t em_codec_abi_version(void) { return EM_CODEC_ABI_VERSION; }
uint64_t em_codec_features(void) { return 0x7fu; }
EMCodec *em_codec_create(uint32_t v, EMCodecError *e) { if(e)*e=EM_CODEC_OK; if(v!=EM_CODEC_ABI_VERSION){if(e)*e=EM_CODEC_INVALID_ARGUMENT;return NULL;} EMCodec*c=new(std::nothrow)EMCodec();if(!c&&e)*e=EM_CODEC_INTERNAL_ERROR;if(c)c->abi=v;return c; }
void em_codec_destroy(EMCodec *c) { delete c; }
void em_codec_buffer_free(EMCodecBuffer *b) { if(b){delete[] b->data;b->data=NULL;b->size=0;} }

EMCodecError em_codec_encode_frame(EMCodec*c,uint32_t command,const char*app,const char*name,const char*domain,const char*resource,const uint8_t*p,size_t n,EMCodecBuffer*out){
    if(start_output(c,out)!=EM_CODEC_OK||(!p&&n)||!valid_command(command)||!valid_text(app)||!valid_text(name)||!valid_text(domain)||!valid_text(resource))return EM_CODEC_INVALID_ARGUMENT;
    if(!bounded(n,EM_CODEC_MAX_INPUT_BYTES))return EM_CODEC_LIMIT_EXCEEDED;
    try{JID j;if(app)j.set_app_key(app);if(name)j.set_name(name);if(domain)j.set_domain(domain);if(resource)j.set_client_resource(resource);std::string payload;if(n)payload.assign((const char*)p,n);return serialize_envelope(command,&j,payload,NULL,out);}catch(...){clear_buffer(out);return EM_CODEC_INTERNAL_ERROR;}
}
EMCodecError em_codec_encode_provision(EMCodec*c,const EMCodecJID*g,const char*version,const char*resource,const uint8_t*auth,size_t n,EMCodecBuffer*out){
    if(start_output(c,out)!=EM_CODEC_OK||!valid_jid(g)||!valid_text(version)||!valid_text(resource)||(!auth&&n))return EM_CODEC_INVALID_ARGUMENT;if(!bounded(n,EM_CODEC_MAX_INPUT_BYTES))return EM_CODEC_LIMIT_EXCEEDED;
    try{Provision p;p.set_os_type(Provision_OsType_OS_GO);p.add_compress_type(Provision_CompressType_COMPRESS_NONE);p.add_protocol_compress_type(Provision_CompressType_COMPRESS_NONE);p.set_protocol_compress_direction(Provision_CompressDirection_BI);if(version)p.set_version(version);if(resource)p.set_resource(resource);if(n)p.set_auth_token(auth,n);std::string b;if(!p.SerializeToString(&b))return EM_CODEC_SERIALIZE_FAILED;JID j;set_jid(&j,g);return serialize_envelope(MSync_Command_PROVISION,&j,b,version,out);}catch(...){clear_buffer(out);return EM_CODEC_INTERNAL_ERROR;}
}
EMCodecError em_codec_encode_unread(EMCodec*c,EMCodecBuffer*out){if(start_output(c,out)!=EM_CODEC_OK)return EM_CODEC_INVALID_ARGUMENT;try{const std::string b;return serialize_envelope(MSync_Command_UNREAD,NULL,b,NULL,out);}catch(...){return EM_CODEC_INTERNAL_ERROR;}}
EMCodecError em_codec_encode_sync_queue(EMCodec*c,const EMCodecJID*q,uint64_t key,EMCodecBuffer*out){if(start_output(c,out)!=EM_CODEC_OK||!valid_jid(q))return EM_CODEC_INVALID_ARGUMENT;try{CommSyncUL u;if(key)u.set_key(key);set_jid(u.mutable_queue(),q);std::string b;if(!u.SerializeToString(&b))return EM_CODEC_SERIALIZE_FAILED;return serialize_envelope(MSync_Command_SYNC,NULL,b,NULL,out);}catch(...){return EM_CODEC_INTERNAL_ERROR;}}
EMCodecError em_codec_encode_logout(EMCodec*c,const char*sid,const char*reason,EMCodecBuffer*out){if(start_output(c,out)!=EM_CODEC_OK||!valid_text(sid)||!valid_text(reason))return EM_CODEC_INVALID_ARGUMENT;try{Logout l;if(sid)l.set_session_id(sid);if(reason)l.set_reason(reason);std::string b;if(!l.SerializeToString(&b))return EM_CODEC_SERIALIZE_FAILED;return serialize_envelope(MSync_Command_LOGOUT,NULL,b,NULL,out);}catch(...){return EM_CODEC_INTERNAL_ERROR;}}
EMCodecError em_codec_encode_sync_send(EMCodec*c,const EMCodecSendRequest*r,EMCodecBuffer*out){
    if(start_output(c,out)!=EM_CODEC_OK||!r||r->struct_size!=sizeof(*r)||!valid_jid(&r->from)||!valid_jid(&r->to)||!valid_message_type(r->message_type)||!valid_route(r->route_type)||(!r->contents&&r->content_count)||(!r->extensions&&r->extension_count)||(!r->directed_users&&r->directed_user_count)||!valid_request_budget(r))return EM_CODEC_INVALID_ARGUMENT;
    try{CommSyncUL u;Meta*m=u.mutable_meta();m->set_id(r->client_message_id);m->set_ns(Meta_NameSpace_CHAT);m->set_routetype((Meta_RouteType)r->route_type);set_bare_jid(m->mutable_to(),&r->to);
        MessageBody body;body.set_type((MessageBody_Type)r->message_type);set_bare_jid(body.mutable_from(),&r->from);set_bare_jid(body.mutable_to(),&r->to);
        for(size_t i=0;i<r->directed_user_count;i++){if(!valid_text(r->directed_users[i]))return EM_CODEC_INVALID_ARGUMENT;m->add_directed_users(r->directed_users[i]?r->directed_users[i]:"");}
        for(size_t i=0;i<r->extension_count;i++)if(!set_kv(body.add_ext(),r->extensions[i]))return EM_CODEC_INVALID_ARGUMENT;
        for(size_t i=0;i<r->content_count;i++){const EMCodecMessageContent&in=r->contents[i];if(in.struct_size!=sizeof(in)||!valid_content_type(in.type)||!valid_text(in.text)||!valid_text(in.action)||!valid_text(in.custom_event)||!valid_count(in.value_count)||(!in.values&&in.value_count))return EM_CODEC_INVALID_ARGUMENT;MessageBody_Content*o=body.add_contents();o->set_type((MessageBody_Content_Type)in.type);if(in.text)o->set_text(in.text);if(in.action)o->set_action(in.action);if(in.custom_event)o->set_customevent(in.custom_event);for(size_t k=0;k<in.value_count;k++){KeyValue*v=in.type==MessageBody_Content_Type_COMMAND?o->add_params():o->add_customexts();if(!set_kv(v,in.values[k]))return EM_CODEC_INVALID_ARGUMENT;}}
        std::string mb;if(!body.SerializeToString(&mb))return EM_CODEC_SERIALIZE_FAILED;m->set_payload(mb);std::string b;if(!u.SerializeToString(&b))return EM_CODEC_SERIALIZE_FAILED;return serialize_envelope(MSync_Command_SYNC,NULL,b,NULL,out);
    }catch(...){clear_buffer(out);return EM_CODEC_INTERNAL_ERROR;}}

EMCodecError em_codec_encode_message_body(EMCodec*c,const EMCodecSendRequest*r,EMCodecBuffer*out){
    if(start_output(c,out)!=EM_CODEC_OK||!r||r->struct_size!=sizeof(*r)||!valid_jid(&r->from)||!valid_jid(&r->to)||!valid_message_type(r->message_type)||(!r->contents&&r->content_count)||(!r->extensions&&r->extension_count)||!valid_request_budget(r))return EM_CODEC_INVALID_ARGUMENT;
    try{MessageBody body;body.set_type((MessageBody_Type)r->message_type);set_jid(body.mutable_from(),&r->from);set_jid(body.mutable_to(),&r->to);for(size_t i=0;i<r->extension_count;i++)if(!set_kv(body.add_ext(),r->extensions[i]))return EM_CODEC_INVALID_ARGUMENT;for(size_t i=0;i<r->content_count;i++){const EMCodecMessageContent&in=r->contents[i];if(in.struct_size!=sizeof(in)||!valid_content_type(in.type)||!valid_text(in.text)||!valid_text(in.action)||!valid_text(in.custom_event)||!valid_count(in.value_count)||(!in.values&&in.value_count))return EM_CODEC_INVALID_ARGUMENT;MessageBody_Content*o=body.add_contents();o->set_type((MessageBody_Content_Type)in.type);if(in.text)o->set_text(in.text);if(in.action)o->set_action(in.action);if(in.custom_event)o->set_customevent(in.custom_event);for(size_t k=0;k<in.value_count;k++){KeyValue*v=in.type==MessageBody_Content_Type_COMMAND?o->add_params():o->add_customexts();if(!set_kv(v,in.values[k]))return EM_CODEC_INVALID_ARGUMENT;}}std::string b;if(!body.SerializeToString(&b))return EM_CODEC_SERIALIZE_FAILED;if(!bounded(b.size(),EM_CODEC_MAX_OUTPUT_BYTES))return EM_CODEC_LIMIT_EXCEEDED;out->data=new(std::nothrow)uint8_t[b.size()];if(!out->data&&!b.empty())return EM_CODEC_INTERNAL_ERROR;out->size=b.size();if(!b.empty())std::memcpy(out->data,b.data(),b.size());return EM_CODEC_OK;}catch(...){clear_buffer(out);return EM_CODEC_INTERNAL_ERROR;}}
EMCodecError em_codec_encode_sync_meta(EMCodec*c,uint64_t id,const EMCodecJID*from,const EMCodecJID*to,uint64_t ts,uint32_t ns,uint32_t route,const uint8_t*p,size_t n,const char*const*users,size_t count,EMCodecBuffer*out){if(start_output(c,out)!=EM_CODEC_OK||!valid_jid(from)||!valid_jid(to)||!valid_namespace(ns)||!valid_route(route)||(!p&&n)||(!users&&count)||!valid_count(count)||!bounded(n,EM_CODEC_MAX_INPUT_BYTES))return EM_CODEC_INVALID_ARGUMENT;try{CommSyncUL u;Meta*m=u.mutable_meta();m->set_id(id);m->set_timestamp(ts);m->set_ns((Meta_NameSpace)ns);m->set_routetype((Meta_RouteType)route);set_jid(m->mutable_from(),from);set_jid(m->mutable_to(),to);if(n)m->set_payload(p,n);for(size_t i=0;i<count;i++){if(!valid_text(users[i]))return EM_CODEC_INVALID_ARGUMENT;m->add_directed_users(users[i]?users[i]:"");}std::string b;if(!u.SerializeToString(&b))return EM_CODEC_SERIALIZE_FAILED;return serialize_envelope(MSync_Command_SYNC,NULL,b,NULL,out);}catch(...){clear_buffer(out);return EM_CODEC_INTERNAL_ERROR;}}

EMCodecError em_codec_decode_frame(EMCodec*c,const uint8_t*d,size_t n,EMCodecFrame**out){
    if(!c||!out||(!d&&n))return EM_CODEC_INVALID_ARGUMENT;*out=NULL;if(!bounded(n,EM_CODEC_MAX_INPUT_BYTES)||n>(size_t)INT_MAX)return EM_CODEC_LIMIT_EXCEEDED;EMCodecFrame*f=new(std::nothrow)EMCodecFrame();if(!f)return EM_CODEC_INTERNAL_ERROR;
    try{if(!f->envelope.ParseFromArray(d,(int)n)){delete f;return EM_CODEC_MALFORMED_FRAME;}const std::string&p=f->envelope.payload();bool ok=false;switch(f->envelope.command()){
        case MSync_Command_PROVISION:f->kind=EM_CODEC_FRAME_PROVISION;ok=f->provision.ParseFromString(p);break;
        case MSync_Command_UNREAD:f->kind=EM_CODEC_FRAME_UNREAD;ok=f->unread.ParseFromString(p);break;
        case MSync_Command_NOTICE:f->kind=EM_CODEC_FRAME_NOTICE;ok=f->notice.ParseFromString(p);break;
        case MSync_Command_LOGOUT:f->kind=EM_CODEC_FRAME_LOGOUT;ok=f->logout.ParseFromString(p);break;
        case MSync_Command_SYNC:ok=f->sync.ParseFromString(p);if(ok){f->kind=f->sync.meta_id()>0?EM_CODEC_FRAME_SYNC_ACK:EM_CODEC_FRAME_SYNC_BATCH;if((size_t)f->sync.metas_size()>4096u){delete f;return EM_CODEC_LIMIT_EXCEEDED;}size_t nodes=(size_t)f->sync.metas_size();for(int i=0;i<f->sync.metas_size();i++){ParsedMeta pm;const Meta&m=f->sync.metas(i);if((size_t)m.directed_users_size()>4096u-nodes){delete f;return EM_CODEC_LIMIT_EXCEEDED;}nodes+=(size_t)m.directed_users_size();if(m.ns()==Meta_NameSpace_CHAT){pm.has_message=pm.message.ParseFromString(m.payload());if(!pm.has_message){delete f;return EM_CODEC_MALFORMED_FRAME;}if(!valid_message_budget(pm.message)||nodes>4096u-(size_t)pm.message.contents_size()-(size_t)pm.message.ext_size()){delete f;return EM_CODEC_LIMIT_EXCEEDED;}nodes+=(size_t)pm.message.contents_size()+(size_t)pm.message.ext_size();for(int j=0;j<pm.message.contents_size();j++){const MessageBody_Content&ct=pm.message.contents(j);size_t values=(size_t)ct.params_size()+(size_t)ct.customexts_size();if(values>4096u||nodes>4096u-values){delete f;return EM_CODEC_LIMIT_EXCEEDED;}nodes+=values;std::string raw;pm.message.contents(j).SerializeToString(&raw);pm.raw_contents.push_back(raw);}}else if(m.ns()==Meta_NameSpace_STATISTIC)pm.has_statistic=pm.statistic.ParseFromString(m.payload());f->parsed.push_back(pm);}}break;
        default:f->kind=EM_CODEC_FRAME_RAW;ok=true;break;}
        if(!ok){delete f;return EM_CODEC_MALFORMED_FRAME;}
        const Status *status=frame_status(f);if(status&&(size_t)status->redirect_info_size()>4096u){delete f;return EM_CODEC_LIMIT_EXCEEDED;}
        if(f->kind==EM_CODEC_FRAME_UNREAD&&(size_t)f->unread.unread_size()>4096u){delete f;return EM_CODEC_LIMIT_EXCEEDED;}
        *out=f;return EM_CODEC_OK;
    }catch(...){delete f;return EM_CODEC_INTERNAL_ERROR;}}
EMCodecError em_codec_decode_message_body(EMCodec*c,const uint8_t*d,size_t n,EMCodecFrame**out){if(!c||!out||(!d&&n))return EM_CODEC_INVALID_ARGUMENT;*out=NULL;if(!bounded(n,EM_CODEC_MAX_INPUT_BYTES)||n>(size_t)INT_MAX)return EM_CODEC_LIMIT_EXCEEDED;EMCodecFrame*f=new(std::nothrow)EMCodecFrame();if(!f)return EM_CODEC_INTERNAL_ERROR;try{ParsedMeta p;p.has_message=p.message.ParseFromArray(d,(int)n);if(!p.has_message){delete f;return EM_CODEC_MALFORMED_FRAME;}if(!valid_message_budget(p.message)){delete f;return EM_CODEC_LIMIT_EXCEEDED;}for(int i=0;i<p.message.contents_size();i++){std::string raw;p.message.contents(i).SerializeToString(&raw);p.raw_contents.push_back(raw);}f->parsed.push_back(p);*out=f;return EM_CODEC_OK;}catch(...){delete f;return EM_CODEC_INTERNAL_ERROR;}}
EMCodecError em_codec_decode_statistic(EMCodec*c,const uint8_t*d,size_t n,EMCodecFrame**out){if(!c||!out||(!d&&n))return EM_CODEC_INVALID_ARGUMENT;*out=NULL;if(!bounded(n,EM_CODEC_MAX_INPUT_BYTES)||n>(size_t)INT_MAX)return EM_CODEC_LIMIT_EXCEEDED;EMCodecFrame*f=new(std::nothrow)EMCodecFrame();if(!f)return EM_CODEC_INTERNAL_ERROR;try{ParsedMeta p;p.has_statistic=p.statistic.ParseFromArray(d,(int)n);if(!p.has_statistic){delete f;return EM_CODEC_MALFORMED_FRAME;}f->parsed.push_back(p);*out=f;return EM_CODEC_OK;}catch(...){delete f;return EM_CODEC_INTERNAL_ERROR;}}

void em_codec_frame_free(EMCodecFrame*f){delete f;}
uint32_t em_codec_frame_command(const EMCodecFrame*f){return f&&f->envelope.has_command()?(uint32_t)f->envelope.command():0;} uint64_t em_codec_frame_trace_id(const EMCodecFrame*f){return f?f->envelope.trace_id():0;}
const uint8_t*em_codec_frame_payload(const EMCodecFrame*f,size_t*n){if(n)*n=f?f->envelope.payload().size():0;return f&&!f->envelope.payload().empty()?(const uint8_t*)f->envelope.payload().data():NULL;}
const char*em_codec_frame_guid_app_key(const EMCodecFrame*f){return f&&f->envelope.has_guid()?f->envelope.guid().app_key().c_str():NULL;}
const char*em_codec_frame_guid_name(const EMCodecFrame*f){return f&&f->envelope.has_guid()?f->envelope.guid().name().c_str():NULL;}
const char*em_codec_frame_guid_domain(const EMCodecFrame*f){return f&&f->envelope.has_guid()?f->envelope.guid().domain().c_str():NULL;}
const char*em_codec_frame_guid_resource(const EMCodecFrame*f){return f&&f->envelope.has_guid()?f->envelope.guid().client_resource().c_str():NULL;}
uint32_t em_codec_frame_kind(const EMCodecFrame*f){return f?f->kind:EM_CODEC_FRAME_RAW;}
int32_t em_codec_frame_status_code(const EMCodecFrame*f){const Status*s=frame_status(f);return s?(int32_t)s->error_code():-1;}
const char*em_codec_frame_status_reason(const EMCodecFrame*f){const Status*s=frame_status(f);return s?s->reason().c_str():NULL;}
size_t em_codec_frame_redirect_count(const EMCodecFrame*f){const Status*s=frame_status(f);return s?(size_t)s->redirect_info_size():0;}
const char*em_codec_frame_redirect_host(const EMCodecFrame*f,size_t i){const Status*s=frame_status(f);return s&&i<(size_t)s->redirect_info_size()?s->redirect_info((int)i).host().c_str():NULL;}
uint32_t em_codec_frame_redirect_port(const EMCodecFrame*f,size_t i){const Status*s=frame_status(f);return s&&i<(size_t)s->redirect_info_size()?s->redirect_info((int)i).port():0;}
const char*em_codec_frame_session_id(const EMCodecFrame*f){if(!f)return NULL;if(f->kind==EM_CODEC_FRAME_PROVISION)return f->provision.session_id().c_str();if(f->kind==EM_CODEC_FRAME_LOGOUT)return f->logout.session_id().c_str();return NULL;}
const uint8_t*em_codec_frame_auth_token(const EMCodecFrame*f,size_t*n){const std::string*s=f&&f->kind==EM_CODEC_FRAME_PROVISION?&f->provision.auth_token():NULL;if(n)*n=s?s->size():0;return s&&!s->empty()?(const uint8_t*)s->data():NULL;}
uint64_t em_codec_frame_ack_client_id(const EMCodecFrame*f){return f?f->sync.meta_id():0;} uint64_t em_codec_frame_ack_server_id(const EMCodecFrame*f){return f?f->sync.server_id():0;}
uint64_t em_codec_frame_timestamp(const EMCodecFrame*f){if(!f)return 0;if(f->kind==EM_CODEC_FRAME_UNREAD)return f->unread.timestamp();return f->sync.timestamp();}
size_t em_codec_frame_unread_queue_count(const EMCodecFrame*f){return f&&f->kind==EM_CODEC_FRAME_UNREAD?(size_t)f->unread.unread_size():0;}
int em_codec_frame_unread_queue(const EMCodecFrame*f,size_t i,EMCodecJID*q,uint32_t*n){if(!f||f->kind!=EM_CODEC_FRAME_UNREAD||i>=(size_t)f->unread.unread_size())return 0;const MetaQueue&m=f->unread.unread((int)i);if(n)*n=m.n();return copy_jid(m.has_queue()?&m.queue():NULL,q);}
uint64_t em_codec_frame_next_key(const EMCodecFrame*f){return f?f->sync.next_key():0;} int em_codec_frame_is_last(const EMCodecFrame*f){return f&&f->sync.is_last();}
int em_codec_frame_queue(const EMCodecFrame*f,EMCodecJID*q){if(!f)return 0;if(f->kind==EM_CODEC_FRAME_NOTICE)return copy_jid(f->notice.has_queue()?&f->notice.queue():NULL,q);return copy_jid(f->sync.has_queue()?&f->sync.queue():NULL,q);}
size_t em_codec_frame_meta_count(const EMCodecFrame*f){return f?(size_t)f->sync.metas_size():0;}
uint64_t em_codec_meta_id(const EMCodecFrame*f,size_t i){const Meta*m=meta_at(f,i);return m?m->id():0;} uint64_t em_codec_meta_timestamp(const EMCodecFrame*f,size_t i){const Meta*m=meta_at(f,i);return m?m->timestamp():0;}
uint32_t em_codec_meta_namespace(const EMCodecFrame*f,size_t i){const Meta*m=meta_at(f,i);return m?(uint32_t)m->ns():UINT32_MAX;} uint32_t em_codec_meta_route_type(const EMCodecFrame*f,size_t i){const Meta*m=meta_at(f,i);return m?(uint32_t)m->routetype():0;}
const uint8_t*em_codec_meta_payload(const EMCodecFrame*f,size_t i,size_t*n){const Meta*m=meta_at(f,i);if(n)*n=m?m->payload().size():0;return m&&!m->payload().empty()?(const uint8_t*)m->payload().data():NULL;}
int em_codec_meta_from(const EMCodecFrame*f,size_t i,EMCodecJID*j){const Meta*m=meta_at(f,i);return copy_jid(m&&m->has_from()?&m->from():NULL,j);} int em_codec_meta_to(const EMCodecFrame*f,size_t i,EMCodecJID*j){const Meta*m=meta_at(f,i);return copy_jid(m&&m->has_to()?&m->to():NULL,j);}
size_t em_codec_meta_directed_user_count(const EMCodecFrame*f,size_t i){const Meta*m=meta_at(f,i);return m?(size_t)m->directed_users_size():0;} const char*em_codec_meta_directed_user(const EMCodecFrame*f,size_t i,size_t u){const Meta*m=meta_at(f,i);return m&&u<(size_t)m->directed_users_size()?m->directed_users((int)u).c_str():NULL;}
uint32_t em_codec_meta_message_type(const EMCodecFrame*f,size_t i){const ParsedMeta*p=parsed_at(f,i);return p&&p->has_message?(uint32_t)p->message.type():0;} size_t em_codec_meta_content_count(const EMCodecFrame*f,size_t i){const ParsedMeta*p=parsed_at(f,i);return p&&p->has_message?(size_t)p->message.contents_size():0;}
int em_codec_message_from(const EMCodecFrame*f,size_t i,EMCodecJID*j){const ParsedMeta*p=parsed_at(f,i);return copy_jid(p&&p->has_message&&p->message.has_from()?&p->message.from():NULL,j);} int em_codec_message_to(const EMCodecFrame*f,size_t i,EMCodecJID*j){const ParsedMeta*p=parsed_at(f,i);return copy_jid(p&&p->has_message&&p->message.has_to()?&p->message.to():NULL,j);}
uint32_t em_codec_content_type(const EMCodecFrame*f,size_t m,size_t c){const MessageBody_Content*x=content_at(f,m,c);return x?(uint32_t)x->type():UINT32_MAX;} const char*em_codec_content_text(const EMCodecFrame*f,size_t m,size_t c){const MessageBody_Content*x=content_at(f,m,c);return x?x->text().c_str():NULL;}
const char*em_codec_content_action(const EMCodecFrame*f,size_t m,size_t c){const MessageBody_Content*x=content_at(f,m,c);return x?x->action().c_str():NULL;} const char*em_codec_content_custom_event(const EMCodecFrame*f,size_t m,size_t c){const MessageBody_Content*x=content_at(f,m,c);return x?x->customevent().c_str():NULL;}
const uint8_t*em_codec_content_raw(const EMCodecFrame*f,size_t m,size_t c,size_t*n){const ParsedMeta*p=parsed_at(f,m);const std::string*s=p&&c<p->raw_contents.size()?&p->raw_contents[c]:NULL;if(n)*n=s?s->size():0;return s&&!s->empty()?(const uint8_t*)s->data():NULL;}
size_t em_codec_content_key_value_count(const EMCodecFrame*f,size_t m,size_t c){const MessageBody_Content*x=content_at(f,m,c);if(!x)return 0;return x->type()==MessageBody_Content_Type_COMMAND?(size_t)x->params_size():x->type()==MessageBody_Content_Type_CUSTOM?(size_t)x->customexts_size():0;}
size_t em_codec_meta_key_value_count(const EMCodecFrame*f,size_t m){const ParsedMeta*p=parsed_at(f,m);return p&&p->has_message?(size_t)p->message.ext_size():0;}
int em_codec_content_key_value(const EMCodecFrame*f,size_t m,size_t c,size_t i,EMCodecKeyValue*v){return fill_kv(content_kv(content_at(f,m,c),i),v);} int em_codec_meta_key_value(const EMCodecFrame*f,size_t m,size_t i,EMCodecKeyValue*v){const ParsedMeta*p=parsed_at(f,m);return fill_kv(p&&p->has_message&&i<(size_t)p->message.ext_size()?&p->message.ext((int)i):NULL,v);}
int32_t em_codec_meta_statistic_operation(const EMCodecFrame*f,size_t i){const ParsedMeta*p=parsed_at(f,i);return p&&p->has_statistic?(int32_t)p->statistic.operation():-1;} const char*em_codec_meta_statistic_device(const EMCodecFrame*f,size_t i){const ParsedMeta*p=parsed_at(f,i);return p&&p->has_statistic?p->statistic.replace_device_name().c_str():NULL;}
const char*em_codec_meta_statistic_reason(const EMCodecFrame*f,size_t i){const ParsedMeta*p=parsed_at(f,i);return p&&p->has_statistic?p->statistic.reason().c_str():NULL;} const char*em_codec_meta_statistic_session_id(const EMCodecFrame*f,size_t i){const ParsedMeta*p=parsed_at(f,i);return p&&p->has_statistic?p->statistic.session_id().c_str():NULL;}
}
