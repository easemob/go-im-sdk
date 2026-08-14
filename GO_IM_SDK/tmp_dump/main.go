package main

import (
	"fmt"
	"github.com/easemob/go-im-sdk/internal/protocol"
	"github.com/easemob/go-im-sdk/internal/protocol/gopb"
	"github.com/easemob/go-im-sdk/pb"
	"google.golang.org/protobuf/proto"
)

func main() {
	c := gopb.New()
	body := protocol.MessageBody{Kind: protocol.MessageGroupChat,
		From: protocol.JID{Name: "lxm"}, To: protocol.JID{Name: "322143519834113"},
		Contents: []protocol.Content{{Kind: protocol.ContentText, Text: "directed test"}}}
	payload, _ := c.EncodeMessageBody(body)
	meta := protocol.Meta{ID: 42, To: protocol.JID{Name: "322143519834113", Domain: "conference.easemob.com"}, Namespace: protocol.NamespaceChat, Payload: payload, Route: protocol.RouteDirect, DirectedUsers: []string{"lxm2"}}
	frame, _ := c.EncodeSync(protocol.SyncRequest{Meta: &meta})

	var m pb.MSync
	proto.Unmarshal(frame, &m)
	var ul pb.CommSyncUL
	proto.Unmarshal(m.GetPayload(), &ul)
	pm := ul.GetMeta()
	fmt.Printf("Meta.Id=%d Ns=%d Routetype=%d DirectedUsers=%v\n", pm.GetId(), pm.GetNs(), pm.GetRoutetype(), pm.GetDirectedUsers())
	fmt.Printf("Meta.To: name=%q domain=%q appkey=%q\n", pm.GetTo().GetName(), pm.GetTo().GetDomain(), pm.GetTo().GetAppKey())
	var mb pb.MessageBody
	proto.Unmarshal(pm.GetPayload(), &mb)
	fmt.Printf("Body.Type=%d From=%q To=%q\n", mb.GetType(), mb.GetFrom().GetName(), mb.GetTo().GetName())
	// 打印 Routetype 枚举值
	fmt.Printf("ROUTE_DIRECT 枚举值 = %d\n", pb.Meta_ROUTE_DIRECT)
	fmt.Printf("ROUTE_ALL 枚举值 = %d\n", pb.Meta_ROUTE_ALL)
}
