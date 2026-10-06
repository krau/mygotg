package functions_test

import (
	"context"
	"testing"

	"github.com/gotd/td/bin"
	"github.com/gotd/td/tg"
	"github.com/krau/mygotg/functions"
	"github.com/krau/mygotg/storage"
)

type adminTitleInvoker struct {
	title string
}

func (i *adminTitleInvoker) Invoke(_ context.Context, input bin.Encoder, output bin.Decoder) error {
	var requestBuffer bin.Buffer
	if err := input.Encode(&requestBuffer); err != nil {
		return err
	}
	var request tg.ChannelsEditAdminRequest
	if err := request.Decode(&requestBuffer); err != nil {
		return err
	}
	if title, present := request.GetRank(); present {
		i.title = title
	}

	var responseBuffer bin.Buffer
	if err := (&tg.Updates{}).Encode(&responseBuffer); err != nil {
		return err
	}
	return output.Decode(&responseBuffer)
}

func TestEditAdminHelpersSetTitle(t *testing.T) {
	for _, helper := range []struct {
		name string
		call func(context.Context, *tg.Client, *storage.Peer, *storage.Peer, tg.ChatAdminRights, string) (bool, error)
	}{
		{name: "promote", call: functions.PromoteChatMember},
		{name: "demote", call: functions.DemoteChatMember},
	} {
		for _, title := range []struct {
			name  string
			value string
		}{
			{name: "clear", value: ""},
			{name: "replace", value: "Moderator"},
		} {
			t.Run(helper.name+"/"+title.name, func(t *testing.T) {
				invoker := &adminTitleInvoker{title: "Existing title"}
				chat := &storage.Peer{ID: 101, AccessHash: 202}
				user := &storage.Peer{ID: 303, AccessHash: 404}
				rights := tg.ChatAdminRights{DeleteMessages: true}
				ok, err := helper.call(context.Background(), tg.NewClient(invoker), chat, user, rights, title.value)
				if err != nil || !ok {
					t.Fatalf("edit admin: ok=%v, err=%v", ok, err)
				}
				if invoker.title != title.value {
					t.Errorf("server title = %q, want %q", invoker.title, title.value)
				}
			})
		}
	}
}
