package mygotg

import (
	"context"
	"fmt"

	"github.com/gotd/td/telegram/updates"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/krau/mygotg/storage"
	"go.uber.org/zap"
)

type channelRecoveryAPI struct {
	updates.API
	hasher *storage.AccessHasher
	selfID int64
	logger *zap.Logger
}

func (a channelRecoveryAPI) UpdatesGetChannelDifference(ctx context.Context, request *tg.UpdatesGetChannelDifferenceRequest) (tg.UpdatesChannelDifferenceClass, error) {
	channel, ok := request.Channel.(*tg.InputChannel)
	if !ok {
		return a.API.UpdatesGetChannelDifference(ctx, request)
	}
	hash, found, err := a.hasher.GetChannelAccessHash(ctx, a.selfID, channel.ChannelID)
	if err != nil {
		return nil, fmt.Errorf("load channel credentials: %w", err)
	}
	if found && hash != channel.AccessHash {
		replacement := *channel
		replacement.AccessHash = hash
		updated := *request
		updated.Channel = &replacement
		request, channel = &updated, &replacement
	}
	diff, err := a.API.UpdatesGetChannelDifference(ctx, request)
	if !tgerr.Is(err, "CHANNEL_INVALID") {
		return diff, err
	}
	hash, found, lookupErr := a.hasher.GetChannelAccessHash(ctx, a.selfID, channel.ChannelID)
	if lookupErr != nil {
		return nil, fmt.Errorf("load channel credentials after %v: %w", err, lookupErr)
	}
	if found && hash != channel.AccessHash {
		replacement := *channel
		replacement.AccessHash = hash
		retry := *request
		retry.Channel = &replacement
		diff, err = a.API.UpdatesGetChannelDifference(ctx, &retry)
		if !tgerr.Is(err, "CHANNEL_INVALID") {
			return diff, err
		}
		channel = &replacement
	}
	if invalidateErr := a.hasher.InvalidateChannelAccessHash(ctx, channel.ChannelID, channel.AccessHash); invalidateErr != nil {
		return nil, fmt.Errorf("invalidate channel credentials after %v: %w", err, invalidateErr)
	}
	if a.logger != nil {
		a.logger.Warn("Channel recovery paused until fresh peer metadata is available",
			zap.Int64("channel_id", channel.ChannelID), zap.Error(err))
	}
	// gotd stops channel workers on CHANNEL_PRIVATE; invalid credentials need the same lifecycle.
	return nil, fmt.Errorf("channel credentials rejected (%v): %w", err, tgerr.New(400, "CHANNEL_PRIVATE"))
}
