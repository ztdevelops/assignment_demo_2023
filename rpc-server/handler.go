package main

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"time"

	"github.com/TikTokTechImmersion/assignment_demo_2023/rpc-server/kitex_gen/rpc"
)

// IMServiceImpl implements the last service interface defined in the IDL.
type IMServiceImpl struct{}

func (s *IMServiceImpl) Send(ctx context.Context, req *rpc.SendRequest) (*rpc.SendResponse, error) {
	if err := validateRequest(req); err != nil {
		return nil, err
	}

	timestamp := time.Now().Unix()
	message := &Message{
		Message:   req.Message.GetText(),
		Sender:    req.Message.GetSender(),
		Timestamp: timestamp,
	}

	rid, err := getRoomId(req.Message.GetChat())
	if err != nil {
		return nil, err
	}

	err = redisClient.CreateMessage(ctx, rid, message)
	if err != nil {
		return nil, err
	}

	resp := rpc.NewSendResponse()
	resp.Code, resp.Msg = 0, "success"
	return resp, nil
}

func (s *IMServiceImpl) Pull(ctx context.Context, req *rpc.PullRequest) (*rpc.PullResponse, error) {
	rid, err := getRoomId(req.GetChat())
	if err != nil {
		return nil, err
	}

	limit := int64(req.GetLimit())
	start := req.GetCursor()
	end := start + limit

	messages, err := redisClient.RetrieveMessageByRoomId(ctx, rid, start, end, req.GetReverse())
	if err != nil {
		return nil, err
	}

	responseMessages := make([]*rpc.Message, 0)
	counter := int64(0)
	nextCursor := int64(0)
	hasMore := false

	for _, msg := range messages {
		if counter+1 > limit {
			hasMore = true
			nextCursor = end
			break
		}

		tmp := &rpc.Message{
			Chat:     req.GetChat(),
			Text:     msg.Message,
			Sender:   msg.Sender,
			SendTime: msg.Timestamp,
		}
		responseMessages = append(responseMessages, tmp)
		counter++
	}

	resp := rpc.NewPullResponse()
	resp.Messages = responseMessages
	resp.Code = 0
	resp.Msg = "success"
	resp.HasMore = &hasMore
	resp.NextCursor = &nextCursor

	return resp, nil
}

func areYouLucky() (int32, string) {
	if rand.Int31n(2) == 1 {
		return 0, "success"
	} else {
		return 500, "oops"
	}
}

func validateRequest(req *rpc.SendRequest) error {
	members := strings.Split(req.Message.Chat, ":")
	if len(members) != 2 {
		return fmt.Errorf("Invalid Chat ID %s, expected in the format of a1:a2", req.Message.GetChat())
	}

	member1, member2 := members[0], members[1]
	sender := req.Message.GetSender()
	if member1 != sender && member2 != sender {
		return fmt.Errorf("Sender %s cannot be found", &sender)
	}

	return nil
}

func getRoomId(chat string) (string, error) {
	var rid string

	chatToLower := strings.ToLower(chat)
	members := strings.Split(chatToLower, ":")
	if len(members) != 2 {
		return "", fmt.Errorf("Invalid Chat ID %s, expected in the format of a1:a2", chat)
	}

	member1, member2 := members[0], members[1]
	if member1 < member2 {
		rid = fmt.Sprintf("%s:%s", member1, member2)
	} else {
		rid = fmt.Sprintf("%s:%s", member2, member1)
	}

	return rid, nil
}
