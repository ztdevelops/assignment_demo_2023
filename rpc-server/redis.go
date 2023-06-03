package main

import (
	"context"
	"encoding/json"

	"github.com/redis/go-redis/v9"
)

type RedisClient struct {
	cli *redis.Client
}

func (c *RedisClient) Initialise(ctx context.Context, address string, password string) error {
	r := redis.NewClient(&redis.Options{
		Addr:     address,
		Password: password,
		DB:       0,
	})

	if err := r.Ping(ctx).Err(); err != nil {
		return err
	}

	c.cli = r
	return nil
}

func (c *RedisClient) CreateMessage(ctx context.Context, rid string, message *Message) error {
	msg, err := json.Marshal(message)
	if err != nil {
		return err
	}

	member := &redis.Z{
		Member: msg,
		Score:  float64(message.Timestamp),
	}

	_, err = c.cli.ZAdd(ctx, rid, *member).Result()
	return err
}

func (c *RedisClient) RetrieveMessageByRoomId(ctx context.Context, rid string, start int64, end int64, reverse bool) ([]*Message, error) {
	var (
		raw      []string
		messages []*Message
		err      error
	)

	if reverse {
		raw, err = c.cli.ZRevRange(ctx, rid, start, end).Result()
	} else {
		raw, err = c.cli.ZRange(ctx, rid, start, end).Result()
	}

	if err != nil {
		return nil, err
	}

	for _, msg := range raw {
		tmp := &Message{}
		err := json.Unmarshal([]byte(msg), tmp)
		if err != nil {
			return nil, err
		}

		messages = append(messages, tmp)
	}

	return messages, nil
}
