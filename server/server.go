func (s *server) GetMessages(request *empty.Empty, stream chat.Chat_GetMessagesServer) error {
	client := NewRedisClient()

	pubsub := client.Subscribe(Channel)
	defer pubsub.Close()

	_, err := pubsub.Receive()
	if err != nil {
		log.Fatal(err)
	}

	ch := pubsub.Channel()

	// Consume messages.
	for msg := range ch {
		var message chat.Message
		err := json.Unmarshal([]byte(msg.Payload), &message)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(msg.Channel, msg.Payload)

		if err := stream.Send(&message); err != nil {
			return err
		}
	}

	return nil
}

func (s *server) PostMessage(ctx context.Context, request *chat.Message) (*chat.Result, error) {
	client := NewRedisClient()

	if request.GetCreatedAt() == nil {
		request.CreatedAt = ptypes.TimestampNow()
	}

	message, _ := json.Marshal(request)

	_ = client.Publish(Channel, message).Err()

	result := &chat.Result{
		Result: true,
	}

	return result, nil
}

func NewRedisClient() *redis.Client {
	client := redis.NewClient(&redis.Options{
		Addr:     "redis:6379",
		Password: "", // no password set
		DB:       0,  // use default DB
	})

	return client
}
