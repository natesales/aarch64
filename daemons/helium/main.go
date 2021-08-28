package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/fosshostorg/aarch64/daemons/internal/commons"
	"github.com/fosshostorg/aarch64/daemons/internal/message"
	"github.com/nsqio/go-nsq"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func handleMessage(m *nsq.Message) error {
	// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
	if len(m.Body) == 0 {
		return nil
	}

	var msg message.Message
	if err := json.Unmarshal(m.Body, &msg); err != nil {
		log.Printf("Invalid NSQ Message: %s\n", m.Body)
		return nil
		// No point in even returning an error if we can't unmarshal the message as we don't want that requeued.
	}

	// NSQ does not guarantee messages are not duplicated. We'll check
	if seenIDs[msg.ID] {
		log.Printf("Dropped duplicate message, ID %d\n", msg.ID) // No need to send this to the error logger, it's natural
		return nil
	}
	seenIDs[msg.ID] = true
	log.Printf("Received message: %s\n", m.Body)

	if msg.Action == message.NewVMState {
		objID, err := primitive.ObjectIDFromHex(msg.MessageData.Name)
		if err != nil {
			log.Println(err)
			return nil
		}

		_, err = vms_col.UpdateOne(
			ctx,
			bson.M{"_id": objID},
			bson.M{
				"$set": bson.M{
					"state": msg.MessageData.Num,
				},
			},
		)
	}
	return nil
}

// Let's define our variables needed through the program
var (
	seenIDs  = make(map[int64]bool)
	hostname string
	ctx      context.Context
	vms_col  *mongo.Collection
)

func main() {
	var nsqConnectURI, mongoConnectURI string
	flag.StringVar(&nsqConnectURI, "nsq-connect-uri", commons.NSQCoreUrl, "The URI for NSQ producers & consumers to connect to")
	flag.StringVar(&mongoConnectURI, "mongo-connect-uri", "mongodb://localhost/aarch64", "The URI for MongoDB to connect to")
	flag.Parse()

	ctx = context.Background()

	// Options to the database.
	clientOpts := options.Client().ApplyURI(mongoConnectURI)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		fmt.Println(err)
		return
	}
	mg_db := client.Database("aarch64")
	vms_col = mg_db.Collection("vms")

	// Set seenID to true so that packets without an ID get dropped
	seenIDs[0] = true

	// Time for NSQ
	hostControlConsumer := commons.CreateNSQConsumer(nsqConnectURI, "aarch64-power", "helium", nsq.HandlerFunc(handleMessage))
	defer hostControlConsumer.Stop()

	// Let's allow our queues to drain properly during shutdown.
	// We'll create a channel to listen for SIGINT (Ctrl+C) to signal
	// to our application to gracefully shutdown.
	shutdown := make(chan os.Signal, 2)
	signal.Notify(shutdown, syscall.SIGINT)

	// This is our main loop. It will continue to read off of our nsq
	// channel until either the consumer dies or our application is signaled
	// to stop.
	for {
		select {
		case <-hostControlConsumer.StopChan:
			return
		case <-shutdown:
			return
		}
	}
}
