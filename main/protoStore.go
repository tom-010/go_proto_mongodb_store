package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"google.golang.org/protobuf/encoding/protojson"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

// ProtoStore is the gateway to the database and knows how to access
// it. Thus, it is a good way to do stuff like caching, pooling, etc.
// As we have to know via the user-realm, which database to connect to
// and need the current context of the caller, ProtoStore is not
// intended for direct usage. Rather, you call Bind() to bind it to a
// concrete user and context. You do this many times (for every request)
// as both have differenc lifecycles: ProtoStore is bound to the lifecycle
// of the application, BoundProtoStore to the lifecycle of a single
// request.
type ProtoStore struct {
	client *mongo.Client
}

func NewProtoStoreFromEnv() ProtoStore {
	host := os.Getenv("DB_HOST")
	port := os.Getenv("DB_PORT")
	protocol := os.Getenv("DB_PROTOCOL")
	user := os.Getenv("DB_USER")
	password := os.Getenv("DB_PASSWORD")
	s := protocol + "://" + user + ":" + password + "@" + host + ":" + port
	return NewProtoStore(s)
}

func NewProtoStore(dbConnectionString string) ProtoStore {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI("mongodb://localhost:27017").SetAuth(options.Credential{
		Username: "admin",
		Password: "admin",
	}))

	if err != nil {
		panic(err)
	}

	return ProtoStore{
		client: client,
	}
}

func (p *ProtoStore) Bind(context context.Context, user *User) BoundProtoStore {
	return BoundProtoStore{
		protoStore: p,
		ctx:        context,
		user:       user,
	}
}

// BoundProtoStore is bound to a current user and context of a request by a
// client and only uses the ProtoStore internally. It has access to all database
// stuff via the proto-stuff, but knows about the current user (and context) as
// well. The context is important, as if the user aborts the connection, we can
// also abort the database connection.
type BoundProtoStore struct {
	protoStore *ProtoStore
	ctx        context.Context
	user       *User
}

func (p *BoundProtoStore) Store(message protoreflect.ProtoMessage) (string, error) {

	doc := toMap(message)

	table := message.ProtoReflect().Descriptor().FullName()
	existingIdSet := false
	if id, ok := doc["id"]; ok {
		if idS, ok := id.(string); ok {
			objectId, err := primitive.ObjectIDFromHex(idS)
			if err != nil {
				log.Fatalf("could not create ObjectId from %s: %v", idS, err)
			}
			doc["_id"] = objectId
			existingIdSet = true
		} else {
			log.Fatalf("the current id is no string: %v", id)
		}
	}

	if !existingIdSet {
		doc["_id"] = primitive.NewObjectID()
	}

	doc["type"] = fmt.Sprintf("%s:%d", string(table), 1)
	doc["createdBy"] = p.user.ID

	opts := options.Update().SetUpsert(true)
	_, err := p.db(p.user.Realm).Collection(string(table)).UpdateByID(p.ctx, doc["_id"], bson.D{bson.E{Key: "$set", Value: doc}}, opts)
	if err != nil {
		log.Fatalf("Could not insert document: %v", err)
	}

	id := doc["_id"]

	if r, ok := id.(primitive.ObjectID); ok {
		return r.Hex(), nil
	}

	return "", fmt.Errorf("id was not of type []byte, but %v", id)
}

func (p *BoundProtoStore) Filter(model func() protoreflect.ProtoMessage, filters ...bson.D) []protoreflect.ProtoMessage {
	tableName := model().ProtoReflect().Descriptor().FullName()

	filter := bson.D{}
	if len(filters) > 1 { // a $and with Value: [] is always false
		filter = bson.D{bson.E{Key: "$and", Value: filters}}
	} else if len(filters) == 1 {
		filter = filters[0]
	}

	log.Println(filter)

	db := p.db(p.user.Realm)
	rows, err := db.Collection(string(tableName)).Find(p.ctx, filter)

	if err != nil {
		log.Fatalf("Could not read table %s: %v", tableName, err)
	}

	protoReader := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}

	res := make([]protoreflect.ProtoMessage, 0)
	var results []bson.M

	err = rows.All(p.ctx, &results)
	if err != nil {
		log.Fatalf("could do a .All call to mongodb: %v", err)
	}

	for _, doc := range results {
		doc["id"] = doc["_id"]
		jsonEncoded, err := json.Marshal(doc)
		if err != nil {
			log.Fatalf("Could not reencode json")
		}
		m := model()
		err = protoReader.Unmarshal(jsonEncoded, m)
		if err != nil {
			log.Fatalf("could not read protobuf message: %v", err)
		}
		res = append(res, m)
	}
	return res
}

func (p *BoundProtoStore) All(model func() protoreflect.ProtoMessage) []protoreflect.ProtoMessage {
	return p.Filter(model)
}

func (p *BoundProtoStore) Get(model func() protoreflect.ProtoMessage, id string) (protoreflect.ProtoMessage, bool) {

	oid, err := primitive.ObjectIDFromHex(id)
	if err != nil {
		log.Fatalf("Could not decode object-id %s: %v", id, err)
	}
	models := p.Filter(model, bson.D{bson.E{Key: "_id", Value: oid}})
	if len(models) < 1 {
		return nil, false
	}
	if len(models) > 1 {
		log.Fatalf("Found %d entries for unique id %s", len(models), id)
	}
	return models[0], true
}

// db returns the database with the given name. If it does not
// exist, it creates it on the fly.
func (p *BoundProtoStore) db(name string) *mongo.Database {
	db := p.protoStore.client.Database(name)
	return db
}

func toMap(message protoreflect.ProtoMessage) map[string]interface{} {
	encoded, err := protojson.Marshal(message)
	if err != nil {
		log.Fatalf("Could not encode proto-mesage: %v", err)
	}
	var res map[string]interface{}
	json.Unmarshal(encoded, &res)
	return res
}

func Eq(col string, value interface{}) bson.D {
	return bson.D{
		bson.E{Key: "$and",
			Value: bson.A{
				bson.D{
					bson.E{Key: col, Value: bson.D{bson.E{Key: "$eq", Value: value}}},
				},
			},
		},
	}
}
