package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/go-kivik/kivik/v3"
	uuid "github.com/satori/go.uuid"
	"google.golang.org/protobuf/encoding/protojson"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
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
	client *kivik.Client
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
	client, err := kivik.New("couch", dbConnectionString)
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

	var docId string = ""
	if val, ok := doc["id"]; ok {
		if id, ok := val.(string); ok {
			parts := strings.Split(id, ":")
			docId = parts[0]
			if len(parts) > 1 {
				doc["_rev"] = parts[1]
			}
		}
	}

	if docId == "" {
		docId = uuid.NewV4().String()
	}

	doc["id"] = docId
	doc["type"] = message.ProtoReflect().Descriptor().FullName()
	doc["typeVersion"] = 1
	doc["createdBy"] = p.user.ID
	rev, err := p.db(p.user.Realm).Put(p.ctx, docId, doc)
	return rev, err
}

func (p *BoundProtoStore) Filter(model func() protoreflect.ProtoMessage, filters ...map[string]interface{}) []protoreflect.ProtoMessage {
	tableName := model().ProtoReflect().Descriptor().FullName()

	selector := map[string]interface{}{
		"type": map[string]interface{}{
			"$eq": tableName,
		},
	}

	// merge in the filters
	for _, filter := range filters {
		for k, v := range filter {
			selector[k] = v
		}
	}

	query := map[string]interface{}{
		"selector": selector,
	}
	encoded, err := json.Marshal(query)

	if err != nil {
		log.Fatalf("could not encode query: %v", err)
	}
	rows, err := p.db(p.user.Realm).Find(p.ctx, encoded)
	if err != nil {
		log.Fatalf("Could not read table %s: %v", tableName, err)
	}

	protoReader := protojson.UnmarshalOptions{
		DiscardUnknown: true,
	}

	res := make([]protoreflect.ProtoMessage, 0)

	for rows.Next() {
		var doc map[string]interface{}
		if err := rows.ScanDoc(&doc); err != nil {
			panic(err)
		}

		doc["id"] = toIdWithRev(doc)

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

func toIdWithRev(doc map[string]interface{}) string {
	var res string
	if v, ok := doc["_id"].(string); ok {
		res += v
	}
	if v, ok := doc["_rev"].(string); ok {
		res += ":" + v
	}
	return res
}

func (p *BoundProtoStore) All(model func() protoreflect.ProtoMessage) []protoreflect.ProtoMessage {
	return p.Filter(model)
}

func (p *BoundProtoStore) Get(model func() protoreflect.ProtoMessage, id string) (protoreflect.ProtoMessage, bool) {

	models := p.Filter(model, Eq("id", strings.Split(id, ":")[0]))
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
func (p *BoundProtoStore) db(name string) *kivik.DB {
	db := p.protoStore.client.DB(p.ctx, name)
	if db.Err() != nil {
		err := p.protoStore.client.CreateDB(p.ctx, name)
		if err != nil {
			log.Fatalf("Could not create database %s: %v", name, err)
		}
	}
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

func Eq(col string, value interface{}) map[string]interface{} {
	return map[string]interface{}{
		col: map[string]interface{}{
			"$eq": value,
		},
	}
}
