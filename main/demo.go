package main

import (
	"context"
	"log"

	uuid "github.com/satori/go.uuid"
	protoreflect "google.golang.org/protobuf/reflect/protoreflect"
)

type User struct {
	ID    uuid.UUID
	Realm string
}

func person() protoreflect.ProtoMessage {
	return &Person{}
}

func main() {
	currentUser := User{
		ID:    uuid.NewV4(),
		Realm: "skytala",
	}
	p := Person{
		Name: "Tom22",
	}
	ctx := context.Background()
	s := NewProtoStoreFromEnv()
	store := s.Bind(ctx, &currentUser)

	id, err := store.Store(&p)
	if err != nil {
		log.Fatalf("could not insert: %v", err)
	}
	log.Printf("inserted new, with id: %s", id)

	persons := store.Filter(person,
		Eq("name", "Tom22"))

	for _, person := range persons {
		if p, ok := person.(*Person); ok {
			log.Printf("%s: %s", p.Id, p.Name)
		}
	}
	if len(persons) == 0 {
		log.Fatalf("Could not query filter persons (returned len(0))")
	}

	if p, ok := persons[0].(*Person); ok {
		foundPerson, ok := store.Get(person, p.Id)
		if ok {
			log.Printf("Found person by id: %v", foundPerson)
		} else {
			log.Fatalf("Person not found by id: %s", p.Id)
		}

		p.Name = "Updated name"
		id, err := store.Store(p)
		if err != nil {
			log.Fatalf("could not update: %v", err)
		}
		log.Printf("updated entry with id %s", id)
	}

}
