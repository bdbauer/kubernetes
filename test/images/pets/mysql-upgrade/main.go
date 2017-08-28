/*
Copyright 2017 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

type server struct {
	mux            *http.ServeMux
	readDBAddress  string
	writeDBAddress string
}

func newServer(readDBAddress, writeDBAddress string) (*server, error) {
	s := &server{
		mux:            http.NewServeMux(),
		readDBAddress:  readDBAddress,
		writeDBAddress: writeDBAddress,
	}

	s.mux.HandleFunc("/addName", func(w http.ResponseWriter, r *http.Request) {
		name := r.FormValue("name")
		if name == "" {
			http.Error(w, "name must be specified", http.StatusBadRequest)
			return
		}
		if err := s.writeName(name); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	s.mux.HandleFunc("/countNames", func(w http.ResponseWriter, r *http.Request) {
		numNames, err := s.countNames()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(numNames)
	})

	s.mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	return s, nil
}

func (s *server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	s.mux.ServeHTTP(w, r)
}

func (s *server) createDBConnection(read bool) (*sql.DB, error) {
	addr := s.writeDBAddress
	if read {
		addr = s.readDBAddress
	}
	db, err := sql.Open("mysql", addr)
	if err != nil {
		return nil, err
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}
	return db, nil

}

func (s *server) writeName(name string) error {
	db, err := s.createDBConnection(false)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec("INSERT INTO testing.users (name) VALUEs(?);", name)
	return err
}

func (s *server) countNames() (int, error) {
	db, err := s.createDBConnection(true)
	if err != nil {
		return 0, err
	}
	defer db.Close()

	var count int
	err = db.QueryRow("SELECT COUNT(*) FROM testing.users;").Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (s *server) setupDatabase() error {
	db, err := s.createDBConnection(false)
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec("CREATE DATABASE IF NOT EXISTS testing;")
	if err != nil {
		return err
	}
	_, err = db.Exec("CREATE TABLE IF NOT EXISTS testing.users (name VARCHAR(15) NOT NULL, PRIMARY KEY (name));")
	if err != nil {
		return err
	}
	return nil
}

func main() {
	stop := make(chan os.Signal)
	signal.Notify(stop, syscall.SIGTERM)
	s, err := newServer("root@tcp(mysql-read:3306)/", "root@tcp(mysql-0.mysql:3306)/")
	if err != nil {
		log.Fatalf("could not create a server: %v", err)
	}

	if err := s.setupDatabase(); err != nil {
		log.Fatalf("could not set up database: %v", err)
	}

	h := &http.Server{Handler: s, Addr: ":8080"}
	go func() {
		log.Println("Serving on 0.0.0.0:8080")
		if err := h.ListenAndServe(); err != nil {
			log.Fatal(err)
		}
	}()

	// Wait for SIGTERM.
	<-stop

	time.Sleep(15 * time.Second)

	log.Println("Entering lame duck mode")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	h.Shutdown(ctx)

	log.Println("Gracefully shutdown")
}
