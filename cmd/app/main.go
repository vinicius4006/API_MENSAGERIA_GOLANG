package main

import (
	"database/sql"
	"encoding/json"
	"log"
	"net/http"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/go-chi/chi/v5"
	_ "github.com/go-sql-driver/mysql"
	"github.com/vinicius4006/imersao12-go-esquenta/internal/infra/akafka"
	"github.com/vinicius4006/imersao12-go-esquenta/internal/infra/repository"
	"github.com/vinicius4006/imersao12-go-esquenta/internal/infra/web"
	"github.com/vinicius4006/imersao12-go-esquenta/internal/usecase"
)

func main() {
	db, err := sql.Open("mysql", "root:root@tcp(host.docker.internal:3306)/products")
	if err != nil {
		log.Panic(err)
	}
	defer db.Close()

	repository := repository.NewProductRepositoryMysql(db)
	createProductUsecase := usecase.NewCreateProductUseCase(repository)
	listProductsUsecase := usecase.NewListProductsUseCase(repository)

	productHandlers := web.NewProductHandlers(createProductUsecase, listProductsUsecase)

	r := chi.NewRouter()
	r.Post("/products", productHandlers.CreateProductHandler)
	r.Get("/products", productHandlers.ListProductsHandler)

	go http.ListenAndServe(":8000", r)

	msgChan := make(chan *kafka.Message)

	go akafka.Consume([]string{"products"}, "host.docker.internal:9094", msgChan)

	for msg := range msgChan {
		dto := usecase.CreateProductInputDto{}
		err := json.Unmarshal(msg.Value, &dto)
		if err != nil {
			log.Println(err)
		}
		_, err = createProductUsecase.Execute(dto)
		if err != nil {
			log.Println(err)
		}

	}
}
