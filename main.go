package main

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/gorilla/mux"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
)

type Inventory struct {
	ProductID int `json:"product_id"`
	Quantity  int `json:"quantity"`
}

var db *pgxpool.Pool

func connectDB() {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env")
	}

	dbURL := os.Getenv("DATABASE_URL")
	pool, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatal("❌ Cannot connect to DB:", err)
	}
	db = pool
	log.Println("✅ Connected to PostgreSQL")
}

func getInventory(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	productID := vars["product_id"]

	var quantity int
	err := db.QueryRow(context.Background(),
		`SELECT quantity FROM inventories WHERE product_id = $1`, productID).Scan(&quantity)

	if err != nil {
		http.Error(w, "Product not found", http.StatusNotFound)
		return
	}

	resp := map[string]any{"available": quantity > 0, "quantity": quantity}
	json.NewEncoder(w).Encode(resp)
}

func createInventory(w http.ResponseWriter, r *http.Request) {
	var inv Inventory
	json.NewDecoder(r.Body).Decode(&inv)

	_, err := db.Exec(context.Background(),
		`INSERT INTO inventories (product_id, quantity, updated_at) VALUES ($1, $2, $3)`,
		inv.ProductID, inv.Quantity, time.Now())

	if err != nil {
		http.Error(w, "Insert failed", http.StatusBadRequest)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func updateInventory(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	productID := vars["product_id"]

	var inv Inventory
	json.NewDecoder(r.Body).Decode(&inv)

	_, err := db.Exec(context.Background(),
		`UPDATE inventories SET quantity = $1, updated_at = $2 WHERE product_id = $3`,
		inv.Quantity, time.Now(), productID)

	if err != nil {
		http.Error(w, "Update failed", http.StatusBadRequest)
		return
	}
}

// kafka
// struct event
type OrderCreatedEvent struct {
	OrderID   int     `json:"orderId"`
	UserID    int     `json:"userId"`
	ProductID int     `json:"productId"`
	Quantity  int     `json:"quantity"`
	Total     float64 `json:"total"`
}

type InventoryReservedEvent struct {
	OrderID int    `json:"orderId"`
	Status  string `json:"status"`
	Message string `json:"message"`
}

// Consumer Kafka
func consumeOrderCreated() {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "orders",
		GroupID: "inventory-service-group",
	})

	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			log.Println("❌ Kafka read error:", err)
			continue
		}

		var event OrderCreatedEvent
		if err := json.Unmarshal(m.Value, &event); err != nil {
			log.Println("❌ JSON unmarshal error:", err)
			continue
		}

		log.Printf("📥 Nhận OrderCreatedEvent: %+v\n", event)

		// Kiểm tra tồn kho
		var quantity int
		err = db.QueryRow(context.Background(),
			`SELECT quantity FROM inventories WHERE product_id=$1`, event.ProductID).Scan(&quantity)

		if err != nil {
			log.Println("❌ Sản phẩm không tồn tại:", event.ProductID)
			publishInventoryFailed(event.OrderID, "Product not found")
			continue
		}

		if quantity >= event.Quantity {
			// Giữ hàng (trừ tạm số lượng)
			_, err := db.Exec(context.Background(),
				`UPDATE inventories SET quantity = quantity - $1 WHERE product_id=$2`,
				event.Quantity, event.ProductID)
			if err != nil {
				publishInventoryFailed(event.OrderID, "Cannot reserve inventory")
				continue
			}
			log.Println("✅ Giữ hàng thành công cho order", event.OrderID)
			publishInventoryReserved(event.OrderID, "Reserved successfully")
		} else {
			log.Println("❌ Không đủ hàng cho order", event.OrderID)
			publishInventoryFailed(event.OrderID, "Not enough stock")
		}
	}
}

// Producer Kafka
var writerReserved *kafka.Writer
var writerFailed *kafka.Writer

func initKafkaWriters() {
	writerReserved = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "inventory-reserved",
	})
	writerFailed = kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{"localhost:9092"},
		Topic:   "inventory-failed",
	})
}

func publishInventoryReserved(orderID int, message string) {
	event := InventoryReservedEvent{OrderID: orderID, Status: "RESERVED", Message: message}
	data, _ := json.Marshal(event)
	_ = writerReserved.WriteMessages(context.Background(), kafka.Message{Value: data})
}

func publishInventoryFailed(orderID int, message string) {
	event := InventoryReservedEvent{OrderID: orderID, Status: "FAILED", Message: message}
	data, _ := json.Marshal(event)
	_ = writerFailed.WriteMessages(context.Background(), kafka.Message{Value: data})
}

func main() {

	start := time.Now()
	connectDB()

	initKafkaWriters()
	go consumeOrderCreated() // chạy background

	r := mux.NewRouter()
	r.Use(corsMiddleware) // ✅ Middleware CORS chạy trước mọi route

	r.HandleFunc("/inventory/{product_id}", getInventory).Methods("GET")
	r.HandleFunc("/inventory/{product_id}", updateInventory).Methods("PUT")
	r.HandleFunc("/inventory", createInventory).Methods("POST")

	r.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		enableCORS(w)
		http.Error(w, "404 Not Found", http.StatusNotFound)
	})

	// When ready:
	elapsed := time.Since(start)
	log.Printf("🚀 Startup complete in %s\n", elapsed)

	log.Println("📦 inventory-service running on :8086")
	log.Fatal(http.ListenAndServe(":8086", r))
}
