package main

import (
	"compress/gzip"
	"context"
	crand "crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/felixge/fgprof"
	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var db *sqlx.DB

func main() {
	mux := setup()
	slog.Info("Listening on :8080")
	http.ListenAndServe(":8080", mux)
}

func setup() http.Handler {
	host := os.Getenv("ISUCON_DB_HOST")
	if host == "" {
		host = "127.0.0.1"
	}
	port := os.Getenv("ISUCON_DB_PORT")
	if port == "" {
		port = "3306"
	}
	_, err := strconv.Atoi(port)
	if err != nil {
		panic(fmt.Sprintf("failed to convert DB port number from ISUCON_DB_PORT environment variable into int: %v", err))
	}
	user := os.Getenv("ISUCON_DB_USER")
	if user == "" {
		user = "isucon"
	}
	password := os.Getenv("ISUCON_DB_PASSWORD")
	if password == "" {
		password = "isucon"
	}
	dbname := os.Getenv("ISUCON_DB_NAME")
	if dbname == "" {
		dbname = "isuride"
	}

	dbConfig := mysql.NewConfig()
	dbConfig.User = user
	dbConfig.Passwd = password
	dbConfig.Addr = net.JoinHostPort(host, port)
	dbConfig.Net = "tcp"
	dbConfig.DBName = dbname
	dbConfig.ParseTime = true
	dbConfig.InterpolateParams = true

	_db, err := sqlx.Connect("mysql", dbConfig.FormatDSN())
	if err != nil {
		panic(err)
	}
	db = _db

	// プール内に保持できるアイドル接続数の制限を設定 (default: 2)
	db.SetMaxIdleConns(1024)
	// 接続してから再利用できる最大期間
	db.SetConnMaxLifetime(0)
	// アイドル接続してから再利用できる最大期間
	db.SetConnMaxIdleTime(0)

	http.DefaultTransport.(*http.Transport).MaxIdleConns = 0           // default: 100
	http.DefaultTransport.(*http.Transport).MaxIdleConnsPerHost = 1024 // default: 2
	http.DefaultTransport.(*http.Transport).ForceAttemptHTTP2 = true
	http.DefaultClient.Timeout = 5 * time.Second // 問題の切り分け用

	{
		mux := chi.NewRouter()
		mux.Handle("/debug/log/httplog", NewTailHandler("/var/log/nginx/access.log"))
		mux.Handle("/debug/log/slowlog", NewTailHandler("/var/log/mysql/mysql-slow.log"))

		mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
		mux.Handle("/debug/pprof/cmdline", http.HandlerFunc(pprof.Cmdline))
		mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
		mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
		mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
		mux.Handle("/debug/fgprof", fgprof.Handler())
		go http.ListenAndServe(":3000", mux)
	}

	mux := chi.NewRouter()
	mux.Use(middleware.Logger)
	mux.Use(middleware.Recoverer)
	mux.HandleFunc("POST /api/initialize", postInitialize)

	// app handlers
	{
		mux.HandleFunc("POST /api/app/users", appPostUsers)

		authedMux := mux.With(appAuthMiddleware)
		authedMux.HandleFunc("POST /api/app/payment-methods", appPostPaymentMethods)
		authedMux.HandleFunc("GET /api/app/rides", appGetRides)
		authedMux.HandleFunc("POST /api/app/rides", appPostRides)
		authedMux.HandleFunc("POST /api/app/rides/estimated-fare", appPostRidesEstimatedFare)
		authedMux.HandleFunc("POST /api/app/rides/{ride_id}/evaluation", appPostRideEvaluatation)
		authedMux.HandleFunc("GET /api/app/notification", appGetNotification)
		authedMux.HandleFunc("GET /api/app/nearby-chairs", appGetNearbyChairs)
	}

	// owner handlers
	{
		mux.HandleFunc("POST /api/owner/owners", ownerPostOwners)

		authedMux := mux.With(ownerAuthMiddleware)
		authedMux.HandleFunc("GET /api/owner/sales", ownerGetSales)
		authedMux.HandleFunc("GET /api/owner/chairs", ownerGetChairs)
	}

	// chair handlers
	{
		mux.HandleFunc("POST /api/chair/chairs", chairPostChairs)

		authedMux := mux.With(chairAuthMiddleware)
		authedMux.HandleFunc("POST /api/chair/activity", chairPostActivity)
		authedMux.HandleFunc("POST /api/chair/coordinate", chairPostCoordinate)
		authedMux.HandleFunc("GET /api/chair/notification", chairGetNotification)
		authedMux.HandleFunc("POST /api/chair/rides/{ride_id}/status", chairPostRideStatus)
	}

	// internal handlers
	{
		mux.HandleFunc("GET /api/internal/matching", internalGetMatching)
	}

	return mux
}

type postInitializeRequest struct {
	PaymentServer string `json:"payment_server"`
}

type postInitializeResponse struct {
	Language string `json:"language"`
}

func postInitialize(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &postInitializeRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}

	if out, err := exec.Command("../sql/init.sh").CombinedOutput(); err != nil {
		writeError(w, http.StatusInternalServerError, fmt.Errorf("failed to initialize: %s: %w", string(out), err))
		return
	}
	
    // 各椅子の総移動距離を初期化
    if err := initializeChairTotalDistance(ctx); err != nil {
        writeError(w, http.StatusInternalServerError, err)
        return
    }

	if _, err := db.ExecContext(ctx, "UPDATE settings SET value = ? WHERE name = 'payment_gateway_url'", req.PaymentServer); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	go func() {
		if _, err := http.Get("http://57.180.38.84:9000/api/group/collect"); err != nil {
			log.Printf("failed to communicate with pprotein: %v", err)
		}
	}()

	writeJSON(w, http.StatusOK, postInitializeResponse{Language: "go"})
}

type Coordinate struct {
	Latitude  int `json:"latitude"`
	Longitude int `json:"longitude"`
}

func bindJSON(r *http.Request, v interface{}) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func writeJSON(w http.ResponseWriter, statusCode int, v interface{}) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	buf, err := json.Marshal(v)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(statusCode)
	w.Write(buf)
}

func writeError(w http.ResponseWriter, statusCode int, err error) {
	w.Header().Set("Content-Type", "application/json;charset=utf-8")
	w.WriteHeader(statusCode)
	buf, marshalError := json.Marshal(map[string]string{"message": err.Error()})
	if marshalError != nil {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte(`{"error":"marshaling error failed"}`))
		return
	}
	w.Write(buf)

	slog.Error("error response wrote", err)
}

func secureRandomStr(b int) string {
	k := make([]byte, b)
	if _, err := crand.Read(k); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%x", k)
}

type (
	TailHandler struct {
		filename string
	}
)

func NewTailHandler(filename string) *TailHandler {
	return &TailHandler{filename}
}

func (h *TailHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if err := h.serve(w, r); err != nil {
		log.Printf("serve failed: %v", err)
	}
}
func (h *TailHandler) serve(w http.ResponseWriter, r *http.Request) error {
	seconds, err := strconv.Atoi(r.URL.Query().Get("seconds"))
	if err != nil {
		seconds = 30
	}

	var output io.Writer = w
	if strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
		ew, err := gzip.NewWriterLevel(w, gzip.DefaultCompression)
		if err != nil {
			return fmt.Errorf("failed to initialize gzip writer: %w", err)
		}
		defer ew.Close()

		output = ew
		w.Header().Set("Content-Encoding", "gzip")
	}

	if err := h.tail(output, time.Duration(seconds)*time.Second); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		output.Write([]byte(err.Error()))

		return fmt.Errorf("failed to tail: %w", err)
	}

	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
	return nil
}

func (h *TailHandler) tail(w io.Writer, duration time.Duration) error {
	file, err := os.Open(h.filename)
	if err != nil {
		return fmt.Errorf("failed to open: %w", err)
	}
	defer file.Close()

	startPos, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek: %w", err)
	}

	time.Sleep(duration)

	finfo, err := file.Stat()
	if err != nil {
		return fmt.Errorf("failed to stat: %w", err)
	}

	r := io.LimitReader(file, finfo.Size()-startPos)
	if _, err := io.Copy(w, r); err != nil {
		return fmt.Errorf("failed to copy: %w", err)
	}
	return nil
}

func initializeChairTotalDistance(ctx context.Context) error {
	// 全ての位置情報を一度に取得
	var locations []ChairLocation
	if err := db.SelectContext(
		ctx,
		&locations,
		`SELECT * FROM chair_locations ORDER BY chair_id, created_at ASC`,
	); err != nil {
		return err
	}

	// chair_idごとに位置情報をグループ化
	chairLocations := make(map[string][]ChairLocation)
	for _, loc := range locations {
		chairLocations[loc.ChairID] = append(chairLocations[loc.ChairID], loc)
	}

	// バルクアップデート用のクエリを構築
	var values []string
	var args []interface{}
	
	for chairID, locs := range chairLocations {
		var totalDistance int
		for i := 1; i < len(locs); i++ {
			totalDistance += calculateDistance(
				locs[i].Latitude,
				locs[i].Longitude,
				locs[i-1].Latitude,
				locs[i-1].Longitude,
			)
		}

		var updatedAt time.Time
		var lastLatitude, lastLongitude int
		if len(locs) > 0 {
			updatedAt = locs[len(locs)-1].CreatedAt
			lastLatitude = locs[len(locs)-1].Latitude
			lastLongitude = locs[len(locs)-1].Longitude
		}

		values = append(values, "(?, ?, ?, ?, ?)")
		args = append(args, chairID, totalDistance, updatedAt, lastLatitude, lastLongitude)
	}

	// バルクアップデートのクエリを修正
	if len(values) > 0 {
		query := `
			UPDATE chairs 
			SET total_distance = CASE id 
				%s
				END,
			total_distance_updated_at = CASE id 
				%s
				END,
			last_latitude = CASE id
				%s
				END,
			last_longitude = CASE id
				%s
				END
			WHERE id IN (%s)`

		// WHENケースを構築
		var distanceCases []string
		var timestampCases []string
		var latitudeCases []string
		var longitudeCases []string
		var chairIDs []string
		
		for i := 0; i < len(args); i += 5 {
			chairID := args[i].(string)
			distanceCases = append(distanceCases, fmt.Sprintf("WHEN '%s' THEN ?", chairID))
			timestampCases = append(timestampCases, fmt.Sprintf("WHEN '%s' THEN ?", chairID))
			latitudeCases = append(latitudeCases, fmt.Sprintf("WHEN '%s' THEN ?", chairID))
			longitudeCases = append(longitudeCases, fmt.Sprintf("WHEN '%s' THEN ?", chairID))
			chairIDs = append(chairIDs, "'"+chairID+"'")
		}

		// 最終的なクエリを組み立て
		query = fmt.Sprintf(
			query,
			strings.Join(distanceCases, "\n"),
			strings.Join(timestampCases, "\n"),
			strings.Join(latitudeCases, "\n"),
			strings.Join(longitudeCases, "\n"),
			strings.Join(chairIDs, ","),
		)

		// パラメータを再構築
		var execArgs []interface{}
		for i := 0; i < len(args); i += 5 {
			execArgs = append(execArgs, args[i+1]) // distance
		}
		for i := 0; i < len(args); i += 5 {
			execArgs = append(execArgs, args[i+2]) // timestamp
		}
		for i := 0; i < len(args); i += 5 {
			execArgs = append(execArgs, args[i+3]) // last_latitude
		}
		for i := 0; i < len(args); i += 5 {
			execArgs = append(execArgs, args[i+4]) // last_longitude
		}

		if _, err := db.ExecContext(ctx, query, execArgs...); err != nil {
			return fmt.Errorf("failed to update chair distances: %w", err)
		}
	}

	return nil
}
