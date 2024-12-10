package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"strconv"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/oklog/ulid/v2"
)

type appPostUsersRequest struct {
	Username       string  `json:"username"`
	FirstName      string  `json:"firstname"`
	LastName       string  `json:"lastname"`
	DateOfBirth    string  `json:"date_of_birth"`
	InvitationCode *string `json:"invitation_code"`
}

type appPostUsersResponse struct {
	ID             string `json:"id"`
	InvitationCode string `json:"invitation_code"`
}

func appPostUsers(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostUsersRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Username == "" || req.FirstName == "" || req.LastName == "" || req.DateOfBirth == "" {
		writeError(w, http.StatusBadRequest, errors.New("required fields(username, firstname, lastname, date_of_birth) are empty"))
		return
	}

	userID := ulid.Make().String()
	accessToken := secureRandomStr(32)
	invitationCode := secureRandomStr(15)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO users (id, username, firstname, lastname, date_of_birth, access_token, invitation_code) VALUES (?, ?, ?, ?, ?, ?, ?)",
		userID, req.Username, req.FirstName, req.LastName, req.DateOfBirth, accessToken, invitationCode,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 初回登録キャンペーンのクーポンを付与
	_, err = tx.ExecContext(
		ctx,
		"INSERT INTO coupons (user_id, code, discount) VALUES (?, ?, ?)",
		userID, "CP_NEW2024", 3000,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 招待コードを使った登録
	if req.InvitationCode != nil && *req.InvitationCode != "" {
		// 招待する側の招待数をチェック
		var coupons []Coupon
		err = tx.SelectContext(ctx, &coupons, "SELECT * FROM coupons WHERE code = ? FOR UPDATE", "INV_"+*req.InvitationCode)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		if len(coupons) >= 3 {
			writeError(w, http.StatusBadRequest, errors.New("この招待コードは使用できません。"))
			return
		}

		// ユーザーチェック
		var inviter User
		err = tx.GetContext(ctx, &inviter, "SELECT * FROM users WHERE invitation_code = ?", *req.InvitationCode)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusBadRequest, errors.New("この招待コードは使用できません。"))
				return
			}
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		// 招待クーポン付与
		_, err = tx.ExecContext(
			ctx,
			"INSERT INTO coupons (user_id, code, discount) VALUES (?, ?, ?)",
			userID, "INV_"+*req.InvitationCode, 1500,
		)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		// 招待した人にもRewardを付与
		_, err = tx.ExecContext(
			ctx,
			"INSERT INTO coupons (user_id, code, discount) VALUES (?, CONCAT(?, '_', FLOOR(UNIX_TIMESTAMP(NOW(3))*1000)), ?)",
			inviter.ID, "RWD_"+*req.InvitationCode, 1000,
		)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	http.SetCookie(w, &http.Cookie{
		Path:  "/",
		Name:  "app_session",
		Value: accessToken,
	})

	writeJSON(w, http.StatusCreated, &appPostUsersResponse{
		ID:             userID,
		InvitationCode: invitationCode,
	})
}

type appPostPaymentMethodsRequest struct {
	Token string `json:"token"`
}

func appPostPaymentMethods(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostPaymentMethodsRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Token == "" {
		writeError(w, http.StatusBadRequest, errors.New("token is required but was empty"))
		return
	}

	user := ctx.Value("user").(*User)

	_, err := db.ExecContext(
		ctx,
		`INSERT INTO payment_tokens (user_id, token) VALUES (?, ?)`,
		user.ID,
		req.Token,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	w.WriteHeader(http.StatusNoContent)
}

type getAppRidesResponse struct {
	Rides []getAppRidesResponseItem `json:"rides"`
}

type getAppRidesResponseItem struct {
	ID                    string                       `json:"id"`
	PickupCoordinate      Coordinate                   `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate                   `json:"destination_coordinate"`
	Chair                 getAppRidesResponseItemChair `json:"chair"`
	Fare                  int                          `json:"fare"`
	Evaluation            int                          `json:"evaluation"`
	RequestedAt           int64                        `json:"requested_at"`
	CompletedAt           int64                        `json:"completed_at"`
}

type getAppRidesResponseItemChair struct {
	ID    string `json:"id"`
	Owner string `json:"owner"`
	Name  string `json:"name"`
	Model string `json:"model"`
}

func appGetRides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := ctx.Value("user").(*User)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	rides := []Ride{}
	if err := tx.SelectContext(
		ctx,
		&rides,
		`SELECT * FROM rides WHERE user_id = ? ORDER BY created_at DESC`,
		user.ID,
	); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if len(rides) == 0 {
		if err := tx.Commit(); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, &getAppRidesResponse{Rides: []getAppRidesResponseItem{}})
		return
	}

	// 最新ステータスを一括取得
	rideIDs := make([]string, 0, len(rides))
	for _, ride := range rides {
		rideIDs = append(rideIDs, ride.ID)
	}

	query, args, err := sqlx.In(`
		SELECT rs.ride_id, rs.status FROM ride_statuses rs
		INNER JOIN (
			SELECT ride_id, MAX(created_at) as max_created 
			FROM ride_statuses 
			WHERE ride_id IN (?) 
			GROUP BY ride_id
		) t ON rs.ride_id = t.ride_id AND rs.created_at = t.max_created
	`, rideIDs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	query = tx.Rebind(query)

	type latestStatusRow struct {
		RideID string `db:"ride_id"`
		Status string `db:"status"`
	}
	latestStatuses := []latestStatusRow{}
	if err := tx.SelectContext(ctx, &latestStatuses, query, args...); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	statusMap := make(map[string]string, len(latestStatuses))
	for _, s := range latestStatuses {
		statusMap[s.RideID] = s.Status
	}

	// COMPLETEDなライドのみ残す
	completedRides := []Ride{}
	for _, ride := range rides {
		if statusMap[ride.ID] == "COMPLETED" {
			completedRides = append(completedRides, ride)
		}
	}

	if len(completedRides) == 0 {
		if err := tx.Commit(); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		writeJSON(w, http.StatusOK, &getAppRidesResponse{Rides: []getAppRidesResponseItem{}})
		return
	}

	// Chair一括取得
	chairIDs := []string{}
	for _, ride := range completedRides {
		if ride.ChairID.Valid {
			chairIDs = append(chairIDs, ride.ChairID.String)
		}
	}

	chairMap := make(map[string]*Chair)
	ownerIDs := []string{}
	if len(chairIDs) > 0 {
		queryChairs, argsChairs, err := sqlx.In(`SELECT * FROM chairs WHERE id IN (?)`, chairIDs)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		queryChairs = tx.Rebind(queryChairs)

		chairs := []Chair{}
		if err := tx.SelectContext(ctx, &chairs, queryChairs, argsChairs...); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		for _, c := range chairs {
			chairMap[c.ID] = &c
			ownerIDs = append(ownerIDs, c.OwnerID)
		}
	}

	// Owner一括取得
	ownerMap := make(map[string]*Owner)
	if len(ownerIDs) > 0 {
		queryOwners, argsOwners, err := sqlx.In(`SELECT * FROM owners WHERE id IN (?)`, ownerIDs)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		queryOwners = tx.Rebind(queryOwners)

		owners := []Owner{}
		if err := tx.SelectContext(ctx, &owners, queryOwners, argsOwners...); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		for _, o := range owners {
			ownerMap[o.ID] = &o
		}
	}

	fares, err := calculateFaresForRides(ctx, tx, user.ID, completedRides)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	items := []getAppRidesResponseItem{}
	for _, ride := range completedRides {
		fare := fares[ride.ID]
		item := getAppRidesResponseItem{
			ID:                    ride.ID,
			PickupCoordinate:      Coordinate{Latitude: ride.PickupLatitude, Longitude: ride.PickupLongitude},
			DestinationCoordinate: Coordinate{Latitude: ride.DestinationLatitude, Longitude: ride.DestinationLongitude},
			Fare:                  fare,
			Evaluation:            *ride.Evaluation,
			RequestedAt:           ride.CreatedAt.UnixMilli(),
			CompletedAt:           ride.UpdatedAt.UnixMilli(),
		}

		if ride.ChairID.Valid {
			if c, ok := chairMap[ride.ChairID.String]; ok {
				item.Chair.ID = c.ID
				item.Chair.Name = c.Name
				item.Chair.Model = c.Model
				if o, ok := ownerMap[c.OwnerID]; ok {
					item.Chair.Owner = o.Name
				}
			}
		}

		items = append(items, item)
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &getAppRidesResponse{
		Rides: items,
	})
}

type appPostRidesRequest struct {
	PickupCoordinate      *Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate *Coordinate `json:"destination_coordinate"`
}

type appPostRidesResponse struct {
	RideID string `json:"ride_id"`
	Fare   int    `json:"fare"`
}

type executableGet interface {
	Get(dest interface{}, query string, args ...interface{}) error
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

func getLatestRideStatus(ctx context.Context, tx executableGet, rideID string) (string, error) {
	status := ""
	if err := tx.GetContext(ctx, &status, `SELECT status FROM ride_statuses WHERE ride_id = ? ORDER BY created_at DESC LIMIT 1`, rideID); err != nil {
		return "", err
	}
	return status, nil
}

func appPostRides(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostRidesRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.PickupCoordinate == nil || req.DestinationCoordinate == nil {
		writeError(w, http.StatusBadRequest, errors.New("required fields(pickup_coordinate, destination_coordinate) are empty"))
		return
	}

	user := ctx.Value("user").(*User)
	rideID := ulid.Make().String()

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	rides := []Ride{}
	if err := tx.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE user_id = ?`, user.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// ここで rides から rideIDs を抽出して一括で最新ステータスを取得する
	rideIDs := make([]string, len(rides))
	for i, ride := range rides {
		rideIDs[i] = ride.ID
	}

	statusMap := make(map[string]string)
	if len(rideIDs) > 0 {
		query, args, err := sqlx.In(`
			SELECT rs.ride_id, rs.status FROM ride_statuses rs
			INNER JOIN (
				SELECT ride_id, MAX(created_at) as max_created
				FROM ride_statuses
				WHERE ride_id IN (?)
				GROUP BY ride_id
			) t ON rs.ride_id = t.ride_id AND rs.created_at = t.max_created
		`, rideIDs)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		query = tx.Rebind(query)
		type latestStatusRow struct {
			RideID string `db:"ride_id"`
			Status string `db:"status"`
		}
		latestStatuses := []latestStatusRow{}
		if err := tx.SelectContext(ctx, &latestStatuses, query, args...); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		for _, s := range latestStatuses {
			statusMap[s.RideID] = s.Status
		}
	}

	// 継続中ライド数を計算
	continuingRideCount := 0
	for _, ride := range rides {
		status := statusMap[ride.ID]
		if status != "COMPLETED" && status != "" {
			continuingRideCount++
		}
	}

	if continuingRideCount > 0 {
		writeError(w, http.StatusConflict, errors.New("ride already exists"))
		return
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO rides (id, user_id, pickup_latitude, pickup_longitude, destination_latitude, destination_longitude)
				  VALUES (?, ?, ?, ?, ?, ?)`,
		rideID, user.ID, req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude,
	); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if _, err := tx.ExecContext(
		ctx,
		`INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)`,
		ulid.Make().String(), rideID, "MATCHING",
	); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	var rideCount int
	if err := tx.GetContext(ctx, &rideCount, `SELECT COUNT(*) FROM rides WHERE user_id = ? `, user.ID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	var coupon Coupon
	if rideCount == 1 {
		// 初回利用で、初回利用クーポンがあれば必ず使う
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL FOR UPDATE", user.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusInternalServerError, err)
				return
			}

			// 無ければ他のクーポンを付与された順番に使う
			if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE", user.ID); err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
			} else {
				if _, err := tx.ExecContext(
					ctx,
					"UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = ?",
					rideID, user.ID, coupon.Code,
				); err != nil {
					writeError(w, http.StatusInternalServerError, err)
					return
				}
			}
		} else {
			if _, err := tx.ExecContext(
				ctx,
				"UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = 'CP_NEW2024'",
				rideID, user.ID,
			); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		}
	} else {
		// 他のクーポンを付与された順番に使う
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1 FOR UPDATE", user.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		} else {
			if _, err := tx.ExecContext(
				ctx,
				"UPDATE coupons SET used_by = ? WHERE user_id = ? AND code = ?",
				rideID, user.ID, coupon.Code,
			); err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		}
	}

	ride := Ride{}
	if err := tx.GetContext(ctx, &ride, "SELECT * FROM rides WHERE id = ?", rideID); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	fare, err := calculateDiscountedFare(ctx, tx, user.ID, &ride, req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusAccepted, &appPostRidesResponse{
		RideID: rideID,
		Fare:   fare,
	})
}

type appPostRidesEstimatedFareRequest struct {
	PickupCoordinate      *Coordinate `json:"pickup_coordinate"`
	DestinationCoordinate *Coordinate `json:"destination_coordinate"`
}

type appPostRidesEstimatedFareResponse struct {
	Fare     int `json:"fare"`
	Discount int `json:"discount"`
}

func appPostRidesEstimatedFare(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	req := &appPostRidesEstimatedFareRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.PickupCoordinate == nil || req.DestinationCoordinate == nil {
		writeError(w, http.StatusBadRequest, errors.New("required fields(pickup_coordinate, destination_coordinate) are empty"))
		return
	}

	user := ctx.Value("user").(*User)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	discounted, err := calculateDiscountedFare(ctx, tx, user.ID, nil, req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &appPostRidesEstimatedFareResponse{
		Fare:     discounted,
		Discount: calculateFare(req.PickupCoordinate.Latitude, req.PickupCoordinate.Longitude, req.DestinationCoordinate.Latitude, req.DestinationCoordinate.Longitude) - discounted,
	})
}

// マンハッタン距離を求める
func calculateDistance(aLatitude, aLongitude, bLatitude, bLongitude int) int {
	return abs(aLatitude-bLatitude) + abs(aLongitude-bLongitude)
}
func abs(a int) int {
	if a < 0 {
		return -a
	}
	return a
}

type appPostRideEvaluationRequest struct {
	Evaluation int `json:"evaluation"`
}

type appPostRideEvaluationResponse struct {
	CompletedAt int64 `json:"completed_at"`
}

func appPostRideEvaluatation(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	rideID := r.PathValue("ride_id")

	req := &appPostRideEvaluationRequest{}
	if err := bindJSON(r, req); err != nil {
		writeError(w, http.StatusBadRequest, err)
		return
	}
	if req.Evaluation < 1 || req.Evaluation > 5 {
		writeError(w, http.StatusBadRequest, errors.New("evaluation must be between 1 and 5"))
		return
	}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE id = ?`, rideID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, errors.New("ride not found"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	status, err := getLatestRideStatus(ctx, tx, ride.ID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if status != "ARRIVED" {
		writeError(w, http.StatusBadRequest, errors.New("not arrived yet"))
		return
	}

	result, err := tx.ExecContext(
		ctx,
		`UPDATE rides SET evaluation = ? WHERE id = ?`,
		req.Evaluation, rideID)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	if count, err := result.RowsAffected(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	} else if count == 0 {
		writeError(w, http.StatusNotFound, errors.New("ride not found"))
		return
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO ride_statuses (id, ride_id, status) VALUES (?, ?, ?)`,
		ulid.Make().String(), rideID, "COMPLETED")
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE id = ?`, rideID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, errors.New("ride not found"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	paymentToken := &PaymentToken{}
	if err := tx.GetContext(ctx, paymentToken, `SELECT * FROM payment_tokens WHERE user_id = ?`, ride.UserID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusBadRequest, errors.New("payment token not registered"))
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	fare, err := calculateDiscountedFare(ctx, tx, ride.UserID, ride, ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	paymentGatewayRequest := &paymentGatewayPostPaymentRequest{
		Amount: fare,
	}

	var paymentGatewayURL string
	if err := tx.GetContext(ctx, &paymentGatewayURL, "SELECT value FROM settings WHERE name = 'payment_gateway_url'"); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := requestPaymentGatewayPostPayment(ctx, paymentGatewayURL, paymentToken.Token, paymentGatewayRequest, func() ([]Ride, error) {
		rides := []Ride{}
		if err := tx.SelectContext(ctx, &rides, `SELECT * FROM rides WHERE user_id = ? ORDER BY created_at ASC`, ride.UserID); err != nil {
			return nil, err
		}
		return rides, nil
	}); err != nil {
		if errors.Is(err, erroredUpstream) {
			writeError(w, http.StatusBadGateway, err)
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &appPostRideEvaluationResponse{
		CompletedAt: ride.UpdatedAt.UnixMilli(),
	})
}

type appGetNotificationResponse struct {
	Data         *appGetNotificationResponseData `json:"data"`
	RetryAfterMs int                             `json:"retry_after_ms"`
}

type appGetNotificationResponseData struct {
	RideID                string                           `json:"ride_id"`
	PickupCoordinate      Coordinate                       `json:"pickup_coordinate"`
	DestinationCoordinate Coordinate                       `json:"destination_coordinate"`
	Fare                  int                              `json:"fare"`
	Status                string                           `json:"status"`
	Chair                 *appGetNotificationResponseChair `json:"chair,omitempty"`
	CreatedAt             int64                            `json:"created_at"`
	UpdateAt              int64                            `json:"updated_at"`
}

type appGetNotificationResponseChair struct {
	ID    string                               `json:"id"`
	Name  string                               `json:"name"`
	Model string                               `json:"model"`
	Stats appGetNotificationResponseChairStats `json:"stats"`
}

type appGetNotificationResponseChairStats struct {
	TotalRidesCount    int     `json:"total_rides_count"`
	TotalEvaluationAvg float64 `json:"total_evaluation_avg"`
}

func appGetNotification(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	user := ctx.Value("user").(*User)

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	ride := &Ride{}
	if err := tx.GetContext(ctx, ride, `SELECT * FROM rides WHERE user_id = ? ORDER BY created_at DESC LIMIT 1`, user.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeJSON(w, http.StatusOK, &appGetNotificationResponse{
				// 状態変更から3秒以内に通知されている必要があるため、2秒後にリトライする
				// see: https://gist.github.com/wtks/8eadf471daf7cb59942de02273ce7884#通知エンドポイント
				RetryAfterMs: 100,
			})
			return
		}
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	yetSentRideStatus := RideStatus{}
	status := ""
	if err := tx.GetContext(ctx, &yetSentRideStatus, `SELECT * FROM ride_statuses WHERE ride_id = ? AND app_sent_at IS NULL ORDER BY created_at ASC LIMIT 1`, ride.ID); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			status, err = getLatestRideStatus(ctx, tx, ride.ID)
			if err != nil {
				writeError(w, http.StatusInternalServerError, err)
				return
			}
		} else {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	} else {
		status = yetSentRideStatus.Status
	}

	fare, err := calculateDiscountedFare(ctx, tx, user.ID, ride, ride.PickupLatitude, ride.PickupLongitude, ride.DestinationLatitude, ride.DestinationLongitude)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	response := &appGetNotificationResponse{
		Data: &appGetNotificationResponseData{
			RideID: ride.ID,
			PickupCoordinate: Coordinate{
				Latitude:  ride.PickupLatitude,
				Longitude: ride.PickupLongitude,
			},
			DestinationCoordinate: Coordinate{
				Latitude:  ride.DestinationLatitude,
				Longitude: ride.DestinationLongitude,
			},
			Fare:      fare,
			Status:    status,
			CreatedAt: ride.CreatedAt.UnixMilli(),
			UpdateAt:  ride.UpdatedAt.UnixMilli(),
		},
		// 状態変更から3秒以内に通知されている必要があるため、2秒後にリトライする
		// see: https://gist.github.com/wtks/8eadf471daf7cb59942de02273ce7884#通知エンドポイント
		RetryAfterMs: 100,
	}

	if ride.ChairID.Valid {
		chair := &Chair{}
		if err := tx.GetContext(ctx, chair, `SELECT * FROM chairs WHERE id = ?`, ride.ChairID); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		stats, err := getChairStats(ctx, tx, chair.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}

		response.Data.Chair = &appGetNotificationResponseChair{
			ID:    chair.ID,
			Name:  chair.Name,
			Model: chair.Model,
			Stats: stats,
		}
	}

	if yetSentRideStatus.ID != "" {
		_, err := tx.ExecContext(ctx, `UPDATE ride_statuses SET app_sent_at = CURRENT_TIMESTAMP(6) WHERE id = ?`, yetSentRideStatus.ID)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
	}

	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, response)
}

func getChairStats(ctx context.Context, tx *sqlx.Tx, chairID string) (appGetNotificationResponseChairStats, error) {
	stats := appGetNotificationResponseChairStats{}

	rides := []Ride{}
	err := tx.SelectContext(
		ctx,
		&rides,
		`SELECT * FROM rides WHERE chair_id = ? ORDER BY updated_at DESC`,
		chairID,
	)
	if err != nil {
		return stats, err
	}

	totalRideCount := 0
	totalEvaluation := 0.0
	for _, ride := range rides {
		rideStatuses := []RideStatus{}
		err = tx.SelectContext(
			ctx,
			&rideStatuses,
			`SELECT * FROM ride_statuses WHERE ride_id = ? ORDER BY created_at`,
			ride.ID,
		)
		if err != nil {
			return stats, err
		}

		var arrivedAt, pickupedAt *time.Time
		var isCompleted bool
		for _, status := range rideStatuses {
			if status.Status == "ARRIVED" {
				arrivedAt = &status.CreatedAt
			} else if status.Status == "CARRYING" {
				pickupedAt = &status.CreatedAt
			}
			if status.Status == "COMPLETED" {
				isCompleted = true
			}
		}
		if arrivedAt == nil || pickupedAt == nil {
			continue
		}
		if !isCompleted {
			continue
		}

		totalRideCount++
		totalEvaluation += float64(*ride.Evaluation)
	}

	stats.TotalRidesCount = totalRideCount
	if totalRideCount > 0 {
		stats.TotalEvaluationAvg = totalEvaluation / float64(totalRideCount)
	}

	return stats, nil
}

type appGetNearbyChairsResponse struct {
	Chairs      []appGetNearbyChairsResponseChair `json:"chairs"`
	RetrievedAt int64                             `json:"retrieved_at"`
}

type appGetNearbyChairsResponseChair struct {
	ID                string     `json:"id"`
	Name              string     `json:"name"`
	Model             string     `json:"model"`
	CurrentCoordinate Coordinate `json:"current_coordinate"`
}

func appGetNearbyChairs(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	latStr := r.URL.Query().Get("latitude")
	lonStr := r.URL.Query().Get("longitude")
	distanceStr := r.URL.Query().Get("distance")
	if latStr == "" || lonStr == "" {
		writeError(w, http.StatusBadRequest, errors.New("latitude or longitude is empty"))
		return
	}

	lat, err := strconv.Atoi(latStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("latitude is invalid"))
		return
	}

	lon, err := strconv.Atoi(lonStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, errors.New("longitude is invalid"))
		return
	}

	distance := 50
	if distanceStr != "" {
		distance, err = strconv.Atoi(distanceStr)
		if err != nil {
			writeError(w, http.StatusBadRequest, errors.New("distance is invalid"))
			return
		}
	}

	coordinate := Coordinate{Latitude: lat, Longitude: lon}

	tx, err := db.Beginx()
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	defer tx.Rollback()

	// 全ての椅子を一度に取得
	chairs := []Chair{}
	err = tx.SelectContext(
		ctx,
		&chairs,
		`SELECT * FROM chairs`,
	)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// 有効な椅子のIDを抽出
	activeChairIDs := []string{}
	for _, chair := range chairs {
		if chair.IsActive {
			activeChairIDs = append(activeChairIDs, chair.ID)
		}
	}

	if len(activeChairIDs) == 0 {
		if err := tx.Commit(); err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		// 有効な椅子がない場合はそのままレスポンス
		writeJSON(w, http.StatusOK, &appGetNearbyChairsResponse{
			Chairs:      []appGetNearbyChairsResponseChair{},
			RetrievedAt: time.Now().UnixMilli(),
		})
		return
	}

	// 全ての有効な椅子について、ride をまとめて取得
	// chair_id IN (?) を使用し、一度のクエリで全てのライドを取り出す
	rides := []Ride{}
	queryRides, argsRides, err := sqlx.In(`
        SELECT * FROM rides 
        WHERE chair_id IN (?) 
        ORDER BY created_at DESC
    `, activeChairIDs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	queryRides = tx.Rebind(queryRides)
	if err := tx.SelectContext(ctx, &rides, queryRides, argsRides...); err != nil && !errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	// ride を chair_id ごとにグループ化
	ridesByChair := make(map[string][]Ride)
	for _, ride := range rides {
		if ride.ChairID.Valid {
			ridesByChair[ride.ChairID.String] = append(ridesByChair[ride.ChairID.String], ride)
		}
	}

	// 全ての ride_id を収集
	rideIDs := []string{}
	for _, rds := range ridesByChair {
		for _, rd := range rds {
			rideIDs = append(rideIDs, rd.ID)
		}
	}

	// ride_id について最新ステータスを一括取得
	statusMap := make(map[string]string)
	if len(rideIDs) > 0 {
		queryStatus, argsStatus, err := sqlx.In(`
            SELECT rs.ride_id, rs.status FROM ride_statuses rs
            INNER JOIN (
                SELECT ride_id, MAX(created_at) as max_created
                FROM ride_statuses
                WHERE ride_id IN (?)
                GROUP BY ride_id
            ) t ON rs.ride_id = t.ride_id AND rs.created_at = t.max_created
        `, rideIDs)
		if err != nil {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		queryStatus = tx.Rebind(queryStatus)
		type latestStatusRow struct {
			RideID string `db:"ride_id"`
			Status string `db:"status"`
		}
		latestStatuses := []latestStatusRow{}
		if err := tx.SelectContext(ctx, &latestStatuses, queryStatus, argsStatus...); err != nil && !errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusInternalServerError, err)
			return
		}
		for _, s := range latestStatuses {
			statusMap[s.RideID] = s.Status
		}
	}

	// 最新のchair位置情報を一回で取得
	// 最終位置情報は chair_id ごとに最新一件を取得する
	chairLocations := []ChairLocation{}
	queryLocations, argsLocations, err := sqlx.In(`
        SELECT cl.* 
        FROM chair_locations cl
        INNER JOIN (
            SELECT chair_id, MAX(created_at) AS max_created 
            FROM chair_locations
            WHERE chair_id IN (?)
            GROUP BY chair_id
        ) t ON cl.chair_id = t.chair_id AND cl.created_at = t.max_created
    `, activeChairIDs)
	if err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}
	queryLocations = tx.Rebind(queryLocations)
	if err := tx.SelectContext(ctx, &chairLocations, queryLocations, argsLocations...); err != nil && !errors.Is(err, sql.ErrNoRows) {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	locationMap := make(map[string]*ChairLocation)
	for i := range chairLocations {
		loc := &chairLocations[i]
		locationMap[loc.ChairID] = loc
	}

	// 椅子ごとに "未完了ライドが存在しないか" チェック
	// 未完了ライド(=COMPLETED以外)があればスキップ
	nearbyChairs := []appGetNearbyChairsResponseChair{}
	for _, chair := range chairs {
		if !chair.IsActive {
			continue
		}

		rds := ridesByChair[chair.ID]
		skip := false
		for _, ride := range rds {
			if status, ok := statusMap[ride.ID]; ok && status != "COMPLETED" {
				skip = true
				break
			}
		}

		if skip {
			continue
		}

		// 最新位置情報がない場合はスキップ
		loc := locationMap[chair.ID]
		if loc == nil {
			continue
		}

		// 距離判定
		if calculateDistance(coordinate.Latitude, coordinate.Longitude, loc.Latitude, loc.Longitude) <= distance {
			nearbyChairs = append(nearbyChairs, appGetNearbyChairsResponseChair{
				ID:    chair.ID,
				Name:  chair.Name,
				Model: chair.Model,
				CurrentCoordinate: Coordinate{
					Latitude:  loc.Latitude,
					Longitude: loc.Longitude,
				},
			})
		}
	}

	retrievedAt := time.Now()
	if err := tx.Commit(); err != nil {
		writeError(w, http.StatusInternalServerError, err)
		return
	}

	writeJSON(w, http.StatusOK, &appGetNearbyChairsResponse{
		Chairs:      nearbyChairs,
		RetrievedAt: retrievedAt.UnixMilli(),
	})
}

func calculateFare(pickupLatitude, pickupLongitude, destLatitude, destLongitude int) int {
	meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
	return initialFare + meteredFare
}

func calculateDiscountedFare(ctx context.Context, tx *sqlx.Tx, userID string, ride *Ride, pickupLatitude, pickupLongitude, destLatitude, destLongitude int) (int, error) {
	var coupon Coupon
	discount := 0
	if ride != nil {
		destLatitude = ride.DestinationLatitude
		destLongitude = ride.DestinationLongitude
		pickupLatitude = ride.PickupLatitude
		pickupLongitude = ride.PickupLongitude

		// すでにクーポンが紐づいているならそれの割引額を参照
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE used_by = ?", ride.ID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}
		} else {
			discount = coupon.Discount
		}
	} else {
		// 初回利用クーポンを最優先で使う
		if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND code = 'CP_NEW2024' AND used_by IS NULL", userID); err != nil {
			if !errors.Is(err, sql.ErrNoRows) {
				return 0, err
			}

			// 無いなら他のクーポンを付与された順番に使う
			if err := tx.GetContext(ctx, &coupon, "SELECT * FROM coupons WHERE user_id = ? AND used_by IS NULL ORDER BY created_at LIMIT 1", userID); err != nil {
				if !errors.Is(err, sql.ErrNoRows) {
					return 0, err
				}
			} else {
				discount = coupon.Discount
			}
		} else {
			discount = coupon.Discount
		}
	}

	meteredFare := farePerDistance * calculateDistance(pickupLatitude, pickupLongitude, destLatitude, destLongitude)
	discountedMeteredFare := max(meteredFare-discount, 0)

	return initialFare + discountedMeteredFare, nil
}

// calculateFaresForRides は、completedRidesに対する割引後運賃を一括で計算するヘルパー関数です。
// calculateDiscountedFareを直接変更せず、同等のロジックをここで実行することでN+1を回避します。
func calculateFaresForRides(ctx context.Context, tx *sqlx.Tx, userID string, rides []Ride) (map[string]int, error) {
	fares := make(map[string]int, len(rides))
	if len(rides) == 0 {
		return fares, nil
	}

	// rideごとにused_byに対応するクーポンを取得
	rideIDs := make([]string, 0, len(rides))
	for _, r := range rides {
		rideIDs = append(rideIDs, r.ID)
	}

	query, args, err := sqlx.In("SELECT * FROM coupons WHERE used_by IN (?)", rideIDs)
	if err != nil {
		return nil, err
	}
	query = tx.Rebind(query)

	var usedCoupons []Coupon
	if err := tx.SelectContext(ctx, &usedCoupons, query, args...); err != nil {
		return nil, err
	}

	// used_byをkeyにクーポンマップ化
	couponMap := make(map[string]*Coupon, len(usedCoupons))
	for i := range usedCoupons {
		c := usedCoupons[i]
		if c.UsedBy != nil {
			couponMap[*c.UsedBy] = &c
		}
	}

	// calculateDiscountedFareと同等の割引後計算ロジック
	// rideがある場合:
	//   1. couponMapから該当クーポン取得
	//   2. 距離計算 -> fare
	//   3. クーポン割引適用
	//   fare = initialFare + max(meteredFare - discount, 0)

	for _, r := range rides {
		discount := 0
		// coupon取得
		if c, ok := couponMap[r.ID]; ok {
			discount = c.Discount
		} else {
			// クーポンなしの場合、割引0として処理
		}

		// 距離計算
		dist := calculateDistance(r.PickupLatitude, r.PickupLongitude, r.DestinationLatitude, r.DestinationLongitude)
		meteredFare := farePerDistance * dist
		discounted := max(meteredFare-discount, 0)
		fare := initialFare + discounted

		fares[r.ID] = fare
	}

	return fares, nil
}
